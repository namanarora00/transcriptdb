//! Requests in the shapes other model APIs use (OpenAI Chat Completions, OpenAI Responses,
//! Gemini) and bodies that are not conversations at all, each with the response the model
//! gave: stored once per message, rebuilt exactly, steps and text read from every shape.

use transcriptdb::query::{self, exec};
use transcriptdb::synth::{jstr, Rng};
use transcriptdb::{check, Db, Options, Put, SyncMode};

fn tmpdir(name: &str) -> std::path::PathBuf {
    let d = std::env::temp_dir().join(format!("tdb-fmt-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&d);
    d
}

fn opts() -> Options {
    Options { sync: SyncMode::Os, flush_bytes: 48 << 10, merge_at: 2, seg_bytes: 128 << 10, ..Options::default() }
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum Api {
    Chat,
    Responses,
    Gemini,
}

/// One stored call: its body and the response it got.
#[derive(Clone)]
struct Call {
    session: String,
    id: String,
    time: String,
    body: Vec<u8>,
    response: Vec<u8>,
}

/// A session of `turns` agent turns in `api`'s shape. Each turn is two calls: the model
/// answers the user with a tool call, then answers the tool's result with text; the app
/// sends the model's messages back in the next call, as agents do. Turn `t` fails its tool
/// call when `t % 3 == 2` (where the API can say so). The user says "zebra{s}" in each turn.
fn session(api: Api, s: usize, turns: usize) -> Vec<Call> {
    let mut r = Rng::new(s as u64 * 7 + 1);
    let mut msgs: Vec<String> = Vec::new();
    if api == Api::Chat {
        msgs.push(r#"{"role":"system","content":"You are a careful agent."}"#.into());
    }
    let mut out = Vec::new();
    let body = |msgs: &[String]| -> String {
        match api {
            Api::Chat => format!(r#"{{"model":"gpt-4o","messages":[{}],"tools":[{{"type":"function","function":{{"name":"run","parameters":{{}}}}}}],"temperature":0}}"#, msgs.join(",")),
            Api::Responses => format!(r#"{{"model":"gpt-5","instructions":"You are a careful agent.","input":[{}],"tools":[{{"type":"function","name":"run"}}]}}"#, msgs.join(",")),
            Api::Gemini => format!(r#"{{"systemInstruction":{{"parts":[{{"text":"You are a careful agent."}}]}},"contents":[{}],"tools":[{{"functionDeclarations":[{{"name":"run"}}]}}]}}"#, msgs.join(",")),
        }
    };
    let mut k = 0;
    let mut call = |out: &mut Vec<Call>, msgs: &[String], response: String| {
        out.push(Call { session: format!("{api:?}-{s}"), id: format!("{api:?}-{s}-{k}"), time: format!("{}", 1_700_000_000 + s * 1000 + k), body: body(msgs).into_bytes(), response: response.into_bytes() });
        k += 1;
    };
    for t in 0..turns {
        let user = format!("turn {t}: please check zebra{s} and number {}", r.below(1000));
        let cmd = format!("ls /src/{s}/{t}");
        let output = format!("file_{}.rs\nfile_{}.rs", r.below(100), r.below(100));
        let failed = t % 3 == 2;
        let reply = format!("Done with turn {t}, all good");
        let (u, call_msg, result, text) = match api {
            Api::Chat => (
                format!(r#"{{"role":"user","content":{}}}"#, jstr(&user)),
                format!(r#"{{"role":"assistant","content":null,"tool_calls":[{{"id":"call_{s}_{t}","type":"function","function":{{"name":"run","arguments":{}}}}}]}}"#, jstr(&format!(r#"{{"command":{}}}"#, jstr(&cmd)))),
                format!(r#"{{"role":"tool","tool_call_id":"call_{s}_{t}","content":{}}}"#, jstr(&output)),
                format!(r#"{{"role":"assistant","content":{}}}"#, jstr(&reply)),
            ),
            Api::Responses => (
                format!(r#"{{"role":"user","content":[{{"type":"input_text","text":{}}}]}}"#, jstr(&user)),
                format!(r#"{{"type":"function_call","call_id":"fc_{s}_{t}","name":"run","arguments":{}}}"#, jstr(&format!(r#"{{"command":{}}}"#, jstr(&cmd)))),
                format!(r#"{{"type":"function_call_output","call_id":"fc_{s}_{t}","output":{}}}"#, jstr(&output)),
                format!(r#"{{"type":"message","role":"assistant","content":[{{"type":"output_text","text":{}}}]}}"#, jstr(&reply)),
            ),
            Api::Gemini => (
                format!(r#"{{"role":"user","parts":[{{"text":{}}}]}}"#, jstr(&user)),
                format!(r#"{{"role":"model","parts":[{{"functionCall":{{"name":"run","args":{{"command":{}}}}}}}]}}"#, jstr(&cmd)),
                if failed {
                    format!(r#"{{"role":"user","parts":[{{"functionResponse":{{"name":"run","response":{{"error":{}}}}}}}]}}"#, jstr(&output))
                } else {
                    format!(r#"{{"role":"user","parts":[{{"functionResponse":{{"name":"run","response":{{"output":{}}}}}}}]}}"#, jstr(&output))
                },
                format!(r#"{{"role":"model","parts":[{{"text":{}}}]}}"#, jstr(&reply)),
            ),
        };
        let wrap = |m: &str| match api {
            Api::Chat => format!(r#"{{"id":"chatcmpl-{s}-{t}","object":"chat.completion","choices":[{{"index":0,"message":{m},"finish_reason":"stop"}}],"usage":{{"prompt_tokens":10,"completion_tokens":5}}}}"#),
            Api::Responses => format!(r#"{{"id":"resp_{s}_{t}","object":"response","output":[{m}],"usage":{{"input_tokens":10,"output_tokens":5}}}}"#),
            Api::Gemini => format!(r#"{{"candidates":[{{"content":{m},"finishReason":"STOP"}}],"usageMetadata":{{"promptTokenCount":10}}}}"#),
        };
        msgs.push(u);
        call(&mut out, &msgs, wrap(&call_msg));
        msgs.push(call_msg);
        msgs.push(result);
        call(&mut out, &msgs, wrap(&text));
        msgs.push(text);
    }
    out
}

fn put(db: &mut Db, c: &Call) {
    db.put(&Put { session: &c.session, agent: "", id: &c.id, time: &c.time, meta: b"{}", body: &c.body, response: &c.response }).unwrap();
}

fn check_calls(db: &Db, calls: &[Call]) {
    for c in calls {
        let ri = db.request_index(&c.id).unwrap_or_else(|| panic!("{} missing", c.id));
        assert_eq!(db.rebuild(ri).unwrap(), c.body, "{} body", c.id);
        assert_eq!(db.response(ri).unwrap().as_deref(), Some(&c.response[..]), "{} response", c.id);
    }
}

/// The cells of a query's result, as text.
fn cells(db: &Db, q: &str) -> Vec<Vec<String>> {
    let r = query::run(db, q, 2).unwrap_or_else(|e| panic!("{}", e.show(q)));
    exec::render(db, &r.table, 1000).unwrap().1
}

#[test]
fn every_api_shape_with_responses() {
    let dir = tmpdir("apis");
    let turns = 9;
    let mut sessions: Vec<Vec<Call>> = Vec::new();
    for api in [Api::Chat, Api::Responses, Api::Gemini] {
        for s in 0..3 {
            sessions.push(session(api, s, turns));
        }
    }
    // one session arrives out of order, as calls of one run do across batches
    let mut late = sessions[1].clone();
    let mut r = Rng::new(3);
    for i in (1..late.len()).rev() {
        late.swap(i, r.below(i as u64 + 1) as usize);
    }
    sessions[1] = late;
    let all: Vec<Call> = sessions.iter().flatten().cloned().collect();
    let body_bytes: usize = all.iter().map(|c| c.body.len()).sum();
    {
        let mut db = Db::open(&dir, opts()).unwrap();
        // interleaved, the way traffic from several runs arrives
        let mut queues = sessions.clone();
        while queues.iter().any(|q| !q.is_empty()) {
            for q in queues.iter_mut().filter(|q| !q.is_empty()) {
                put(&mut db, &q.remove(0));
            }
        }
        db.commit().unwrap();
        check_calls(&db, &all);
        db.settle().unwrap();
        check_calls(&db, &all);
        // stored once per message: far less than the bodies
        let st = db.stats();
        assert!((st.node_bytes as usize) * 4 < body_bytes, "messages stored once: {} of {body_bytes} body bytes", st.node_bytes);
    }
    let mut db = Db::open(&dir, opts()).unwrap();
    check_calls(&db, &all);
    db.compact().unwrap();
    check_calls(&db, &all);

    // steps: the engine agrees with the slow reference, in every shape
    let sc = check::check_steps(&db, 2).unwrap();
    assert!(sc.ok(), "{sc:?}");
    assert_eq!(sc.engine_steps, 9 * turns, "one step per turn, the response's copy of a call not counted again");
    // tool names and inputs, decoded from OpenAI's argument strings too
    assert_eq!(cells(&db, r#"summarize n = count() by tool"#), vec![vec!["run".to_string(), (9 * turns).to_string()]]);
    assert_eq!(cells(&db, r#"where command == "ls /src/2/4" | summarize n = count()"#), vec![vec!["3".to_string()]]);
    // Gemini says when a call failed
    assert_eq!(cells(&db, r#"where failed | summarize n = count() by session | order by session asc"#), vec![vec!["Gemini-0".to_string(), "3".into()], vec!["Gemini-1".into(), "3".into()], vec!["Gemini-2".into(), "3".into()]]);

    // text: the user's words in each shape; system prompts and responses are not messages
    assert_eq!(cells(&db, r#"messages | where role == "user" and text has "zebra1" | summarize n = count()"#), vec![vec![(3 * turns).to_string()]]);
    assert_eq!(cells(&db, r#"messages | where role == "assistant" and text has "done" | summarize n = count()"#), vec![vec![(9 * (turns - 1)).to_string()]], "each reply once, from the call that sent it back; the last only in its response");
    assert_eq!(cells(&db, r#"messages | where text has "careful" | summarize n = count()"#), vec![vec!["0".to_string()]]);
    drop(db);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn bodies_that_are_not_conversations() {
    let dir = tmpdir("other");
    let bodies: Vec<&[u8]> = vec![
        br#"{"model":"text-embedding-3-small","input":["first text","second text"]}"#,
        br#"{"model":"text-embedding-3-small","input":"one string"}"#,
        br#"["a","json","array"]"#,
        b"plain text, not JSON",
        br#"{"text":"Hello there","voice_settings":{"stability":0.5}}"#,
        br#"{"broken": "json""#,
        b"",
    ];
    {
        let mut db = Db::open(&dir, opts()).unwrap();
        for (i, b) in bodies.iter().enumerate() {
            let id = format!("e{i}");
            db.put(&Put { session: "misc", agent: "", id: &id, time: "1700000000", meta: b"", body: b, response: br#"{"ok":true}"# }).unwrap();
        }
        db.commit().unwrap();
        for (i, b) in bodies.iter().enumerate() {
            let ri = db.request_index(&format!("e{i}")).unwrap();
            assert_eq!(&db.rebuild(ri).unwrap()[..], *b);
        }
        db.settle().unwrap();
    }
    let db = Db::open(&dir, opts()).unwrap();
    for (i, b) in bodies.iter().enumerate() {
        let ri = db.request_index(&format!("e{i}")).unwrap();
        assert_eq!(&db.rebuild(ri).unwrap()[..], *b, "body {i}");
        assert_eq!(db.response(ri).unwrap().as_deref(), Some(&br#"{"ok":true}"#[..]));
    }
    drop(db);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn responses_are_not_messages() {
    // Anthropic responses look like messages (a role and content blocks): still not read as
    // any, not walked through, and no steps of their own
    let dir = tmpdir("resp");
    let mut db = Db::open(&dir, opts()).unwrap();
    let mut msgs: Vec<String> = Vec::new();
    for t in 0..6 {
        msgs.push(format!(r#"{{"role":"user","content":"question {t} about penguins"}}"#));
        let body = format!(r#"{{"model":"claude","max_tokens":100,"messages":[{}]}}"#, msgs.join(","));
        let reply = format!(r#"{{"type":"text","text":"answer {t} about walruses"}},{{"type":"tool_use","id":"tu{t}","name":"Lookup","input":{{"q":"walrus {t}"}}}}"#);
        let response = format!(r#"{{"id":"msg_{t}","type":"message","role":"assistant","content":[{reply}],"stop_reason":"tool_use"}}"#);
        db.put(&Put { session: "a", agent: "", id: &format!("r{t}"), time: "1700000000", meta: b"", body: body.as_bytes(), response: response.as_bytes() }).unwrap();
        // the app sends the reply back without the response's other fields
        msgs.push(format!(r#"{{"role":"assistant","content":[{reply}]}}"#));
        msgs.push(format!(r#"{{"role":"user","content":[{{"type":"tool_result","tool_use_id":"tu{t}","content":"found"}}]}}"#));
    }
    db.commit().unwrap();
    for pass in 0..2 {
        // each answer once (from the request that sent it back), the last only in its response
        assert_eq!(cells(&db, r#"messages | where text has "walruses" | summarize n = count()"#), vec![vec!["5".to_string()]], "pass {pass}");
        assert_eq!(cells(&db, r#"messages | where text has "penguins" | summarize n = count()"#), vec![vec!["6".to_string()]]);
        assert_eq!(cells(&db, r#"summarize n = count()"#), vec![vec!["5".to_string()]]);
        let sc = check::check_steps(&db, 1).unwrap();
        assert!(sc.ok(), "{sc:?}");
        db.settle().unwrap();
    }
    drop(db);
    let _ = std::fs::remove_dir_all(&dir);
}
