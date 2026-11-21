//! HTML pages for looking at a store: the sessions, and one session as a timeline of steps.
//! Served on localhost by `tdb serve`; pages are plain HTML with no scripts or outside files.

use crate::codec::Result;
use crate::db::Db;
use crate::json;
use crate::record::NONE;
use crate::steps;
use std::fmt::Write as _;

pub fn esc(s: &str) -> String {
    let mut o = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '<' => o.push_str("&lt;"),
            '>' => o.push_str("&gt;"),
            '&' => o.push_str("&amp;"),
            '"' => o.push_str("&quot;"),
            '\'' => o.push_str("&#39;"),
            c => o.push(c),
        }
    }
    o
}

fn lossy(b: &[u8]) -> String {
    String::from_utf8_lossy(b).into_owned()
}

fn cut(s: &str, n: usize) -> String {
    if s.chars().count() <= n {
        return s.to_string();
    }
    let mut o: String = s.chars().take(n).collect();
    o.push('…');
    o
}

fn hue(tool: u32) -> u32 {
    (tool.wrapping_mul(2_654_435_761) >> 7) % 360
}

fn bytes(n: u64) -> String {
    match n {
        0..=999 => format!("{n} B"),
        1000..=999_999 => format!("{:.1} KB", n as f64 / 1e3),
        _ => format!("{:.1} MB", n as f64 / 1e6),
    }
}

fn dur(ms: i64) -> String {
    match ms {
        i64::MIN..=-1 => "–".into(),
        0..=999 => format!("{ms} ms"),
        1000..=59_999 => format!("{:.1} s", ms as f64 / 1e3),
        _ => format!("{:.1} min", ms as f64 / 60e3),
    }
}

const STYLE: &str = r#"
:root { --bg:#fbfbfa; --fg:#1d1d1b; --muted:#6b6b66; --line:#e6e5e1; --card:#fff; --bad:#c2410c; --accent:#2f5d8a; }
@media (prefers-color-scheme: dark) { :root { --bg:#161615; --fg:#ecebe7; --muted:#9a9993; --line:#2c2c2a; --card:#1e1e1c; --bad:#fb923c; --accent:#8db4dc; } }
* { box-sizing: border-box; }
body { margin:0; background:var(--bg); color:var(--fg); font:14px/1.45 -apple-system, system-ui, sans-serif; }
main { max-width: 1180px; margin: 0 auto; padding: 24px 16px 64px; }
h1 { font-size: 20px; margin: 0 0 4px; } h2 { font-size: 15px; margin: 28px 0 8px; }
a { color: var(--accent); text-decoration: none; } a:hover { text-decoration: underline; }
.muted { color: var(--muted); } .bad { color: var(--bad); font-weight: 600; }
table { width:100%; border-collapse: collapse; background: var(--card); border:1px solid var(--line); border-radius: 8px; overflow: hidden; }
th, td { text-align:left; padding: 6px 10px; border-bottom: 1px solid var(--line); vertical-align: top; }
th { font-weight: 600; color: var(--muted); font-size: 12px; text-transform: uppercase; letter-spacing: .03em; }
td.num, th.num { text-align: right; font-variant-numeric: tabular-nums; white-space: nowrap; }
tr:last-child td { border-bottom: 0; }
.tool { display:inline-block; padding: 1px 7px; border-radius: 10px; font-size: 12px; font-weight: 600; white-space: nowrap; }
.strip { display:flex; flex-wrap: wrap; gap: 1px; margin: 8px 0 4px; }
.strip i { width: 6px; height: 14px; border-radius: 1px; display:block; }
.strip i.f { outline: 2px solid var(--bad); outline-offset: -2px; }
.fork { color: var(--accent); font-size: 12px; white-space: nowrap; }
code, pre { font: 12px/1.45 ui-monospace, SFMono-Regular, Menlo, monospace; }
pre { background: var(--card); border:1px solid var(--line); border-radius: 8px; padding: 12px; overflow:auto; max-height: 70vh; white-space: pre-wrap; word-break: break-word; }
.kv { color: var(--muted); } .kv b { color: var(--fg); font-weight: 500; }
.wrap { overflow-x: auto; }
.stats { display:flex; gap: 24px; flex-wrap: wrap; margin: 12px 0; }
.stats div b { display:block; font-size: 18px; }
"#;

fn page(title: &str, body: &str) -> String {
    format!("<!doctype html><html><head><meta charset=\"utf-8\"><meta name=\"viewport\" content=\"width=device-width, initial-scale=1\"><title>{}</title><style>{STYLE}</style></head><body><main>{body}</main></body></html>", esc(title))
}

fn tool_chip(db: &Db, t: u32) -> String {
    let h = hue(t);
    format!(
        "<span class=\"tool\" style=\"background:hsl({h} 70% 50% / .16);color:hsl({h} 55% 42%)\">{}</span>",
        esc(&lossy(db.step_dicts().tools.get(t)))
    )
}

fn strip(db: &Db, s: u32, max: usize) -> String {
    let ss = db.steps(s);
    let mut o = String::from("<div class=\"strip\">");
    for r in ss.rows.iter().take(max) {
        let _ = write!(o, "<i class=\"{}\" style=\"background:hsl({} 60% 55%)\"></i>", if r.failed { "f" } else { "" }, hue(r.tool));
    }
    if ss.rows.len() > max {
        let _ = write!(o, "<span class=\"muted\">&nbsp;+{}</span>", ss.rows.len() - max);
    }
    o.push_str("</div>");
    o
}

pub fn index_page(db: &Db) -> String {
    let st = db.stats();
    let mut b = String::new();
    let _ = write!(
        b,
        "<h1>TranscriptDB</h1><div class=\"muted\">{} sessions · {} requests · {} steps · {} on disk</div>",
        st.sessions,
        st.requests,
        st.steps,
        bytes(st.seg_file_bytes + st.opq_file_bytes)
    );
    let mut order: Vec<u32> = (0..db.session_count() as u32).collect();
    order.sort_by_key(|&s| std::cmp::Reverse(db.steps(s).rows.len()));
    b.push_str("<h2>Sessions</h2><div class=\"wrap\"><table><tr><th>Session</th><th class=\"num\">Requests</th><th class=\"num\">Messages</th><th class=\"num\">Steps</th><th class=\"num\">Failed</th><th>Steps in order</th></tr>");
    let reqs_per = {
        let mut v = vec![0usize; db.session_count()];
        for i in 0..db.request_count() as u32 {
            v[db.request(i).session as usize] += 1;
        }
        v
    };
    for s in order {
        let (sid, agent) = db.session_key(s);
        let ss = db.steps(s);
        let failed = ss.rows.iter().filter(|r| r.failed).count();
        let _ = write!(
            b,
            "<tr><td><a href=\"/s/{s}\"><code>{}</code></a>{}</td><td class=\"num\">{}</td><td class=\"num\">{}</td><td class=\"num\">{}</td><td class=\"num\">{}</td><td>{}</td></tr>",
            esc(&cut(sid, 13)),
            if agent.is_empty() { String::new() } else { format!(" <span class=\"muted\">agent {}</span>", esc(&cut(agent, 10))) },
            reqs_per[s as usize],
            db.session_len(s),
            ss.rows.len(),
            if failed > 0 { format!("<span class=\"bad\">{failed}</span>") } else { "0".into() },
            strip(db, s, 120)
        );
    }
    b.push_str("</table></div>");
    page("TranscriptDB", &b)
}

pub fn session_page(db: &Db, s: u32) -> String {
    let ss = db.steps(s);
    let sd = db.step_dicts();
    let (sid, agent) = db.session_key(s);
    let mut b = String::new();
    let _ = write!(b, "<div><a href=\"/\">← sessions</a></div><h1><code>{}</code></h1>", esc(sid));
    if !agent.is_empty() {
        let _ = write!(b, "<div class=\"muted\">agent {}</div>", esc(agent));
    }
    let failed = ss.rows.iter().filter(|r| r.failed).count();
    let span = match (ss.rows.first(), ss.rows.last()) {
        (Some(a), Some(z)) if a.time_ms > 0 && z.time_ms > 0 => dur(z.time_ms - a.time_ms),
        _ => "–".into(),
    };
    let reqs = db.session_requests(s).len();
    let _ = write!(
        b,
        "<div class=\"stats\"><div><b>{}</b><span class=\"muted\">steps</span></div><div><b>{failed}</b><span class=\"muted\">failed</span></div><div><b>{reqs}</b><span class=\"muted\">requests</span></div><div><b>{}</b><span class=\"muted\">messages stored</span></div><div><b>{span}</b><span class=\"muted\">first to last step</span></div></div>",
        ss.rows.len(),
        db.session_len(s)
    );
    b.push_str(&strip(db, s, 2000));
    b.push_str("<h2>Timeline</h2><div class=\"wrap\"><table><tr><th class=\"num\">#</th><th class=\"num\">After</th><th>Tool</th><th>Input</th><th class=\"num\">Output</th><th></th></tr>");
    for (i, r) in ss.rows.iter().enumerate() {
        let i32_ = i as u32;
        let after = if r.prev == NONE { "start".to_string() } else { dur(r.time_ms - ss.rows[r.prev as usize].time_ms) };
        let fork = if r.prev != NONE && r.prev + 1 != i32_ && (i == 0 || ss.rows[i - 1].prev != r.prev) {
            format!(" <span class=\"fork\">↳ after #{}</span>", r.prev)
        } else if i > 0 && ss.rows[i - 1].prev == r.prev && ss.rows[i - 1].call_node == r.call_node {
            " <span class=\"fork\">∥ same turn</span>".to_string()
        } else {
            String::new()
        };
        let mut inputs = String::new();
        for &(f, v) in ss.fields_of(i32_) {
            let val = if v == steps::LONG { "<i>long</i>".to_string() } else { esc(&cut(&lossy(sd.values[f as usize].get(v)), 90)) };
            let _ = write!(inputs, "<div class=\"kv\">{}: <b>{}</b></div>", esc(&lossy(sd.fields.get(f))), val);
        }
        let _ = write!(
            b,
            "<tr><td class=\"num\"><a href=\"/s/{s}/{i}\">{i}</a></td><td class=\"num muted\">{after}</td><td>{}{fork}</td><td>{inputs}</td><td class=\"num\">{}</td><td>{}</td></tr>",
            tool_chip(db, r.tool),
            bytes(r.out_bytes as u64),
            if r.failed { "<span class=\"bad\">failed</span>" } else { "" }
        );
    }
    b.push_str("</table></div>");
    page(&format!("Session {}", cut(sid, 13)), &b)
}

/// Indented JSON, tokens kept as they are.
pub fn pretty(b: &[u8], max: usize) -> String {
    fn go(b: &[u8], n: &json::Node, depth: usize, out: &mut String, max: usize) {
        if out.len() > max {
            return;
        }
        let pad = "  ".repeat(depth + 1);
        match n {
            json::Node::Tok(s) => out.push_str(&String::from_utf8_lossy(&b[s.clone()])),
            json::Node::Arr(v) if v.is_empty() => out.push_str("[]"),
            json::Node::Obj(v) if v.is_empty() => out.push_str("{}"),
            json::Node::Arr(v) => {
                out.push_str("[\n");
                for (i, x) in v.iter().enumerate() {
                    out.push_str(&pad);
                    go(b, x, depth + 1, out, max);
                    out.push_str(if i + 1 < v.len() { ",\n" } else { "\n" });
                }
                out.push_str(&"  ".repeat(depth));
                out.push(']');
            }
            json::Node::Obj(v) => {
                out.push_str("{\n");
                for (i, (k, x)) in v.iter().enumerate() {
                    out.push_str(&pad);
                    out.push_str(&String::from_utf8_lossy(&b[k.clone()]));
                    out.push_str(": ");
                    go(b, x, depth + 1, out, max);
                    out.push_str(if i + 1 < v.len() { ",\n" } else { "\n" });
                }
                out.push_str(&"  ".repeat(depth));
                out.push('}');
            }
        }
    }
    match json::parse_all(b) {
        Ok(n) => {
            let mut o = String::new();
            go(b, &n, 0, &mut o, max);
            if o.len() > max {
                o.truncate(o.char_indices().take_while(|(i, _)| *i < max).last().map(|(i, _)| i).unwrap_or(0));
                o.push_str("\n… (cut)");
            }
            o
        }
        Err(_) => cut(&String::from_utf8_lossy(b), max),
    }
}

fn block_of(msg: &[u8], k: u16) -> Vec<u8> {
    let Ok(ms) = json::members(msg, &(0..msg.len())) else { return Vec::new() };
    let Some(c) = json::find_member(msg, &ms, "content") else { return Vec::new() };
    let Ok((els, _)) = json::elements(msg, &c.value) else { return Vec::new() };
    els.get(k as usize).map(|e| msg[e.clone()].to_vec()).unwrap_or_default()
}

pub fn step_page(db: &Db, s: u32, i: u32) -> Result<String> {
    let ss = db.steps(s);
    let r = ss.rows[i as usize];
    let call = block_of(&db.message(s, r.call_node)?, r.call_block);
    let result = block_of(&db.message(s, r.result_node)?, r.result_block);
    let mut b = String::new();
    let _ = write!(b, "<div><a href=\"/s/{s}\">← session</a></div><h1>Step {i} {}</h1>", tool_chip(db, r.tool));
    let _ = write!(
        b,
        "<div class=\"muted\">call in message {} block {} · result in message {} block {} · {} · {}</div>",
        r.call_node,
        r.call_block,
        r.result_node,
        r.result_block,
        bytes(r.out_bytes as u64),
        if r.failed { "<span class=\"bad\">failed</span>" } else { "ok" }
    );
    let _ = write!(b, "<h2>Call</h2><pre>{}</pre><h2>Result</h2><pre>{}</pre>", esc(&pretty(&call, 200_000)), esc(&pretty(&result, 200_000)));
    Ok(page(&format!("Step {i}"), &b))
}
