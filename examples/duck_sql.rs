//! Time SQL variants on a DuckDB file `tdb duck` left behind: each query best of `reps`, with
//! its row count and a hash of its rows, so rewrites can be checked to give the same answer.
//! `cargo run --release --features duckdb --example duck_sql -- FILE QUERIES.sql [reps]`; queries
//! are separated by lines holding `;;`, each may start with `-- name`.
fn main() {
    let a: Vec<String> = std::env::args().collect();
    let conn = duckdb::Connection::open(&a[1]).unwrap();
    let text = std::fs::read_to_string(&a[2]).unwrap();
    let reps: usize = a.get(3).map_or(7, |r| r.parse().unwrap());
    for q in text.split("\n;;\n").map(str::trim).filter(|q| !q.is_empty()) {
        let name = q.lines().next().filter(|l| l.starts_with("--")).unwrap_or("query").trim_start_matches("--").trim().to_string();
        let mut best = f64::MAX;
        let mut out = (0usize, 0u64);
        for _ in 0..reps {
            let t = std::time::Instant::now();
            let rows = transcriptdb::duck::rows(&conn, q).unwrap_or_else(|e| panic!("{name}: {e}"));
            best = best.min(t.elapsed().as_secs_f64() * 1e3);
            let mut s: Vec<String> = rows.iter().map(|r| format!("{r:?}")).collect();
            s.sort();
            out = (rows.len(), xxhash_rust::xxh3::xxh3_64(s.join("\n").as_bytes()));
        }
        println!("{best:>9.3} ms  {:>6} rows  {:016x}  {name}", out.0, out.1);
    }
}
