#!/usr/bin/env python3
"""Rewrite the benchmark section of README.md from measured `tdb h2h` output.

Numbers are parsed from the outputs, never typed. Everything above the line
"## Benchmarks against PostgreSQL" is kept as it is.

    scripts/readme.py FULL.txt SAMPLE.txt NOTES.json [README.md]

FULL.txt and SAMPLE.txt are `tdb h2h --split x` outputs on every log file and on the
200-file sample. NOTES.json holds what the outputs do not: "files" (log files read),
"units" (text blocks), "date", "machine" (one sentence), and "notes" (a list of bullets
for "Reading these numbers", in Markdown).
"""
import json
import re
import sys

HEADING = "## Benchmarks against PostgreSQL"

# (title, first query, number of queries, lead paragraph or "") in the order `tdb h2h` runs them
SECTIONS = [
    ("Sequence queries", 8, ""),
    ("Other queries", 7, ""),
    ("Text search", 6, ""),
    ("Rule columns", 4, "Two rules are defined on both sides: the program a shell command runs, and the number of\nfailed tests a test run reports."),
]


def parse(path):
    q, reads, size = [], [], {}
    with open(path) as f:
        for line in f:
            m = re.match(r"^\s{2}(.+?)\s{2,}yes\s+([\d.]+) ms\s+([\d.]+) ms\s+([\d.]+) ms\s+([\d.]+)x\s+(.*)$", line)
            if m:
                q.append((m.group(1).strip(), m.group(2), float(m.group(4)), m.group(5)))
            if re.match(r"^\s{2}.+?\s{2,}NO\s", line):
                sys.exit(f"{path}: an answer differs: {line.strip()}")
            m = re.match(r"^\s{2}(.+?)\s{2,}same (\d+)/(\d+)\s+TDB ([\d.]+) / ([\d.]+)\s+PostgreSQL ([\d.]+) / ([\d.]+)\s+([\d.]+)x", line)
            if m:
                if m.group(2) != m.group(3):
                    sys.exit(f"{path}: reads differ: {line.strip()}")
                reads.append(m.groups())
            m = re.match(r"^\s+TranscriptDB store: ([\d.]+) MB", line)
            if m:
                size["tdb"] = m.group(1)
            m = re.match(r"^\s+PostgreSQL tables with indexes and TOAST: ([\d.]+) MB", line)
            if m:
                size["pg"] = m.group(1)
            m = re.match(r"^\s+PostgreSQL / TranscriptDB: ([\d.]+)x", line)
            if m:
                size["x"] = m.group(1)
            m = re.match(r"^loaded (\d+) sessions, (\d+) distinct messages \(([\d.]+) MB\), (\d+) requests, (\d+) steps", line)
            if m:
                size["load"] = m.groups()
            m = re.match(r"^\s+texts\s+heap\s+([\d.]+) MB\s+maps\s+[\d.]+ MB\s+TOAST\s+([\d.]+) MB\s+indexes\s+([\d.]+) MB", line)
            if m:
                size["texts"] = tuple(float(x) for x in m.groups())
    return q, reads, size


def ms(x):
    if x >= 100:
        return f"{round(x):,}"
    if x >= 10:
        return f"{x:.1f}"
    return f"{x:.2f}" if x >= 1 else f"{x:.3f}"


def times(v):
    return f"{int(float(v)):,}" if "." not in v else v


def main():
    full_path, sample_path, notes_path = sys.argv[1:4]
    readme = sys.argv[4] if len(sys.argv) > 4 else "README.md"
    with open(notes_path) as f:
        notes = json.load(f)
    fq, fr, fs = parse(full_path)
    sq, _, ss = parse(sample_path)
    want = sum(s[1] for s in SECTIONS)
    if len(fq) != want or len(sq) != want or len(fr) != 4:
        sys.exit(f"expected {want} queries and 4 reads, got {len(fq)} / {len(sq)} queries and {len(fr)} reads")
    n = lambda s: f"{int(s):,}"
    sess, _, mb, reqs, steps = fs["load"]
    ssess, _, _, _, ssteps = ss["load"]

    out = [HEADING, ""]
    out.append(f"Both sides hold the same data: {n(notes['files'])} Claude Code log files from one Mac, which is {n(sess)}")
    out.append(f"sessions, {n(reqs)} requests, {n(steps)} steps, {n(notes['units'])} text blocks and {mb} MB of distinct")
    out.append("message text. Every answer and every read was identical on both sides.")
    out.append("")
    out.append("- **PostgreSQL 18** has a tuned schema: each distinct message stored once, a step table with")
    out.append("  indexes, a materialized table of turn paths for the sequence queries, and everything needed to")
    out.append("  rebuild whole requests. For search it has a table of the same text with a word index and a")
    out.append("  trigram index, and a trigram index on commands. For the rule columns its step tables")
    out.append("  hold the same two columns, each with an index. It may use up to 4 parallel workers.")
    out.append("- **TranscriptDB** runs on one thread.")
    out.append(f"- Times are the best of 7 with warm caches, measured on {notes['date']}. {notes['machine']}")
    out.append("")
    out.append("### Size on disk, with search on both sides")
    out.append("")
    out.append("| Data | TranscriptDB | PostgreSQL | Smaller by |")
    out.append("|---|---|---|---|")
    out.append(f"| Every log file | {fs['tdb']} MB | {float(fs['pg']):,.1f} MB | {fs['x']}x |")
    out.append(f"| 200-file sample | {ss['tdb']} MB | {float(ss['pg']):,.1f} MB | {ss['x']}x |")
    at = 0
    ranges = []
    for title, count, lead in SECTIONS:
        rows = fq[at:at + count]
        ranges.append((title.lower(), sq[at:at + count]))
        at += count
        out += ["", f"### {title}", ""]
        if lead:
            out += [lead, ""]
        out.append("| Query | TranscriptDB | PostgreSQL | Faster by |")
        out.append("|---|---|---|---|")
        for name, tdb, pg, x in rows:
            out.append(f"| {name.replace('git commit', '`git commit`')} | {tdb} ms | {ms(pg)} ms | {x}x |")
    span = lambda qs: f"{times(min((q[3] for q in qs), key=float))}x to {times(max((q[3] for q in qs), key=float))}x"
    parts = [f"{t} {span(qs)}" for t, qs in ranges]
    out += ["", f"On the 200-file sample ({n(ssess)} sessions, {n(ssteps)} steps), times faster: " + ", ".join(parts) + "."]
    out += ["", "### Reads", "", "| Read (median / 95th percentile) | TranscriptDB | PostgreSQL | Faster at the median |", "|---|---|---|---|"]
    for g in fr:
        out.append(f"| {g[0][0].upper() + g[0][1:]} | {g[3]} / {g[4]} ms | {g[5]} / {g[6]} ms | {g[7]}x |")
    copy = fs["texts"][0] + fs["texts"][1]
    lean = (float(fs["pg"]) - copy) / float(fs["tdb"])
    out += ["", "### Reading these numbers", ""]
    out.append("- PostgreSQL is reached over loopback TCP, which adds about 0.1 ms to each query. That matters")
    out.append("  for the rows under a millisecond.")
    out.append(f"- PostgreSQL's size includes its searchable copy of the text ({copy:.1f} MB on every log file).")
    out.append(f"  Leaving that copy out, the store is {lean:.2f}x smaller.")
    for b in notes.get("notes", []):
        out.append(f"- {b}")
    out.append("- The data is one person's logs.")
    out += ["", "To run it yourself: `scripts/h2h.sh` (needs Homebrew `postgresql@18`)."]

    with open(readme) as f:
        text = f.read()
    head = text.split(HEADING)[0].rstrip("\n")
    with open(readme, "w") as f:
        f.write(head + "\n\n" + "\n".join(out) + "\n")
    print(f"{readme}: benchmark section rewritten ({len(out)} lines)")


if __name__ == "__main__":
    main()
