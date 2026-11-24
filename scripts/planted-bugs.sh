#!/usr/bin/env bash
# Plant one bug at a time in a copy of the source, build it, and check that the checks
# catch it (they must fail). Covers the query paths added for step summaries, the word
# index, the sparse matcher and the raw-text string test. Synthetic data only.
#   scripts/planted-bugs.sh            # all of them
#   scripts/planted-bugs.sh NAME...    # some of them
#   DRY=1 scripts/planted-bugs.sh      # only check each bug's text is still in the source
set -uo pipefail
cd "$(dirname "$0")/.."
command -v cargo >/dev/null || export PATH="/opt/homebrew/opt/rustup/bin:$HOME/.cargo/bin:$PATH"
WORK="${WORK:-${TMPDIR:-/tmp}/transcriptdb-planted}"
mkdir -p "$WORK"
export CARGO_TARGET_DIR="$WORK/target"
COPY="$WORK/src"
TDB="$CARGO_TARGET_DIR/release/tdb"

# name | file | text to replace (exactly once) | replacement | check
BUGS=(
  "part-table-day|src/query/summary.rs|self.add(tool, r.failed, r.day * DAY_MS, sid, agent, NULL, cif, W { count: r.count, sum: r.sum_out, min_out: r.min_out|self.add(tool, r.failed, (r.day + 1) * DAY_MS, sid, agent, NULL, cif, W { count: r.count, sum: r.sum_out, min_out: r.min_out|agg"
  "part-table-countif|src/query/summary.rs|let cif = pl.countif_bits(tool, r.failed, sb, missing);
                self.add(tool, r.failed, r.day * DAY_MS, sid, agent, NULL, cif, W { count: r.count, sum: r.sum_out, min_out: r.min_out|let cif = pl.countif_bits(tool, false, sb, missing);
                self.add(tool, r.failed, r.day * DAY_MS, sid, agent, NULL, cif, W { count: r.count, sum: r.sum_out, min_out: r.min_out|agg"
  "field-rows-count|src/query/summary.rs|self.add(tool, r.failed, r.day * DAY_MS, sid, agent, fv, cif, W { count: r.count, sum: r.sum_out, min_out: 0, max_out: 0, min_t: 0, max_t: 0 });
                    }
                }|self.add(tool, r.failed, r.day * DAY_MS, sid, agent, fv, cif, W { count: 1, sum: r.sum_out, min_out: 0, max_out: 0, min_t: 0, max_t: 0 });
                    }
                }|agg"
  "word-prefix-as-exact|src/stats.rs|if how == WordMatch::Prefix \|\| w == t {|if w == t {|agg"
  "non-strings-not-tested|src/query/summary.rs|            for &id in &rows.unindexed {|            for &id in rows.unindexed.iter().take(0) {|agg"
  "missing-field-left-out|src/query/summary.rs|        // steps without the field: the part table less the field table
        if needs_missing {|        // steps without the field: the part table less the field table
        if false {|agg"
  "long-values-left-out|src/query/summary.rs|        for li in longs_to_visit {|        for li in longs_to_visit.into_iter().take(0) {|agg"
  "replaced-parts-counted|src/db.rs|let live = if self.manifest.dead.iter().any(\|(n, _)\| *n == num) { Some(self.live_mask(seg)?) } else { None };|let live: Option<Arc<Vec<bool>>> = None;|test:aggregations_skip_parts_that_folds_replaced"
  "unflushed-steps-left-out|src/db.rs|            if (a.steps.flushed as usize) < a.steps.rows.len() {|            if false && (a.steps.flushed as usize) < a.steps.rows.len() {|test:aggregations_from_summaries_match_the_scan_and_the_reference"
  "top-n-cut-too-tight|src/query/summary.rs|if desc0 { x >= t } else { x <= t }|if desc0 { x > t } else { x < t }|agg"
  "values-grouped-by-text|src/query/summary.rs|        let group_of = \|v: u32\| match rows.group[v as usize] {|        let group_of = \|v: u32\| match v {|agg"
  "gap-exclusions-ignored|src/query/matcher.rs|\|\| killed(sp, k, false, hit) {|\|\| killed(sp, k, false, 0) {|match"
  "turns-kept-after-a-step|src/steps.rs|        self.turns.take();
|
|test:patterns_follow_sessions_taking_writes"
  "escaped-hit-counted|src/query/plan.rs|        if slashes % 2 == 0 {|        if true {|lib:raw_contains"
  "shape-mask-bits|src/query/exec.rs|                Some(true) => m \|= 1 << bit,|                Some(true) => m \|= 1 << (bit ^ 1),|match"
  "phrase-next-piece-ignored|src/query/messages.rs|            or(&mut b, &down);|            let _ = &down;|test:phrases_across_pieces_and_shared_session_ids"
  "window-without-margin|src/textindex.rs|pub const WINDOW: usize = 512;|pub const WINDOW: usize = 0;|test:phrases_across_pieces_and_shared_session_ids"
  "long-words-not-read|src/query/messages.rs|        or(&mut read_whole, &h.long);|        let _ = &h.long;|test:phrases_across_pieces_and_shared_session_ids"
  "window-edge-is-a-word-end|src/query/messages.rs|let before = if a == 0 { !w.cut_left } else|let before = if a == 0 { true } else|test:phrases_across_pieces_and_shared_session_ids"
  "grouped-by-stored-session|src/query/messages.rs|        let (sid, agent) = ids[at];|        let (sid, agent) = (s, ids[at].1);|test:phrases_across_pieces_and_shared_session_ids"
  "roles-swapped|src/query/messages.rs|            if *user { and(&mut d, &h.user) } else { and_not(&mut d, &h.user) }|            if *user { and_not(&mut d, &h.user) } else { and(&mut d, &h.user) }|test:message_search_matches_a_scan_at_every_stage"
  "unflushed-messages-not-searched|src/db.rs|            if (a.core.flushed_nodes as usize) < a.core.nodes.len() {|            if false && (a.core.flushed_nodes as usize) < a.core.nodes.len() {|test:message_search_matches_a_scan_at_every_stage"
  "replaced-messages-searched|src/db.rs|            let live = if replaced { Some(self.live_mask(seg)?) } else { None };|            let live: Option<Arc<Vec<bool>>> = if replaced { None } else { None };|test:aggregations_skip_parts_that_folds_replaced"
  "regex-optional-part-required|src/query/relit.rs|        HirKind::Repetition(r) if r.min >= 1 => req(&r.sub),|        HirKind::Repetition(r) => req(&r.sub),|test:phrases_across_pieces_and_shared_session_ids"
  "regex-one-branch-enough|src/query/relit.rs|                    Req::Any => return Req::Any,
                    Req::Or(inner) => out.extend(inner),|                    Req::Any => {}
                    Req::Or(inner) => out.extend(inner),|lib:nothing_when_unsure"
  "regex-either-branch-both-required|src/query/summary.rs|            other => regex_spec(&other).into_iter().collect(),|            other => regex_spec(&other).into_iter().flatten().map(\|w\| vec![w]).collect(),|test:regex_errors_and_step_fields"
  "regex-shortest-spelling-dropped|src/query/relit.rs|                if k >= r.min {|                if k > r.min {|test:phrases_across_pieces_and_shared_session_ids"
  "regex-long-words-not-read|src/query/messages.rs|        or(&mut units, &maybe);|        let _ = &maybe;|test:phrases_across_pieces_and_shared_session_ids"
  "word-suffix-as-inside|src/textindex.rs|if if fit == Fit::Suffix { at + run.len() == end } else { at + run.len() <= end } {|if at + run.len() <= end {|lib:round_trip_and_lookups"
)

store() {
  rm -rf "$WORK/rich" && "$TDB" synth --store "$WORK/rich" --sessions 40 --per 60 --rich x --flush-mb 1 >/dev/null
}

run_check() {
  case "$1" in
    agg)
      store || return 0
      "$TDB" check-queries --store "$WORK/rich" --n 0 --eq 0 --agg 300 >"$WORK/out" 2>&1 || return 1
      "$TDB" compact --store "$WORK/rich" >/dev/null || return 0
      "$TDB" check-queries --store "$WORK/rich" --n 0 --eq 0 --agg 300 --seed 9 >>"$WORK/out" 2>&1 || return 1 ;;
    match)
      store || return 0
      "$TDB" check-queries --store "$WORK/rich" --n 200 --eq 400 --agg 0 >"$WORK/out" 2>&1 || return 1 ;;
    test:*) (cd "$COPY" && cargo test --release --quiet --test query "${1#test:}" >"$WORK/out" 2>&1) || return 1 ;;
    lib:*) (cd "$COPY" && cargo test --release --quiet --lib "${1#lib:}" >"$WORK/out" 2>&1) || return 1 ;;
  esac
  return 0
}

caught=0
missed=0
for b in "${BUGS[@]}"; do
  b=${b//\\|/$'\x01'}
  IFS='|' read -r -d '' name file from to check <<<"$b"
  check=${check%$'\n'}
  from=${from//$'\x01'/|}; to=${to//$'\x01'/|}
  if [ $# -gt 0 ] && [[ " $* " != *" $name "* ]]; then continue; fi
  if [ "${DRY:-0}" = 1 ]; then
    # only check that each bug's text is in its file exactly once
    n=$(FROM="$from" perl -0777 -ne 'BEGIN { $f = $ENV{FROM} } $n = () = /\Q$f\E/g; print $n' "$file")
    echo "$name: $n"; [ "$n" = 1 ] || missed=$((missed + 1)); continue
  fi
  rm -rf "$COPY" && mkdir -p "$COPY" && cp -R Cargo.toml Cargo.lock src tests "$COPY/"
  if ! FROM="$from" TO="$to" perl -0777 -i -pe 'BEGIN { $f = $ENV{FROM}; $t = $ENV{TO} } $n = s/\Q$f\E/$t/g; END { exit($n == 1 ? 0 : 3) }' "$COPY/$file"; then
    echo "$name: the text to replace is not in $file exactly once"; missed=$((missed + 1)); continue
  fi
  if ! (cd "$COPY" && cargo build --release --quiet 2>"$WORK/build"); then
    echo "$name: does not build"; missed=$((missed + 1)); continue
  fi
  if run_check "$check"; then
    echo "MISSED  $name (checks pass with the bug)"; missed=$((missed + 1))
  else
    echo "caught  $name ($check): $(grep -E 'DIFF|[1-9][0-9]* differ|panicked|FAILED' "$WORK/out" | head -1 | cut -c1-120)"; caught=$((caught + 1))
  fi
done
echo "$caught caught, $missed missed"
[ "${KEEP:-0}" = 1 ] || rm -rf "$WORK"
[ "$missed" -eq 0 ]
