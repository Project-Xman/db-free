#!/usr/bin/env bash
# SOT: per-category-test-driver, category-result-table
#
# WHAT:  Runs the engine suite one category at a time, each in its own invocation
#        of test-all-dbs.sh, and prints a single table at the end: per category
#        and per engine, pass / fail / skip, with the wall time each took.
# WHY:   `test-all-dbs.sh` already walks every category in one process, so one
#        engine that wedges the Docker daemon takes the whole run down with it and
#        the summary is lost. Driving each category as a separate child means a
#        category can die without costing the others, and the table is written
#        incrementally — a killed run still leaves a readable result on disk.
# HOW:   ./scripts/test-by-category.sh                    # every category
#        ./scripts/test-by-category.sh relational vector  # only these
#        WITH_HEAVY=1 ./scripts/test-by-category.sh       # include oracle + druid
#        OUT=path/to/report.md ...                        # where the table lands
# WHERE: scripts/test-all-dbs.sh (does the work for one category),
#        docker-compose.test.yml (the stack)
set -uo pipefail

cd "$(dirname "$0")/.." || exit 1
ROOT="$PWD"
OUT="${OUT:-$ROOT/category-report.md}"
LOGDIR="${LOGDIR:-$ROOT/.category-logs}"
mkdir -p "$LOGDIR"

# WHAT:  The category list, kept in the same order the connection picker shows.
# WHY:   Read from test-all-dbs.sh rather than copied, so the two cannot drift.
# HOW:   A read loop, not `mapfile`: macOS ships bash 3.2, where mapfile does not
#        exist and the list would silently come back empty.
CATEGORIES=()
while IFS= read -r line; do
  [ -n "$line" ] && CATEGORIES+=("$line")
done < <(sed -n '/^CATEGORIES=(/,/^)/p' scripts/test-all-dbs.sh |
  sed -n 's/^  "\([a-z-]*\):.*/\1/p')
if [ ${#CATEGORIES[@]} -eq 0 ]; then
  echo "could not read the category list from scripts/test-all-dbs.sh" >&2
  exit 1
fi
[ "${WITH_HEAVY:-0}" = "1" ] && CATEGORIES+=("heavy")

SELECTED=("$@")
wanted() {
  [ ${#SELECTED[@]} -eq 0 ] && return 0
  for s in "${SELECTED[@]}"; do [ "$s" = "$1" ] && return 0; done
  return 1
}

hms() { printf '%dm%02ds' $(($1 / 60)) $(($1 % 60)); }

{
  echo "# Engine test report"
  echo
  echo "_$(date '+%Y-%m-%d %H:%M:%S')_ · host \`$(uname -sm)\` · docker \`$(docker version --format '{{.Server.Version}}' 2>/dev/null || echo n/a)\`"
  echo
  echo "| Category | Passed | Failed | Skipped | Time |"
  echo "|---|---|---|---|---|"
} > "$OUT"

TOTAL_PASS=(); TOTAL_FAIL=(); TOTAL_SKIP=()
FAILED_CATEGORIES=()

for category in "${CATEGORIES[@]}"; do
  wanted "$category" || continue

  printf '\n\033[1m== %s\033[0m\n' "$category"
  log="$LOGDIR/$category.log"
  start=$SECONDS

  # KEEP_GOING so one bad engine does not cut its own category short; the child
  # still tears its containers down on exit.
  KEEP_GOING=1 WITH_HEAVY="${WITH_HEAVY:-0}" ./scripts/test-all-dbs.sh "$category" > "$log" 2>&1
  elapsed=$((SECONDS - start))

  # The child prints "passed  ( n): a b c" for each bucket; reuse that rather
  # than re-deriving the engine list here.
  bucket() { sed 's/\x1b\[[0-9;]*m//g' "$log" | sed -n "s/^$1 *( *[0-9]*): //p" | tail -1; }
  passed=$(bucket passed); failed=$(bucket failed); skipped=$(bucket skipped)
  [ "$passed"  = "none" ] && passed=""
  [ "$failed"  = "none" ] && failed=""
  [ "$skipped" = "none" ] && skipped=""

  # Each bucket is a space-separated engine list; split it deliberately.
  for e in $passed;  do TOTAL_PASS+=("$e"); done
  for e in $failed;  do TOTAL_FAIL+=("$e"); done
  for e in $skipped; do TOTAL_SKIP+=("$e"); done

  n_pass=$(wc -w <<< "$passed" | tr -d ' ')
  n_fail=$(wc -w <<< "$failed" | tr -d ' ')
  n_skip=$(wc -w <<< "$skipped" | tr -d ' ')

  printf '| %s | %s | %s | %s | %s |\n' \
    "$category" \
    "${n_pass:-0}${passed:+ · $passed}" \
    "${n_fail:-0}${failed:+ · **$failed**}" \
    "${n_skip:-0}${skipped:+ · $skipped}" \
    "$(hms "$elapsed")" >> "$OUT"

  if [ -n "$failed" ]; then
    FAILED_CATEGORIES+=("$category")
    printf '   \033[31m%s: %s passed, %s FAILED (%s)\033[0m  %s\n' \
      "$category" "$n_pass" "$n_fail" "$failed" "$(hms "$elapsed")"
    # Surface the reason inline so the table is not the only evidence.
    sed 's/\x1b\[[0-9;]*m//g' "$log" | grep -A1 "panicked at" | head -6 | sed 's/^/      /'
  else
    printf '   \033[32m%s: %s passed, %s skipped\033[0m  %s\n' \
      "$category" "$n_pass" "$n_skip" "$(hms "$elapsed")"
  fi
done

# ---------------------------------------------------------------- final result
{
  echo
  echo "## Result"
  echo
  echo "- **passed (${#TOTAL_PASS[@]})**: ${TOTAL_PASS[*]:-none}"
  echo "- **failed (${#TOTAL_FAIL[@]})**: ${TOTAL_FAIL[*]:-none}"
  echo "- **skipped (${#TOTAL_SKIP[@]})**: ${TOTAL_SKIP[*]:-none}"
  echo
  if [ ${#TOTAL_FAIL[@]} -eq 0 ]; then
    echo "All engines that could be reached passed."
  else
    echo "Failing categories: ${FAILED_CATEGORIES[*]}"
  fi
  echo
  echo "Per-category logs: \`${LOGDIR#"$ROOT"/}/<category>.log\`"
} >> "$OUT"

echo
echo "=================================================================="
printf 'passed  (%2d): %s\n' "${#TOTAL_PASS[@]}" "${TOTAL_PASS[*]:-none}"
printf 'failed  (%2d): %s\n' "${#TOTAL_FAIL[@]}" "${TOTAL_FAIL[*]:-none}"
printf 'skipped (%2d): %s\n' "${#TOTAL_SKIP[@]}" "${TOTAL_SKIP[*]:-none}"
[ ${#FAILED_CATEGORIES[@]} -gt 0 ] && printf 'failing categories: %s\n' "${FAILED_CATEGORIES[*]}"
echo "report: ${OUT#"$ROOT"/}"
echo "=================================================================="
[ ${#TOTAL_FAIL[@]} -eq 0 ]
