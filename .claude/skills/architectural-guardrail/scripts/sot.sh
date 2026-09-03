#!/usr/bin/env bash
# SOT: sot-script, grep-first, keyword-search, context-saver
#
# WHAT:  Find which files own a source of truth, without reading any of them.
# WHY:   `grep -rn` dumps matching file CONTENTS into the context window. `-l`
#        lists filenames only, so nothing enters context until one file is chosen.
#        Narrow first, read second.
# USAGE: ./scripts/sot.sh permissions

set -euo pipefail

if [ $# -eq 0 ]; then
  echo "usage: ./scripts/sot.sh <keyword>" >&2
  echo "example: ./scripts/sot.sh permissions" >&2
  exit 2
fi

KEYWORD="$1"
ROOT="${2:-src}"

echo "Files declaring SOT '${KEYWORD}':"
if ! grep -rl "SOT:.*${KEYWORD}" "$ROOT" 2>/dev/null; then
  echo "  (none — try a broader keyword, or add it to the SOT line of the right file)"
fi
