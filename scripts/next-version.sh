#!/usr/bin/env bash
# SOT: release-versioning, semver-bump, conventional-commits
#
# WHAT:  Prints the next version for a push to main, derived from the commits
#        since the newest v* tag: a breaking change bumps major, a feat bumps
#        minor, anything else bumps patch.
# WHY:   Every main commit ships, so the version has to move on its own; typing
#        it by hand is how a release goes out as the version already published.
# HOW:   Conventional Commit subjects (`feat:`, `fix(scope):`, `feat!:`) and a
#        `BREAKING CHANGE:` trailer anywhere in a body.
# WHERE: .github/workflows/release.yml (version job), scripts/set-version.sh
set -euo pipefail

last=$(git tag --list 'v*' --sort=-v:refname | head -n1)
if [ -z "$last" ]; then
  range=""
  major=0 minor=0 patch=0
else
  range="${last}..HEAD"
  version="${last#v}"
  IFS='.' read -r major minor patch <<<"$version"
fi

subjects=$(git log ${range:+"$range"} --pretty=%s)
bodies=$(git log ${range:+"$range"} --pretty=%B)

if grep -qE '^[a-z]+(\([^)]*\))?!:' <<<"$subjects" || grep -qE '^BREAKING[ -]CHANGE:' <<<"$bodies"; then
  major=$((major + 1)); minor=0; patch=0
elif grep -qE '^feat(\([^)]*\))?:' <<<"$subjects"; then
  minor=$((minor + 1)); patch=0
else
  patch=$((patch + 1))
fi

echo "${major}.${minor}.${patch}"
