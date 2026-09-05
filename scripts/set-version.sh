#!/usr/bin/env bash
# SOT: release-versioning, version-files
#
# WHAT:  Writes one version into every file that carries it: package.json,
#        src-tauri/tauri.conf.json, src-tauri/Cargo.toml and the app's own entry
#        in src-tauri/Cargo.lock.
# WHY:   The updater compares the version in the running bundle with the one in
#        latest.json; a file left behind means an update that installs itself
#        forever, or never.
# WHERE: .github/workflows/release.yml (version job), scripts/next-version.sh
set -euo pipefail

version="${1:?usage: set-version.sh <x.y.z>}"
root="$(cd "$(dirname "$0")/.." && pwd)"

python3 - "$root" "$version" <<'PY'
import json
import pathlib
import re
import sys

root = pathlib.Path(sys.argv[1])
version = sys.argv[2]

for name in ("package.json", "src-tauri/tauri.conf.json"):
    path = root / name
    data = json.loads(path.read_text())
    data["version"] = version
    path.write_text(json.dumps(data, indent=2) + "\n")

cargo = root / "src-tauri/Cargo.toml"
text = cargo.read_text()
text, count = re.subn(r'(?m)^version = "[^"]+"', f'version = "{version}"', text, count=1)
assert count == 1, "no version in Cargo.toml"
cargo.write_text(text)

lock = root / "src-tauri/Cargo.lock"
text = lock.read_text()
text, count = re.subn(
    r'(?ms)^(\[\[package\]\]\nname = "db-free"\nversion = )"[^"]+"',
    lambda m: f'{m.group(1)}"{version}"',
    text,
    count=1,
)
assert count == 1, "no db-free package in Cargo.lock"
lock.write_text(text)

print(f"version set to {version}")
PY
