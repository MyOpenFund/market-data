#!/usr/bin/env bash
# Release guard: PyPI refuses a distribution whose metadata holds a direct reference
# ("name @ git+https://...", "name @ https://...", "name @ file:..."), and hatch only builds one
# with `allow-direct-references`.
#
#   check-no-direct-references.sh [pyproject.toml]   before the build: fails (exit 1) when the
#       pyproject still holds a direct reference or allow-direct-references, comment lines
#       aside (an inline comment naming either fails too: conservative);
#   check-no-direct-references.sh --wheel WHEEL       after the build: fails (exit 1) when the
#       wheel's METADATA has a Requires-Dist with " @ " (a direct reference, whatever wrote it).
#
# Switch the dependency to a released version first.
set -euo pipefail

if [ "${1:-}" = "--wheel" ]; then
  wheel="${2:?usage: $0 --wheel WHEEL}"
  if [ ! -f "$wheel" ]; then
    echo "release guard: $wheel not found"
    exit 2
  fi
  python3 -I - "$wheel" <<'PY'
import sys
import zipfile

wheel = sys.argv[1]
with zipfile.ZipFile(wheel) as z:
    names = [n for n in z.namelist() if n.endswith(".dist-info/METADATA")]
    if len(names) != 1:
        print(f"release guard: {wheel} has {len(names)} METADATA files, expected 1")
        sys.exit(2)
    lines = z.read(names[0]).decode("utf-8").splitlines()
bad = [line for line in lines if line.startswith("Requires-Dist:") and " @ " in line]
if bad:
    print(f"release guard: {wheel} has a direct reference in Requires-Dist, which PyPI refuses:")
    for line in bad:
        print(f"  {line}")
    sys.exit(1)
print(f"release guard: no direct reference in the Requires-Dist of {wheel}")
PY
  exit 0
fi

file="${1:-pyproject.toml}"
if [ ! -f "$file" ]; then
  echo "release guard: $file not found"
  exit 2
fi
found="$(awk '!/^[[:space:]]*#/ && (/@[[:space:]]*(git\+|https?:\/\/|file:)/ || /allow-direct-references/) {
  print "  line " NR ": " $0
}' "$file")"
if [ -n "$found" ]; then
  echo "release guard: $file holds a direct reference, which PyPI refuses:"
  echo "$found"
  echo "pin a released version instead and remove allow-direct-references, then release."
  exit 1
fi
echo "release guard: no direct reference in $file"
