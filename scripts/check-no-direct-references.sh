#!/usr/bin/env bash
# Release guard: PyPI refuses a distribution whose metadata holds a direct reference
# ("name @ git+https://...", "name @ https://...", "name @ file:..."), and hatch only builds one
# with `allow-direct-references`. Fails (exit 1) when the pyproject (default ./pyproject.toml)
# still holds either, comment lines aside: switch the dependency to a released version first.
set -euo pipefail
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
