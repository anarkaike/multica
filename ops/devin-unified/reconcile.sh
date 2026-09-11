#!/usr/bin/env bash
set -euo pipefail

# Reconciles a host-local Multica daemon against one immutable binary.
# Usage: devin-reconcile.sh <expected-binary> <installed-binary> <manifest>

expected=${1:?expected binary}
installed=${2:?installed binary}
manifest=${3:?manifest path}

hash_file() {
  if command -v shasum >/dev/null 2>&1; then
    shasum -a 256 "$1" | awk '{print $1}'
  else
    sha256sum "$1" | awk '{print $1}'
  fi
}

[[ -f "$expected" ]] || { echo "missing expected binary: $expected" >&2; exit 2; }
mkdir -p "$(dirname "$installed")" "$(dirname "$manifest")"

expected_sha=$(hash_file "$expected")
if [[ -e "$installed" ]]; then
  installed_sha=$(hash_file "$installed")
  if [[ "$installed_sha" != "$expected_sha" ]]; then
    cp "$installed" "${installed}.previous"
    cp "$expected" "$installed"
  fi
else
  cp "$expected" "$installed"
fi
chmod +x "$installed"

installed_sha=$(hash_file "$installed")
[[ "$installed_sha" == "$expected_sha" ]] || { echo "hash mismatch after install" >&2; exit 3; }

umask 077
cat > "$manifest" <<EOF
{
  "expected_sha256": "$expected_sha",
  "installed_sha256": "$installed_sha",
  "expected_binary": "$expected",
  "installed_binary": "$installed"
}
EOF
echo "reconciled $installed ($installed_sha)"
