#!/usr/bin/env bash
# The CI entry point for schema checks (docs/architecture/schema-style.md):
# always runs the vendored descriptor-set breaking gate; additionally runs
# buf lint/breaking when buf is installed (optional, stricter style checks).
set -euo pipefail
cd "$(dirname "$0")/.."

if command -v buf >/dev/null 2>&1; then
    echo "==> buf lint"
    buf lint
    if git rev-parse --verify --quiet main >/dev/null; then
        echo "==> buf breaking (against main)"
        buf breaking --against ".git#branch=main"
    else
        echo "==> buf breaking skipped (no local main branch)"
    fi
else
    echo "==> buf not installed; relying on the vendored descriptor diff"
fi

echo "==> descriptor-set breaking gate"
cargo test -p coppice-proto --test breaking
