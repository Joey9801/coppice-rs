#!/usr/bin/env bash
# The CI entry point for the sqlx query-metadata cache (docs/architecture/
# docker-executor.md §8.4): this is the repo's SQL convention entry point.
# Queries are checked at compile time in offline mode against a cache
# checked into each crate's `.sqlx/` directory, so run this after changing
# any query or migration:
#
#   scripts/sqlx-prepare.sh          regenerate the checked-in cache
#   scripts/sqlx-prepare.sh --check  verify the checked-in cache is current
set -euo pipefail
cd "$(dirname "$0")/.."

MODE="write"
if [[ "${1:-}" == "--check" ]]; then
    MODE="check"
fi

if ! cargo sqlx --version >/dev/null 2>&1; then
    echo "error: sqlx-cli is not installed (cargo sqlx not found)" >&2
    echo "install it with (0.8.x, matching the workspace's sqlx minor):" >&2
    echo "  cargo install sqlx-cli --version '^0.8' --no-default-features --features rustls,sqlite --locked" >&2
    exit 1
fi

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

DB_URL="sqlite:$TMP/telemetry.db"

echo "==> preparing scratch telemetry database"
cargo sqlx database create --database-url "$DB_URL"
cargo sqlx migrate run \
    --source crates/coppice-agent/migrations/telemetry \
    --database-url "$DB_URL"

echo "==> cargo sqlx prepare (coppice-agent, telemetry)"
(
    cd crates/coppice-agent
    if [[ "$MODE" == "check" ]]; then
        DATABASE_URL="$DB_URL" cargo sqlx prepare --check -- --all-targets --all-features
    else
        DATABASE_URL="$DB_URL" cargo sqlx prepare -- --all-targets --all-features
    fi
)
