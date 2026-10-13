#!/usr/bin/env bash
# Pulls every image the docker_executor suite uses (the list in
# crates/coppice-agent/tests/docker-test-images.txt), retrying each with
# backoff. CI runs this as its own step before nextest so a registry outage
# fails one clearly named step instead of dozens of tests, and a transient
# one is ridden out (issue #175). Safe to run locally too.
set -euo pipefail
cd "$(dirname "$0")/.."

LIST=crates/coppice-agent/tests/docker-test-images.txt

while IFS= read -r image; do
    image="${image#"${image%%[![:space:]]*}"}"
    image="${image%"${image##*[![:space:]]}"}"
    [[ -z "$image" || "$image" == \#* ]] && continue
    for attempt in 1 2 3 4 5; do
        if docker pull --quiet "$image" >/dev/null; then
            echo "==> pulled $image"
            continue 2
        fi
        if [[ "$attempt" -lt 5 ]]; then
            delay=$((attempt * 10))
            echo "==> pull of $image failed (attempt $attempt), retrying in ${delay}s" >&2
            sleep "$delay"
        fi
    done
    echo "error: could not pull $image after 5 attempts" >&2
    exit 1
done < "$LIST"
