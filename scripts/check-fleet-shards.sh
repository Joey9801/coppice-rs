#!/usr/bin/env bash
# Verifies that the binaries the ci profile excludes are exactly the ones the
# ci-fleet-* shard profiles run: same set, no binary in two shards. Guards
# against a new fleet-serial suite being added to profile.ci's exclusion
# without a shard picking it up — the one drift that silently drops tests
# from CI. (The converse drifts are safe: a name in neither list, or a binary
# renamed out from under both, just runs in the ordinary test job via
# profile.ci's `not(...)`.)
#
# A pure config check on .config/nextest.toml — deliberately no cargo: the
# coppice-api build script is always-dirty while web/dist is absent, so any
# extra cargo invocation in CI rebuilds three heavy crates for nothing.
set -euo pipefail
cd "$(dirname "$0")/.."

python3 - <<'EOF'
import re
import sys
import tomllib

with open(".config/nextest.toml", "rb") as f:
    profiles = tomllib.load(f)["profile"]

def binaries(filterset):
    return set(re.findall(r"binary\(([^)]+)\)", filterset))

excluded = binaries(profiles["ci"]["default-filter"])

shards = {
    name: binaries(profile["default-filter"])
    for name, profile in profiles.items()
    if name.startswith("ci-fleet-")
}
if not shards:
    sys.exit("no ci-fleet-* profiles found in .config/nextest.toml")

ok = True
sharded = set()
for name, suites in sorted(shards.items()):
    if doubled := suites & sharded:
        ok = False
        print(f"{name} repeats suites already sharded: {sorted(doubled)}")
    sharded |= suites

if dropped := excluded - sharded:
    ok = False
    print(f"excluded by profile.ci but in no ci-fleet-* shard: {sorted(dropped)}")
if extra := sharded - excluded:
    ok = False
    print(f"in a ci-fleet-* shard but not excluded by profile.ci: {sorted(extra)}")

if not ok:
    sys.exit(1)
print(f"profile.ci exclusions == shard union: {sorted(sharded)}")
EOF
