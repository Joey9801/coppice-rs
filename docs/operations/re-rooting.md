# Re-rooting and Root Rotation

How to replace the cluster CA — the trust root every machine credential
chains to — and how to tell when the lesser procedure will do instead. The
rules being operated here are decided in
[ADR 0037](../decisions/0037-coordinator-discovery-and-self-converging-membership.md)
§4 (a cluster-owned CA whose private key lives on voter disks, with
re-rooting as the compromise response) and §5 (enrollment tokens, and the
distinction between revoking a token and revoking an identity). The
security model this sits inside is [security.md](security.md).

Re-rooting is disruptive by construction. It is the **only** mechanism
that invalidates an already-issued certificate before its expiry, and
that is exactly why it is the compromise response — and why it is not the
answer to problems the lesser procedure solves.

## When to re-root

Re-root when anything **root-equivalent** may be compromised. ADR 0037 §4
is explicit that removal cannot make a disk forget: identity revocation
and seat removal do not recall signing authority from a disk that already
has it. The root-equivalent set is:

- **Every current voter disk.** The CA private key resides on voter
  disks (`<data_dir>/ca.key`).
- **Every former voter disk**, and any snapshot or backup image of one.
  A removed voter is a removed voter, not a disarmed one.
- **Every promotion candidate past the key-transfer gate** — including
  one whose promotion **never committed**. A leader crash in the transfer
  window abandons the candidate as a learner, but abandons it *holding
  the key*. These are the entries `admin status` reports under
  `pending_key_transfers`: unresolved intents, deliberately conservative.
- **Any coordinator enrollment token.** Through a legitimate vacancy or
  an evidence-gated removal window, a coordinator token can place a
  learner that is subsequently staged for promotion and keyed. ADR 0037
  §5 states the consequence plainly: the coordinator token is
  root-equivalent, and the long-lived launch-template form of it is an
  *explicitly accepted risk*.

**Total loss of every voter disk** lands in this runbook too, but as a
different procedure — see [Total voter-disk loss](#total-voter-disk-loss).
It is not a rotation: there is no cluster left to rotate.

The custody accounting is the inventory. Read it before deciding:

```
coppice coordinator admin --config /etc/coppice/coordinator.toml status
```

```
key holders (ADR 0037 §4):
  node 4739272934893806964 voter
  node 9156952040390162700 voter
  node 17882036273301565489 voter
pending key transfers (unresolved intents, ADR 0037 §4):
  (none)
```

A node listed under either heading has, or may have, the key. While a
rotation is staged, `rotate-ca status` adds two more accounting lines for
the *incoming* root — `staged key` holders and its own pending
transfers — plus, per current voter, whether it holds the staged key yet.
Staged holders are root-equivalent **already**, not only once the
rotation activates: the pending root is a trust anchor from the moment it
is staged, so a leaf minted with the staged key verifies fleet-wide right
away. At activation those entries merge into `key holders`. Outside a
rotation, or once one completes, the count of disks holding the *active*
key is unchanged by having rotated — a re-root changes *which* key a
disk holds, never how many disks are root-equivalent to it.

### The lesser tier, and how to tell

Do **not** re-root for an illegitimate enrollment that never reached a
voter seat. An attacker who redeemed an **agent** token holds an agent
leaf: it can run workloads, and nothing more. An attacker who redeemed a
**coordinator** token holds a learner — which replicates all cluster
state, so it is a read-everything credential — but until that learner is
staged for promotion and keyed, it is not root-equivalent.

The test is custody, not suspicion: **if the suspect node id never
appears in `key_holders` or `pending_key_transfers`, the lesser procedure
is sufficient.** If it does appear, re-root.

## The lesser procedure: token rotation + identity revocation

Revoking a token stops *future* enrollments. It does nothing to leaves
already issued from it, and those leaves keep renewing. Evicting an
attacker who already enrolled therefore takes **both** halves (ADR 0037
§5).

Mint the replacement first, so the fleet is never without a working
launch-template credential:

```
coppice node --target coord-1:7071 \
  --ca /etc/coppice/pki/ca.crt --cert operator.crt --key operator.key \
  enroll-token mint --role coordinator --ttl 15m --label replacement-coordinator
```

```
minted enrollment token token-019fe227-b209-7630-8be6-a8760b0f4333 (replacement-coordinator)
expires at 2026-08-08T16:29:40.136288Z
store this now — the secret is printed once and cannot be recovered; re-mint if it is lost
cpk_pXCws7GzpC4e9seo9wonPK6p9VhkIaQIfpzlgzjtWM0
```

Then revoke the suspect token:

```
coppice node --target … enroll-token revoke --id token-019fe223-2602-79e0-a434-377af254083e
```

```
revoked enrollment token token-019fe223-2602-79e0-a434-377af254083e: no further enrollments
will be accepted with it. Leaves already issued from it keep renewing — revoke those
identities too (ADR 0037 §5)
```

Then revoke every identity that redeemed it. `--node` for a compute node,
`--machine` for a coordinator machine identity:

```
coppice node --target … revoke-identity --node node-eaee5d06-c4e1-458c-8685-6f697c52556e
```

```
identity revoked: the leader now refuses its renewals, and its current leaf stays valid
until it expires (ADR 0037 §5 — no CRL, no OCSP)
```

**Verify.** `enroll-token list` must show the suspect token `revoked` and
the replacement `live`:

```
TOKEN                                         ROLE          LABEL                     EXPIRES               STATE
token-019fe223-2602-79e0-a434-377af254083e    coordinator   dryrun-launch-template    never                 revoked
token-019fe223-2604-77b3-87e5-9a448432f869    agent         dryrun-agent-launch-template  never              live
token-019fe227-b209-7630-8be6-a8760b0f4333    coordinator   replacement-coordinator   2026-08-08T16:29:40Z  live
```

For a coordinator machine identity, `admin status --json`'s `bindings`
array is the cluster-side record. For an **agent**, there is no
`admin status` surface: the observable is the agent's own log, which
escalates to `error` as expiry approaches, and a re-enrollment attempt
answered `HTTP 401`:

```
INFO coppice_enroll::client: enrolling for a cluster-signed leaf (ADR 0037 §4) claim=Node(NodeId(eaee5d06-…))
Error: enrolling for a cluster-signed agent leaf (config [enrollment])
    the enrollment endpoint refused the request: HTTP 401
```

**The bound.** Renewal refusal *is* revocation, so eviction is a bounded
delay, not a cutoff: the revoked node keeps working until its current
leaf expires — at most one leaf lifetime (30 days), and typically less.
If you need the credential dead sooner than that, the lesser procedure is
not enough, and re-rooting is the only mechanism that will do it.

## Re-rooting procedure

Three verbs, all on the daemon's **local Unix socket** — the same plane
as `init` and `issue-operator-cert`, because re-rooting is
root-equivalent authority and authorizing it with a certificate issued by
the CA being replaced would make every operator credential a re-rooting
credential:

```
coppice coordinator admin --config <config> rotate-ca begin
coppice coordinator admin --config <config> rotate-ca status [--json]
coppice coordinator admin --config <config> rotate-ca complete [--force]
```

`begin` and `complete` must run **on the leader's host**. Off the leader
they refuse and name the leader:

```
Error: this daemon (node 4739272934893806964) is not the leader; node 17882036273301565489
is. Re-rooting stages the incoming CA key on the disk it runs on and drives the transfer
that puts it on every other voter — run this on the leader's host.
```

`status` is read-only and deliberately **not** leader-gated: several of
the questions it answers are about the replica it runs on.

### The invariant

> **A root never becomes the active signing root until its private key is
> durably held by every current voter.**

The procedure below exists to make that sentence true no matter what
crashes and when. It replaces an earlier design that made the incoming
root the active signing root the instant `begin` committed, with its
private key existing only in the leader's memory until a transfer loop
got around to writing it out. A leader that crashed in that window left
a cluster whose *recorded* active root had no private half on any
disk — unrecoverable, because the key that would have to be recovered
never touched a disk in the first place. The redesign closes that window
by never letting the state exist: signing does not move until every
current voter already holds the incoming key durably.

One consequence an operator responding to a compromise needs up front:
**the outgoing key keeps issuing new leaves for the entire staged
phase.** `begin` does not stop it — only activation does (phase 3,
below). `begin` alone does not contain a compromise; if you need the
exposure closed fast, drive coverage to completion (or replace the
unreachable voter) as quickly as you can and let `begin` activate.

### Step 0 — record the outgoing root

You will need its identity to prove, later, that it is gone. Both fields
are what `openssl` prints for the same file, so the check is a string
comparison and not a matter of trusting the verb's account of itself:

```
coppice coordinator admin --config <config> rotate-ca status
```

```
rotation      none (single root)
recorded at   1786205400123456 (unix us)
  root 0      ACTIVE (signs)  cn=coppice-cluster-ca  serial=6d580861a02747bf212191a018176f96552ecf9d  ski=f5e5a3acb7fa495deceb9e8dca275d9b12bd9c57
this replica  anchor current · leaf renewed onto the active root · signing key signs the active root
node          17882036273301565489 (leader Some(17882036273301565489))
key holders   [4739272934893806964, 9156952040390162700, 17882036273301565489]   pending transfers []
```

```
openssl x509 -in /etc/coppice/pki/ca.crt -noout -subject -serial -ext subjectKeyIdentifier
```

```
subject=CN=coppice-cluster-ca
serial=6D580861A02747BF212191A018176F96552ECF9D
X509v3 Subject Key Identifier:
    F5:E5:A3:AC:B7:FA:49:5D:EC:EB:9E:8D:CA:27:5D:9B:12:BD:9C:57
```

### Step 1 — `rotate-ca begin`

`begin` drives a rotation through up to three phases in one call, and
`rotate-ca status` (Step 4) names which phase a rotation that did not
finish is parked in:

1. **Stage.** Mint the incoming root, write its key and certificate to
   this leader's disk (`<data_dir>/ca-staged.key`, `ca-staged.crt`,
   owner-only), and only then record the bundle
   `[outgoing (ACTIVE, still signing), incoming (PENDING)]`. Dual trust
   starts here — both roots are verifiable anchors from this commit — but
   **signing does not move**.
2. **Distribute.** Push the staged key to every other current voter over
   the same `TransferCaKey` RPC a promotion uses. Each confirmed durable
   receipt is recorded in **replicated** state, so a resume on a
   different leader knows exactly who holds it.
3. **Activate — gated on total coverage.** Only once every current voter
   has a replicated staged-key confirmation does `begin` commit the
   reordered bundle `[incoming (ACTIVE), outgoing]` and promote its own
   staged key to `ca.key`. If any voter is missing, `begin` **refuses to
   activate**: it exits non-zero with the `ROTATION_COVERAGE_INCOMPLETE`
   marker, names the missing voters, and leaves the rotation parked in
   the staged phase. That parked state is a working state
   indefinitely — both roots verify, the outgoing root signs. The fix is
   a **membership** change (`admin replace-voter` or `admin remove-node`
   for the unreachable voter), then a re-run of `begin`, which re-checks
   coverage against the CURRENT voter set.

A fresh rotation where every voter is reachable moves through all three
in one call:

```
coppice coordinator admin --config /etc/coppice/coordinator.toml rotate-ca begin
```

```
re-root staged: the incoming root is recorded as PENDING, and both roots are now trust anchors.
re-root ACTIVATED: every current voter durably holds the incoming key, so it is now the active
signing root.

outgoing CA key preserved at /var/lib/coppice/ca.key.superseded-1786205475506664
  that file is root-equivalent for as long as the outgoing root is trusted; treat it as such.

staged-key distribution to the other current voters:
  node 4739272934893806964 localhost:21111  installed
  node 9156952040390162700 localhost:21121  installed

rotation      ACTIVE (dual trust) — ready for `rotate-ca complete` once its clock allows
recorded at   1786205475480625 (unix us)
  root 0      ACTIVE (signs)  cn=coppice-cluster-ca  serial=4aa7415493660160097274841353e965bd357714  ski=3f522d58a7be720c63cfd590a6924a36eabace0a
  root 1      trusted only   cn=coppice-cluster-ca  serial=6d580861a02747bf212191a018176f96552ecf9d  ski=f5e5a3acb7fa495deceb9e8dca275d9b12bd9c57
this replica  anchor current · leaf renewed onto the active root · signing key signs the active root
node          17882036273301565489 (leader Some(17882036273301565489))
complete at   1788797475480625 (unix us) — one leaf lifetime (2592000s) after begin; --force overrides
key holders   [4739272934893806964, 9156952040390162700, 17882036273301565489]   pending transfers []

next: wait for every coordinator and agent to renew onto the new root (watch `rotate-ca
status` on each), then run `rotate-ca complete`.
```

The header is now two lines because they report two independent facts:
what this call did about *staging*, and whether it went on to
*activate*. Folding them into one sentence used to read as a falsehood
whenever a single call did both — the staging line would say the
outgoing root still signs, moments after activation had moved it.

The "outgoing CA key preserved" message appears only once activation
actually happens — the staged phase never touches the live signing key.

When a current voter cannot be reached, the same call parks instead of
activating:

```
coppice coordinator admin --config /etc/coppice/coordinator.toml rotate-ca begin
```

```
re-root staged: the incoming root is recorded as PENDING, and both roots are now trust anchors.
the OUTGOING root still signs. Activation waits until every current voter durably holds the
incoming key.

staged-key distribution to the other current voters:
  node 4739272934893806964 localhost:21111  installed
  node 9156952040390162700 localhost:21121  FAILED: Unavailable: transport error

rotation      DISTRIBUTING — staged key going to the voters; the OUTGOING root still signs
recorded at   1786205475480625 (unix us)
  root 0      ACTIVE (signs)  cn=coppice-cluster-ca  serial=6d580861a02747bf212191a018176f96552ecf9d  ski=f5e5a3acb7fa495deceb9e8dca275d9b12bd9c57
  root 1      PENDING        cn=coppice-cluster-ca  serial=4aa7415493660160097274841353e965bd357714  ski=3f522d58a7be720c63cfd590a6924a36eabace0a
  voter 4739272934893806964     holds the staged key
  voter 9156952040390162700     DOES NOT hold the staged key
  voter 17882036273301565489    holds the staged key
activation    BLOCKED on [9156952040390162700] — `rotate-ca begin` will refuse to activate until every current voter holds the staged key. Repair or replace those voters, then re-run it.
this replica  anchor current · leaf renewed onto the active root · signing key signs the active root
node          17882036273301565489 (leader Some(17882036273301565489))
complete at   1788797475480625 (unix us) — one leaf lifetime (2592000s) after begin; --force overrides
key holders   [4739272934893806964, 9156952040390162700, 17882036273301565489]   pending transfers []
staged key    holders [4739272934893806964, 17882036273301565489]   pending transfers []

Error: ROTATION_COVERAGE_INCOMPLETE: the incoming root was NOT activated, because these current
voters do not hold its key: [9156952040390162700].

A root never becomes the active signing root until every current voter durably holds its
private key — otherwise a failover could land the cluster on a voter that cannot sign. The
rotation is parked in the staged phase, which is a working state: both roots are trust
anchors and the outgoing root still signs, indefinitely if need be.

Fix membership first — repair those hosts, or `admin replace-voter` / `admin remove-node`
them — then re-run `rotate-ca begin`. It re-checks coverage against the CURRENT voter set
and mints nothing it does not have to.
```

`rotate-ca begin` exits non-zero in this outcome. That is the correct
exit code — the rotation did not finish — but it is not an error in the
rotation: staging succeeded, dual trust is in place, and the outgoing
root keeps signing exactly as it did before you ran the command.

What happened, per plane:

- **Replicated state** records the incoming root as **pending** and, in
  the activated case, promotes it to position 0 (the active signing
  root) once coverage completes in the same call. Every certificate in
  the bundle is a trust anchor throughout, so leaves under either root
  authenticate — including the pending one, from the moment it is
  staged.
- **The outgoing key keeps minting** for as long as the rotation stays in
  the staged or distributing phase. Nothing about `begin` alone stops it;
  only the activation commit (coverage complete) does.
- **The leader's `ca.key`** becomes the incoming key only at activation,
  and only after this leader's own staged-key confirmation and every
  peer's do. The outgoing key is preserved beside it as
  `ca.key.superseded-<µs>`, owner-only, at that same moment. It is every
  bit as root-equivalent as it was before; destroy it when the rotation
  is complete.
- **Every other current voter** is keyed over the same `TransferCaKey`
  RPC a promotion uses, and each confirmed durable receipt is recorded in
  replicated state — not merely acknowledged to this process, so a resume
  on a different leader reads the same coverage. **A `FAILED` line is not
  advisory now: it is the reason activation does not happen.** Repair the
  peer and re-run `rotate-ca begin` — it mints nothing the second time
  and resumes distribution, skipping voters that already confirmed.
- **Trust moved before signing did.** The order is a safety property, not
  an implementation detail: the bundle recording the incoming root as an
  anchor commits, every replica adopts it into its own trust store on its
  own timer (no dial, no leader, no signature required), and only once
  every current voter also durably holds the *key* does signing move. A
  leader that re-signed its serving leaf before the fleet trusted the
  incoming root would be unverifiable to a follower that had not yet
  adopted it — and a follower that cannot verify the leader cannot dial
  it to renew, which is the one failure mode here that does not heal
  itself.
- **No downtime, no restarts.** All three listeners keep serving
  throughout; nothing is restarted at any point in this procedure.

### Step 2 — refresh operator trust anchors (do this immediately)

**This step is not optional and it is time-critical.** The dual-trust
window protects the *client-certificate* direction — the cluster keeps
accepting your old operator certificate. It does **not** protect the
*server-certificate* direction: your `--ca` file is a static snapshot,
and every coordinator re-signs its serving leaf under the new root within
about 15 seconds of **activation** (not of `begin`, if `begin` parked in
the staged phase — signing has not moved yet in that case). An operator
who skips this step is locked out of the network admin plane almost at
once after activation:

```
Error: connecting to admin target localhost:21101
Caused by:
    0: transport error
    1: invalid peer certificate: BadSignature
```

Copy the recorded bundle from any coordinator's `[tls] ca_path` to every
workstation and automation host that holds one:

```
scp coord-1:/etc/coppice/pki/ca.crt ./ca.crt
```

With the refreshed bundle, the **day-0 operator certificate — issued
under the outgoing root — still works**, which is the whole point of the
window:

```
coppice node --target coord-1:7071 --ca ./ca.crt \
  --cert operator-day0.crt --key operator-day0.key enroll-token list
```

```
TOKEN                                         ROLE          LABEL                     EXPIRES  STATE
token-019fe223-2602-79e0-a434-377af254083e    coordinator   dryrun-launch-template    never    live
```

### Step 3 — refresh agent trust anchors

Coordinators renew themselves. **Agents do not.** An agent renews on its
own timer at two thirds of its leaf lifetime — about 20 days — and it has
no fast path for a CA change. Two consequences, both verified:

- An agent with an **established** session keeps working across
  activation indefinitely. The stream is already up and nothing
  revalidates it.
- An agent that **reconnects** — a restart, a coordinator failover, a
  network blip — cannot verify the new-root serving certificate, and it
  does not self-heal. It retries forever without ever re-enrolling:

  ```
  WARN coppice_agent::session::runner: session error; reconnecting endpoint="localhost:21102" error=transport error
  WARN coppice_agent::session::runner: session error; reconnecting endpoint="localhost:21112" error=transport error
  ```

The remedy is to push the recorded bundle into each agent's
`[tls] ca_path`. **No restart is needed** — the agent's TLS store
hot-reloads it on the mtime poll:

```
scp coord-1:/etc/coppice/pki/ca.crt agent-7:/etc/coppice/pki/ca.crt
```

```
INFO coppice_tls: tls reload: swapped in new certificate material plane=tls cert=/…/node.crt
```

and the session errors stop. Treat this as a fleet-wide push, not a
per-incident fix: every agent needs it, and an agent that has not had it
is one reconnect away from being stranded.

### Step 4 — confirm the coordinators turned over

Coordinators pick the new root up automatically, with no restart and no
dropped connection. Two things move, and they move at different speeds:

- **Trust anchors.** The recorded bundle is replicated state, so a
  coordinator installs it without dialing anyone, starting at the stage
  commit. Every current voter had it before `begin` returned from
  distribution; anyone else adopts it within one
  `[pacing] renewal_reevaluate_interval` (15s by default).
- **The leaf.** Re-signing under the incoming root takes a signature from
  the leader, and it cannot happen until **activation** — the renewal
  task notices it is not on the recorded active root and renews, usually
  within a second or two of activation.

```
INFO coppice_coordinator::tasks::renewal: renewal: installed a re-issued coordinator leaf
```

Check every one of them:

```
coppice coordinator admin --config <config> rotate-ca status | grep 'this replica'
```

```
this replica  anchor current · leaf renewed onto the active root · signing key signs the active root
```

`anchor current` means this replica trusts the recorded bundle. `leaf
renewed onto the active root` means it has re-signed — and that is the
one step 6 can strand, so read it, not just the anchor field. Either
reading `STALE` means investigate before proceeding: a replica whose
anchor is stale has not seen the rotation at all, and one whose leaf is
stale cannot reach the leader for a signature (or activation has not
happened yet — check the `rotation` line first).

### Step 5 — wait out the leaf turnover

Everything issued under the outgoing root was issued before activation
committed, and no leaf outlives 30 days. One leaf lifetime after
activation, no leaf under the outgoing root can still be unexpired —
whatever any node was doing in the meantime. `complete` enforces exactly
that bound:

```
Error: the dual-trust window has 29days 23h 57m 10s left to run. Every leaf the outgoing
root signed was signed before this rotation began, and no leaf outlives 30days, so after
that point none can remain — completing now can strand any node that has not yet renewed
(an agent that cannot verify the cluster retries forever; it does not re-enrol). Verify
turnover as the re-rooting runbook describes and re-run with --force to complete early.
```

`--force` is the verb you run *after* verifying turnover yourself, not
instead of verifying it. Verifying means: for every node in the fleet,
its leaf chains to the incoming root. For a node whose leaf you can read:

```
openssl x509 -in /etc/coppice/pki/ca.crt -outform pem > new-root.pem   # first block = active root
openssl verify -CAfile new-root.pem /etc/coppice/pki/node.crt
```

```
/etc/coppice/pki/node.crt: OK
```

A node still under the outgoing root fails it plainly:

```
error 7 at 0 depth lookup: certificate signature failure
error /…/node.crt: verification failed
```

Under a genuine compromise, forcing early is often correct — the outgoing
key is in an attacker's hands for as long as the window stays open. The
cost is stated in the next step.

### Step 6 — `rotate-ca complete`

`complete` refuses outright while a rotation is still staged — that is,
while the recorded bundle's active root (position 0) is still the
OUTGOING one. Completing in that state would retire the *incoming* root
instead and leave the compromised one as the cluster's sole anchor, which
is the exact inverse of the intent:

```
Error: this rotation is still staged: the recorded bundle's active root is the OUTGOING
one and 4aa7415493660160097274841353e965bd357714 is only pending, so completing now would
retire the incoming root and leave the outgoing one as the cluster's sole trust anchor.
Run `rotate-ca begin` until it reports the rotation activated — `rotate-ca status` names
any voter still missing the staged key.
```

Drive activation first (Step 1), then complete:

```
coppice coordinator admin --config /etc/coppice/coordinator.toml rotate-ca complete
```

```
re-root complete: the outgoing root is no longer trusted

retired  serial 6d580861a02747bf212191a018176f96552ecf9d  ski f5e5a3acb7fa495deceb9e8dca275d9b12bd9c57

rotation      none (single root)
recorded at   1788797475480625 (unix us)
  root 0      ACTIVE (signs)  cn=coppice-cluster-ca  serial=4aa7415493660160097274841353e965bd357714  ski=3f522d58a7be720c63cfd590a6924a36eabace0a
```

This is the irreversible step and the only one that strands anything. A
node whose leaf is still under the outgoing root is refused from its next
reconnect on — established sessions survive until they drop, which makes
the damage look smaller than it is for a while.

The recovery for a stranded node is a **fresh enrollment**: remove its
leaf so startup enrollment re-runs.

```
systemctl stop coppice-agent
rm -f /etc/coppice/pki/node.crt /etc/coppice/pki/node.key /etc/coppice/pki/ca.crt
systemctl start coppice-agent
```

```
INFO coppice_enroll::client: enrolled; the issued leaf and CA bundle are installed
INFO coppice_agent: startup enrollment settled outcome=Enrolled
```

If that node's identity was revoked (the lesser procedure), enrollment is
refused `HTTP 401` and the answer is a new installation with a new node
id — which is the immutable-infrastructure answer anyway.

Finally, destroy the preserved outgoing key on the leader, and every
other copy of it, once you are satisfied:

```
shred -u /var/lib/coppice/ca.key.superseded-*
```

### Crash-resume

`rotate-ca begin` is the only verb an operator ever re-runs. It resolves
what to do from replicated state plus whatever this disk holds, and every
crash window is covered:

- **Crash during or after staging, before distribution finishes.** Safe
  by construction: the outgoing root is still active and every voter that
  held its key still holds it, so the cluster keeps signing throughout.
  Re-run `begin` on whatever host now leads.
- **Resuming on a leader that holds the staged key** — its `ca-staged.key`
  is the private half of the recorded pending root — it reuses it, and
  voters that already confirmed are not re-pushed.
- **Resuming on a leader that does not hold it** — a leader change
  mid-rotation — `begin` mints a **replacement** pending root and swaps
  it into the bundle in place of the one this leader cannot serve. This
  is safe precisely because a pending root has never signed anything:
  nothing chains to it, so dropping it from the bundle costs nothing, and
  it un-anchors the discarded root at the same time, which is what makes
  any copy of its key on another disk inert rather than a silently
  root-equivalent leftover. `begin` reports this as
  `replaced_pending_root`, with the console message:

  ```
  re-root resumed: the recorded pending root's key was not on this host's disk, so a
  REPLACEMENT pending root was minted and staged here. The superseded one left the bundle
  in the same commit, so it is no longer a trust anchor and any copy of its key is inert.
  ```

- **Crash between the activation commit and the local key swap.**
  Self-healing on every replica, with no operator action: the coverage
  gate guarantees every voter already holds the key that just became
  active, so each daemon promotes its own staged key to `ca.key` on its
  next renewal pass (`[pacing] renewal_reevaluate_interval`, 15s by
  default), startup included. A re-run of `begin` on any leader
  short-circuits straight to reporting done.
- **Membership changes during a staged rotation.** A promotion — including
  the one inside `replace-voter` — keys the candidate with **both** the
  live root's key and the staged key before its joint change commits, so
  raising a new voter while a rotation is parked cannot reopen the
  coverage gap. That is what makes "replace the dead voter, then re-run
  `begin`" a real fix rather than a way to trade one missing voter for
  another.
- **Re-resolve the leader before `complete`.** Coordinators renewing
  during a rotation swap their serving material out from under
  connections that are already open, and in-flight raft dials fail
  (`BadSignature`, transport errors) until they redial against the new
  material — brief and self-healing, but enough to cost the incumbent its
  term. Do not assume the leader you ran `begin` on is still the leader
  by the time you run `complete`:

  ```
  coppice coordinator admin --config <config> --target coord-1:7071 status | grep '^leader'
  ```

## Total voter-disk loss

Losing every voter disk is not a rotation. There is no CA key, no
replicated log, and no cluster — a rotation needs a live cluster to
propose into, and there is none. Distinguish it from rotation by what
survives:

- **A quorum of the recorded voter set survives** (for example, 2 of 3):
  restore those disks, the cluster elects a leader normally, and the
  membership verbs work as usual — `admin replace-voter` and
  `add-learner`/`promote` rebuild the voter set back to full width, per
  [cluster-lifecycle.md](cluster-lifecycle.md). The CA is intact.
- **Fewer than quorum survive** (for example, 1 of 3): there is **no
  leader and there can be none**, so no membership change can be
  committed — not even one that would shrink the recorded voter set down
  to something the survivor could satisfy on its own. Membership changes
  are committed Raft entries; they need a leader, and a leader needs a
  quorum of the *recorded* voter set, which by definition you do not
  have. The shipped answers here are exactly two: restore enough voter
  disks from backup to reach quorum, or deliberately re-init a new
  cluster (below). **There is no forced single-voter membership
  override** — do not go looking for a flag that lets the lone survivor
  declare itself the cluster; none is shipped. The surviving disk still
  holds the CA key and the replicated state, so it is still
  root-equivalent and still a restore source — it just cannot form a
  cluster by itself.

The procedure is a deliberate re-init, and the decision to make first is
restore-versus-reinit:

1. **Look for a restorable backup** of any voter's data directory. If one
   exists, restoring it is a cluster restore, not a re-init, and the CA
   comes back with it — including its key, which means every disk that
   ever held that key is still root-equivalent. If the disk loss was a
   *compromise* rather than an accident, restore and then re-root by the
   procedure above.
2. **If nothing is restorable**, park the fleet. Stop every coordinator
   and agent. Do not leave daemons converging against a cluster id you
   are about to re-create — a parked daemon that finds a new cluster with
   the configured `cluster_id` will join it, and you want that to happen
   deliberately, after step 4.
3. **Choose a new `cluster_id`.** Re-using the old one invites a
   surviving agent or a stale registration to attach to a cluster that
   shares its name and nothing else. A coordinator that later comes back
   on a surviving old volume does not quietly serve it either way: once
   it loses contact with the peers it remembers and observes a formed
   cluster answering to the same `cluster_id` on a different
   `history_id`, it publishes phase `history-superseded` on `/readyz` and
   exits nonzero (see [cluster-lifecycle.md](cluster-lifecycle.md)) — so
   re-using the old `cluster_id` turns every survivor into a
   restart-looping alarm rather than a silent second cluster.
4. **Wipe the data directories** of the coordinators you will form with,
   and `init` exactly one of them — the ordinary formation ceremony from
   [cluster-lifecycle.md](cluster-lifecycle.md). This mints a brand-new
   CA; there is no continuity with the old root and no dual-trust window,
   because there is nothing to be dual with.
5. **Re-enroll the fleet.** Every agent needs its `[tls]` material
   removed so startup enrollment re-runs, and every agent and coordinator
   needs the new `cluster_id` in its config. Collect a fresh operator
   credential from the `init` output.

## Verification

The rotation is finished when the outgoing root is trusted nowhere. Three
independent checks, none of which takes the rotation verb's word for it.

**1. The recorded bundle holds one root, and it is the incoming one.**

```
coppice coordinator admin --config <config> rotate-ca status --json
```

`roots` must be a single entry whose `serial` and `subject_key_id` are
the incoming root's, and `rotation_in_progress` must be `false`.

**2. Every listener refuses the outgoing root.** The strongest available
check is the day-0 operator certificate, which was issued under it. It
must now fail:

```
coppice node --target coord-1:7071 --ca ./ca.crt \
  --cert operator-day0.crt --key operator-day0.key enroll-token list
```

```
Error: probe failed (Unknown): transport error
```

and a certificate issued after the rotation must succeed. Get one from
the local socket — the same day-0 recovery path as ever, and it works
because the CA key is on that host's disk:

```
coppice coordinator admin --config <config> issue-operator-cert \
  --operator-cn post-rotation --out-dir ./post-rotation
coppice node --target coord-1:7071 --ca ./post-rotation/ca.crt \
  --cert ./post-rotation/operator.crt --key ./post-rotation/operator.key enroll-token list
```

Run the refusal check against **each** coordinator's `raft_addr` and
`agent_addr` in turn, not just one: the trust decision is made per
listener, at accept time.

**Allow a few seconds, and poll rather than asserting once.** Retirement
lands on the two planes at different moments. The *authorization* plane
re-reads the replicated CA on every accept, so it refuses the outgoing
root the instant `complete` commits. The *handshake* plane is built from
each node's on-disk anchor file, which only trims to one root when that
node next renews — within about 15 seconds. Until then a node can still
complete a TLS handshake against an outgoing-root peer even though the
cluster has retired it. Verify after every node reports a single anchor
(the `grep -c` below), not before.

**3. Custody accounting still adds up.** `key_holders` and
`pending_key_transfers` must list the same nodes as before the rotation —
a rotation changes which key those disks hold, never how many disks are
root-equivalent. A node that appeared during the rotation is a node that
was keyed during it, and needs explaining. (`staged_key_holders` and
`staged_key_transfers` are gone from the output by now — they only exist
while a rotation is staged, and merged into the lines above at
activation.)

```
key holders   [4739272934893806964, 9156952040390162700, 17882036273301565489]   pending transfers []
```

Finally, confirm every host's on-disk anchor count dropped back to one —
a host still holding two anchors has not renewed since `complete` and is
still trusting a root the cluster has retired:

```
grep -c 'BEGIN CERTIFICATE' /etc/coppice/pki/ca.crt
```

```
1
```

## Limits

What stays honest about this procedure:

- **An issued leaf is valid until it expires.** There is no CRL and no
  OCSP (ADR 0037 §4). `complete` is what invalidates outstanding leaves,
  and it does so by removing their trust anchor — bluntly, for all of
  them at once. Nothing invalidates one leaf.
- **The dual-trust window is one-directional.** It covers client
  certificates the cluster verifies. It does not cover trust-anchor files
  that clients hold, because those are static snapshots — hence steps 2
  and 3, which are manual and which the cluster cannot perform for you.
- **Agents have no CA-change fast path.** Coordinators renew within
  seconds of activation; agents renew on a ~20-day timer and never
  re-enroll on their own. Every agent's anchor file must be pushed. An
  agent that is stranded stays stranded, logging reconnect failures,
  until an operator intervenes.
- **`complete`'s gate is a clock, not an observation.** Nothing
  replicated records which root a given leaf was signed under, and an
  agent that has not dialed in for a week still holds a valid leaf. One
  leaf lifetime is the only bound that is true without observing anything
  — which is why `--force` exists and why using it means verifying
  turnover by hand.
- **A rotation does not shrink the root-equivalent set.** The same disks
  hold the new key. If the compromise was a disk you are keeping,
  re-rooting buys you nothing until that disk is gone; remove the voter
  first, then re-root.
- **The window is not perfectly quiet.** Coordinators renewing under it
  break their own in-flight connections briefly, which is enough to
  trigger a raft election. No data is at risk and it heals itself, but
  do not schedule a re-root alongside anything else that needs stable
  leadership.
- **`begin` is safe; `complete` is not — and now that is precise, not
  just reassuring.** `begin` is safe *because* signing never moves until
  every current voter durably holds the incoming key: a rotation begun
  and never driven to activation costs the cluster nothing but a second
  trust anchor, indefinitely, and can be abandoned or resumed at any
  time. The corollary an operator needs is the one stated at the top of
  the procedure section: the outgoing key keeps *issuing* for the whole
  staged phase, so `begin` alone does not contain a compromise — only
  activation, and ultimately `complete`, do that.

The designed upgrade path, if operational experience shows this too
costly to be the compromise response, is a cluster-held **rotating
intermediate** under a longer-lived root (ADR 0037 §4). That bounds the
authority a voter disk retains to the intermediate's lifetime, so the
common case stops being a re-root at all. It was chosen over threshold
signing, which is out of proportion to this system's needs. The bundle
format already accommodates it: a bundle is a chain, and verification
treats every certificate in it as an anchor.
</content>
