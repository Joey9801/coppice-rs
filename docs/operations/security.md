# Security Model

Coppice has three identity planes, all decided in
[ADR 0022](../decisions/0022-oidc-identity-and-authentication.md) and
[ADR 0023](../decisions/0023-scoped-role-bindings.md):

- **Humans and services** authenticate through one OIDC issuer per cluster.
- **Operators** additionally hold client certificates under the
  control-plane trust root (break-glass and day-0 administration).
- **Nodes** hold per-node certificates under the same root (mutual TLS,
  fencing identity).

## Identity and authentication

A **principal is the IdP's `sub` claim**. Coppice keeps no user database:
no replicated principal records, no provisioning lifecycle. Identity
appears in replicated state only as principal strings on things principals
did — job ownership, actors on committed commands.

The API accepts **JWT access tokens as bearer credentials** and validates
them offline on every replica: signature via cached JWKS, issuer, audience,
expiry with small skew allowance. No IdP call sits on the request path, so
follower reads ([ADR 0007](../decisions/0007-per-endpoint-read-consistency.md))
authenticate as locally as leader writes. Revocation latency therefore
equals access-token lifetime — configure short tokens (≤ 15 minutes) at the
IdP; there is no token denylist to replicate.

Flows by client kind:

- **Web UI** — static single-page client, authorization-code + PKCE, bearer
  tokens; no server-side sessions on coordinators.
- **CLI** — authorization-code + PKCE with loopback redirect; device flow
  for headless hosts; token cache in `~/.config/coppice/` (0600).
- **Services** — OAuth2 client-credentials against the same issuer. Service
  onboarding is an IdP operation; Coppice stores nothing.

Connection parameters (`issuer`, `client_id`, `audience`, client-secret
path) are node config; the **groups-claim name and everything
authorization-shaped are replicated policy**
([ADR 0020](../decisions/0020-node-config-vs-replicated-policy.md)).

### Operator certificates (break-glass and day 0)

The client API listener also accepts mutual TLS with client certificates
carrying the operator profile (`OU=coppice-operators`) under the
control-plane trust root — the **cluster-owned CA** minted at formation
([ADR 0037](../decisions/0037-coordinator-discovery-and-self-converging-membership.md)).
The first operator certificate is signed by the local
`coppice coordinator init` at formation; later ones come from
`coppice-cli pki issue-operator-cert` authorized by an existing operator
credential, and the local-socket `admin issue-operator-cert` verb
(authority: filesystem access on a coordinator host) recovers from
losing them all. They authenticate as principal `cert:<CN>` with
implicit unscoped admin — usable when the IdP is down. Day-0 authority
is *not* this certificate: the initial policy bindings are committed by
`init` itself under local Unix-socket authority (the operator
certificate is minted during that same operation, so it cannot
authenticate the act that creates it); the certificate is for everything
*after* formation. Operator actions are ordinary actor-carrying
commands: break-glass is audited, not exempt.

### IdP outage posture

Already-issued tokens keep validating from cached JWKS until expiry; new
logins and refreshes fail; operator certificates cover administration.
Agents, the scheduler, and running jobs are unaffected — the machine plane
does not touch the IdP.

## Authorization

Decided in [ADR 0023](../decisions/0023-scoped-role-bindings.md):
**subtree-scoped role bindings over the quota-entity tree**, deny by
default, no negative grants.

- **Reads are open** to any authenticated principal in v1: jobs, queues,
  nodes, quota usage, events, logs and artifacts. Debuggability is the
  default; fairness is the quota system's job.
- **Ownership**: jobs record `submitted_by`; a principal may always abort
  and retry its own jobs.
- **Roles** (closed set in v1): `submitter` (submit under entities in
  scope), `operator` (+ manage anyone's jobs in scope; unscoped: drain
  nodes), `admin` (+ configure quota entities in scope; unscoped: policy,
  authorization, cluster version, coordinator membership, enrollment
  administration).
- **Bindings** are replicated policy:
  `(Group(name) | Principal(sub)) → role [@ quota-entity subtree]`.
  Unscoped bindings are cluster-wide; cluster verbs (node operations,
  policy, authorization, cluster version, coordinator membership RPCs,
  minting enrollment tokens) require an unscoped binding.
  Changed via `coppice-cli policy` as a full-replacement
  `UpdateAuthorization` command, which rejects a bindings list with no
  unscoped admin (`AuthorizationLockout`).
- **Enforcement**: the API layer authenticates, evaluates, and rejects
  synchronously; every API-proposed command carries an
  `Actor { principal, groups, operator_cert }` and **apply re-checks the
  decision deterministically** against replicated bindings and ownership,
  rejecting with `PermissionDenied`
  (see the [command catalog](../architecture/command-catalog.md)).
  Revocation races resolve in log order, identically on every replica.

Operational sharp edges, accepted and documented: groups match by exact
string (an IdP-side group rename orphans bindings until policy is updated),
and token group claims ride in commands (filter oversized group lists at
the IdP).

## Audit

Every actor-carrying command in the Raft log — accepted or rejected — is an
ordered, replayable audit record. The job-history store
([ADR 0012](../decisions/0012-data-retention.md)) preserves `submitted_by`
and the aborting actor past the 72-hour eviction of replicated state. Read
auditing is best-effort API access logging, outside replicated state.

## Container execution posture

Decided in [ADR 0011](../decisions/0011-container-security-posture.md):
default-deny.

- No privileged containers, no host mounts, no host network; containers get
  their own network namespace with outbound access.
- Containers run as a non-root UID by default; UID 0 is not requestable.
- Resource limits are always enforced.
- Exceptions (a host mount path, host networking, a privileged capability) are
  admin-allowlisted per queue or node pool, replicated as policy, and audited.
  They are never user-requestable directly.
- Stronger runtime isolation (gVisor/Kata) is out of scope for v1, but the
  agent design must not preclude swapping the container runtime later.

## Node identity and the control-plane PKI

Coordinator↔agent communication uses mutual TLS: a node bootstraps with a
role-scoped enrollment token, submits a CSR, and receives a per-node
certificate. `NodeId` is bound to that certificate identity, which also
underpins the fencing protocol's authenticity assumptions
([ADR 0009](../decisions/0009-fencing-and-reconciliation.md)).
Coordinator↔coordinator (Raft) traffic uses the same mutual-TLS posture
under the same trust root, and coordinators obtain their leaves through
the same enrollment flow.

The trust root is decided
([ADR 0037](../decisions/0037-coordinator-discovery-and-self-converging-membership.md)):
**the cluster owns its CA**, minted at formation. The CA certificate is
replicated; the private key never enters replicated state — it normally
resides on voter disks and may also reside on a promotion candidate past
the key-transfer gate, and every disk that has ever received it is
root-equivalent (compromise response: re-rooting; the runbook is a
required pre-production deliverable). Enrollment transport is explicitly
secure or explicitly insecure — clients verify the server against a
pinned cluster-CA fingerprint/bundle or system roots, never
trust-on-first-use. Enrollment tokens are salted hashes in replicated
policy, revocable by policy write, with the stated limits: token
revocation stops future enrollments but does not recall issued leaves
(renewal refusal plus short leaf lifetimes is v1's certificate
revocation), the long-lived agent launch-template token is the supported
default, and the coordinator token is classified root-equivalent — the
long-lived variant is an explicitly accepted risk, short-lived
per-refresh minting the recommended stronger posture. External PKI
remains a supported substitution behind the same `[tls]` paths.

## Secrets

Secrets should not be stored casually in job definitions. **v1 stores no
secrets**: job environment comes only from the job spec, which is treated as
non-secret, and the platform says so. Secret-manager integration
(reference-only injection at container start) is future work; nothing in v1
may create a place where secret values land in logs, events, snapshots, or UI.
The only credentials Coppice itself issues are X.509 certificates
(coordinator, agent, and operator, under the cluster-owned control-plane
trust root) and enrollment tokens; user and service credentials live in
the IdP
([ADR 0022](../decisions/0022-oidc-identity-and-authentication.md)).
