# 22. Identity and authentication: OIDC principals with certificate break-glass

- **Status:** Accepted
- **Date:** 2026-07-08

## Context

The security model has said "user-facing access should use SSO" since the
first draft, and [ADR 0020](0020-node-config-vs-replicated-policy.md) already
placed the pieces: OIDC *connection* parameters are node config, anything
*authorization-shaped* is replicated policy. The machine plane is settled —
coordinator↔agent mutual TLS with enrollment-token bootstrap
([ADR 0011](0011-container-security-posture.md),
[ADR 0009](0009-fencing-and-reconciliation.md)). What was never decided:
what a principal *is*, what artifact the API actually validates, how each
kind of client (web UI, CLI, headless service) obtains one, and what happens
when the identity provider is down.

Three forces shape the answer:

- **The API runs on every replica** and serves follower reads
  ([ADR 0007](0007-per-endpoint-read-consistency.md)). Authentication must be
  decidable locally on any replica — a design that routes authn through the
  leader, or through the IdP on the request path, silently downgrades the
  availability of every read.
- **The control plane is engineered to survive coordinator failure**
  ([ADR 0016](0016-coordinator-rebuild-learner-join.md)). Coupling every
  request to IdP liveness would import a weaker availability class into a
  stronger system — and the IdP is the one dependency Coppice does not
  operate.
- **v1 stores no secrets.** A local user database with credentials would
  instantly be the largest secret store in the platform, contradicting the
  posture in [operations/security.md](../operations/security.md).

The [overview](../overview.md) also promises submissions from "users and
services", so non-human clients are v1 scope, not an extension.

## Decision

### One OIDC issuer per cluster; principals are its subjects

A cluster trusts exactly one OIDC issuer, configured per node (issuer URL,
client id, audience, client-secret path — the `[sso]` block, per ADR 0020).
A **principal is the `sub` claim**: an opaque string owned by the IdP.
Coppice keeps **no replicated principal records** — no user table, no
provisioning lifecycle, no deactivation state machine. The IdP is the only
user database; what Coppice stores about identity is confined to principal
strings on the things principals did (job ownership, the command log).

Group membership arrives in a token claim. The claim **name** is replicated
policy (`groups_claim`, default `"groups"`), not node config: two
coordinators disagreeing on which claim carries groups would enforce
different admin lists — the exact divergence ADR 0020's litmus test exists
to exclude. The connection identity (`issuer`, `client_id`, `audience`)
stays in the file: a mismatch there makes one node reject tokens loudly,
which is misconfiguration, not silent privilege divergence.

Email and display-name claims are presentation only. They are never keys,
never matched by bindings, never stored in replicated state.

### Bearer JWTs, validated offline on every replica

Clients present an OAuth2 **JWT access token** as a bearer credential. Every
replica validates it locally: signature against the issuer's JWKS (cached,
refreshed in the background and on unknown key ids), `iss`, `aud` equal to
the cluster's configured audience, `exp`/`nbf` with a small clock-skew
allowance. **No token introspection, no IdP call on the request path** —
follower reads authenticate exactly as locally as leader writes.

The consequence is embraced rather than hidden: revocation latency equals
access-token lifetime. Deployments are directed to configure short access
tokens (≤ 15 minutes) with refresh-token renewal; Coppice deliberately has
no token denylist to replicate.

The IdP must therefore mint JWT access tokens carrying a configurable
audience. Mainstream IdPs (Keycloak, Dex, Okta, Entra, Auth0) all can;
an opaque-token-only setup needs a translating IdP in front. Accepted.

### Flows per client kind

- **Web UI** — a static single-page client of the public API
  ([components.md](../architecture/components.md)): authorization-code +
  PKCE, bearer header on API calls. **No server-side sessions**: there is
  nothing to replicate, nothing sticky, and any replica can serve any
  request — session state on coordinators would be the first thing to break
  the "API runs everywhere" property.
- **CLI** — authorization-code + PKCE with a loopback redirect where a
  browser exists; the device-authorization grant as the headless fallback.
  Tokens cache under `~/.config/coppice/` (mode 0600) with refresh-token
  renewal.
- **Services** (CI, cron submitters) — the **client-credentials grant**
  against the same issuer. The service's principal is whatever `sub` the IdP
  mints for that client; validation is the identical JWKS path. Zero
  Coppice-side state: onboarding a service is an IdP operation, deliberately,
  so that v1 stores no service credentials either.

### Break-glass and day 0: operator certificates under the control-plane trust root

The client API listener additionally accepts **mutual TLS with operator
client certificates** chaining to the control-plane trust root — the same
root that anchors node and coordinator certificates — exercised through
`coppice-cli pki issue-operator-cert --cn <name>`. Whether that root is
cluster-held or fronted by external PKI is deliberately *not* decided here:
that is OD-14/OD-15's open question
([roadmap/open-decisions.md](../roadmap/open-decisions.md)); this ADR fixes
only the semantics of an operator-profile leaf, whoever signs it. A
certificate carrying the operator profile (`OU=coppice-operators`)
authenticates as principal `cert:<CN>` with full administrative authority;
it is the one grant not represented in replicated bindings
([ADR 0023](0023-scoped-role-bindings.md)).

This solves two problems with one mechanism, and adds no new secret type:

- **Break-glass.** When the IdP is down, unreachable, or misconfigured,
  operators can still drain nodes, fix policy, and administer the cluster.
  The credential is X.509 under a CA the operators already hold for node
  enrollment — no standing password, no rotation regime beyond the PKI's own.
- **Day 0.** `coppice-cli cluster init --policy` runs under an operator
  certificate *before any SSO binding exists*, so initial policy (including
  the first role bindings) has a clean authentication story with no
  chicken-and-egg and no bootstrap-only config section (ADR 0020's rule).

Operator-cert actions are ordinary actor-carrying commands in the log —
break-glass is fully audited, not exempt.

### IdP outage posture

Cached JWKS keeps validating already-issued tokens until they expire; new
logins and refreshes fail; operator certificates cover administrative verbs
for the duration. Agents, the scheduler, and running jobs are untouched —
the machine plane's mTLS is independent of the IdP by construction. This is
documented as the expected degraded mode, not an incident in itself.

### Clarification: Raft peer traffic

Coordinator↔coordinator (Raft) connections use the same mutual-TLS posture
under the same trust root as coordinator↔agent traffic. This was previously
unstated; [operations/security.md](../operations/security.md) now says it.
Where those coordinator certificates come from in a templated deployment
(and the reload seam for short-lived leaves) remains OD-14.

## Consequences

- Authentication is replica-local and IdP-free on the request path: adding
  SSO changes the availability class of no read and no write.
- The platform stores no passwords and no user records. The only credentials
  Coppice itself issues remain X.509 certificates (node and operator), all
  under one CA.
- Revocation latency is bounded by access-token lifetime; short tokens are
  an IdP-side deployment requirement, documented in operations. A stolen
  token is good for minutes, not until someone edits a denylist.
- Requires an IdP that issues JWT access tokens with a configurable
  audience. Single issuer per cluster; federation or multi-IdP would be a
  new ADR.
- Groups are strings transcribed from tokens; there is no directory sync. An
  IdP-side group rename silently changes who matches bindings — an
  operational sharp edge recorded in
  [operations/security.md](../operations/security.md).
- The web UI's statelessness commits the API to serving it as a pure OAuth2
  resource server; any future server-rendered UI must bring its own session
  story without weakening this one.
