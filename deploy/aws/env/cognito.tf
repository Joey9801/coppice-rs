# Cognito is the environment's OIDC issuer (ADRs 0022/0023): the coordinator is
# a resource server that validates tokens offline against the pool's JWKS and
# never authenticates *to* Cognito, so there is no client secret anywhere here.
# Every role binding is replicated policy, not a Cognito group.

resource "aws_cognito_user_pool" "this" {
  name = local.name_prefix

  # Email is the username, and it is auto-verified so the seeded user is
  # usable immediately without a confirmation code being mailed anywhere.
  username_attributes      = ["email"]
  auto_verified_attributes = ["email"]

  admin_create_user_config {
    # No self sign-up: the only account in this pool is the one Terraform
    # seeds, and the pool is reachable from the public internet.
    allow_admin_create_user_only = true
  }

  password_policy {
    minimum_length                   = 12
    require_uppercase                = true
    require_lowercase                = true
    require_numbers                  = true
    require_symbols                  = true
    temporary_password_validity_days = 1
  }

  # MFA off: the environment is disposable, its one user is a generated
  # password held in SSM, and CI has to authenticate non-interactively.
  mfa_configuration = "OFF"

  # The environment is destroyed by automation; deletion protection would make
  # `down.sh` fail.
  deletion_protection = "INACTIVE"
}

# The hosted-UI domain prefix is global across all AWS accounts, so it carries a
# random suffix rather than risking a collision with someone else's `coppice-*`.
resource "random_id" "cognito_domain" {
  byte_length = 3
}

resource "aws_cognito_user_pool_domain" "this" {
  domain       = "${local.name_prefix}-${random_id.cognito_domain.hex}"
  user_pool_id = aws_cognito_user_pool.this.id
}

resource "aws_cognito_user_pool_client" "web" {
  name         = "${local.name_prefix}-web"
  user_pool_id = aws_cognito_user_pool.this.id

  # A public client: the web UI runs authorization-code + PKCE in the browser,
  # where a secret could not be kept anyway.
  generate_secret = false

  # USER_PASSWORD_AUTH is what lets CI obtain a token in one API call with the
  # seeded user's password; SRP and refresh are what the browser flow uses.
  explicit_auth_flows = [
    "ALLOW_USER_PASSWORD_AUTH",
    "ALLOW_USER_SRP_AUTH",
    "ALLOW_REFRESH_TOKEN_AUTH",
  ]

  allowed_oauth_flows_user_pool_client = true
  allowed_oauth_flows                  = ["code"]
  allowed_oauth_scopes                 = ["openid", "email", "profile"]
  supported_identity_providers         = ["COGNITO"]

  # The web UI's own callback path; the logout target is the app root.
  callback_urls = ["https://${local.fqdn}/auth/callback"]
  logout_urls   = ["https://${local.fqdn}/"]

  # Minutes. Short access and id tokens keep the demo honest about refresh
  # working; the refresh token's day is plenty for an environment that rarely
  # outlives an afternoon.
  access_token_validity  = 15
  id_token_validity      = 15
  refresh_token_validity = 1

  token_validity_units {
    access_token  = "minutes"
    id_token      = "minutes"
    refresh_token = "days"
  }

  # A failed login must not reveal whether the address exists — the pool is
  # public and holds one predictable username.
  prevent_user_existence_errors = "ENABLED"
}

# The one seeded user. `SUPPRESS` because `demo_user_email` is a name in a
# domain with no mailbox behind it; the password is permanent (no
# force-change-at-first-login state to clear) and readable from SSM.
resource "aws_cognito_user" "demo" {
  user_pool_id = aws_cognito_user_pool.this.id
  username     = var.demo_user_email

  attributes = {
    email          = var.demo_user_email
    email_verified = true
  }

  password       = random_password.demo_user.result
  message_action = "SUPPRESS"
}

# Membership of this group is what the cluster's day-0 authorization binds to
# (deploy/examples/policy.toml, `[[authorization.binding]] group =
# "coppice-admins"`): Cognito lists a user's groups under `cognito:groups` in
# the ID token, and formation seeds a binding for the group rather than for the
# demo user's `sub`, so adding an operator later is a group membership, not a
# policy edit.
resource "aws_cognito_user_group" "admins" {
  name         = "coppice-admins"
  user_pool_id = aws_cognito_user_pool.this.id
  description  = "Unscoped admins of the coppice ${var.env_name} cluster"
}

resource "aws_cognito_user_in_group" "demo_admin" {
  user_pool_id = aws_cognito_user_pool.this.id
  group_name   = aws_cognito_user_group.admins.name
  username     = aws_cognito_user.demo.username
}
