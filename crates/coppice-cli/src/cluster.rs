//! `coppice cluster`: operator-facing cluster-lifecycle verbs.
//!
//! Today the only verb is `init` — the one deliberate act at cluster birth
//! (ADR 0037 §3): it forms a brand-new single-voter cluster against one parked
//! coordinator and seeds the optional bootstrap policy in the same call. The
//! subcommand shape leaves room for future policy verbs (`cluster policy …`)
//! without another top-level command.
//!
//! Unlike `coppice coordinator admin`, `cluster init` takes **no config file**:
//! an operator runs it from a workstation or a provisioning step, away from any
//! daemon's `coordinator.toml`, so the mTLS material is passed explicitly
//! (`--ca`/`--cert`/`--key`, an operator-profile leaf) and the target is named
//! directly (`--target`). The cluster identity is discovered from the target
//! itself, so there is nothing else to configure.
//!
//! The formation token is durable *outside* this process so a rerun presents
//! the same token rather than manufacturing a conflict (ADR 0037 §3):
//!
//! - `--formation-token <string>`: use a value already stable in the
//!   provisioning system (a stack id, an SSM value) verbatim;
//! - `--formation-token-file <path>`: created exclusively on first use with a
//!   freshly minted token, and re-read on later runs;
//! - neither: mint a token, print it prominently, and re-supply it on retry.
//!
//! The two token flags are mutually exclusive (enforced by clap).

use std::io::Write as _;
use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};

/// `coppice cluster` argument group.
#[derive(Debug, clap::Args)]
pub struct ClusterArgs {
    #[command(subcommand)]
    pub command: ClusterCommand,
}

#[derive(Debug, clap::Subcommand)]
pub enum ClusterCommand {
    /// Form a brand-new cluster (ADR 0037 §3): the one deliberate act at
    /// cluster birth. Run exactly once per cluster lifetime, against any one
    /// parked coordinator, with an operator-profile certificate. Idempotent
    /// under the same formation token — safe to retry from automation.
    Init(InitArgs),
}

#[derive(Debug, clap::Args)]
#[command(group(
    // The two token sources are mutually exclusive; supplying neither mints one
    // interactively (ADR 0037 §3).
    clap::ArgGroup::new("formation-token-source")
        .args(["formation_token", "formation_token_file"])
))]
pub struct InitArgs {
    /// The `host:port` of a parked coordinator's raft/admin listener to form.
    #[arg(long)]
    pub target: String,

    /// Optional bootstrap-policy TOML applied idempotently as part of
    /// formation (priority-multiplier table and quota entities).
    #[arg(long)]
    pub policy: Option<PathBuf>,

    /// PEM trust root (the cluster CA). Required: `cluster init` runs away from
    /// any daemon config, so there is no `[tls]` fallback.
    #[arg(long)]
    pub ca: PathBuf,

    /// PEM operator-profile leaf certificate (`OU=coppice-operator`). Machine
    /// certificates are refused this verb server-side (ADR 0037 §6).
    #[arg(long)]
    pub cert: PathBuf,

    /// PEM private key for `--cert`.
    #[arg(long)]
    pub key: PathBuf,

    /// A durable formation token stable in the provisioning system (a stack id,
    /// an SSM value). Used verbatim. Mutually exclusive with
    /// `--formation-token-file`.
    #[arg(long)]
    pub formation_token: Option<String>,

    /// A path holding the formation token: created exclusively on first use
    /// with a freshly minted token, re-read on later runs. Mutually exclusive
    /// with `--formation-token`.
    #[arg(long)]
    pub formation_token_file: Option<PathBuf>,
}

/// Run the selected `coppice cluster` verb.
pub async fn run(args: ClusterArgs) -> Result<()> {
    match args.command {
        ClusterCommand::Init(args) => init(args).await,
    }
}

/// How a formation token was obtained, for the operator-facing narration.
enum TokenSource {
    /// Passed verbatim on `--formation-token`.
    Explicit,
    /// Read back from an existing `--formation-token-file`.
    FileExisting,
    /// Freshly minted into a new `--formation-token-file`.
    FileMinted,
    /// Minted interactively (neither flag given); must be re-supplied on retry.
    InteractiveMinted,
}

/// Resolve the formation token per ADR 0037 §3, returning the token and how it
/// was obtained.
fn resolve_token(args: &InitArgs) -> Result<(String, TokenSource)> {
    if let Some(token) = &args.formation_token {
        if token.trim().is_empty() {
            bail!("--formation-token must not be empty");
        }
        return Ok((token.clone(), TokenSource::Explicit));
    }
    if let Some(path) = &args.formation_token_file {
        return resolve_token_file(path);
    }
    // Neither flag: mint one. The caller prints it prominently so a retry can
    // re-supply it.
    Ok((mint_token(), TokenSource::InteractiveMinted))
}

/// Create `path` exclusively (`O_CREAT|O_EXCL`) with a freshly minted token on
/// first use, or re-read it when the file already exists (ADR 0037 §3). The
/// exclusive create is what makes a retried provisioning step present the same
/// token rather than a fresh one.
fn resolve_token_file(path: &Path) -> Result<(String, TokenSource)> {
    match std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
    {
        Ok(mut file) => {
            let token = mint_token();
            writeln!(file, "{token}")
                .with_context(|| format!("writing formation token file {}", path.display()))?;
            Ok((token, TokenSource::FileMinted))
        }
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
            let raw = std::fs::read_to_string(path)
                .with_context(|| format!("reading formation token file {}", path.display()))?;
            let token = raw.trim().to_string();
            if token.is_empty() {
                bail!(
                    "formation token file {} exists but is empty; delete it to mint a fresh \
                     token, or write the recorded token into it",
                    path.display()
                );
            }
            Ok((token, TokenSource::FileExisting))
        }
        Err(e) => {
            Err(e).with_context(|| format!("opening formation token file {}", path.display()))
        }
    }
}

/// Mint a fresh opaque formation token — a UUID v4 string (ADR 0037 §3).
fn mint_token() -> String {
    uuid::Uuid::new_v4().to_string()
}

/// Print the minted token prominently to stderr, with instructions to
/// re-supply it on retry. Interactive-mint only: an explicit or file-backed
/// token is already durable outside this process.
fn announce_minted_token(token: &str, source: &TokenSource) {
    match source {
        TokenSource::InteractiveMinted => {
            eprintln!(
                "\n\
                 ============================================================\n\
                 FORMATION TOKEN (save this):\n\
                 \n\
                 \x20   {token}\n\
                 \n\
                 No --formation-token / --formation-token-file was given, so\n\
                 this token was minted for you. If `cluster init` fails or its\n\
                 outcome is ambiguous, re-run with:\n\
                 \n\
                 \x20   --formation-token {token}\n\
                 \n\
                 Re-running with a DIFFERENT token is refused, naming the one\n\
                 already recorded (ADR 0037 §3).\n\
                 ============================================================\n"
            );
        }
        TokenSource::FileMinted => {
            eprintln!("minted a fresh formation token into the token file; later runs re-read it");
        }
        TokenSource::Explicit | TokenSource::FileExisting => {}
    }
}

async fn init(args: InitArgs) -> Result<()> {
    let (token, source) = resolve_token(&args)?;
    // Print the minted token BEFORE dialing, so the operator has it even if the
    // call then fails or times out with an ambiguous outcome.
    announce_minted_token(&token, &source);

    let ca = std::fs::read(&args.ca)
        .with_context(|| format!("reading CA certificate {}", args.ca.display()))?;
    let cert = std::fs::read(&args.cert)
        .with_context(|| format!("reading operator certificate {}", args.cert.display()))?;
    let key = std::fs::read(&args.key)
        .with_context(|| format!("reading operator key {}", args.key.display()))?;
    let policy = args
        .policy
        .as_ref()
        .map(|p| std::fs::read(p).with_context(|| format!("reading policy file {}", p.display())))
        .transpose()?;

    let outcome =
        coppice_coordinator::admin::cluster_init(&args.target, &ca, &cert, &key, &token, policy)
            .await?;

    if outcome.already_initialized {
        println!(
            "cluster already formed (node {}); formation is idempotent — the recorded token \
             matched, nothing to do",
            outcome.node_id
        );
    } else {
        println!(
            "cluster formed: node {} is the founding voter. Every parked coordinator that can \
             discover it will now join automatically.",
            outcome.node_id
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args_with(token: Option<&str>, file: Option<PathBuf>) -> InitArgs {
        InitArgs {
            target: "coord-1:7071".to_string(),
            policy: None,
            ca: PathBuf::from("ca.crt"),
            cert: PathBuf::from("op.crt"),
            key: PathBuf::from("op.key"),
            formation_token: token.map(str::to_string),
            formation_token_file: file,
        }
    }

    #[test]
    fn explicit_token_is_used_verbatim() {
        let (token, _) = resolve_token(&args_with(Some("stack-42"), None)).unwrap();
        assert_eq!(token, "stack-42");
    }

    #[test]
    fn empty_explicit_token_is_rejected() {
        assert!(resolve_token(&args_with(Some("   "), None)).is_err());
    }

    #[test]
    fn interactive_mint_yields_a_fresh_token_each_call() {
        let (a, _) = resolve_token(&args_with(None, None)).unwrap();
        let (b, _) = resolve_token(&args_with(None, None)).unwrap();
        assert_ne!(a, b, "each interactive mint is a distinct token");
        assert!(matches!(
            resolve_token(&args_with(None, None)).unwrap().1,
            TokenSource::InteractiveMinted
        ));
    }

    #[test]
    fn token_file_is_created_exclusively_then_re_read() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("token");

        // First use: exclusive create + freshly minted token.
        let (first, source) = resolve_token_file(&path).expect("first use creates");
        assert!(matches!(source, TokenSource::FileMinted));
        assert!(!first.is_empty());
        assert!(path.exists(), "the token file now exists");

        // Second use: the SAME token is re-read (a retry presents it, not a new
        // token that would conflict).
        let (second, source) = resolve_token_file(&path).expect("second use re-reads");
        assert!(matches!(source, TokenSource::FileExisting));
        assert_eq!(first, second, "the retry presents the recorded token");
    }

    #[test]
    fn an_empty_token_file_is_rejected() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("empty");
        std::fs::write(&path, "   \n").expect("write empty");
        assert!(resolve_token_file(&path).is_err());
    }
}
