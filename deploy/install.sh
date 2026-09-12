#!/usr/bin/env bash
#
# Install the Coppice binary and its systemd units on a host, from an unpacked
# release tarball. Run as root:
#
#   mkdir -p /opt/coppice-release
#   tar xzf coppice-<version>-<target>.tar.gz -C /opt/coppice-release
#   /opt/coppice-release/deploy/install.sh --role coordinator
#
# Deliberately does NOT write configuration and does NOT enable or start
# anything: which role a host runs, what its TOML says, and when the unit comes
# up are cloud-init's and the operator's decisions, not the package's. This
# script is idempotent — re-running it over an existing install replaces the
# binary and the units and leaves everything else alone.
set -euo pipefail

usage() {
	cat <<'EOF'
usage: install.sh --role coordinator|agent

  --role   which daemon this host runs. Both units are installed either way;
           the role only decides which example config to point the operator
           at and which unit the closing message says to enable.
EOF
}

role=""
while [ $# -gt 0 ]; do
	case "$1" in
	--role)
		[ $# -ge 2 ] || {
			echo "install.sh: --role needs a value" >&2
			exit 2
		}
		role="$2"
		shift 2
		;;
	--role=*)
		role="${1#--role=}"
		shift
		;;
	-h | --help)
		usage
		exit 0
		;;
	*)
		echo "install.sh: unknown argument: $1" >&2
		usage >&2
		exit 2
		;;
	esac
done

case "$role" in
coordinator | agent) ;;
"")
	echo "install.sh: --role is required" >&2
	usage >&2
	exit 2
	;;
*)
	echo "install.sh: unknown role: $role (expected coordinator or agent)" >&2
	exit 2
	;;
esac

if [ "$(id -u)" -ne 0 ]; then
	echo "install.sh: must run as root" >&2
	exit 1
fi

# The tarball root is this script's parent's parent: deploy/install.sh sits one
# level below the `coppice` binary.
root="$(cd -- "$(dirname -- "$0")/.." && pwd)"

for required in "$root/coppice" "$root/deploy/systemd/coppice-coordinator.service" \
	"$root/deploy/systemd/coppice-agent.service"; do
	[ -f "$required" ] || {
		echo "install.sh: missing $required — is this an unpacked release tarball?" >&2
		exit 1
	}
done

# Both service users, on every host: the units for both roles are installed, so
# a host that is later re-purposed needs no second pass. `--system` accounts,
# no login shell, no home directory to own.
for user in coppice coppice-agent; do
	if ! getent group "$user" >/dev/null; then
		groupadd --system "$user"
	fi
	if ! getent passwd "$user" >/dev/null; then
		useradd --system --gid "$user" --no-create-home \
			--home-dir /nonexistent --shell /usr/sbin/nologin "$user"
	fi
done

install -m 0755 "$root/coppice" /usr/local/bin/coppice

install -d -m 0755 /etc/systemd/system
install -m 0644 "$root/deploy/systemd/coppice-coordinator.service" /etc/systemd/system/
install -m 0644 "$root/deploy/systemd/coppice-agent.service" /etc/systemd/system/

# /etc/coppice holds the config and the enrollment token, both written by the
# operator or cloud-init, never by the daemon: it stays root-owned 0755 and
# the daemon writes nothing under it. Cluster-managed TLS material lives
# under the role's own state directory instead (/var/lib/coppice/pki or
# /var/lib/coppice-agent/pki), created by StateDirectory=.
install -d -m 0755 -o root -g root /etc/coppice

systemctl daemon-reload

echo "installed coppice ($(/usr/local/bin/coppice --version)) for role '$role'"
echo "next: write /etc/coppice/${role}.toml (see deploy/examples/), then"
echo "      systemctl enable --now coppice-${role}"
