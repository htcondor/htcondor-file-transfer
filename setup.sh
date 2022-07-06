#!/bin/bash
# vim: tabstop=4

# Install and configure an HTCondor file transfer host. The caller must
# supply the hostname of the central manager to use and a unique name to
# identify this host.

set -eu

INSTALL_DIR="$PWD/condor"
SLOT_USER="$(whoami)"
THIS_DIR="$(cd -- "$(dirname -- "$0")" && pwd)"

usage() {
    cat 1>&2 <<EOF
Usage: $0 ...

Required arguments:
    -c <central manager's hostname>
    -n <unique name for this EP>

Optional arguments:
    -p <directory to install into> [default: $INSTALL_DIR]
    -u <slot user> [default: $SLOT_USER]
EOF
    exit 1
}

info() {
    printf '%s\n' "$*"
}

warn() {
    printf '%s\n' "WARNING: $*" 1>&2
}

fail_no_exit() {
    echo "ERROR: $*" 1>&2
}

fail() {
    fail_no_exit "$@"
    if [ -n "${LOGFILE:-}" ]; then
        echo "Check $LOGFILE for more details" 1>&2
    fi
    exit 1
}

inplace_sed() {
    case "$(uname -s)" in
        Darwin)
            sed -i "" "$@"
            ;;
        Linux)
            sed -i "$@"
            ;;
        *)
            fail "This host's operating system is not supported."
            ;;
    esac
}

#---------------------------------------------------------------------------


while getopts "c:n:p:u:" OPTION; do
    case "$OPTION" in
        c)
            CENTRAL_MANAGER="$OPTARG"
            ;;
        n)
            MACHINE_NAME="$OPTARG"
            ;;
        p)
            INSTALL_DIR="$OPTARG"
            ;;
        u)
            SLOT_USER="$OPTARG"
            ;;
        \?)
            usage
            ;;
    esac
done

if [ -z "${CENTRAL_MANAGER:-}" ] || [ -z "${MACHINE_NAME:-}" ]; then
    fail_no_exit "Missing requirement arguments."
    usage
fi

if ! host "$CENTRAL_MANAGER" 1>/dev/null 2>&1; then
    fail "'$CENTRAL_MANAGER' is not a valid hostname."
fi

if [[ ! "$MACHINE_NAME" =~ ^[A-Za-z0-9_]+$ ]]; then
    fail "The name for this EP may contain only alphanumeric characters and underscores."
fi

if ! id -u "$SLOT_USER" 1>/dev/null 2>&1; then
    fail "'$SLOT_USER' is not a valid user."
fi

if [ "$SLOT_USER" = "root" ]; then
    fail "Transfer jobs cannot run as 'root'."
fi

if [ -e "$INSTALL_DIR" ]; then
    fail "Cannot install into '$INSTALL_DIR' because it already exists."
fi

#---------------------------------------------------------------------------

echo "Creating '$(dirname -- "$INSTALL_DIR")'..."
mkdir -p "$(dirname -- "$INSTALL_DIR")"

echo "Downloading and expanding HTCondor tarball..."
curl -fsSL https://get.htcondor.org | /bin/bash -s -- --download
tar -x -f condor.tar.gz
mv condor-*stripped "$INSTALL_DIR"

echo "Configuring HTCondor..."
(cd "$INSTALL_DIR" && ./bin/make-personal-from-tarball)

# Remove the personal pool that was just configured.
# The transfer host configuration is intended to stand on its own.
rm -f "$INSTALL_DIR"/local/config.d/*

# Update the configuration template using a temporary file in order to
# reduce the chance of leaving behind a broken HTCondor configuration should
# this script fail in some unexpected way.
tmp_config="$(mktemp "$THIS_DIR"/10-xfer-host.XXXXX)"
cp "$THIS_DIR"/templates/10-xfer-host "$tmp_config"

inplace_sed -e "s/^CONDOR_HOST.*/CONDOR_HOST = $CENTRAL_MANAGER/" "$tmp_config"
inplace_sed -e "s/^SlotUser.*/SlotUser = $SLOT_USER/" "$tmp_config"
inplace_sed -e "s/^UniqueName.*/UniqueName = $MACHINE_NAME/" "$tmp_config"

mv "$tmp_config" "$INSTALL_DIR"/local/config.d/10-xfer-host

fail "Not implemented: Requesting an IDTOKEN."
