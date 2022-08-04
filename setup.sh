#!/bin/bash
# vim: tabstop=4

# Install and configure an HTCondor file transfer host. The caller must
# supply the hostname of the central manager to use and a unique name for
# identifying this host.

set -eu

INSTALL_DIR="${PWD}/condor"
THIS_DIR="$(cd -- "$(dirname -- "$0")" && pwd)"

usage() {
    cat 1>&2 <<EOF
Usage: $0 ...

Required arguments:
    -c <central manager's hostname>
    -n <unique name for this EP>
    -u <name of the transfer jobs' owner>

Optional arguments:
    -i <token identity> [default: $(whoami)@<central manager>]
    -p <directory to install into> [default: ${INSTALL_DIR}]
EOF
    exit 1
}

#---------------------------------------------------------------------------

info() {
    printf '%s\n' "$*"
}

warn() {
    printf '%s\n' "WARNING: $*" 1>&2
}

fail_no_exit() {
    printf '%s\n' "ERROR: $*" 1>&2
}

fail_with_usage() {
    fail_no_exit "$@"
    echo
    usage
}

fail() {
    fail_no_exit "$@"
    exit 1
}

inplace_sed() {
    sed -i "$@"
}

#---------------------------------------------------------------------------

while getopts "c:n:u:i:p:" OPTION; do
    case "${OPTION}" in
        c)
            CENTRAL_MANAGER="${OPTARG}"
            ;;
        n)
            UNIQUE_NAME="${OPTARG}"
            ;;
        u)
            JOB_OWNER="${OPTARG}"
            ;;
        i)
            TOKEN_IDENTITY="${OPTARG}"
            ;;
        p)
            INSTALL_DIR="${OPTARG}"
            ;;
        \?)
            usage
            ;;
    esac
done

if [ -z "${CENTRAL_MANAGER:-}" ]; then
    fail_with_usage "Missing required argument (-c)."
fi

if ! host "${CENTRAL_MANAGER}" 1>/dev/null 2>&1; then
    fail "'${CENTRAL_MANAGER}' is not a valid hostname."
fi

if [ -z "${UNIQUE_NAME:-}" ]; then
    fail_with_usage "Missing required argument (-n)."
fi

if [[ ! "${UNIQUE_NAME}" =~ ^[A-Za-z0-9_]+$ ]]; then
    fail "The name for this EP may contain only alphanumeric characters and underscores."
fi

if [ -z "${JOB_OWNER:-}" ]; then
    fail_with_usage "Missing required argument (-u)."
fi

if [ -z "${TOKEN_IDENTITY:-}" ]; then
    TOKEN_IDENTITY="$(whoami)@${CENTRAL_MANAGER}"
fi

if [ -e "${INSTALL_DIR}" ]; then
    fail "Cannot install into '${INSTALL_DIR}' because it already exists."
fi

#---------------------------------------------------------------------------

info "Creating '$(dirname -- "${INSTALL_DIR}")'..."
mkdir -p "$(dirname -- "${INSTALL_DIR}")"

info "Downloading and expanding HTCondor tarball..."
curl -fsSL https://get.htcondor.org | /bin/bash -s -- --download
tar -x -f condor.tar.gz
mv condor-*stripped "${INSTALL_DIR}"

info "Configuring HTCondor..."
(cd "${INSTALL_DIR}" && ./bin/make-personal-from-tarball)

# Remove the personal pool that was just configured.
# The transfer host configuration is intended to stand on its own.
rm -f "${INSTALL_DIR}"/local/config.d/*

# Update the configuration template using a temporary file in order to
# reduce the chance of leaving behind a broken HTCondor configuration should
# this script fail in some unexpected way.
TMP_CONFIG="$(mktemp "${THIS_DIR}"/10-xfer-host.XXXXXX)"
cp "${THIS_DIR}"/templates/10-xfer-host "${TMP_CONFIG}"

inplace_sed -e "s/__CONDOR_HOST__/${CENTRAL_MANAGER}/" "${TMP_CONFIG}"
inplace_sed -e "s/__JobOwner__/${JOB_OWNER}/" "${TMP_CONFIG}"
inplace_sed -e "s/__UniqueName__/${UNIQUE_NAME}/" "${TMP_CONFIG}"

mv "${TMP_CONFIG}" "${INSTALL_DIR}"/local/config.d/10-xfer-host

#---------------------------------------------------------------------------

info "Checking connectivity to '${CENTRAL_MANAGER}'..."

if ! CONDOR_CONFIG="${INSTALL_DIR}/etc/condor_config" "${INSTALL_DIR}"/bin/condor_ping \
        -pool "${CENTRAL_MANAGER}" \
        -type collector \
        ADVERTISE_MASTER ADVERTISE_STARTD
then
    mkdir -p ~/.condor/tokens.d
    chmod 700 ~/.condor/tokens.d

    TOKEN_FILE=~/.condor/tokens.d/advertise_"${UNIQUE_NAME}"_"$(date +%Y%m%d_%H%M%S)"

    info "Requesting token from '${CENTRAL_MANAGER}'..."

    CONDOR_CONFIG="${INSTALL_DIR}/etc/condor_config" "${INSTALL_DIR}"/bin/condor_token_request \
        -authz ADVERTISE_MASTER \
        -authz ADVERTISE_STARTD \
        -identity "${TOKEN_IDENTITY}" \
        -token "${TOKEN_FILE}"
fi

#---------------------------------------------------------------------------

cat <<EOF

Setup completed.

Run the following to start your file transfer EP:

    (source "${INSTALL_DIR}"/condor.sh && condor_master)

Run the following to shut down your file transfer EP:

    (source "${INSTALL_DIR}"/condor.sh && condor_off -master)

EOF
