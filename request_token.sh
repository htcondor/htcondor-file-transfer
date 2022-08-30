#!/bin/bash
# shellcheck disable=SC1091
# vim: tabstop=4

# Interactively request an IDTOKEN for an HTCondor file transfer host.

set -eu

THIS_DIR="$(cd -- "$(dirname -- "$0")" && pwd)"

TOKEN_FILE="${THIS_DIR}/secrets/token"

source "${THIS_DIR}"/.env.release

usage() {
    cat 1>&2 <<EOF
Usage: $0 ...

Required arguments:
    -c <central manager's hostname>

Optional arguments:
    -i <token identity> [default: $(whoami)@<central manager>]
    -p <file to save token into> [default: ${TOKEN_FILE}]
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

#---------------------------------------------------------------------------

while getopts "c:i:p:" OPTION; do
    case "${OPTION}" in
        c)
            CENTRAL_MANAGER="${OPTARG}"
            ;;
        i)
            TOKEN_IDENTITY="${OPTARG}"
            ;;
        p)
            TOKEN_FILE="${OPTARG}"
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

if [ -z "${TOKEN_IDENTITY:-}" ]; then
    TOKEN_IDENTITY="$(whoami)@${CENTRAL_MANAGER}"
fi

if [ -e "${TOKEN_FILE}" ]; then
    fail "Cannot save token to '${TOKEN_FILE}' because it already exists."
fi

TOKEN_DIR="$(dirname -- "${TOKEN_FILE}")"

#---------------------------------------------------------------------------

info "Creating '${TOKEN_DIR}'..."
mkdir -p "${TOKEN_DIR}"
chmod 0700 "${TOKEN_DIR}"

info "Requesting token from '${CENTRAL_MANAGER}'..."

# Note: We set _condor_SEC_TOKEN_DIRECTORY in the environment so that
# `condor_token_request -token` writes the token to the desired location.

docker run -t --rm \
    -e CENTRAL_MANAGER="${CENTRAL_MANAGER}" \
    -e _condor_SEC_TOKEN_DIRECTORY=/output \
    -v "$(cd -- "${TOKEN_DIR}" && pwd)":/output \
    -u "$(id -u)":"$(id -g)" \
    "${DOCKER_IMAGE}" condor_token_request \
        -authz ADVERTISE_MASTER \
        -authz ADVERTISE_STARTD \
        -identity "${TOKEN_IDENTITY}" \
        -token "$(basename -- "${TOKEN_FILE}")"

#---------------------------------------------------------------------------

cat <<EOF

Token request completed.

The token has been saved to:

    ${TOKEN_FILE}

EOF
