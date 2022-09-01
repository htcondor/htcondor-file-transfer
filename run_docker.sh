#!/bin/bash
# shellcheck disable=SC1091
# vim: tabstop=4

# Start the EP as a Docker container.
# Configuration should be specified in the '.env' file.

set -eu

THIS_DIR="$(cd -- "$(dirname -- "$0")" && pwd)"

source "${THIS_DIR}"/.run_common.sh

docker run -d \
  \
  -u "$(id -u)":"$(id -g)" \
  \
  --env CENTRAL_MANAGER="${CENTRAL_MANAGER}" \
  --env JOB_OWNER="${JOB_OWNER}" \
  --env UNIQUE_NAME="${UNIQUE_NAME}" \
  \
  -v "${TOKEN_FILE}":/condor/tokens.d/token \
  -v "${DATA_DIR}":/data \
  -v "${LOCAL_DIR}":/condor/local \
  \
  --name "${1:-htcondor-file-transfer-ep}" \
  \
  "${DOCKER_IMAGE}"
