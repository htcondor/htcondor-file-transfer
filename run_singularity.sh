#!/bin/bash
# vim: tabstop=4

# Start the EP as a Singularity container.
# Configuration should be specified in the '.env' file.

set -eu

THIS_DIR="$(cd -- "$(dirname -- "$0")" && pwd)"

source "${THIS_DIR}"/.run_common.sh

singularity instance start \
  \
  --env CENTRAL_MANAGER="${CENTRAL_MANAGER}" \
  --env JOB_OWNER="${JOB_OWNER}" \
  --env UNIQUE_NAME="${UNIQUE_NAME}" \
  \
  -B "${TOKEN_FILE}":/condor/tokens.d/token \
  -B "${DATA_DIR}":/data \
  -B "${LOCAL_DIR}":/condor/local \
  \
  "${SINGULARITY_IMAGE}" "${1:-htcondor-file-transfer-ep}"
