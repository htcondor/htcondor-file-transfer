#!/bin/bash

set -eu

THIS_DIR="$(cd -- "$(dirname -- "$0")" && pwd)"

source "${THIS_DIR}"/.run_common.sh

docker run -d \
  \
  -u "$(id -u)":"$(id -g)" \
  \
  -e CENTRAL_MANAGER="${CENTRAL_MANAGER}" \
  -e JOB_OWNER="${JOB_OWNER}" \
  -e UNIQUE_NAME="${UNIQUE_NAME}" \
  \
  -v "${TOKEN_FILE}":/condor/tokens.d/token \
  -v "${DATA_DIR}":/data \
  -v "${LOCAL_DIR}":/condor/local \
  \
  ${FILE_TRANSFER_EP_IMAGE}
