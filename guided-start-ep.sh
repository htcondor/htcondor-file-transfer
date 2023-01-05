#!/bin/bash
set -e

prompt () {
  var=$1
  shift
  echo "$@"
  read -p "> " "$var"
}

fail () { echo "$@" >&2; exit 1; }

git clone https://github.com/brianaydemir/htcondor_file_transfer_ep
cd htcondor_file_transfer_ep

prompt CM "Enter the central manager's hostname; Default: cm.chtc.wisc.edu"

[[ $CM ]] || CM=cm.chtc.wisc.edu

echo
echo "Please note the following Request ID, and ask CHTC staff to approve"
echo "this request on $CM with: condor_token_request_approve -reqid ID"
echo

./request_token.sh -c "$CM"

cp .env.template .env

echo
prompt JO "Enter your user name on the AP"

[[ $JO ]] || fail "Sorry, there's no default for this setting."


xxx=$(dd bs=4c count=1 if=/dev/urandom 2>/dev/null | xxd -p)
UN_default=$JO-$xxx

prompt UN "Enter a Unique Name for this EP.  Default: $UN_default"

[[ $UN ]] || UN=$UN_default

echo
echo "Enter directory on THIS HOST to mount on /data inside EP container."
prompt DD "Default: /data"

[[ $DD ]] || DD=/data

echo
echo "Enter directory on THIS HOST to store the largest file being transferred"
echo "plus 1G for log files and other ephemeral data."
prompt LD "Default: /tmp/htcondor-file-transfer-ep-$xxx"

[[ $LD ]] || LD=/tmp/htcondor-file-transfer-ep-$xxx


echo
echo "Use these settings?

  CENTRAL_MANAGER=$CM;
  UNIQUE_NAME=$UN;
  JOB_OWNER=$JO;
  DATA_DIR=$DD;
  LOCAL_DIR=$LD;
"
prompt OK "OK? [y/N]"

[[ $OK = [yY] ]] || fail Quitting.

sed -i "s;CENTRAL_MANAGER=.*;CENTRAL_MANAGER=$CM;
        s;UNIQUE_NAME=.*;UNIQUE_NAME=$UN;
        s;JOB_OWNER=.*;JOB_OWNER=$JO;
        s;DATA_DIR=.*;DATA_DIR=$DD;
        s;LOCAL_DIR=.*;LOCAL_DIR=$LD;" .env

echo
echo "Starting the singularity container..."
echo
./run_singularity.sh
echo
echo "To stop, run: singularity instance stop"

