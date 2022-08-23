#!/bin/bash


# In order to ensure that HTCondor can actually write its dynamic data as
# whatever user happens to be running the image, we create the directories
# here, under the assumption that the base directory is world-writeable.


local_dir=${HTCONDOR_LOCAL_DIR:-/condor/local}

mkdir -p "${local_dir}"/lib/condor/execute/
mkdir -p "${local_dir}"/lock/condor/
mkdir -p "${local_dir}"/log/condor/
mkdir -p "${local_dir}"/run/condor/


# With that out of the way, we can now start HTCondor.

exec /usr/sbin/condor_master -f
