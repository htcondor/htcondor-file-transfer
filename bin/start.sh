#!/bin/bash

set -eu


# The HTCondor configuration in use here runs jobs as the user starting
# HTCondor itself. Jobs should never run as 'root'.


if [ "$(id -u)" = "0" ]; then
  printf '%s\n' "This container must be run as a non-root user."
  exit 1
fi


# Prepare HTCondor's "local" directory by creating the necessary
# subdirectories as the user running the container. For whatever reason, it
# seems that HTCondor will not create the directories on its own.


local_dir="/condor/local"

mkdir -p "${local_dir}"/lib/condor/execute/
mkdir -p "${local_dir}"/lock/condor/
mkdir -p "${local_dir}"/log/condor/
mkdir -p "${local_dir}"/run/condor/


# With that out of the way, we can now start HTCondor.


exec /usr/sbin/condor_master -f
