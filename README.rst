HTCondor File Transfer Execution Point
======================================

HTCondor execution point setup and configuration for using HTCondor file
transfer to synchronize a directory between two hosts.

Intended for use with `xfer.py`_.

.. _xfer.py: https://github.com/HTPhenotyping/htcondor_file_transfer


Requirements
------------

* The ability to run software as a user that can read and write files in
  the directory being synchronized.

* The hostname of the central manager to which the execution point should
  report and the ability to contact the central manager's administrators.

* Outgoing network connectivity to the central manager.

* The name of the owner of the transfer jobs that should run on the EP.


Quickstart: Running the EP as a container
-----------------------------------------

In this setup, the EP runs as a Docker container. Configuration and the
directory to synchronize are provided via environment variables and volume
mounts.

1. Clone this repository::

    git clone https://github.com/brianaydemir/htcondor_file_transfer_ep.git

2. Request a token, which the EP will use for authentication::

    cd htcondor_file_transfer_ep

    ./request_token.sh -c <central manager's hostname>

   Run the script without any arguments to see all of the available options.

3. Start the EP container. In the command below, replace ``<...>`` with the
   appropriate values.

   ::

    docker run -d \
      -e CENTRAL_MANAGER=<central manager's hostname> \
      -e JOB_OWNER=<name of the transfer jobs' owner> \
      -e UNIQUE_NAME=<unique name for this EP> \
      -v <full path to the token from step 2>:/etc/condor/tokens.d/token \
      -v <full path to the directory to synchronize>:/data \
      hub.opensciencegrid.org/opensciencegrid/htcondor-file-transfer-ep:latest

   You might need to provide the following additional arguments to ``docker
   run``:

   ``--user <name|uid>[:<group|gid>]``
      Run the container as the given user (and group). This user should have
      the ability to read and write files to the directory being synchronized.

   ``--name <string>``
      Assign a name to the container. This can make it easier to identify
      and manage.

Note: When running `xfer.py`_, the "remote" path should be given as
``/data``, because the path needs to be what the EP sees from inside the
running container.

::

    xfer.py sync push <local directory> /data ...


Quickstart: Running the EP directly on a host
---------------------------------------------

In this setup, the EP runs directly on a host. Configuration is specified
via the setup script (see below), and the directory to synchronize must be
directly accessible via the host's file system.

1. Clone this repository::

    git clone https://github.com/brianaydemir/htcondor_file_transfer_ep.git

2. Start the setup script::

    cd htcondor_file_transfer_ep

    ./setup.sh -c <central manager's hostname> -n <unique name for this EP> -u <name of the transfer jobs' owner>

   Run the script without any arguments to see all of the available options.

3. When prompted by the script, ask an administrator of the central manager
   to approve the given token request ID.

4. After the script completes successfully, start the execution point by
   following the instructions in the script's final output.
