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

In this setup, the EP runs as a container using Docker or Singularity.
Configuration and the directory to synchronize are provided via environment
variables and volume mounts.

1. Clone this repository::

    git clone https://github.com/brianaydemir/htcondor_file_transfer_ep.git

2. Request a token, which the EP will use for authentication::

    cd htcondor_file_transfer_ep

    ./request_token.sh -c <central manager's hostname>

   For the CHTC, note that the central manager's hostname is "cm.chtc.wisc.edu".

   Run the script without any arguments to see all of the available options.

   Note that this command will hang until the token request is approved. Run
   it while in contact with the central manager's administrators.  

   Take note of the request ID, and if you are CHTC staff with root on the CM,
   you can approve the token request yourself: log into cm.chtc.wisc.edu and
   as root/sudo run::

    condor_token_request_approve -reqid ID

   If you lost the request ID, you can see what's pending there with::

    condor_token_request_list

3. Copy ``.env.template`` to ``.env`` and update it for your setup.

   You will need to edit your ``.env`` file.  At a minimum, you need to set::

    CENTRAL_MANAGER=cm.chtc.wisc.edu
    UNIQUE_NAME=pick-a-unique-name-here
    JOB_OWNER=your-user-on-the-AP

   Also consider setting ``DATA_DIR`` to a directory on the HOST where you will
   run the EP, which will be mounted onto ``/data`` inside the EP container.

4. Start the EP as a Docker container::

    ./run_docker.sh [optional name for the container]

   Or as a Singularity container::

    ./run_singularity.sh [optional name for the instance]

5. Use ``docker container stop`` or ``singularity instance stop`` to stop
   the container.


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


Quickstart: Transferring files between EP and AP at CHTC
--------------------------------------------------------

1. Take a minute to note the distro version inside the ``Dockerfile``;
   it should have a line like::

    FROM htcondor/mini:9.10-el7

   The "el7" there corresponds to ``"OpSysMajorVer == 7"``.  You may need
   to add this later to your transfer job requirements in order for your
   transfer job to match properly.

2. Log into your AP (eg ``submit2.chtc.wisc.edu``).
  
   You should be able to find a slot for your running container like so::

    condor_status | grep your-unique-name-here

   The slot name should look something like::

      "slot1@your-unique-name-here@conainerhash"

   If that's all good you can now submit the transfer job...

3. The tool repo to clone is:

    https://github.com/HTPhenotyping/htcondor_file_transfer

   Clone this repo locally and enter the repo checkout.

4. Create subdirs for logs, and source and/or dest.

   Make a "working dir" locally for all the condor logs to live; eg::

    mkdir working

   Make a destination dir, if you are pulling files from the EP; eg::

    mkdir dest

   Make a source dir, if you are pushing files to the EP; eg::

    mkdir src   # and put stuff in it

   If you want to pull files from the ``DATA_DIR`` on your host, remember
   that this is mounted to ``/data`` inside the EP container.

5. Submit the transfer job.

   The command line to start the transfer job to sync your ``DATA_DIR``
   from your host to your newly-created dest dir on the AP would be::

    req='UniqueName == "mystras-xfer-ep" && OpSysMajorVer == 7'
    ./xfer.py sync --requirements "$req" \
                   --working-dir working/ pull dest/ /data

   And again note that OpSysMajorVer should match what you found in the
   Dockerfile from the EP container setup (in htcondor_file_transfer_ep).

   To go the other way, to send a src dir on the AP to the ``DATA_DIR`` on
   you host, use the "push" command instead::

    req='UniqueName == "mystras-xfer-ep" && OpSysMajorVer == 7'
    ./xfer.py sync --requirements "$req" \
                   --working-dir working/ push src/ /data

   You can watch progress on the AP by doing condor_q, or watching the
   logs under your new "working" dir.  It may take some time for everything
   to finish.

