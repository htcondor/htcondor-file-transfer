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


Quickstart
----------

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
