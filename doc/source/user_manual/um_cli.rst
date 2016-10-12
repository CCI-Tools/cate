======================
Command-Line Interface
======================

The CCI Toolbox comprises a single command-line executable, which is called ``ect`` and is available after installing
the CCI Toolbox on your computer. See section :doc:`um_install` for more information. The command-line
interface allows for accessing local and remote datasets as well as running virtually all CCI Toolbox
operations on them.

The most easy way to use ``ect`` is running the ``ect-cli`` script found in ``bin`` directory of your CCI Toolbox
installation directory. Windows and Unix users will find a link to this script in their start menu or on their desktop.
Opening the link will open a new console / terminal window configured to run ``ect``.

**Developers only:** If you build and install the CCI Toolbox from Python sources into your current Python environment,
``ect`` will be registered as an executable script. It can be found as ``$PYTHON_PREFIX/bin/ect.sh`` on Unix systems
and as ``%PYTHON_PREFIX%\\Scripts\\ect.exe`` on Windows systems where ``PYTHON_PREFIX`` is the path to the current
Python environment.

In the console / terminal window type::

    ect -h

This should output the following usage help:::

    usage: ect [-h] [--version] [--license] [--docs] [--traceback] COMMAND ...

    ESA CCI Toolbox command-line interface, version 0.5.0a02

    positional arguments:
      COMMAND      One of the following commands. Type "COMMAND -h" to get
                   command-specific help.
        ds         Manage data sources.
        op         Manage data operations.
        ws         Manage workspaces.
        res        Manage workspace resources.
        run        Run an operation or Workflow file.

    optional arguments:
      -h, --help   show this help message and exit
      --version    show program's version number and exit
      --license    show software license and exit
      --docs       show software documentation in a browser window
      --traceback  show (Python) stack traceback for the last error



``ect`` uses up to two sub-command levels. Each sub-command has its own set of options and arguments and can display
help when used with the option ```--help`` or ``-h``. The first sub-command level comprises the following list of
commands:::

* ``ect ds`` :ref:`_cli_ect_ds`
* ``ect op`` :ref:`_cli_ect_op`
* ``ect run`` :ref:`_cli_ect_run`

The following first level sub-commands are used to work interactively with datasets and operations:::

* ``ect ws`` :ref:`_cli_ect_ws`
* ``ect res`` :ref:`_cli_ect_res`


.. _cli_ect_ds:

Dataset Management
==================



.. _cli_ect_op:

Operation Management
====================

.. _cli_ect_run:

Running Operations and Workflows
================================

.. _cli_ect_ws:

Workspace Management
====================

.. _cli_ect_res:

Workspace Resource Management
=============================



The command-line executable can be used to list available data sources and to synchronise subsets of remote data store
contents on the user's computer to make them available to the CCI Toolbox. It also allows for listing available
operations as well as running operations and workflows.

The command-line executable uses sub-commands for a specific functionality. The most important commands are

* ``run`` to run an operation of workflow with given arguments.
* ``ds`` to display data source information and to synchronise remote data sources with locally cached versions of it.
* ``list`` to list registered data stores, data sources, operations and plugins


Given here is an early version of the ``ect``'s usage::

   $ ect -h
   usage: ect [-h] [--version] COMMAND ...

   ESA CCI Toolbox command-line interface, version 0.1.0

   positional arguments:
     COMMAND     One of the following commands. Type "COMMAND -h" to get command-
                 specific help.
       list      List items of a various categories.
       run       Run an operation OP with given arguments.
       ds        Data source operations.
       cr        Print copyright information.
       lic       Print license information.
       doc       Display documentation in a browser window.

   optional arguments:
     -h, --help  show this help message and exit
     --version   show program's version number and exit



In the following are given some usage examples.

Lists available data sources (ECVs) from ESA's CCI FTP server::

    $ ect list ds
    ...
    91: SOIL_MOISTURE_DAILY_FILES_ACTIVE_V02.2
    ...

Display (meta-) information about some ECV::

    $ ect ds SOIL_MOISTURE_DAILY_FILES_ACTIVE_V02.2

Make an ECV data source locally available for a given time period::

    $ ect ds SOIL_MOISTURE_DAILY_FILES_ACTIVE_V02.2 --sync --time 2006-05,2006-07

The section :doc:`um_config` describes, how to configure the data cache directory used by this command.
