======================
Command-Line Interface
======================

Overview
========

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

    ESA CCI Toolbox command-line interface, version 0.5.0

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
commands:

* :ref:`cli_ect_ds`
* :ref:`cli_ect_op`
* :ref:`cli_ect_run`

The following first level sub-commands are used to work interactively with datasets and operations:

* :ref:`cli_ect_ws`
* :ref:`cli_ect_res`

When you encounter any error while using ``ect`` and you want to `report the problem <https://github.com/CCI-Tools/ect-core/issues>`_
to the development team, we kindly ask you to rerun the command with option ``--traceback`` and include the Python stack
traceback with a short description of your problem.


Examples
========

The following examples shall help you understand the basic concepts behind the various ``ect`` commands.

Manage datasets
---------------

To query all available datasets, type::

    ect ds list

To query all datasets that have ``ozone`` in their name, type::

    ect ds list -n ozone

To get more detailed information on a specific dataset, e.g. ``esacci.OZONE.mon.L3...``, type::

    ect ds info esacci.OZONE.mon.L3.NP.multi-sensor.multi-platform.MERGED.fv0002.r1

To add a local Dataset from all netCDF files in e.g. ``data/sst_v3`` and name it e.g. ``SSTV3``, type::

    ect ds def SSTV3 data/sst_v3/*.nc

Make sure it is there::

    ect ds list -n SSTV3

To make a temporal subset ECV dataset locally available, i.e. avoid remote data access during its usage::

    ect ds sync esacci.OZONE.mon.L3.NP.multi-sensor.multi-platform.MERGED.fv0002.r1 2006 2007

The section :doc:`um_config` describes, how to configure the directory where ``ect`` stores such synchronised
data.

Inspect available operations
----------------------------

To list all available operations, type::

    ect op list

To display more details about a particular operation, e.g. ``tseries_point``, type::

    ect op info tseries_point

Run an operation
----------------

To run the ``tseries_point`` operation on a dataset, e.g. the ``local.SSTV3`` (from above), at lat=0 and lon=0, type::

    ect run --open ds=local.SSTV3 --write ts2.nc tseries_point ds=ds lat=0 lon=0

To run the ``tseries_point`` operation on a netCDF file, e.g. ``test/ui/precip_and_temp.nc`` at lat=0 and lon=0, type::

    ect run --read ds=test/ui/precip_and_temp.nc --write ts2.nc tseries_point ds=ds lat=0 lon=0


Interactive session
-------------------

The following command sequence is a simple interactive example for a session with the ECT command-line::

    ect ws new
    ect res open ds local.SSTV3
    ect res set ts tseries_point ds=ds lat=0 lon=0
    ect res plot ts
    ect res write ts ts.nc
    ect ws status

The steps above explained:

1. ``ect ws new`` is used to create a new in-memory *workspace*. A workspace can hold any number of
   named *workspace resources* which may refer to opened datasets or any other ingested or computed objects.
2. ``ect res open`` is used to open a dataset from the available data stores and
   assign the opened dataset to the workspace resource ``ds``. Accordingly, ``ect res read`` could have been used to
   read from a local netCDF file.
3. ``ect res set`` assign the result of the ``tseries_point`` applied to ``ds`` to workspace resource ``ts``.
4. ``ect res plot`` plots the workspace resource ``ts``.
5. ``ect res write`` writes the workspace resource ``ts`` to a netCDF file ``./ts.nc``.
6. ``ect ws status`` shows the current workspace status and lists all workspace resource assignments.

We could now save the current workspace state and close it::

    ect ws save
    ect ws close

``ect ws save`` creates a hidden sub-directory ``.ect-workspace`` and herewith makes the current directory a
*workspace directory*. ``ect`` uses this hidden directory to persist the workspace state information.
At a later point in time, you could ``cd`` into any of your workspace directories, and::

    ect ws open
    ect ws status

in order to reopen it, display its status, and continue interactively working with its resources.

The following subsections provide detailed information about the ``ect`` commands.

.. _cli_ect_ds:

``ect ds`` - Dataset Management
===============================

.. argparse::
   :module: ect.ui.cli
   :func: make_parser
   :prog: ect
   :path: ds



.. _cli_ect_op:

``ect op`` - Operation Management
=================================


.. argparse::
   :module: ect.ui.cli
   :func: make_parser
   :prog: ect
   :path: op

.. _cli_ect_run:

``ect run`` - Running Operations and Workflows
==============================================

.. argparse::
   :module: ect.ui.cli
   :func: make_parser
   :prog: ect
   :path: run

.. _cli_ect_ws:

``ect ws``: Workspace Management
================================

.. argparse::
   :module: ect.ui.cli
   :func: make_parser
   :prog: ect
   :path: ws

.. _cli_ect_res:

``ect res`` - Workspace Resources Management
============================================


.. argparse::
   :module: ect.ui.cli
   :func: make_parser
   :prog: ect
   :path: res

