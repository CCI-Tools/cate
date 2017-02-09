======================
Command-Line Interface
======================

Overview
========

The CCI Toolbox comprises a single command-line executable, which is called ``cate`` and is available after installing
the CCI Toolbox on your computer. See section :doc:`um_install` for more information. The command-line
interface allows for accessing local and remote datasets as well as running virtually all CCI Toolbox
operations on them.

The most easy way to use ``cate`` is running the ``cate-cli`` script found in ``bin`` directory of your CCI Toolbox
installation directory. Windows and Unix users will find a link to this script in their start menu or on their desktop.
Opening the link will open a new console / terminal window configured to run ``cate``.

**Developers only:** If you build and install the CCI Toolbox from Python sources into your current Python environment,
``cate`` will be registered as an executable script. It can be found as ``$PYTHON_PREFIX/bin/cate.sh`` on Unix systems
and as ``%PYTHON_PREFIX%\\Scripts\\cate.exe`` on Windows systems where ``PYTHON_PREFIX`` is the path to the current
Python environment.

In the console / terminal window type::

    cate -h

This should output the following usage help:::

    usage: cate [-h] [--version] [--license] [--docs] [--traceback] COMMAND ...

    ESA CCI Toolbox command-line interface, version 0.6.0

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



``cate`` uses up to two sub-command levels. Each sub-command has its own set of options and arguments and can display
help when used with the option ```--help`` or ``-h``. The first sub-command level comprises the following list of
commands:

* :ref:`cli_cate_ds`
* :ref:`cli_cate_op`
* :ref:`cli_cate_run`

The following first level sub-commands are used to work interactively with datasets and operations:

* :ref:`cli_cate_ws`
* :ref:`cli_cate_res`

When you encounter any error while using ``cate`` and you want to `report the problem <https://github.com/CCI-Tools/cate-core/issues>`_
to the development team, we kindly ask you to rerun the command with option ``--traceback`` and include the Python stack
traceback with a short description of your problem.


Examples
========

The following examples shall help you understand the basic concepts behind the various ``cate`` commands.

Manage datasets
---------------

To query all available datasets, type::

    cate ds list

To query all datasets that have ``ozone`` in their name, type::

    cate ds list -n ozone

To get more detailed information on a specific dataset, e.g. ``esacci.OZONE.mon.L3...``, type::

    cate ds info esacci.OZONE.mon.L3.NP.multi-sensor.multi-platform.MERGED.fv0002.r1

To add a local Dataset from all netCDF files in e.g. ``data/sst_v3`` and name it e.g. ``SSTV3``, type::

    cate ds def SSTV3 data/sst_v3/*.nc

Make sure it is there::

    cate ds list -n SSTV3

To make a temporal subset ECV dataset locally available, i.e. avoid remote data access during its usage::

    cate ds sync esacci.OZONE.mon.L3.NP.multi-sensor.multi-platform.MERGED.fv0002.r1 2006 2007

The section :doc:`um_config` describes, how to configure the directory where ``cate`` stores such synchronised
data.

Inspect available operations
----------------------------

To list all available operations, type::

    cate op list

To display more details about a particular operation, e.g. ``tseries_point``, type::

    cate op info tseries_point

Run an operation
----------------

To run the ``tseries_point`` operation on a dataset, e.g. the ``local.SSTV3`` (from above), at lat=0 and lon=0, type::

    cate run --open ds=local.SSTV3 --write ts2.nc tseries_point ds=ds lat=0 lon=0

To run the ``tseries_point`` operation on a netCDF file, e.g. ``test/ui/precip_and_temp.nc`` at lat=0 and lon=0, type::

    cate run --read ds=test/ui/precip_and_temp.nc --write ts2.nc tseries_point ds=ds lat=0 lon=0


Interactive session
-------------------

The following command sequence is a simple interactive example for a session with the Cate command-line::

    cate ws new
    cate res open ds local.SSTV3
    cate res set ts tseries_point ds=ds lat=0 lon=0
    cate res plot ts
    cate res write ts ts.nc
    cate ws status

The steps above explained:

1. ``cate ws new`` is used to create a new in-memory *workspace*. A workspace can hold any number of
   named *workspace resources* which may refer to opened datasets or any other ingested or computed objects.
2. ``cate res open`` is used to open a dataset from the available data stores and
   assign the opened dataset to the workspace resource ``ds``. Accordingly, ``cate res read`` could have been used to
   read from a local netCDF file.
3. ``cate res set`` assign the result of the ``tseries_point`` applied to ``ds`` to workspace resource ``ts``.
4. ``cate res plot`` plots the workspace resource ``ts``.
5. ``cate res write`` writes the workspace resource ``ts`` to a netCDF file ``./ts.nc``.
6. ``cate ws status`` shows the current workspace status and lists all workspace resource assignments.

We could now save the current workspace state and close it::

    cate ws save
    cate ws close

``cate ws save`` creates a hidden sub-directory ``.cate-workspace`` and herewith makes the current directory a
*workspace directory*. ``cate`` uses this hidden directory to persist the workspace state information.
At a later point in time, you could ``cd`` into any of your workspace directories, and::

    cate ws open
    cate ws status

in order to reopen it, display its status, and continue interactively working with its resources.

The following subsections provide detailed information about the ``cate`` commands.

.. _cli_cate_ds:

``cate ds`` - Dataset Management
===============================

.. argparse::
   :module: cate.cli.main
   :func: _make_parser
   :prog: cate
   :path: ds



.. _cli_cate_op:

``cate op`` - Operation Management
=================================


.. argparse::
   :module: cate.cli.main
   :func: _make_parser
   :prog: cate
   :path: op

.. _cli_cate_run:

``cate run`` - Running Operations and Workflows
==============================================

.. argparse::
   :module: cate.cli.main
   :func: _make_parser
   :prog: cate
   :path: run

.. _cli_cate_ws:

``cate ws``: Workspace Management
================================

.. argparse::
   :module: cate.cli.main
   :func: _make_parser
   :prog: cate
   :path: ws

.. _cli_cate_res:

``cate res`` - Workspace Resources Management
============================================


.. argparse::
   :module: cate.cli.main
   :func: _make_parser
   :prog: cate
   :path: res

