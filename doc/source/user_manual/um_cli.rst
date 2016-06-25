======================
Command-Line Interface
======================

The CCI Toolbox comprises a single command-line executable, which is called ``ect`` and is made available
from your Python installation after installing the CCI Toolbox (Python) Core. See section :doc:`um_install`
for more information.

.. warning:: The command-line executable is under development and its current functionality is rather limited.

The command-line executable can be used to list available data sources and to synchronise subsets of remote data store
contents on the user's computer to make them available to the CCI Toolbox. It also allows for listing available
operations as well as running operations and workflows.

The command-line executable uses sub-commands for a specific functionality. The most important commands are

* ``run`` to run an operation of workflow with given arguments.
* ``ds`` to display data source information and to synchronise remote data sources with locally cached versions of it.
* ``list`` to list registered data stores, data sources, operations and plugins

Each command has its own set of options and arguments and can display help when used with the option ```--help``
or ``-h``.

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
