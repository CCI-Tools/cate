.. _Matplotlib Color Maps Reference: https://matplotlib.org/examples/color/colormaps_reference.html


=====
Setup
=====

System Requirements
===================

Hardware
--------

It is recommended to use an up-to-date computer, with at least 8GB of RAM and a multi-core CPU.
The most important bottlenecks will first be the data transfer rate from local data caches into the
executing program, so it is advised to use fast solid state disks. Secondly, the internet connection
speed matters, because Cate will frequently have to download data from remote services
in order to cache it locally.

Operating Systems
-----------------

The Cate is supposed to work on up-to-date Windows, Mac OS X, and Linux operating systems.


Installation
============


Installers for the Linux, Mac OS X, and Windows platform can be downloaded from the project's
web page at `cci-tools.github.io <https://cci-tools.github.io/>`_
or on Cate's release page on `GitHub <https://github.com/CCI-Tools/cate/releases>`_.

We provide two Cate installers for *Cate Core* and *Cate Desktop*, Cate's graphical user interface.
Note that *Cate Desktop* cannot be run without *Cate Core* installed. This may change in the future.

Installing Cate Core
--------------------

*Cate Core* includes a Python runtime environment, bundled with the Cate Python package.  The latter provides
the Cate command-line interface (CLI) and Cate Python API.

The installers for the supported platforms are:

* ``cate-1.0.0-MacOSX-x86_64.sh`` for OS X
* ``cate-1.0.0-Linux-x86_64.sh`` for Linux
* ``cate-1.0.0-Windows-x86_64.sh`` for Windows


The Cate Core installers are currently customized `Anaconda <https://www.continuum.io/why-anaconda>`_
installers. In the following, we provide some notes regarding its usage on Windows, Mac OS X, and Linux systems.

**Windows Installer**

When you run the installer on Windows, make sure you un-check **Add Anaconda to my PATH environment variable**.
Otherwise the Anaconda Python distribution used by the Cate would become your system's default Python.

.. figure:: ../_static/figures/installer-win.png
   :scale: 100 %
   :align: center


**Mac OS X and Linux Installers**

On Mac OS X and Linux systems, the downloaded installer is a shell script. To run it, open a terminal window,
``cd`` into the directory where you've downloaded the installer and execute the shell script using ``bash``:

.. code-block:: console

    $ cd ~/Downloads
    $ bash cate-1.0.0-Linux-x86_64.sh

By default, the installer will install Cate Core into ``~/cate``. If you want it in another location, use the
``-p`` (=prefix) option, e.g.

.. code-block:: console

    $ bash cate-1.0.0-Linux-x86_64.sh -p cate-1.0.0

Use the ``-h`` option to display other install options.

After successful installation a link to "Cate CLI" will be created on a Linux desktop (if any) aor as a Startmenu entry
on Windows.

The actual Cate CLI executables ``cate-cli`` can be found in the Cate Python environment:

* ``cate/bin/cate-cli`` on Linux
* ``cate/bin/cate-cli.app`` on Mac
* ``cate/Scripts/cate-cli.bat`` on Windows

As ``cate-cli`` is an application Mac, it can started using a double-click.


Updating an existing Cate Core
------------------------------

The Cate Core installers are pretty large files because they include a complete Python 3 environment bundled
with various "heavy" Python packages such as numpy, pandas, matplotlib, gdal, etc.

When you install a Cate Core software update you can not use the same target directory again, because the installer
requires it to be non-existing or empty. So you either have to choose a different target directory,
or you uninstall the previous version first, or you simply remove all contained files in the old directory.

Another option is to entirely avoid downloading and installing a new Python environment for every Cate software update
by updating it in place using the bundled ``conda`` package manager. To update to a specific Cate version,
e.g. version 1.0.1, bring up the Cate CLI and type

.. code-block:: console

    $ conda install --no-shortcuts -c ccitools -c conda-forge cate-cli=1.0.1

To update to the latest Cate, use ``cate-cli`` without version number:

.. code-block:: console

    $ conda install --no-shortcuts -c ccitools -c conda-forge cate-cli

For the future, we are planning to drastically simplifying Cate installation and updates.

Installing Cate Core from Sources
---------------------------------

If you are a developer you may wish to build and install Cate from Python sources.
In this case, please follow the instructions given in the project's
`README <https://github.com/CCI-Tools/cate/blob/master/README.md>`_ on GitHub.


Installing Cate Desktop
-----------------------

*Cate Desktop* is Cate's graphical user interface and depends on Cate Core.
Hence, you need a compatible Cate Core installation before you can install and run Cate Desktop.

The Cate Desktop installers for the supported platforms are:

* ``Cate.Desktop-1.0.0.dmg`` for OS X
* ``cate-desktop-1.0.0-x86_64.AppImage`` for Linux
* ``Cate.Desktop.Setup.1.0.0.exe`` for Windows

All Cate Desktop installers are light-weight and executed by double clicking them.
They don't require any extra user input.

Configuration
=============

Cate's configuration file is called ``conf.py`` and is located in the ``~/.cate/1.0.0`` directory, where ``~`` is
the current user's home directory.

Given here is an overview of the possible configuration parameters:

:``data_stores_path``:
    Directory where Cate stores information about data stores and also saves local data files synchronized with their
    remote versions. Use the tilde '~' (also on Windows) within the path to point to your home directory.
    This directory can become rather populated once after a while and it is advisable to place it where there exists
    a high transfer rate and sufficient capacity. Ideally, you would let it point to a dedicated solid state disc (SSD).
    The default value for ``data_stores_path`` is the ``~/.cate/data_stores`` directory.

:``use_workspace_imagery_cache``:
    If set to ``True``, Cate will maintain a per-workspace
    cache for imagery generated from dataset variables. Such cache can accelerate
    image display, however at the cost of disk space.

:``included_data_sources``:
    If ``included_data_sources`` is a list, its entries are expected to be wildcard patterns for the identifiers of data
    sources to be included. By default, or if 'included_data_sources' is None, all data sources are included.

:``excluded_data_sources``:
    If ``excluded_data_sources`` is a list, its entries are expected to be wildcard patterns for the identifiers of data
    sources to be excluded. By default, or if 'excluded_data_sources' is None, no data sources are excluded.
    If both ``included_data_sources`` and ``excluded_data_sources`` are lists, we first include data sources using
    ``included_data_sources`` then remove entries that match any result from applying ``excluded_data_sources``.

:``variable_display_settings``:
    Configure / overwrite default variable display settings as used in various plot_<type>() operations
    and in the Cate Desktop GUI.
    Each entry maps a variable name to a dictionary with the following entries:
    * ``color_map``   - name of a color map taken from from `Matplotlib Color Maps Reference`_
    * ``display_min`` - minimum variable value that corresponds to the lower end of the color map
    * ``display_max`` - maximum variable value that corresponds to the upper end of the color map

    For example:::

        variable_display_settings = {
            'my_var': dict(color_map='viridis', display_min=0.1, display_max=0.8),
        }

:``default_color_map``:
    Default color map to be used for any variable not configured in 'variable_display_settings'
    'default_color_map' must be the name of a color map taken from from `Matplotlib Color Maps Reference`_.
    If not specified, the ultimate default is ``'inferno'``.
