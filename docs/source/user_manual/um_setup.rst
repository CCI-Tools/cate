.. _conda: https://conda.io/docs/intro.html
.. _Anaconda: https://www.anaconda.com/download/
.. _Miniconda: https://conda.io/miniconda.html
.. _running tests: https://facebook.github.io/create-react-app/docs/running-tests
.. _deployment: https://facebook.github.io/create-react-app/docs/deployment
.. _Cate README: https://github.com/CCI-Tools/cate/blob/master/README.md
.. _Cate App README: https://github.com/CCI-Tools/cate-app/blob/master/README.md



===========
Local Setup
===========

This section describes how to setup Cate on a computer of your choice.

Cate at its core is a Python package that provides Cate's command-line interface (CLI) and
application programming interface (API). In addition, the Python package provides a visualisation and
processing service for *Cate App*, Cate's graphical user interface (GUI).

If you only want the Cate CLI or API, you can install just the Python package into a dedicated Miniconda_
or Anaconda_ Python 3 environment. In this case, please read :ref:`setup_install_cate`.

System Requirements
===================

**Hardware**: It is recommended to use an up-to-date computer, with at least 8GB of RAM and a multi-core CPU.
The most important bottlenecks will first be the data transfer rate from local data caches into the
executing program, so it is advised to use fast solid state disks. Secondly, the internet connection
speed matters, because Cate will frequently have to download data from remote services
in order to cache it locally.

**Operating Systems**: Cate is supposed to work on up-to-date Linux, Mac OS X, and Windows operating systems.

.. _setup_install_cate:

Installing Cate (CLI, API)
==========================

First time installs
-------------------

The Cate Python package requires a *Conda environment* for Python 3.6+ either provided by a
`Miniconda <https://conda.io/miniconda.html>`_ or `Anaconda <https://www.anaconda.com/download/>`_ installation.

If you haven't yet installed either of the two, we recommend you install Miniconda for Python 3 first.

With Miniconda/Anaconda installed and accessible (installation path should be on ``PATH`` environment variable)
open a shell / terminal window (Windows users type "cmd" in search field of start menu).

The steps are:

    1. create a dedicate Python environment for Cate so it doesn't interfere with other Python packages you
       might already have installed;
    2. activate that newly create Python environment for Cate;
    3. install the Cate Python package;
    4. test the installation by invoking the Cate command-line interface.

Mac OS / Linux:

.. code-block:: console

    $ conda env create -n cate-env
    $ source activate cate-env
    $ conda install -c conda-forge -c ccitools cate-cli
    $ cate --help

Windows:

.. code-block:: console

    > conda env create -n cate-env
    > activate cate-env
    > conda install -c conda-forge -c ccitools cate-cli
    > cate --help


Updating
--------

You can easily update an existing Cate installation using the ``cate upd`` command:

.. code-block:: console

    $ cate upd

Or you use Conda to install the latest version:

.. code-block:: console

    $ conda update -c conda-forge -c ccitools cate-cli

Installing from Sources
=======================

.. _install_cate_from_sources:

Installing Cate from Sources
----------------------------

If you are a developer you may wish to build and install Cate from Python sources.
Cate's sources are organised as follows:

* `setup.py` - main build script to be run with Python 3.6+
* `cate/` - main package and production code
* `test/` - test package and test code
* `doc/` - documentation in Sphinx/RST format

We recommend installing Cate into an isolated Python 3 environment, because this
approach avoids clashes with existing versions of Cate's 3rd-party Python package
requirements.
Using Miniconda_ or Anaconda_ will usually prevent platform-specific issues
caused by module native binaries.

The first step is to clone the latest Cate code and step into the checked out
directory:

.. code-block:: console

    $ git clone https://github.com/CCI-Tools/cate.git
    $ cd cate


### Using Conda

Conda_ is the package manager used by the Miniconda or Anaconda Python distributions.

Creating a new Python environment for Cate will require around 2.2 GB disk space on
Linux/Darwin and and 1.2 GB on Windows.
To create a new Conda environment `cate-env` in your Anaconda/Miniconda installation directory, type:

.. code-block:: console

    $ conda env create

If you want the environment to be installed in another location, e.g. due to disk space limitations, type:

.. code-block:: console

    $ conda env create --prefix some/other/location/for/cate

Next step is to activate the new environment.

.. code-block:: console

    $ conda activate cate-env

You can now safely install Cate sources into the new `cate-env` environment.

.. code-block:: console

    (cate-env) $ python setup.py install

You can find more information about this in the `Cate README`_ on GitHub.

.. _install_cate_app_from_sources:

Installing Cate App from Sources
--------------------------------

To install Cate App, you need to check out Cate-App like Cate above and
step into the project directory:

.. code-block:: console

    $ git clone https://github.com/CCI-Tools/cate-app.git
    $ cd cate-app

There you can run:

.. code-block:: console

    $ yarn

to initially install or update all project dependencies.

.. code-block:: console

    $ yarn start

Runs the app in the development mode.
Open http://localhost:3000 to view it in a browser.

The page will reload if you make edits.
You will also see any lint errors in the console.

.. code-block:: console

    yarn test

Launches the test runner in the interactive watch mode.

See the section about `running tests`_ for more information.

.. code-block:: console

    $ yarn build

Builds the app for production to the `build` folder.
It correctly bundles React in production mode and optimizes the build for the best performance.

The build is minified, and the filenames include the hashes.

See the section about deployment_ and the `Cate App README`_ on GitHub for more information.
