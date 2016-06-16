[![Build Status](https://travis-ci.org/CCI-Tools/ect-core.svg?branch=master)](https://travis-ci.org/CCI-Tools/ect-core)
[![codecov.io](https://codecov.io/github/CCI-Tools/ect-core/coverage.svg?branch=master)](https://codecov.io/github/CCI-Tools/ect-core?branch=master)
[![Documentation Status](https://readthedocs.org/projects/ect-core/badge/?version=latest)](http://ect-core.readthedocs.io/en/latest/?badge=latest)
                
# ect-core

The Python core of the ESA CCI Toolbox (ECT).

## Contents

* ``setup.py`` -- main build script to be run with Python 3.5
* ``ect/`` -- ECT main package and production code
* ``notebooks/`` -- various IPython notebooks demonstrating the ECT API
* ``test/`` -- ECT test package and test code
* ``doc/`` -- documentation in RST format

## Installation

ECT requires Python 3.5 and prefers a Miniconda or Anaconda environment.

Check out latest ECT code 

    $ git clone https://github.com/CCI-Tools/ect-core.git
    $ cd ect-core

ECT can be run from sources directly, once the following dependencies are resolved:

* ``xarray``
* ``dask``
* ``numpy``
* ``scipy``
* ``matplotlib``
* ``numba``
* ``h5py``
* ``h5netcdf``

The most up-to-date module requirements list is found in the project's ``setup.py`` file.  

### Using a Miniconda / Anaconda environment (recommended)

It is recommended to install ECT into an isolated Miniconda or Anaconda environment, because this approach avoids 
clashes with existing 3rd-party module versions and also usually avoids platform-specific issues of module binaries. 

If you use a Miniconda or Anaconda, you can create a isolated environment for ECT like so

    $ conda env create -f environment.yml
    
Then activate the new environment ``ect``:
     
    $ source activate ect
    
Windows users can omit the ``source`` command and just type ``activate ect``.
Unfortunately, the ``h5netcdf`` dependency is not on the Anaconda default channel and need to be installed separately:   
    
    $ conda install -c IOOS h5netcdf 

You can now safely install ECT into the new, isolated ``ect`` conda environment.    
    
    $ python setup.py install
    
### Using a plain Python 3 environment

ECT requires a Python 3.5+ environment. To install ECT globally for the current user, use

    $ python3 setup.py install --user
    
To install ECT for development and for current user, use

    $ python3 setup.py develop --user

Unfortunately, the installation fails on many platforms. In most cases the failure will be caused by the 
``h5py`` module dependency, which expects pre-installed HDF-5 C-libraries to be present on your computer. 

On Windows, you may get around this by pre-installing the ECT dependencies (which you'll find in ``setup.py``) using 
Christoph Gohlke's [Unofficial Windows Binaries for Python Extension Packages](http://www.lfd.uci.edu/~gohlke/pythonlibs/).

## Getting started

To test the installation, first run the ECT command-line interface. Type
    
    $ ect -h

IPython notebooks for various ECT use cases are on the way, they will appear in
https://github.com/CCI-Tools/ect-core/tree/master/notebooks.

To use them interactively, you'll need to install Jupyter and run its Notebook app:

    $ conda install jupyter
    $ jupyter notebook

Open the ``notebooks`` folder and select a use case.
