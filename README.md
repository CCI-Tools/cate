<img alt="ECT: ESA CCI Toolbox" align="right" src="https://raw.githubusercontent.com/CCI-Tools/ect-core/master/doc/source/_static/logo/cci-toolbox-logo-latex.jpg" />


[![Build Status](https://travis-ci.org/CCI-Tools/ect-core.svg?branch=master)](https://travis-ci.org/CCI-Tools/ect-core)
[![codecov.io](https://codecov.io/github/CCI-Tools/ect-core/coverage.svg?branch=master)](https://codecov.io/github/CCI-Tools/ect-core?branch=master)
[![Documentation Status](https://readthedocs.org/projects/ect-core/badge/?version=latest)](http://ect-core.readthedocs.io/en/latest/?badge=latest)
[![GPLv3](https://www.gnu.org/graphics/gplv3-88x31.png)](https://www.gnu.org/licenses/gpl-3.0.en.html)
                
# ect-core

The Python core of the ESA CCI Toolbox (ECT).

## Contents

* ``setup.py`` -- main build script to be run with Python 3.5
* ``ect/`` -- ECT main package and production code
* ``notebooks/`` -- various IPython notebooks demonstrating the ECT API
* ``test/`` -- ECT test package and test code
* ``doc/`` -- documentation in RST format

## Installation

ECT requires Python 3.5+ and prefers a Miniconda or Anaconda environment.

Check out latest ECT code 

    $ git clone https://github.com/CCI-Tools/ect-core.git
    $ cd ect-core

ECT can be run from sources directly, once the following module requirements are resolved:

* ``xarray``
* ``dask``
* ``numpy``
* ``scipy``
* ``matplotlib``
* ``numba``
* ``h5py``
* ``h5netcdf``

The most up-to-date list of module requirements is found in the project's ``setup.py`` file. Do not install now, please read further first.

### Installation into a Conda environment (recommended)

It is recommended to install ECT into an isolated Conda ([Miniconda](http://conda.pydata.org/miniconda.html) or 
[Anaconda](https://www.continuum.io/downloads)) environment, because this approach avoidsclashes with existing versions of 
ECT's 3rd-party module requirements and also usually avoids platform-specific issues caused by module native binaries.

Using Conda, you can create a isolated environment for ECT like so

    $ conda env create -f environment.yml
    
Then you activate the new environment ``ect``:
     
    $ source activate ect
    
Windows users can omit the ``source`` command and just type ``activate ect``.
Unfortunately, the ``h5netcdf`` dependency is not on the Anaconda default channel and needs to be installed separately:   
    
    $ conda install -c IOOS h5netcdf 

You can now safely install ECT into the new, isolated ``ect`` conda environment.    
    
    $ python setup.py install
    
### Installation into an existing Python 3 environment (not recommended) 

To install ECT into an existing Python 3.5+ environment just for the current user, use

    $ python3 setup.py install --user
    
To install ECT for development and for the current user, use

    $ python3 setup.py develop --user

Unfortunately, the installation fails on many platforms. In most cases the failure will be caused by the 
``h5py`` module dependency, which expects pre-installed HDF-5 C-libraries to be present on your computer. 

On Windows, you may get around this by pre-installing the ECT dependencies (which you'll find in ``setup.py``) on your own, for example by using Christoph Gohlke's [Unofficial Windows Binaries for Python Extension Packages](http://www.lfd.uci.edu/~gohlke/pythonlibs/).

## Getting started

To test the installation, first run the ECT command-line interface. Type
    
    $ ect -h

IPython notebooks for various ECT use cases are on the way, they will appear in
https://github.com/CCI-Tools/ect-core/tree/master/notebooks.

To use them interactively, you'll need to install Jupyter and run its Notebook app:

    $ conda install jupyter
    $ jupyter notebook

Open the ``notebooks`` folder and select a use case.
