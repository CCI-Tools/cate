<img alt="Cate: ESA CCI Toolbox" align="right" src="https://raw.githubusercontent.com/CCI-Tools/cate-core/master/doc/source/_static/logo/cci-toolbox-logo-latex.jpg" />


[![Build Status](https://travis-ci.org/CCI-Tools/cate-core.svg?branch=master)](https://travis-ci.org/CCI-Tools/cate-core)
[![Build status](https://ci.appveyor.com/api/projects/status/leugvo8fq7nx6kym?svg=true)](https://ci.appveyor.com/project/ccitools/cate-core)
[![codecov.io](https://codecov.io/github/CCI-Tools/cate-core/coverage.svg?branch=master)](https://codecov.io/github/CCI-Tools/cate-core?branch=master)
[![Documentation Status](https://readthedocs.org/projects/ect-core/badge/?version=latest)](http://ect-core.readthedocs.io/en/latest/?badge=latest)
                
# cate-core

The Python core of the ESA CCI Toolbox (Cate).

## Contents

* `setup.py` - main build script to be run with Python 3.5
* `cate/` - main package and production code
* `test/` - test package and test code
* `doc/` - documentation in Sphinx/RST format

## Installation

Cate relies on latest Python language features and therefore requires Python 3.5+.

Check out latest Cate code 

    $ git clone https://github.com/CCI-Tools/cate-core.git
    $ cd cate-core

Cate can be run from sources directly, once the following module requirements are resolved:

* `cartopy`
* `dask`
* `jdcal`
* `matplotlib`
* `netcdf4`
* `numba`
* `numpy`
* `pillow`
* `pyqt`
* `scipy`
* `tornado`
* `xarray`

The most up-to-date list of module requirements is found in the project's `setup.py` file. Do not install now, please read further first.

As stated above, we recommend installing Cate into an isolated Python 3 environment, because this approach avoids clashes 
with existing versions of Cate's 3rd-party module requirements. We recommend using Conda 
([Miniconda](http://conda.pydata.org/miniconda.html) or [Anaconda](https://www.continuum.io/downloads)) 
which will usually also avoid platform-specific issues caused by module native binaries.

Note, after installing Miniconda or Anaconda on Unix and Mac OS you'll need to close and re-open your terminal window for the changes to take effect.

### Installation into a new Conda environment 

Using Conda, you can create a isolated environment for Cate and add all required packages like so

    $ conda env create --file environment.yml

Some packages are not available on Anaconda default channels and we have to find them on
another channel (option `-c CHANNEL`). IN the `environment.yml` file we use the channel `conda-forge`.

Then you activate the new environment `cate`:
     
    $ source activate cate
    
Windows users can omit the `source` command and just type 

    $ activate cate

You can now safely install Cate into the `cate` environment.
    
    (cate) $ python setup.py install
    
### Installation into an existing Python 3 environment 

If you run it with the [standard CPython](https://www.python.org/downloads/) installation,
make sure you use a 64-bit version.

To install Cate into an existing Python 3.5+ environment just for the current user, use

    $ python3 setup.py install --user
    
To install Cate for development and for the current user, use

    $ python3 setup.py develop --user

Unfortunately, the installation fails on many platforms. In most cases the failure will be caused by the 
`h5py` module dependency, which expects pre-installed HDF-5 C-libraries to be present on your computer. 

On Windows, you may get around this by pre-installing the Cate dependencies (which you'll find in `setup.py`) 
on your own, for example by using Christoph Gohlke's 
[Unofficial Windows Binaries for Python Extension Packages](http://www.lfd.uci.edu/~gohlke/pythonlibs/).

## Getting started

To test the installation, first run the Cate command-line interface. Type
    
    $ cate -h

IPython notebooks for various Cate use cases are on the way, they will appear in the project's
[notebooks](https://github.com/CCI-Tools/cate-core/tree/master/notebooks) folder.

To use them interactively, you'll need to install Jupyter and run its Notebook app:

    $ conda install jupyter
    $ jupyter notebook

Open the `notebooks` folder and select a use case.

## Conda Deployment

There is a dedicated repository [cate-conda](https://github.com/CCI-Tools/cate-conda)
which provides scripts and configuration files to build Cate's Conda packages and a stand-alone installer.

## Development

### Contributors

Contributors are asked to read and adhere to our [Developer Guide](https://github.com/CCI-Tools/cate-core/wiki/Developer-Guide).

### Unit-testing

For unit testing we use `pytest` and its coverage plugin `pytest-cov`.

To run the unit-tests with coverage, type

    $ export NUMBA_DISABLE_JIT=1
    $ py.test --cov=cate test
    
We need to set environment variable `NUMBA_DISABLE_JIT` to disable JIT compilation by `numba`, so that 
coverage reaches the actual Python code. We use Numba's JIT compilation to speed up numeric Python 
number crunching code.


## License

The CCI Toolbox is distributed under terms and conditions of the [MIT license](https://opensource.org/licenses/MIT).
