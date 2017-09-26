<img alt="Cate: ESA CCI Toolbox" align="right" src="https://raw.githubusercontent.com/CCI-Tools/cate/master/doc/source/_static/logo/cci-toolbox-logo-latex.jpg" />


[![Build Status](https://travis-ci.org/CCI-Tools/cate.svg?branch=master)](https://travis-ci.org/CCI-Tools/cate)
[![Build status](https://ci.appveyor.com/api/projects/status/leugvo8fq7nx6kym/branch/master?svg=true)](https://ci.appveyor.com/project/ccitools/cate-core)
[![codecov.io](https://codecov.io/github/CCI-Tools/cate/coverage.svg?branch=master)](https://codecov.io/github/CCI-Tools/cate?branch=master)
[![Documentation Status](https://readthedocs.org/projects/cate/badge/?version=latest)](http://cate.readthedocs.io/en/latest/?badge=latest)
                
# cate

ESA CCI Toolbox (Cate) Python package, API and CLI.

## Contents

* `setup.py` - main build script to be run with Python 3.5
* `cate/` - main package and production code
* `test/` - test package and test code
* `doc/` - documentation in Sphinx/RST format

## Installation from Sources

We recommend installing Cate into an isolated Python 3 environment, because this
approach avoids clashes with existing versions of Cate's 3rd-party Python package requirements. 
For example, using ([Miniconda](http://conda.pydata.org/miniconda.html) 
or [Anaconda](https://www.continuum.io/downloads)) which will usually also avoid platform-specific 
issues caused by module native binaries.

The first step is to clone latest Cate code and step into the check out directory: 

    $ git clone https://github.com/CCI-Tools/cate.git
    $ cd cate


### Using Conda

[Conda](https://conda.io/docs/intro.html) is the package manager used by the Miniconda or 
Anaconda Python distributions.

Creating a new Python environment for Cate will require around 2.2 GB disk space on Linux/Darwin and and 1.2 
GB on Windows. To create a new Conda environment `cate-env` in your Anaconda/Miniconda installation directory, type:

    $ conda env create --file environment.yml

If you want the environment to be installed in another location, e.g. due to disk space limitations, type:

    $ conda env create --file environment.yml --prefix some/other/location/for/cate

Next step is to activate the new environment. On Linux/Darwin type:

    $ source activate cate-env

In case you used another location use it instead of the name `cate`.
Windows users can omit the `source` command and just type

    > activate cate-env

You can now safely install Cate sources into the new `cate-env` environment.
    
    (cate) $ python setup.py install
    
### Using Standard Python 

If you run it with the [standard CPython](https://www.python.org/downloads/) installation,
make sure you use a 64-bit version. Cate relies on new Python language features and therefore 
requires Python 3.5+.

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

The most up-to-date list of module requirements is found in the project's `environment.yml` file.

To install Cate into an existing Python 3.5+ environment just for the current user, use

    $ python3 setup.py install --user
    
To install Cate for development and for the current user, use

    $ python3 setup.py develop --user

There is a **known issue on Windows** when installing into an existing Python environment. Installation may
fail due to an unresolved dependency to the `h5py` package, which expects pre-installed 
HDF-5 C-libraries to be present on your computer. You may get around this by pre-installing the Cate dependencies (which you'll find in `setup.py`) 
on your own, for example by using Christoph Gohlke's 
[Unofficial Windows Binaries for Python Extension Packages](http://www.lfd.uci.edu/~gohlke/pythonlibs/).

## Getting started

To test the installation, first run the Cate command-line interface. Type
    
    $ cate -h

IPython notebooks for various Cate use cases are on the way, they will appear in the project's
[notebooks](https://github.com/CCI-Tools/cate/tree/master/notebooks) folder.

To use them interactively, you'll need to install Jupyter and run its Notebook app:

    $ conda install jupyter
    $ jupyter notebook

Open the `notebooks` folder and select a use case.

## Conda Deployment

There is a dedicated repository [cate-conda](https://github.com/CCI-Tools/cate-conda)
which provides scripts and configuration files to build Cate's Conda packages and a stand-alone installer.

## Development

### Contributors

Contributors are asked to read and adhere to our [Developer Guide](https://github.com/CCI-Tools/cate/wiki/Developer-Guide).

### Unit-testing

For unit testing we use `pytest` and its coverage plugin `pytest-cov`.

To run the unit-tests with coverage, type

    $ export NUMBA_DISABLE_JIT=1
    $ py.test --cov=cate test
    
We need to set environment variable `NUMBA_DISABLE_JIT` to disable JIT compilation by `numba`, so that 
coverage reaches the actual Python code. We use Numba's JIT compilation to speed up numeric Python 
number crunching code.

### Generating the Documentation

We use the wonderful [Sphinx](http://www.sphinx-doc.org/en/stable/rest.html) tool to generate 
Cate's documentation on [ReadTheDocs](http://ect-core.readthedocs.io/en/latest/?badge=latest). 
If there is a need to build the docs locally, some 
additional software packages are required:

    $ conda install sphinx sphinx_rtd_theme mock
    $ conda install -c conda-forge sphinx-argparse
    $ pip install sphinx_autodoc_annotation

To regenerate the HTML docs, type    
    
    $ cd doc
    $ make html

## License

The CCI Toolbox is distributed under terms and conditions of the [MIT license](https://opensource.org/licenses/MIT).
