<img alt="Cate: ESA CCI Toolbox" align="right" src="https://raw.githubusercontent.com/CCI-Tools/cate/master/doc/source/_static/logo/cci-toolbox-logo-latex.jpg" />

[![Build status](https://ci.appveyor.com/api/projects/status/leugvo8fq7nx6kym/branch/master?svg=true)](https://ci.appveyor.com/project/ccitools/cate-core)
[![GH actions Build status](https://github.com/CCI-Tools/cate/actions/workflows/test.yml/badge.svg)](https://github.com/CCI-Tools/cate/actions/workflows/test.yml/badge.svg)
[![codecov.io](https://codecov.io/github/CCI-Tools/cate/coverage.svg?branch=master)](https://codecov.io/github/CCI-Tools/cate?branch=master)
[![Documentation Status](https://readthedocs.org/projects/cate/badge/?version=latest)](http://cate.readthedocs.io/en/latest/?badge=latest)
                
# cate

ESA CCI Toolbox (Cate) Python package, API and CLI.

## Installation

Cate can be installed into a new or existing Python 3.7 [Miniconda](http://conda.pydata.org/miniconda.html) 
or [Anaconda](https://www.continuum.io/downloads) environment as follows:

    $ conda install -c ccitools cate-cli

## Installation from Sources

Cate's sources (this repository) are organised as follows:

* `setup.py` - main build script to be run with Python 3.6+
* `cate/` - main package and production code
* `test/` - test package and test code
* `doc/` - documentation in Sphinx/RST format

We recommend installing Cate into an isolated Python 3 environment, because this
approach avoids clashes with existing versions of Cate's 3rd-party Python package requirements. 
Using [Miniconda](http://conda.pydata.org/miniconda.html) 
or [Anaconda](https://www.continuum.io/downloads) will usually avoid platform-specific 
issues caused by module native binaries.

The first step is to clone latest Cate code and step into the check out directory: 

    $ git clone https://github.com/CCI-Tools/cate.git
    $ cd cate


### Using Conda

[Conda](https://conda.io/docs/intro.html) is the package manager used by the Miniconda or 
Anaconda Python distributions.

Creating a new Python environment for Cate will require around 2.2 GB disk space on Linux/Darwin and and 1.2 
GB on Windows. To create a new Conda environment `cate-env` in your Anaconda/Miniconda installation directory, type:

    $ conda env create

If you want the environment to be installed in another location, e.g. due to disk space limitations, type:

    $ conda env create --prefix some/other/location/for/cate

Next step is to activate the new environment. 

    $ conda activate cate-env

You can now safely install Cate sources into the new `cate-env` environment.
    
    (cate-env) $ python setup.py install
    
## Using Docker

You can also use pre-build Docker images that contain a Python environment with the 
`cate` package already installed. The images are `quay.io/bcdev/cate:<version>`. E.g. 

    $ docker run -d -v ${my_local_dir}:/home/cate quay.io/bcdev/cate:2.1.1 bash
    (cate-env) $ cate -h  

where `${my_local_dir}` refers to any directory on your computer that you may want to access from 
within the running Docker container. 

## Getting started

To test the installation, first run the Cate command-line interface. Type
    
    $ cate -h

IPython notebooks for various Cate use cases are on the way, they will appear in the project's
[notebooks](https://github.com/CCI-Tools/cate/tree/master/notebooks) folder.

To use them interactively, you'll need to install Jupyter and run its Notebook app:

    $ conda install jupyter
    $ jupyter notebook

Open the `notebooks` folder and select a use case.

## Running Cate App in Stand-Alone mode

To run the the graphical user interface [Cate App](https://github.com/CCI-Tools/cate-webui) in 
stand-alone mode you'll need to start a _Cate Web API service_. To do so, first install the `cate` 
Python package as described above. Then Cate Web API service is started from the command-line. 
To run the service on port 9090 on your local computer, type:

    $ cate-webapi-start --port 9090 

Then open Cate App in a browser and enter the URL `http://localhost:9090`. Press the
"Cate Stand-Alone Mode" button above. This will launch the Cate App in stand-alone mode.
If you wish to run a service with limited file system access (sandboxed), 
you can specify the `root` option that defines a new file system root:

    $ cate-webapi-start --port 9090 --root /home/fritz
    
Use CTRL+C or the command

    $ cate-webapi-stop --port 9090

to stop the service.

To run the service from the docker image, type:

    $ docker run -it -v ${my_local_dir}:/home/cate -p 9090:4000 quay.io/bcdev/cate:2.1.1 bash
    (cate-env) $ cate-webapi-start --port 4000 --root ${my_local_dir}    
    
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

Other recognized environment variables to customize the unit-level tests are

    CATE_DISABLE_WEB_TESTS=1
    CATE_DISABLE_PLOT_TESTS=1
    CATE_DISABLE_GEOPANDAS_TESTS=1
    CATE_DISABLE_CLI_UPDATE_TESTS=1

### Generating the Documentation

We use the wonderful [Sphinx](http://www.sphinx-doc.org/en/stable/rest.html) tool to generate 
Cate's documentation on [ReadTheDocs](https://cate.readthedocs.io/en/latest/index.html). 
If there is a need to build the docs locally, first create a Conda environment:

    $ cd cate
    $ conda env create -f environment-rtd.yml

To regenerate the HTML docs, type    
    
    $ cd doc
    $ make html

## License

The CCI Toolbox is distributed under terms and conditions of the [MIT license](https://opensource.org/licenses/MIT).
