[![Build Status](https://travis-ci.org/CCI-Tools/ect-core.svg?branch=master)](https://travis-ci.org/CCI-Tools/ect-core)
[![codecov.io](https://codecov.io/github/CCI-Tools/ect-core/coverage.svg?branch=master)](https://codecov.io/github/CCI-Tools/ect-core?branch=master)


# ect-core

This is the Python core of the ESA CCI Toolbox (ECT).

## Contents

* ``setup.py`` -- main build script to be run with Python 3.5
* ``ect/`` -- ECT main package and production code
* ``notebooks/`` -- various IPython notebooks demonstrating the ECT API
* ``test/`` -- ECT test package and test code
* ``doc/`` -- documentation in RST format

## Installation

ECT requires Python 3.5 and prefers a Miniconda or Anaconda environment.
ECT can be run from sources directly, once the following dependencies are resolved:

* ``xarray``
* ``numpy``
* ``scipy``
* ``matplotlib``
* ``numba``
* ``h5py``

To install it in your current Python development, use

    $ https://github.com/CCI-Tools/ect-core.git
    $ cd ect-core
    $ python3 setup.py install

To install it for development only, use

    $ python3 setup.py develop

## Get started

IPython notebooks for various ECT use cases are on the way, they will appear in
https://github.com/CCI-Tools/ect-core/tree/master/notebooks.

To use them interactively, you'll need to install Jupyter and run its Notebook app:

    $ conda install jupyter
    $ jupyter notebook
