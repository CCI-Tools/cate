#!/usr/bin/env python3

# The MIT License (MIT)
# Copyright (c) 2016 by the Cate Development Team and contributors
#
# Permission is hereby granted, free of charge, to any person obtaining a copy of
# this software and associated documentation files (the "Software"), to deal in
# the Software without restriction, including without limitation the rights to
# use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies
# of the Software, and to permit persons to whom the Software is furnished to do
# so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.


from setuptools import setup, find_packages

packages = find_packages(exclude=["test", "test.*"])

# Same effect as "from cate import __version__", but avoids importing cate:
__version__ = None
with open('cate/version.py') as f:
    exec(f.read())

setup(
    name="cate",
    version=__version__,
    description='ESA CCI Toolbox (Cate) Python Core',
    license='MIT',
    author='Cate Development Team',
    packages=packages,
    data_files=[('cate/ds', ['cate/ds/esa_cci_ftp.json'])],
    entry_points={
        'console_scripts': [
            'cate = cate.cli.main:main',
            'cate-webapi = cate.webapi.main:main',
        ],
        'cate_plugins': [
            'cate_ops = cate.ops:cate_init',
            'cate_ds = cate.ds:cate_init',
        ],
    },
    install_requires=[
                      'matplotlib >= 1.5',
                      'xarray >= 0.8',
                      'netcdf4 >= 1.2',
                      'dask >= 0.8',
                      'tornado >= 4.4',
                      'numba >= 0.26',
                      'numpy >= 1.7',
                      'scipy >= 0.17',
                      'pillow >= 3.4',
                      'pandas >= 0.18',
                      'jdcal >= 1.3',
                      ],
)
