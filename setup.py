from setuptools import setup, find_packages

packages = find_packages(exclude=["test", "test.*"])

# Same effect as "from ect import __version__", but avoids importing ect:
with open('ect/version.py') as f:
    exec(f.read())

setup(
    name="ect-core",
    version=__version__,
    description='ESA CCI Toolbox Python Core',
    license='GPL 3',
    author='ESA CCI Toolbox Development Team',
    packages=packages,
    data_files=[('ect/ds', ['ect/ds/esa_cci_ftp.json'])],
    entry_points={
        'console_scripts': [
            'ect = ect.ui.cli:main',
        ],
        'ect_plugins': [
            'ect_ops = ect.ops:ect_init',
            'ect_ds = ect.ds:ect_init',
        ],
    },
    install_requires=['xarray >= 0.8',
                      'netcdf4 >= 1.2.4',
                      'dask >= 0.8',
                      'numba >= 0.26',
                      'numpy >= 1.7',
                      'scipy >= 0.17',
                      'matplotlib >= 1.5',
                      ],
)
