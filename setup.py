from setuptools import setup, find_packages

setup(
    name="ect-core",
    version="0.1.0",
    description='ESA CCI Toolbox Python Core',
    license='GPL 3',
    author='ESA CCI Toolbox Development Team',
    packages=find_packages('src'),
    package_dir={'': 'src'},
    entry_points={
        'console_scripts': [
            'ect-cli = ect.core.cli:main',
        ],
        'ect_plugins': [
            'example_plugin = ect.core.plugin:ExamplePlugin',
        ],
    },
    install_requires=['h5py >= 2.5',
                      'numpy >= 1.7',
                      'scipy >= 0.17',
                      'matplotlib >= 1.5',
                      'numexpr >= 2.5',
                      'dask >= 0.8',
                      'xarray >= 0.7'],
)
