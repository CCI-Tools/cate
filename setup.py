from setuptools import setup

# Same effect as "from ect import __version__", but avoids importing ect:
with open('ect/version.py') as f:
    exec(f.read())

setup(
    name="ect-core",
    version=__version__,
    description='ESA CCI Toolbox Python Core',
    license='GPL 3',
    author='ESA CCI Toolbox Development Team',
    packages=['ect'],
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
                      'dask >= 0.8',
                      'xarray >= 0.7',
                      ],
)
