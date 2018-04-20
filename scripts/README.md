## Purpose

This `scripts` directory contains quickstart, example and utility scripts for CCI Toolbox development.

The `happy_path` scripts have been moved to [Cate E2E](https://github.com/CCI-Tools/cate-e2e) repository.

## Usage

All scripts assume that 

* this `scripts` directory is your current directory, and 
* a valid Python environment is active. 

The latter is ensured if you use the Cate CLI from the Cate installer. If you like to run
the scripts from Python sources, you will have to create a suitable Python environment first.
The Cate development team uses Conda and a new environment is created like so
  
    conda env create --file environment.yml
       
which will create a new Conda environment named `cate-env`.

To run any script on **Linux** and **OS X**:
 
    $ source activate cate-env
    $ python setup.py develop
    $ cd scripts
    
    $ ./<script-1>.sh
    $ ./<script-2>.sh
    $ ...
      
On **Windows**:

    > activate cate-env
    > python setup.py develop
    > cd scripts
    
    > <script-1>.bat
    > <script-2>.bat
    > ...
