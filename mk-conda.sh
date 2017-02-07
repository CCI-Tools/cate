#!/bin/bash
conda build --user ccitools -c conda-forge -c defaults recipes/cate-util
conda build --user ccitools -c conda-forge -c defaults recipes/cate-lib
conda build --user ccitools -c conda-forge -c defaults recipes/cate


