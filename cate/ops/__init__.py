# The MIT License (MIT)
# Copyright (c) 2016, 2017 by the ESA CCI Toolbox development team and contributors
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

"""
Description
===========

The ``ops`` package comprises all specific operation and processor implementations.

This is a plugin package automatically imported by the installation script's entry point ``cate_ops``
(see the projects ``setup.py`` file).

Verification
============

The module's unit-tests are located in `test/ops <https://github.com/CCI-Tools/cate/blob/master/test/ops>`_ and
may be executed using ``$ py.test test/ops/test_<MODULE>.py --cov=cate/ops/<MODULE>.py`` for extra code coverage
information.

Functions
==========
"""
# We need cate_init being accessible to use by the plugin registering logic
# before any attempt to import any of the submodules is made. See Issue #148


def cate_init():
    # Plugin initializer.
    # Left empty because operations are registered automatically via decorators.
    pass


from .select import select_var
from .coregistration import coregister
from .correlation import pearson_correlation_scalar, pearson_correlation
from .normalize import normalize, adjust_temporal_attrs, adjust_spatial_attrs
from .io import open_dataset, save_dataset, read_object, write_object, read_text, write_text, read_json, write_json, \
    read_csv, read_geo_data_frame, read_geo_data_collection, read_netcdf, write_netcdf3, write_netcdf4
from .plot import plot_map, plot, plot_data_frame
from .resampling import resample_2d, downsample_2d, upsample_2d
from .subset import subset_spatial, subset_temporal, subset_temporal_index
from .timeseries import tseries_point, tseries_mean
from .utility import sel, from_dataframe, identity, literal, pandas_fillna
from .aggregate import long_term_average, temporal_aggregation
from .arithmetics import ds_arithmetics, diff
from .anomaly import anomaly_internal, anomaly_external
from .index import enso, enso_nino34, oni
from .outliers import detect_outliers
from .fat import fat_min, fat_max, fat_query


__all__ = [
    # .timeseries
    'tseries_point',
    'tseries_mean',
    # .resampling
    'resample_2d',
    'downsample_2d',
    'upsample_2d',
    # .normalize
    'normalize',
    'adjust_temporal_attrs',
    'adjust_spatial_attrs',
    # .select
    'select_var',
    # .coregistration
    'coregister',
    # .subset
    'subset_spatial',
    'subset_temporal',
    'subset_temporal_index',
    # .correlation
    'pearson_correlation_scalar',
    'pearson_correlation',
    # .plot
    'plot_map',
    'plot',
    'plot_data_frame',
    # .io
    'open_dataset',
    'save_dataset',
    'read_object',
    'write_object',
    'read_text',
    'write_text',
    'read_json',
    'write_json',
    'read_csv',
    'read_geo_data_frame',
    'read_geo_data_collection',
    'read_netcdf',
    'write_netcdf3',
    'write_netcdf4',
    # .utility
    'sel',
    'from_dataframe',
    'identity',
    'literal',
    'pandas_fillna',
    # .aggregate
    'long_term_average',
    'temporal_aggregation',
    # .arithmetics
    'ds_arithmetics',
    'diff',
    # .anomaly
    'anomaly_internal',
    'anomaly_external',
    # .index
    'enso_nino34',
    'enso',
    'oni',
    # .outliers
    'detect_outliers',
]
