from datetime import datetime
from glob import glob
from typing import Sequence

import pandas as pd
import xarray as xr


def open_xarray_dataset(paths, chunks=None, **kwargs):
    """
    Adapted version of the xarray 'open_mfdataset' function.
    """
    if isinstance(paths, str):
        paths = sorted(glob(paths))
    if not paths:
        raise IOError('no files to open')

    # open all datasets
    engine = 'h5netcdf'
    lock = xr.backends.api._default_lock(paths[0], None)
    # TODO (mz) - get correct chunking from netcdf metadata
    datasets = [xr.open_dataset(p, engine=engine, chunks=chunks, lock=lock, **kwargs) for p in paths]

    preprocessed_datasets = []
    file_objs = []
    for ds in datasets:
        pds = _preprocess_datasets(ds)
        if pds is None:
            ds._file_obj.close()
        else:
            pds_decoded = xr.decode_cf(pds)
            preprocessed_datasets.append(pds_decoded)
            file_objs.append(ds._file_obj)

    combined_datasets = _combine_datasets(preprocessed_datasets)
    combined_datasets._file_obj = xr.backends.api._MultiFileCloser(file_objs)
    return combined_datasets


def _combine_datasets(datasets: Sequence[xr.Dataset]) -> xr.Dataset:
    """
    Combines all datasets into a single.
    """
    if len(datasets) == 0:
        raise ValueError()
    if 'time' in datasets[0].dim:
        xr.auto_combine(datasets, concat_dim='time')
    else:
        time_index = [_extract_time_index(ds) for ds in datasets]
        return xr.concat(datasets, pd.Index(time_index, name='time'))


def _preprocess_datasets(dataset: xr.Dataset) -> xr.Dataset:
    """
    Modifies datasets, so that it is netcdf-CF compliant
    """
    for var in dataset.data_vars:
        attrs = dataset[var].attrs
        if '_FillValue' in attrs and 'missing_value' in attrs:
            # xarray as of version 0.7.2 does not handle it correctly,
            # if both values are set to NaN. (because the values are compared using '==')
            # TODO (mz) - report github issue and PR to xarray
            del attrs['missing_value']
    return dataset


def _extract_time_index(ds: xr.Dataset) -> datetime:
    time_coverage_start = ds.attrs['time_coverage_start']
    time_coverage_end = ds.attrs['time_coverage_end']
    try:
        # print(time_coverage_start, time_coverage_end)
        time_start = datetime.strptime(time_coverage_start, "%Y%m%dT%H%M%SZ")
        time_end = datetime.strptime(time_coverage_end, "%Y%m%dT%H%M%SZ")
        return time_end
    except ValueError:
        return None
