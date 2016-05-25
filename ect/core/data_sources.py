from datetime import datetime
from glob import glob

import pandas as pd
import xarray as xr

import ect.core.io as io
from ect.core.cdm_xarray import XArrayDatasetAdapter


def open_xarray_dataset(paths, chunks=None, concat_dim=None, preprocess=None, combine=None, engine=None,
                        **kwargs):
    '''
        Adapted version of the xarray 'open_mfdataset' function.
    '''
    if isinstance(paths, str):
        paths = sorted(glob(paths))
    if not paths:
        raise IOError('no files to open')

    # open all datasets
    lock = xr.backends.api._default_lock(paths[0], None)
    datasets = [xr.open_dataset(p, engine=engine, chunks=chunks, lock=lock, **kwargs) for p in paths]
    file_objs = [ds._file_obj for ds in datasets]

    preprocessed_datasets = datasets
    if preprocess is not None:
        # pre-process datasets
        preprocessed_datasets = []
        file_objs = []
        for ds in datasets:
            pds = preprocess(ds)
            if (pds is not None):
                preprocessed_datasets.append(pds)
                file_objs.append(pds._file_obj)
            else:
                ds._file_obj.close()

    # combine datasets into a single
    if combine is not None:
        combined_ds = combine(datasets)
    else:
        combined_ds = xr.auto_combine(datasets, concat_dim=concat_dim)

    combined_ds._file_obj = xr.backends.api._MultiFileCloser(file_objs)
    return combined_ds


def extract_time_index(ds: xr.Dataset) -> datetime:
    time_coverage_start = ds.attrs['time_coverage_start']
    time_coverage_end = ds.attrs['time_coverage_end']
    try:
        # print(time_coverage_start, time_coverage_end)
        time_start = datetime.strptime(time_coverage_start, "%Y%m%dT%H%M%SZ")
        time_end = datetime.strptime(time_coverage_end, "%Y%m%dT%H%M%SZ")
        return time_end
    except ValueError:
        return None


class XarrayDataSource(io.DataSource):
    def __init__(self, name, glob):
        super(XarrayDataSource, self).__init__(name, glob)


class AerosolMonthlyDataSource(XarrayDataSource):
    def __init__(self, glob):
        super(AerosolMonthlyDataSource, self).__init__("aerosol_monthly", glob)

    def open_dataset(self, **constraints) -> io.Dataset:
        def combine(datasets):
            time_index = [extract_time_index(ds) for ds in datasets]
            return xr.concat(datasets, pd.Index(time_index, name='time'))

        xarray_dataset = open_xarray_dataset(self.glob, combine=combine)
        return XArrayDatasetAdapter(xarray_dataset)
