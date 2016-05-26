import pandas as pd
import xarray as xr

import ect.core.io as io
from ect.core.cdm_xarray import XArrayDatasetAdapter
from ect.core.io_xarray import XarrayDataSource, extract_time_index, open_xarray_dataset


class AerosolMonthlyDataSource(XarrayDataSource):
    def __init__(self, glob):
        super(AerosolMonthlyDataSource, self).__init__("aerosol_monthly", glob)

    def open_dataset(self, **constraints) -> io.Dataset:
        def combine(datasets):
            time_index = [extract_time_index(ds) for ds in datasets]
            return xr.concat(datasets, pd.Index(time_index, name='time'))

        xarray_dataset = open_xarray_dataset(self.glob, combine=combine)
        return XArrayDatasetAdapter(xarray_dataset)
