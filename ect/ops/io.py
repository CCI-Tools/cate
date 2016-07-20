import xarray as xr

from ect.core.io import open_dataset
from ect.core.op import op_input


@op_input('data_source', required=True)
@op_input('start_date', required=True)
@op_input('end_date', required=True)
def load_dataset(data_source: str, start_date: str, end_date: str) -> xr.Dataset:
    return open_dataset(data_source, (start_date, end_date))


@op_input('data_set', required=True)
@op_input('output_file', required=True)
def save_dataset(data_set: xr.Dataset, output_file: str):
    if isinstance(data_set, xr.Dataset):
        data_set.to_netcdf(output_file)
    else:
        raise NotImplementedError('shapefiles are currently not supported')

