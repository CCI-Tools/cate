import xarray as xr

from ect.core.cdm_xarray import XArrayDatasetAdapter
from ect.core.op import op_input
from ect import Dataset


@op_input('lat', value_range=[-90, 90])
@op_input('lon', value_range=[-180, 180])
@op_input('method', value_set=['nearest', 'ffill', 'bfill', None])
def timeseries(ds: Dataset, lat: float, lon: float, method: str='nearest') -> Dataset:
    if isinstance(ds, XArrayDatasetAdapter):
        wrapped_xarray = ds.wrapped_dataset
        xarray_timeseries = _xarray_timeseries(wrapped_xarray, lat=lat, lon=lon, method=method)
        return XArrayDatasetAdapter(xarray_timeseries)
    else:
        raise NotImplementedError('shapefiles are not supported currently')


def _xarray_timeseries(xarray: xr.Dataset, lat: float, lon: float, method: str) -> xr.Dataset:
    lat_dim = _get_lat_dim_name(xarray)
    lon_dim = _get_lon_dim_name(xarray)
    indexers = {lat_dim: lat, lon_dim: lon}
    return xarray.sel(method=method, **indexers)


def _get_lon_dim_name(xarray: xr.Dataset) -> str:
    return _get_dim_name(xarray, ['lon', 'longitude', 'long'])


def _get_lat_dim_name(xarray: xr.Dataset) -> str:
    return _get_dim_name(xarray, ['lat', 'latitude'])


def _get_dim_name(xarray: xr.Dataset, possible_names) -> str:
    for name in possible_names:
        if name in xarray.dims:
            return name
    return None
