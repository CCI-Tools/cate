import json
import os.path
from abc import ABCMeta
from typing import Any

import xarray as xr
from ect.core.io import open_dataset
from ect.core.objectio import OBJECT_IO_REGISTRY, ObjectIO
from ect.core.op import op_input, op_return, op


@op(tags='io')
@op_input('data_source', required=True)
@op_input('start_date', required=True)
@op_input('end_date', required=True)
def load_dataset(data_source: str, start_date: str, end_date: str) -> xr.Dataset:
    return open_dataset(data_source, (start_date, end_date))


@op(tags=['io'])
@op_input('dataset', required=True)
@op_input('file', required=True)
def store_dataset(dataset: xr.Dataset, file: str):
    dataset.to_netcdf(file)


@op(tags='io')
@op_input('file', required=True, data_type=str)
@op_input('encoding', data_type=str)
@op_return(data_type=str)
def read_text(file, encoding=None):
    if isinstance(file, str):
        with open(file, 'r', encoding=encoding) as fp:
            return fp.read()
    else:
        return file.read()


@op(tags='io')
@op_input('obj', required=True, data_type=Any)
@op_input('file', required=True, data_type=str)
@op_input('encoding', data_type=str)
def write_text(obj, file, encoding=None):
    if isinstance(file, str):
        with open(file, 'w', encoding=encoding) as fp:
            fp.write(str(obj))
    else:
        return file.write(str(obj))


@op(tags='io')
@op_input('file', required=True, data_type=str)
@op_input('encoding', data_type=str)
@op_return(data_type=Any)
def read_json(file, encoding=None):
    if isinstance(file, str):
        with open(file, 'r', encoding=encoding) as fp:
            return json.load(fp)
    else:
        return json.load(file)


@op(tags='io')
@op_input('obj', required=True, data_type=Any)
@op_input('file', required=True, data_type=str)
@op_input('encoding', data_type=str)
@op_input('indent', data_type=str)
@op_input('separators', data_type=str)
def write_json(obj, file, encoding=None, indent=None, separators=None):
    if isinstance(file, str):
        with open(file, 'w', encoding=encoding) as fp:
            json.dump(obj, fp, indent=indent, separators=separators)
    else:
        return json.dump(obj, file, indent=indent, separators=separators)


@op(tags='io')
@op_input('file', required=True, data_type=str)
@op_input('drop_variables', data_type=str)
@op_input('decode_cf', data_type=bool)
@op_input('decode_times', data_type=bool)
@op_input('engine', data_type=str)
@op_return(data_type=xr.Dataset)
def read_netcdf(file, drop_variables=None, decode_cf=True, decode_times=True, engine=None):
    return xr.open_dataset(file, drop_variables=drop_variables,
                           decode_cf=decode_cf, decode_times=decode_times, engine=engine)


@op(tags='io')
@op_input('obj', required=True, data_type=xr.Dataset)
@op_input('file', required=True, data_type=str)
@op_input('engine', data_type=str)
def write_netcdf3(obj, file, engine=None):
    obj.to_netcdf(file, format='NETCDF3_64BIT', engine=engine)


@op(tags='io')
@op_input('obj', required=True, data_type=xr.Dataset)
@op_input('file', required=True, data_type=str)
@op_input('engine', data_type=str)
def write_netcdf4(obj, file, engine=None):
    obj.to_netcdf(file, format='NETCDF4', engine=engine)


class TextObjectIO(ObjectIO):
    @property
    def description(self):
        return "Plain text format"

    @property
    def format_name(self):
        return 'TEXT'

    @property
    def filename_ext(self):
        return '.txt'

    @property
    def read_op(self):
        return read_text

    @property
    def write_op(self):
        return write_text

    def read_fitness(self, file):
        # Basically every object can be written to a text file: str(obj)
        return 1 if isinstance(file, str) and os.path.isfile(file) else 0

    def write_fitness(self, obj):
        return 1000 if isinstance(obj, str) else 1


class JsonObjectIO(ObjectIO):
    @property
    def description(self):
        return 'JSON format (plain text, UTF8)'

    @property
    def format_name(self):
        return 'JSON'

    @property
    def filename_ext(self):
        return '.json'

    @property
    def read_op(self):
        return read_json

    @property
    def write_op(self):
        return write_json

    def read_fitness(self, file):
        return 1 if isinstance(file, str) and os.path.isfile(file) else 0

    def write_fitness(self, obj):
        return 1000 if isinstance(obj, str) \
                       or isinstance(obj, float) \
                       or isinstance(obj, int) \
                       or isinstance(obj, list) \
                       or isinstance(obj, dict) else 0


class NetCDFObjectIO(ObjectIO, metaclass=ABCMeta):
    @property
    def filename_ext(self):
        return '.nc'

    def read_fitness(self, file):
        try:
            dataset = self.read(file)
            dataset.close()
            return 100000
        except Exception:
            return -1

    def write_fitness(self, obj):
        # TODO (forman, 20160905): add support for numpy-like arrays
        return 100000 if isinstance(obj, xr.Dataset) or (hasattr(obj, 'to_netcdf') and callable(obj.to_netcdf)) \
            else 0


class NetCDF3ObjectIO(NetCDFObjectIO):
    @property
    def description(self):
        return "netCDF 3 file format, which fully supports 2+ GB files."

    @property
    def format_name(self):
        return 'NETCDF3'

    @property
    def read_op(self):
        return read_netcdf

    @property
    def write_op(self):
        return write_netcdf3


class NetCDF4ObjectIO(NetCDFObjectIO):
    @property
    def description(self):
        return "netCDF 4 file format (HDF5 file format, using netCDF 4 API features)"

    @property
    def format_name(self):
        return 'NETCDF4'

    @property
    def read_op(self):
        return read_netcdf

    @property
    def write_op(self):
        return write_netcdf4


OBJECT_IO_REGISTRY.object_io_list.extend([
    TextObjectIO(),
    JsonObjectIO(),
    NetCDF4ObjectIO(),
    NetCDF3ObjectIO()
])
