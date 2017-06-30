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

import json
import os.path
from abc import ABCMeta

import fiona
import geopandas as gpd
import pandas as pd
import xarray as xr

from cate.core.objectio import OBJECT_IO_REGISTRY, ObjectIO
from cate.core.op import OP_REGISTRY, op_input, op
from cate.core.types import VarNamesLike, TimeRangeLike, PolygonLike, DictLike, FileLike
from cate.ops.harmonize import harmonize as harmonize_op

_ALL_FILE_FILTER = dict(name='All Files', extensions=['*'])


@op(tags=['input'])
@op_input('ds_name')
@op_input('time_range', data_type=TimeRangeLike)
@op_input('region', data_type=PolygonLike)
@op_input('var_names', data_type=VarNamesLike)
def open_dataset(ds_name: str,
                 time_range: TimeRangeLike.TYPE = None,
                 region: PolygonLike.TYPE = None,
                 var_names: VarNamesLike.TYPE = None,
                 harmonize: bool = True) -> xr.Dataset:
    """
    Open a dataset from a data source identified by *ds_name*.

    :param ds_name: The name of data source.
    :param time_range: Optional time range of the requested dataset
    :param region: Optional spatial region of the requested dataset
    :param var_names: Optional names of variables of the requested dataset
    :param monitor: a progress monitor, used only if *sync* is ``True``.
    :param harmonize: Whether to harmonize the dataset upon opening
    :return: An new dataset instance.
    """
    import cate.core.ds
    ds = cate.core.ds.open_dataset(data_source=ds_name, time_range=time_range,
                                   var_names=var_names, region=region)
    if ds and harmonize:
        return harmonize_op(ds)

    return ds


# noinspection PyShadowingBuiltins
@op(tags=['output'], no_cache=True)
@op_input('ds')
@op_input('file', file_open_mode='w', file_filters=[dict(name='NetCDF', extensions=['nc']), _ALL_FILE_FILTER])
@op_input('format', value_set=['NETCDF4', 'NETCDF4_CLASSIC', 'NETCDF3_64BIT', 'NETCDF3_CLASSIC'])
def save_dataset(ds: xr.Dataset, file: str, format: str = None):
    """
    Save a dataset to NetCDF file.

    :param ds: The dataset
    :param file: File path
    :param format: NetCDF format flavour, one of 'NETCDF4', 'NETCDF4_CLASSIC', 'NETCDF3_64BIT', 'NETCDF3_CLASSIC'.
    """
    ds.to_netcdf(file, format=format)


# noinspection PyShadowingBuiltins
@op(tags=['input'])
@op_input('file', file_open_mode='r')
@op_input('format')
def read_object(file: str, format: str = None) -> object:
    """
    Read a data object from a file.

    :param file: The file path.
    :param format: Optional format name.
    :return: The data object.
    """
    import cate.core.objectio
    obj, _ = cate.core.objectio.read_object(file, format_name=format)
    return obj


# noinspection PyShadowingBuiltins
@op(tags=['output'], no_cache=True)
@op_input('obj')
@op_input('file', file_open_mode='w', file_filters=[_ALL_FILE_FILTER])
@op_input('format')
def write_object(obj, file: str, format: str = None):
    """
    Read a data object from a file.

    :param file: The file path.
    :param format: Optional format name.
    :return: The data object.
    """
    import cate.core.objectio
    cate.core.objectio.write_object(obj, file, format_name=format)


@op(tags=['input'])
@op_input('file', file_open_mode='r', file_filters=[dict(name='Plain Text', extensions=['txt']), _ALL_FILE_FILTER])
@op_input('encoding')
def read_text(file: str, encoding: str = None) -> str:
    """
    Read a string object from a text file.

    :param file: The text file path.
    :param encoding: Optional encoding, e.g. "utc-8".
    :return: The string object.
    """
    if isinstance(file, str):
        with open(file, 'r', encoding=encoding) as fp:
            return fp.read()
    else:
        # noinspection PyUnresolvedReferences
        return file.read()


@op(tags=['output'], no_cache=True)
@op_input('obj')
@op_input('file', file_open_mode='w', file_filters=[dict(name='Plain Text', extensions=['txt']), _ALL_FILE_FILTER])
@op_input('encoding')
def write_text(obj: object, file: str, encoding: str = None):
    """
    Write an object as string to a text file.

    :param obj: The data object.
    :param file: The text file path.
    :param encoding: Optional encoding, e.g. "utc-8".
    """
    if isinstance(file, str):
        with open(file, 'w', encoding=encoding) as fp:
            fp.write(str(obj))
    else:
        # noinspection PyUnresolvedReferences
        return file.write(str(obj))


@op(tags=['input'])
@op_input('file', file_open_mode='r', file_filters=[dict(name='JSON', extensions=['json']), _ALL_FILE_FILTER])
@op_input('encoding')
def read_json(file: str, encoding: str = None) -> object:
    """
    Read a data object from a JSON text file.

    :param file: The JSON file path.
    :param encoding: Optional encoding, e.g. "utc-8".
    :return: The data object.
    """
    if isinstance(file, str):
        with open(file, 'r', encoding=encoding) as fp:
            return json.load(fp)
    else:
        return json.load(file)


@op(tags=['output'], no_cache=True)
@op_input('obj')
@op_input('file', file_open_mode='w', file_filters=[dict(name='JSON', extensions=['json']), _ALL_FILE_FILTER])
@op_input('encoding')
@op_input('indent')
def write_json(obj: object, file: str, encoding: str = None, indent: str = None):
    """
    Write a data object to a JSON text file. Note that the data object must be JSON-serializable.

    :param obj: A JSON-serializable data object.
    :param file: The JSON file path.
    :param encoding: Optional encoding, e.g. "utf-8".
    :param indent: indent used in the file, e.g. "  " (two spaces).
    """
    if isinstance(file, str):
        with open(file, 'w', encoding=encoding) as fp:
            json.dump(obj, fp, indent=indent)
    else:
        return json.dump(obj, file, indent=indent)


@op(tags=['input'])
@op_input('file',
          data_type=FileLike,
          file_open_mode='r',
          file_filters=[dict(name='CSV', extensions=['csv', 'txt']), _ALL_FILE_FILTER])
@op_input('delimiter', nullable=True)
@op_input('delim_whitespace', nullable=True)
@op_input('quotechar', nullable=True)
@op_input('comment', nullable=True)
@op_input('index_col', nullable=True)
def read_csv(file: FileLike.TYPE,
             delimiter: str = ',',
             delim_whitespace: bool = False,
             quotechar: str = None,
             comment: str = None,
             index_col: str = None,
             more_args: DictLike.TYPE = None) -> pd.DataFrame:
    """
    Read comma-separated values (CSV) from plain text file into a Pandas DataFrame.

    :param file: The CSV file path.
    :param delimiter: Delimiter to use. If delimiter is None, will try to automatically determine this.
    :param delim_whitespace: Specifies whether or not whitespaces will be used as delimiter.
           If this option is set, nothing should be passed in for the delimiter parameter.
    :param quotechar: The character used to denote the start and end of a quoted item.
           Quoted items can include the delimiter and it will be ignored.
    :param comment: Indicates remainder of line should not be parsed.
           If found at the beginning of a line, the line will be ignored altogether.
           This parameter must be a single character.
    :param index_col: The name of the column that provides unique identifiers
    :param more_args: Other optional pandas.read_csv() keyword arguments
    :return: The DataFrame object.
    """
    # The following code is needed, because Pandas treats any kw given in kwargs as being set, even if just None.
    kwargs = DictLike.convert(more_args)
    if kwargs is None:
        kwargs = {}
    if delimiter:
        kwargs.update(delimiter=delimiter)
    if delim_whitespace:
        kwargs.update(delim_whitespace=delim_whitespace)
    if quotechar:
        kwargs.update(quotechar=quotechar)
    if comment:
        kwargs.update(comment=comment)
    if index_col:
        kwargs.update(index_col=index_col)
    return pd.read_csv(file, **kwargs)


@op(tags=['input'])
@op_input('file', file_open_mode='r', file_filters=[dict(name='ESRI Shapefiles', extensions=['shp']),
                                                    dict(name='GeoJSON', extensions=['json', 'geojson']),
                                                    _ALL_FILE_FILTER])
def read_geo_data_frame(file: str) -> gpd.GeoDataFrame:
    """
    Returns a GeoDataFrame from a file.

    :param file: Is either the absolute or relative path to the file to be
    opened
    :return: A GeoDataFrame
    """
    return gpd.read_file(file, mode="r", enabled_drivers=['GeoJSON', 'ESRI Shapefile'])


@op(tags=['input'])
@op_input('file', file_open_mode='r', file_filters=[dict(name='ESRI Shapefiles', extensions=['shp']),
                                                    dict(name='GeoJSON', extensions=['json', 'geojson']),
                                                    _ALL_FILE_FILTER])
def read_geo_data_collection(file: str) -> fiona.Collection:
    """
    Returns a GeoDataFrame from a file.

    :param file: Is either the absolute or relative path to the file to be
    opened
    :return: A GeoDataFrame
    """
    return fiona.open(file, mode="r")


@op(tags=['input'])
@op_input('file', file_open_mode='r', file_filters=[dict(name='NetCDF', extensions=['nc'])])
@op_input('drop_variables', data_type=VarNamesLike)
@op_input('decode_cf')
@op_input('decode_times')
@op_input('engine')
def read_netcdf(file: str,
                drop_variables: VarNamesLike.TYPE = None,
                decode_cf: bool = True, decode_times: bool = True,
                engine: str = None) -> xr.Dataset:
    """
    Read a dataset from a netCDF 3/4 or HDF file.

    :param file: The netCDF file path.
    :param drop_variables: List of variables to be dropped.
    :param decode_cf: Whether to decode CF attributes and coordinate variables.
    :param decode_times: Whether to decode time information (convert time coordinates to ``datetime`` objects).
    :param engine: Optional netCDF engine name.
    """
    drop_variables = VarNamesLike.convert(drop_variables)
    return xr.open_dataset(file, drop_variables=drop_variables,
                           decode_cf=decode_cf, decode_times=decode_times, engine=engine)


@op(tags=['output'], no_cache=True)
@op_input('obj')
@op_input('file', file_open_mode='w', file_filters=[dict(name='NetCDF 3', extensions=['nc'])])
@op_input('engine')
def write_netcdf3(obj: xr.Dataset, file: str, engine: str = None):
    """
    Write a data object to a netCDF 3 file. Note that the data object must be netCDF-serializable.

    :param obj: A netCDF-serializable data object.
    :param file: The netCDF file path.
    :param engine: Optional netCDF engine to be used
    """
    obj.to_netcdf(file, format='NETCDF3_64BIT', engine=engine)


@op(tags=['output'], no_cache=True)
@op_input('obj')
@op_input('file', file_open_mode='w', file_filters=[dict(name='NetCDF 4', extensions=['nc'])])
@op_input('engine')
def write_netcdf4(obj: xr.Dataset, file: str, engine: str = None):
    """
    Write a data object to a netCDF 4 file. Note that the data object must be netCDF-serializable.

    :param obj: A netCDF-serializable data object.
    :param file: The netCDF file path.
    :param engine: Optional netCDF engine to be used
    """
    obj.to_netcdf(file, format='NETCDF4', engine=engine)


# noinspection PyAbstractClass
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
        return OP_REGISTRY.get_op('read_text')

    @property
    def write_op(self):
        return OP_REGISTRY.get_op('write_text')

    def read_fitness(self, file):
        # Basically every object can be written to a text file: str(obj)
        return 1 if isinstance(file, str) and os.path.isfile(file) else 0

    def write_fitness(self, obj):
        return 1000 if isinstance(obj, str) else 1


# noinspection PyAbstractClass
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
        return OP_REGISTRY.get_op('read_json')

    @property
    def write_op(self):
        return OP_REGISTRY.get_op('write_json')

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
        # noinspection PyBroadException
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


# noinspection PyAbstractClass
class NetCDF3ObjectIO(NetCDFObjectIO):
    @property
    def description(self):
        return "netCDF 3 file format, which fully supports 2+ GB files."

    @property
    def format_name(self):
        return 'NETCDF3'

    @property
    def read_op(self):
        return OP_REGISTRY.get_op('read_netcdf')

    @property
    def write_op(self):
        return OP_REGISTRY.get_op('write_netcdf3')


# noinspection PyAbstractClass
class NetCDF4ObjectIO(NetCDFObjectIO):
    @property
    def description(self):
        return "netCDF 4 file format (HDF5 file format, using netCDF 4 API features)"

    @property
    def format_name(self):
        return 'NETCDF4'

    @property
    def read_op(self):
        return OP_REGISTRY.get_op('read_netcdf')

    @property
    def write_op(self):
        return OP_REGISTRY.get_op('write_netcdf4')


OBJECT_IO_REGISTRY.object_io_list.extend([
    TextObjectIO(),
    JsonObjectIO(),
    NetCDF4ObjectIO(),
    NetCDF3ObjectIO()
])
