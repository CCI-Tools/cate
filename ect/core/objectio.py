"""
Description
===========

Naive implementation of generic :py:func:``read_object`` / :py:func:``write_object`` functions operating
on a global object I/O registry ``OBJECT_IO_REGISTRY``.

"""
import os.path
from abc import ABCMeta, abstractmethod
from collections import OrderedDict
from typing import Any

import xarray as xr

from .monitor import Monitor
from .op import OpRegistration, op_input, op_return


def find_reader(file, format_name=None, **kwargs):
    """
    Find a most suitable reader for given *file* and optional *format_name*.

    :param file: A file path
    :param format_name: An optional format name.
    :param kwargs: Specific reader arguments passed to ``read_fitness`` method of a registered
           :py:class:``ObjectIO``-compatible object
    :return: An :py:class:``ObjectIO``-compatible object or ``None``.
    """
    _, filename_ext = os.path.splitext(file)
    return OBJECT_IO_REGISTRY.find_reader(file, format_name=format_name, filename_ext=filename_ext, **kwargs)


def find_writer(obj, file, format_name=None, **kwargs):
    """
    Find a most suitable writer for a given object *obj*, *file* and optional *format_name*.

    :param obj: Any Python object
    :param file: A file path
    :param format_name: An optional format name.
    :param kwargs: Specific reader arguments passed to ``write_fitness`` method of a registered
           :py:class:``ObjectIO``-compatible object
    :return: An :py:class:``ObjectIO``-compatible object or ``None``.
    """
    _, filename_ext = os.path.splitext(file)
    return OBJECT_IO_REGISTRY.find_writer(obj, format_name=format_name, filename_ext=filename_ext, **kwargs)


def read_object(file, format_name=None, **kwargs):
    """
    Read an object from *file* using an optional *format_name*.

    :param file: The file path.
    :param format_name: name of the file format
    :param kwargs: additional read parameters
    :return: A tuple (*obj*, *reader*), where *obj* is the object read and *reader* is the reader used to read it.
    """
    reader = find_reader(file, format_name=format_name, **kwargs)
    if not reader:
        raise ValueError("no reader found for format '%s'" % format_name if format_name else "no reader found")
    obj = reader.read(file, **kwargs)
    return obj, reader


def write_object(obj, file, format_name=None, **kwargs):
    """
    Write an object *obj* to *file* using an optional *format_name*.

    :param obj: A Python object.
    :param file: The file path.
    :param format_name: name of the file format
    :param kwargs: additional write parameters
    :return: The writer used to write *obj*.
    """
    writer = find_writer(obj, file, format_name=format_name, **kwargs)
    if not writer:
        raise ValueError("no writer found for format '%s'" % format_name if format_name else "no writer found")
    writer.write(obj, file, **kwargs)
    return writer


@op_input('file', required=True, data_type=str)
@op_input('encoding', data_type=str)
@op_return(data_type=str)
def read_text(file, encoding=None):
    if isinstance(file, str):
        with open(file, 'r', encoding=encoding) as fp:
            return fp.read()
    else:
        return file.read()


@op_input('obj', required=True, data_type=Any)
@op_input('file', required=True, data_type=str)
@op_input('encoding', data_type=str)
def write_text(obj, file, encoding=None):
    if isinstance(file, str):
        with open(file, 'w', encoding=encoding) as fp:
            fp.write(str(obj))
    else:
        return file.write(str(obj))


@op_input('file', required=True, data_type=str)
@op_input('encoding', data_type=str)
@op_return(data_type=Any)
def read_json(file, encoding=None):
    import json
    if isinstance(file, str):
        with open(file, 'r', encoding=encoding) as fp:
            return json.load(fp)
    else:
        return json.load(file)


@op_input('obj', required=True, data_type=Any)
@op_input('file', required=True, data_type=str)
@op_input('encoding', data_type=str)
@op_input('indent', data_type=str)
@op_input('separators', data_type=str)
def write_json(obj, file, encoding=None, indent=None, separators=None):
    import json
    if isinstance(file, str):
        with open(file, 'w', encoding=encoding) as fp:
            json.dump(obj, fp, indent=indent, separators=separators)
    else:
        return json.dump(obj, file, indent=indent, separators=separators)


@op_input('file', required=True, data_type=str)
@op_input('drop_variables', data_type=str)
@op_input('decode_cf', data_type=bool)
@op_input('decode_times', data_type=bool)
@op_input('engine', data_type=str)
@op_return(data_type=xr.Dataset)
def read_netcdf(file, drop_variables=None, decode_cf=True, decode_times=True, engine=None):
    return xr.open_dataset(file, drop_variables=drop_variables,
                           decode_cf=decode_cf, decode_times=decode_times, engine=engine)


@op_input('obj', required=True, data_type=xr.Dataset)
@op_input('file', required=True, data_type=str)
@op_input('engine', data_type=str)
def write_netcdf3(obj, file, engine=None):
    obj.to_netcdf(file, format='NETCDF3_64BIT', engine=engine)


@op_input('obj', required=True, data_type=xr.Dataset)
@op_input('file', required=True, data_type=str)
@op_input('engine', data_type=str)
def write_netcdf4(obj, file, engine=None):
    obj.to_netcdf(file, format='NETCDF4', engine=engine)


class ObjectIORegistry:
    """
    Registry of :py:class::`ObjectIO`-like instances.
    """

    def __init__(self):
        self._object_io_list = []

    @property
    def object_io_list(self):
        return self._object_io_list

    def get_format_names(self, mode=None):
        if mode and mode not in ['r', 'w', 'rw']:
            raise ValueError('illegal mode')
        format_names = []
        for object_io in self._object_io_list:
            is_reader = object_io.read_op is not None
            is_writer = object_io.write_op is not None
            if not mode or (mode == 'r' and is_reader) or (mode == 'w' and is_writer) or (
                                mode == 'rw' and is_reader and is_writer):
                format_names.append(object_io.format_name)
        return sorted(format_names)

    def find_reader(self, file=None, format_name=None, filename_ext=None, default_reader=None):
        if not filename_ext and isinstance(file, str):
            _, filename_ext = os.path.splitext(file)
        return self.find_object_io(file=file,
                                   format_name=format_name, filename_ext=filename_ext,
                                   default_object_io=default_reader, mode='r')

    def find_writer(self, obj=None, format_name=None, filename_ext=None, default_writer=None):
        return self.find_object_io(obj=obj,
                                   format_name=format_name, filename_ext=filename_ext,
                                   default_object_io=default_writer, mode='w')

    def find_object_io(self, file=None, obj=None,
                       format_name=None, filename_ext=None, default_object_io=None, mode=None):

        object_io_list = self.find_object_ios(format_name, filename_ext, mode=mode)
        if not object_io_list:
            return default_object_io
        elif len(object_io_list) == 1:
            return object_io_list[0]

        object_io_fitnesses = OrderedDict()

        for object_io in object_io_list:
            fitness = 0
            try:
                if mode == 'r':
                    fitness = object_io.read_fitness(file)
                elif mode == 'w':
                    fitness = object_io.write_fitness(obj)
                elif mode == 'rw':
                    fitness = object_io.read_fitness(file) + object_io.write_fitness(obj)
            except AttributeError:
                fitness = -1
            if fitness >= 0:
                object_io_fitnesses[object_io] = fitness

        best_object_io = None
        max_fitness = 0
        for object_io, fitness in object_io_fitnesses.items():
            if fitness > max_fitness:
                best_object_io = object_io
                max_fitness = fitness

        return best_object_io if best_object_io else default_object_io

    def find_object_ios(self, format_name=None, filename_ext=None, mode=None):
        if mode and mode not in ['r', 'w', 'rw']:
            raise ValueError('illegal mode')

        if not mode:
            all_object_ios = self._object_io_list
        elif mode == 'r':
            all_object_ios = [object_io for object_io in self._object_io_list if object_io.read_op is not None]
        elif mode == 'w':
            all_object_ios = [object_io for object_io in self._object_io_list if object_io.write_op is not None]
        else:
            all_object_ios = [object_io for object_io in self._object_io_list if
                              object_io.read_op is not None and object_io.write_op is not None]

        if not format_name and not filename_ext:
            return all_object_ios

        if format_name:
            object_io_list = []
            for object_io in all_object_ios:
                try:
                    ok = object_io.format_name == format_name
                except AttributeError:
                    ok = False
                if ok:
                    object_io_list.append(object_io)
            all_object_ios = object_io_list

        if not filename_ext or len(all_object_ios) <= 1:
            return all_object_ios

        object_io_list = []
        for object_io in all_object_ios:
            try:
                ok = object_io.filename_ext == filename_ext
            except AttributeError:
                ok = False
            if ok:
                object_io_list.append(object_io)
        return object_io_list


OBJECT_IO_REGISTRY = ObjectIORegistry()


class ObjectIO(metaclass=ABCMeta):
    """
    Interface that objects in the ``OBJECT_IO_REGISTRY`` must adhere to.
    The same ``ObjectIO`` instance can represent both a data reader and data writer for a given file format.
    """

    @property
    @abstractmethod
    def description(self):
        pass

    @property
    @abstractmethod
    def filename_ext(self):
        pass

    @property
    @abstractmethod
    def format_name(self):
        pass

    @property
    def read_op(self) -> OpRegistration:
        return None

    @property
    def write_op(self) -> OpRegistration:
        return None

    def read(self, file, monitor: Monitor = Monitor.NULL, **kwargs):
        """
        Read data from *file*. 
        
        :param file: A file path name or file pointer as returned by the Python ``open()`` function.  
        :param monitor: A progress monitor
        :param kwargs: Reader-specific arguments
        :return: The object read
        :raise NotImplementedError: if the read operation is not supported
        """
        if self.read_op is None:
            raise NotImplementedError('read operation not supported by format "%s"' % self.format_name)
        # TODO: support monitor: return self.read_op(file, monitor=monitor, **kwargs)
        return self.read_op(file, **kwargs)

    def write(self, obj, file, monitor: Monitor = Monitor.NULL, **kwargs):
        """
        Write data to *file*. 

        :param obj: The Python object to write.  
        :param file: A file path name or file pointer as returned by the Python ``open()`` function.  
        :param monitor: A progress monitor
        :param kwargs: Writer-specific arguments
        :return: The object read
        :raise NotImplementedError: if the read operation is not supported
        """
        if self.write_op is None:
            raise NotImplementedError('write operation not supported by format "%s"' % self.format_name)
        # TODO: support monitor: self.write_op(obj, file, monitor=monitor, **kwargs)
        self.write_op(obj, file, **kwargs)

    def read_fitness(self, file):
        return 0

    def write_fitness(self, obj):
        return 0


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
