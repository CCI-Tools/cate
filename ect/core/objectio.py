"""
Description
===========

Naive implementation of generic :py:func:``read_object`` / :py:func:``write_object`` functions operating
on a global object I/O registry ``OBJECT_IO_REGISTRY``.

"""
import os.path
from abc import ABCMeta, abstractmethod
from collections import OrderedDict
import xarray as xr


def find_reader(file_path, format_name=None, **kwargs):
    """
    Find a most suitable reader for given *file_path* and optional *format_name*.

    :param file_path: A file path
    :param format_name: An optional format name.
    :param kwargs: Specific reader arguments passed to ``read_fitness`` method of a registered
           :py:class:``ObjectIO``-compatible object
    :return: An :py:class:``ObjectIO``-compatible object or ``None``.
    """
    _, filename_ext = os.path.splitext(file_path)
    return OBJECT_IO_REGISTRY.find_reader(file_path, format_name=format_name, filename_ext=filename_ext, **kwargs)


def find_writer(obj, file_path, format_name=None, **kwargs):
    """
    Find a most suitable writer for a given object *obj*, *file_path* and optional *format_name*.

    :param obj: Any Python object
    :param file_path: A file path
    :param format_name: An optional format name.
    :param kwargs: Specific reader arguments passed to ``write_fitness`` method of a registered
           :py:class:``ObjectIO``-compatible object
    :return: An :py:class:``ObjectIO``-compatible object or ``None``.
    """
    _, filename_ext = os.path.splitext(file_path)
    return OBJECT_IO_REGISTRY.find_writer(obj, format_name=format_name, filename_ext=filename_ext, **kwargs)


def read_object(file_path, format_name=None, **kwargs):
    """
    Read an object from *file_path* using an optional *format_name*.

    :param file_path: The file path.
    :param format_name: name of the file format
    :param kwargs: additional read parameters
    :return: A tuple (*obj*, *reader*), where *obj* is the object read and *reader* is the reader used to read it.
    """
    reader = find_reader(file_path, format_name=format_name, **kwargs)
    if not reader:
        raise ValueError("no reader found for format '%s'" % format_name if format_name else "no reader found")
    obj = reader.read(file_path, **kwargs)
    return obj, reader


def write_object(obj, file_path, format_name=None, **kwargs):
    """
    Write an object *obj* to *file_path* using an optional *format_name*.

    :param file_path: The file path.
    :param format_name: name of the file format
    :param kwargs: additional write parameters
    :return: The writer used to write *obj*.
    """
    writer = find_writer(obj, file_path, format_name=format_name, **kwargs)
    if not writer:
        raise ValueError("no writer found for format '%s'" % format_name if format_name else "no writer found")
    writer.write(obj, file_path, **kwargs)
    return writer


class ObjectIORegistry:
    """
    Registry of :py:class::`ObjectIO`-like instances.
    """

    def __init__(self):
        self._object_io_list = []

    @property
    def object_io_list(self):
        return self._object_io_list

    @property
    def format_names(self):
        format_names = []
        for object_io in self._object_io_list:
            try:
                format_names.append(object_io.format_name)
            except AttributeError:
                pass
        return sorted(format_names)

    def find_reader(self, file_obj=None, format_name=None, filename_ext=None, default_reader=None, **kwargs):

        object_io_list = self._find_object_ios(format_name, filename_ext)
        if object_io_list is None:
            return default_reader
        elif len(object_io_list) == 1:
            return object_io_list[0]

        reader_fitnesses = OrderedDict()

        for reader in object_io_list:
            try:
                fitness = reader.read_fitness(file_obj, **kwargs)
            except AttributeError:
                fitness = -1
            if fitness >= 0:
                reader_fitnesses[reader] = fitness

        best_reader = None
        max_fitness = 0
        for reader, fitness in reader_fitnesses.items():
            if fitness > max_fitness:
                best_reader = reader
                max_fitness = fitness

        return best_reader if best_reader else default_reader

    def find_writer(self, obj=None, format_name=None, filename_ext=None, default_writer=None, **kwargs):

        object_io_list = self._find_object_ios(format_name, filename_ext)
        if object_io_list is None:
            return default_writer
        elif len(object_io_list) == 1:
            return object_io_list[0]

        if len(object_io_list) == 1:
            return object_io_list[0]

        writer_fitnesses = OrderedDict()

        for writer in object_io_list:
            try:
                fitness = writer.write_fitness(obj, **kwargs)
            except AttributeError:
                fitness = -1
            if fitness >= 0:
                writer_fitnesses[writer] = fitness

        best_writer = None
        max_fitness = 0
        for writer, fitness in writer_fitnesses.items():
            if fitness > max_fitness:
                best_writer = writer
                max_fitness = fitness

        return best_writer if best_writer else default_writer

    def _find_object_ios(self, format_name=None, filename_ext=None):
        all_object_ios = self._object_io_list

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

    @abstractmethod
    def read(self, file_obj, **kwargs):
        pass

    def read_fitness(self, file_obj, **kwargs):
        return 0

    @abstractmethod
    def write(self, obj, file_path, **kwargs):
        pass

    def write_fitness(self, obj, **kwargs):
        return 0


class _TextObjectIO(ObjectIO):
    @property
    def description(self):
        return "Plain text format"

    @property
    def format_name(self):
        return 'TEXT'

    @property
    def filename_ext(self):
        return '.txt'

    def read(self, file_obj, **kwargs):
        if isinstance(file_obj, str):
            with open(file_obj, 'r') as fp:
                return fp.read()
        else:
            return file_obj.read()

    def read_fitness(self, file_obj, **kwargs):
        return 1 if os.path.splitext(str(file_obj))[1] == self.filename_ext else 0

    def write(self, obj, file_path, **kwargs):
        with open(file_path, 'w') as fp:
            fp.write(str(obj))

    def write_fitness(self, obj, **kwargs):
        return 1000 if isinstance(obj, str) else 1


class _JsonObjectIO(ObjectIO):
    @property
    def description(self):
        return 'JSON format (plain text, UTF8)'

    @property
    def format_name(self):
        return 'JSON'

    @property
    def filename_ext(self):
        return '.json'

    def read(self, file_obj, **kwargs):
        import json
        if isinstance(file_obj, str):
            with open(file_obj, 'r') as fp:
                return json.load(fp)
        else:
            return json.load(file_obj)

    def read_fitness(self, file_obj, **kwargs):
        return os.path.splitext(str(file_obj))[1] == self.filename_ext

    def write(self, obj, file_path, **kwargs):
        import json
        with open(file_path, 'w') as fp:
            json.dump(obj, fp)

    def write_fitness(self, obj, **kwargs):
        return 1000 if isinstance(obj, str) \
                       or isinstance(obj, float) \
                       or isinstance(obj, int) \
                       or isinstance(obj, list) \
                       or isinstance(obj, dict) else 0


class _NetCDFObjectIO(ObjectIO, metaclass=ABCMeta):
    @property
    def filename_ext(self):
        return '.nc'

    def read_fitness(self, file_obj, **kwargs):
        try:
            dataset = self.read(file_obj)
            dataset.close()
            return 100000
        except Exception:
            return -1

    def write_fitness(self, obj, **kwargs):
        # TODO (forman, 20160905): add support for numpy-like arrays
        return 100000 if isinstance(obj, xr.Dataset) or (hasattr(obj, 'to_netcdf') and callable(obj.to_netcdf)) \
            else 0


class _NetCDF3ObjectIO(_NetCDFObjectIO):
    @property
    def description(self):
        return "netCDF 3 file format, which fully supports 2+ GB files."

    @property
    def format_name(self):
        return 'NETCDF3'

    def read(self, file_obj, **kwargs):
        return xr.open_dataset(file_obj)

    def write(self, obj, file_path, **kwargs):
        obj.to_netcdf(file_path, format='NETCDF3_64BIT')


class _NetCDF4ObjectIO(_NetCDFObjectIO):
    @property
    def description(self):
        return "HDF5 file format, using netCDF 4 API features."

    @property
    def format_name(self):
        return 'NETCDF4'

    def read(self, file_obj, **kwargs):
        return xr.open_dataset(file_obj)

    def write(self, obj, file_path, **kwargs):
        obj.to_netcdf(file_path, format='NETCDF4')


OBJECT_IO_REGISTRY.object_io_list.extend([
    _TextObjectIO(),
    _JsonObjectIO(),
    _NetCDF4ObjectIO(),
    _NetCDF3ObjectIO()
])
