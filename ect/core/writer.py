"""
Description
===========

Naive implementation of a generic ``write_object`` function that utilises a
global writer registry ``WRITER_REGISTRY``.

"""
import os.path
from abc import ABCMeta, abstractmethod
from collections import OrderedDict


def find_writer(obj, file_path, format_name=None, **kwargs):
    _, filename_ext = os.path.splitext(file_path)
    return WRITER_REGISTRY.find_writer(obj, format_name=format_name, filename_ext=filename_ext, **kwargs)


def write_obj(obj, file_path, format_name=None, **kwargs):
    writer = find_writer(obj, file_path, format_name=format_name, **kwargs)
    if not writer:
        raise ValueError("no writer found for format '%s'" % format_name if format_name else "no writer found")
    writer.write(obj, file_path, **kwargs)
    return writer


class WriterRegistry:
    """
    Registry of writers. A writer is any instance of type :py:class::`Writer` or otherwise compatible object.
    """

    def __init__(self):
        self._writers = []

    @property
    def writers(self):
        return self._writers

    def find_writer(self, obj=None, format_name=None, filename_ext=None, default_writer=None, **kwargs):
        writer_counts = OrderedDict()

        for writer in self._writers:
            try:
                if format_name:
                    if writer.format_name == format_name:
                        counts = writer_counts.get(writer, 0)
                        writer_counts[writer] = counts + 1
                if filename_ext:
                    if writer.filename_ext == filename_ext:
                        counts = writer_counts.get(writer, 0)
                        writer_counts[writer] = counts + 1
                if obj is not None:
                    if writer.can_write(obj, **kwargs):
                        counts = writer_counts.get(writer, 0)
                        writer_counts[writer] = counts + 1
            except AttributeError:
                pass

        best_writer = None
        max_counts = 0
        for writer, counts in writer_counts.items():
            if counts > max_counts:
                best_writer = writer
                max_counts = counts

        return best_writer if best_writer else default_writer


WRITER_REGISTRY = WriterRegistry()


class Writer(metaclass=ABCMeta):
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
    def write(self, obj, file_path, **kwargs):
        pass

    @abstractmethod
    def can_write(self, obj, **kwargs):
        return False


class _TextWriter(Writer):
    @property
    def description(self):
        return "Plain text format"

    @property
    def format_name(self):
        return 'TEXT'

    @property
    def filename_ext(self):
        return '.txt'

    def write(self, obj, file_path, **kwargs):
        with open(file_path, 'w') as fp:
            fp.write(str(obj))

    def can_write(self, obj, **kwargs):
        return isinstance(obj, str)


class _JsonWriter:
    @property
    def description(self):
        return 'JSON format (plain text, UTF8)'

    @property
    def format_name(self):
        return 'JSON'

    @property
    def filename_ext(self):
        return '.json'

    def write(self, obj, file_path, **kwargs):
        import json
        with open(file_path, 'w') as fp:
            json.dump(obj, fp)

    def can_write(self, obj):
        return isinstance(obj, str) \
               or isinstance(obj, float) \
               or isinstance(obj, int) \
               or isinstance(obj, list) \
               or isinstance(obj, dict)


class _NetCDFWriter(Writer, metaclass=ABCMeta):
    @property
    def filename_ext(self):
        return '.nc'

    def can_write(self, obj, **kwargs):
        import xarray
        return isinstance(obj, xarray.Dataset) or (hasattr(obj, 'to_netcdf') and callable(obj.to_netcdf))


class _NetCDF3Writer(_NetCDFWriter):
    @property
    def description(self):
        return "netCDF 3 file format, which fully supports 2+ GB files."

    @property
    def format_name(self):
        return 'NETCDF3'

    def write(self, obj, file_path, **kwargs):
        obj.to_netcdf(file_path, format='NETCDF3_64BIT')


class _NetCDF4Writer(_NetCDFWriter):
    @property
    def description(self):
        return "HDF5 file format, using netCDF 4 API features."

    @property
    def format_name(self):
        return 'NETCDF4'

    def write(self, obj, file_path, **kwargs):
        obj.to_netcdf(file_path, format='NETCDF4', engine='h5netcdf')


WRITER_REGISTRY.writers.extend([
    _TextWriter(),
    _JsonWriter(),
    _NetCDF4Writer(),
    _NetCDF3Writer()
])
