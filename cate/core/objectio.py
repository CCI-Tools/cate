# The MIT License (MIT)
# Copyright (c) 2017 by the Cate Development Team and contributors
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

__author__ = "Norman Fomferra (Brockmann Consult GmbH)"

"""
Description
===========

Implementation of generic :py:func:``read_object`` / :py:func:``write_object`` functions operating
on a global I/O registry ``OBJECT_IO_REGISTRY`` singleton.

"""
import os.path
from abc import ABCMeta, abstractmethod
from collections import OrderedDict

from cate.util.monitor import Monitor
from .op import OpRegistration


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
        max_fitness = -1
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

    def read(self, file, monitor: Monitor = Monitor.NONE, **kwargs):
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
        return self.read_op(file=file, monitor=monitor, **kwargs)

    def write(self, obj, file, monitor: Monitor = Monitor.NONE, **kwargs):
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
        self.write_op(obj=obj, file=file, monitor=monitor, **kwargs)

    def read_fitness(self, file):
        return 0

    def write_fitness(self, obj):
        return 0
