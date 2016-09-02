"""
Description
===========

Naive implementation of a generic ``write_object`` function that utilises a
global writer registry ``WRITER_REGISTRY``.

"""


def write_obj(obj, file_path, format_name=None, **kwargs):
    writer = WRITER_REGISTRY.find_writer(obj, format_name=format_name, **kwargs)
    if not writer:
        raise ValueError("no writer found for format '%s'" % format_name if format_name else "no writer found")
    writer.write(obj, file_path, **kwargs)


class WriterRegistry:
    """
    Registry of writers. A writer is any object that has a ``write`` method, so that that it can
    be called to write an object to a file:::

        writer.write(obj, file_path, **kwargs)

    Optionally, a writer may provide a method ``can_write`` supposed to return a truth value when called to ask
    if a given object can be written:

        writer.can_write(obj, **kwargs)

    Writers are registered by their format names.
    """

    def __init__(self):
        self._writers = dict()

    def register_writer(self, writer, *format_names):
        if not callable(writer):
            raise ValueError('writer must be callable')
        if not format_names:
            raise ValueError('format_names must be given')
        for format_name in format_names:
            self._writers[format_name] = writer

    def find_writer(self, obj, format_name=None, default_writer=None, **kwargs):
        if format_name:
            writer = self._writers.get(format_name, default_writer)
            if writer:
                return writer
        for writer in self._writers.values():
            try:
                if writer.can_write(obj, **kwargs):
                    return writer
            except AttributeError:
                pass
        return default_writer


WRITER_REGISTRY = WriterRegistry()


class _TextWriter:
    def write(self, obj, file_path):
        with open(file_path, 'w') as fp:
            fp.write(obj)

    def can_write(self, obj):
        return isinstance(obj, str)


class _JsonWriter:
    def write(self, obj, file_path):
        import json
        with open(file_path, 'w') as fp:
            json.dump(obj, fp)

    def can_write(self, obj):
        return isinstance(obj, str) \
               or isinstance(obj, float) \
               or isinstance(obj, int) \
               or isinstance(obj, list) \
               or isinstance(obj, dict)


WRITER_REGISTRY.register_writer(_TextWriter(), 'TXT', 'TEXT')
WRITER_REGISTRY.register_writer(_JsonWriter(), 'JSON')
