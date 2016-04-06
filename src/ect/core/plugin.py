from collections import OrderedDict

from pkg_resources import iter_entry_points


class Context:
    """
    The ECT context is passed to ECT plugin callables so that they can register their ECT contributions.
    """

    def __init__(self):
        self.readers = dict()
        self.writers = dict()
        self.processors = dict()

    def register_reader(self, name, reader):
        self.readers[name] = reader

    def register_writer(self, name, writer):
        self.writers[name] = writer

    def register_processor(self, name, processor):
        self.processors[name] = processor


class ExamplePlugin:
    def __init__(self, ect_context):
        ect_context.register_reader('r', 'R')
        ect_context.register_writer('w', 'W')
        ect_context.register_processor('p', 'P')


CONTEXT = Context()


def _load_plugins():
    plugins = OrderedDict()
    for entry_point in iter_entry_points(group='ect_plugins', name=None):
        plugin = entry_point.load()
        if callable(plugin):
            plugins[entry_point.name] = plugin(CONTEXT)
        else:
            print('warning: ect_plugins: requires a callable but got a \'%s\'' % type(plugin))
    return plugins


# Keep plugin references in order to avoid erasing them by garbage collection
PLUGINS = _load_plugins()

del _load_plugins
