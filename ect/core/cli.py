"""
Module Description
==================

This module provides ECT's command-line interface (CLI) API and the CLI executable.

To use the CLI executable, invoke the module file as a script, type ``python3 cli.py [ARGS] [OPTIONS]``. Type
`python3 cli.py --help`` for usage help.

The CLI operates on sub-commands. New sub-commands can be added by inheriting from the :py:class:`Command` class
and extending the ``Command.REGISTRY`` list of known command classes.


Module Reference
================
"""

import argparse
import os.path
import os.path
import sys
from abc import ABCMeta
from collections import OrderedDict
from typing import Tuple, Optional

from ect.core.monitor import ConsoleMonitor, Monitor
from ect.version import __version__

#: Name of the ECT CLI executable.
CLI_NAME = 'ect'

_COPYRIGHT_INFO_PATH = os.path.dirname(__file__) + '/../../COPYRIGHT'
_LICENSE_INFO_PATH = os.path.dirname(__file__) + '/../../LICENSE'

_DOCS_URL = 'http://ect-core.readthedocs.io/en/latest/'


class Command(metaclass=ABCMeta):
    """
    Represents (sub-)command for ECT's command-line interface.
    If a plugin wishes to extend ECT's CLI, it may append a new call derived from ``Command`` to the list
    ``REGISTRY``.
    """

    #: Success value to be returned by :py:meth:`execute`. Its value is ``(0, None)``.
    STATUS_OK = (0, None)

    @classmethod
    def name_and_parser_kwargs(cls):
        """
        Return a tuple (*command_name*, *parser_kwargs*) where *command_name* is a unique command name
        and *parser_kwargs* are the keyword arguments passed to a ``argparse.ArgumentParser(**parser_kwargs)`` call.

        For the possible keywords in *parser_kwargs*,
        refer to https://docs.python.org/3.5/library/argparse.html#argparse.ArgumentParser.

        :return: A tuple (*command_name*, *parser_kwargs*).
        """

    @classmethod
    def configure_parser(cls, parser: argparse.ArgumentParser):
        """
        Configure *parser*, i.e. make any required ``parser.add_argument(*args, **kwargs)`` calls.
        See https://docs.python.org/3.5/library/argparse.html#argparse.ArgumentParser.add_argument

        :param parser: The command parser to configure.
        """

    def execute(self, command_args: argparse.Namespace) -> Optional[Tuple[int, str]]:
        """
        Execute this command and return a tuple (*status*, *message*) where *status* is the CLI executable's
        exit code and *message* a text to be printed before the executable
        terminates. If *status* is zero, the message will be printed to ``sys.stdout``, otherwise to ``sys.stderr``.
        Implementors may can return ``STATUS_OK`` on success.

        The command's arguments in *command_args* are attributes namespace returned by
        ``argparse.ArgumentParser.parse_args()``.
        Also refer to to https://docs.python.org/3.5/library/argparse.html#argparse.ArgumentParser.parse_args


        :param command_args: The command's arguments.
        :return: `None`` (= status ok) or a tuple (*status*, *message*) of type (``int``, ``str``)
                 where *message* may be ``None``.
        """


class RunCommand(Command):
    """
    The ``run`` command is used to invoke registered operations and JSON workflows.
    """

    CMD_NAME = 'run'

    @classmethod
    def name_and_parser_kwargs(cls):
        help_line = 'Run an operation OP with given arguments.'
        return cls.CMD_NAME, dict(help=help_line,
                                  description='%s Type "list ops" to list all available operations.' % help_line)

    @classmethod
    def configure_parser(cls, parser):
        parser.add_argument('--monitor', '-m', action='store_true',
                            help='Display progress information during execution.')
        parser.add_argument('op_name', metavar='OP', nargs='?',
                            help="Fully qualified operation name or alias")
        parser.add_argument('op_args', metavar='...', nargs=argparse.REMAINDER,
                            help="Operation arguments. Use '-h' to print operation details.")

    def execute(self, command_args):
        op_name = command_args.op_name
        if not op_name:
            return 2, "error: command '%s' requires OP argument" % self.CMD_NAME
        is_graph_file = op_name.endswith('.json') and os.path.isfile(op_name)

        op_args = []
        op_kwargs = OrderedDict()
        for arg in command_args.op_args:
            kwarg = arg.split('=', maxsplit=1)
            kw = None
            if len(kwarg) == 2:
                kw, arg = kwarg
                if not kw.isidentifier():
                    return 2, "error: command '%s': keyword '%s' is not a valid identifier" % (self.CMD_NAME, kw)
            try:
                # try converting arg into a Python object
                arg = eval(arg)
            except (SyntaxError, NameError):
                # If it fails, we stay with default type (str)
                pass
            if not kw:
                op_args.append(arg)
            else:
                op_kwargs[kw] = arg

        if is_graph_file:
            return self._invoke_graph(command_args.op_name, command_args.monitor, op_args, op_kwargs)
        else:
            return self._invoke_operation(command_args.op_name, command_args.monitor, op_args, op_kwargs)

    @staticmethod
    def _invoke_operation(op_name: str, op_monitor: bool, op_args: list, op_kwargs: dict):
        from ect.core.op import OP_REGISTRY as OP_REGISTRY
        op = OP_REGISTRY.get_op(op_name)
        if op is None:
            return 1, "error: command '%s': unknown operation '%s'" % (RunCommand.CMD_NAME, op_name)
        print('Running operation %s with args=%s and kwargs=%s' % (op_name, op_args, dict(op_kwargs)))
        if op_monitor:
            monitor = ConsoleMonitor()
        else:
            monitor = Monitor.NULL
        return_value = op(*op_args, monitor=monitor, **op_kwargs)
        print('Output: %s' % return_value)
        return None

    @staticmethod
    def _invoke_graph(graph_file: str, op_monitor: bool, op_args: list, op_kwargs: dict):
        if op_args:
            return 1, "error: command '%s': can't run graph with arguments %s, please provide keywords only" % \
                   (RunCommand.CMD_NAME, op_args)

        from ect.core.graph import Graph
        graph = Graph.load(graph_file)

        for name, value in op_kwargs.items():
            if name in graph.input:
                graph.input[name].value = value

        print('Running graph %s with kwargs=%s' % (graph_file, dict(op_kwargs)))
        if op_monitor:
            monitor = ConsoleMonitor()
        else:
            monitor = Monitor.NULL
        graph.invoke(monitor=monitor)
        for graph_output in graph.output[:]:
            print('Output: %s = %s' % (graph_output.name, graph_output.value))
        return None


class DataSourceCommand(Command):
    """
    The ``ds`` command implements various operations w.r.t. data sources.
    """

    CMD_NAME = 'ds'

    @classmethod
    def name_and_parser_kwargs(cls):
        help_line = 'Data source operations.'
        return cls.CMD_NAME, dict(help=help_line, description=help_line)

    @classmethod
    def configure_parser(cls, parser):
        parser.add_argument('ds_names', metavar='DS_NAME', nargs='+', default='op',
                            help='Data source name. Type "ect list ds" to show all possible names.')
        parser.add_argument('--period', '-p', nargs=1, metavar='PERIOD',
                            help='Limit to date/time period. Format of PERIOD is DATE[,DATE] where DATE is YYYY[-MM[-DD]]')
        parser.add_argument('--info', '-i', action='store_true', default=True,
                            help="Display information about the data source DS_NAME.")
        parser.add_argument('--sync', '-s', action='store_true', default=False,
                            help="Synchronise a remote data source DS_NAME with its local version.")

    def execute(self, command_args):
        from ect.core.io import DATA_STORE_REGISTRY
        data_store = DATA_STORE_REGISTRY.get_data_store('default')

        if command_args.period:
            time_range = self.parse_period(command_args.period[0])
            if not time_range:
                return 2, "invalid PERIOD: " + command_args.period[0]
        else:
            time_range = None

        for ds_name in command_args.ds_names:
            data_sources = data_store.query(name=ds_name)
            if not data_sources or len(data_sources) == 0:
                print("Unknown data source '%s'" % ds_name)
                continue
            data_source = data_sources[0]
            if command_args.info and not command_args.sync:
                print(data_source.info_string)
            if command_args.sync:
                data_source.sync(time_range=time_range, monitor=ConsoleMonitor())

    @staticmethod
    def parse_period(period):
        from datetime import date, timedelta
        period_parts = period.split(',')
        num_period_parts = len(period_parts)
        if num_period_parts < 1 or num_period_parts > 2:
            return None
        if num_period_parts == 1:
            period_parts = period_parts[0], period_parts[0]
        date1_parts = period_parts[0].split('-')
        date2_parts = period_parts[1].split('-')
        num_date_parts = len(date1_parts)
        if num_date_parts < 1 or num_date_parts > 3:
            return None
        if num_date_parts != len(date2_parts):
            return None
        date1_args = [0, 1, 1]
        date2_args = [0, 1, 1]
        try:
            for i in range(num_date_parts):
                date1_args[i] = int(date1_parts[i])
                date2_args[i] = int(date2_parts[i])
            date1 = date(*date1_args)
            date2 = date(*date2_args)
        except ValueError:
            return None

        if num_date_parts == 1:
            date2 = date(date2.year + 1, 1, 1)
        elif num_date_parts == 2:
            year = date2.year
            month = date2.month + 1
            if month == 13:
                year += 1
                month = 1
            date2 = date(year, month, 1)
        else:
            date2 = date(date2.year, date2.month, date2.day) + timedelta(days=1)
        date2 += timedelta(microseconds=-1)
        return date1, date2


class ListCommand(Command):
    """
    The ``list`` command is used to list the content of various ECT registries.
    """

    CMD_NAME = 'list'

    @classmethod
    def name_and_parser_kwargs(cls):
        help_line = 'List items of a various categories.'
        return cls.CMD_NAME, dict(help=help_line, description=help_line)

    @classmethod
    def configure_parser(cls, parser):
        parser.add_argument('category', metavar='CAT', choices=['op', 'ds', 'pi'], nargs='?', default='op',
                            help="Category to list items of. "
                                 "'op' lists operations, 'ds' lists data sources, 'pi' lists plugins")
        parser.add_argument('--pattern', '-p', metavar='PAT', nargs='?', default=None,
                            help="A wildcard pattern to filter listed items. "
                                 "'*' matches zero or many characters, '?' matches a single character. "
                                 "The comparison is case insensitive.")

    def execute(self, command_args):
        if command_args.category == 'pi':
            from ect.core.plugin import PLUGIN_REGISTRY as PLUGIN_REGISTRY
            ListCommand.list_items('plugin', 'plugins', PLUGIN_REGISTRY.keys(), command_args.pattern)
        elif command_args.category == 'ds':
            from ect.core.io import DATA_STORE_REGISTRY
            data_store = DATA_STORE_REGISTRY.get_data_store('default')
            if data_store is None:
                return 2, "error: command '%s': no data_store named 'default' found" % self.CMD_NAME
            ListCommand.list_items('data source', 'data sources',
                                   [data_source.name for data_source in data_store.query()], command_args.pattern)
        elif command_args.category == 'op':
            from ect.core.op import OP_REGISTRY as OP_REGISTRY
            ListCommand.list_items('operation', 'operations', OP_REGISTRY.op_registrations.keys(), command_args.pattern)

    @staticmethod
    def list_items(category_singular_name: str, category_plural_name: str, names, pattern: str):
        if pattern:
            # see https://docs.python.org/3.5/library/fnmatch.html
            import fnmatch
            pattern = pattern.lower()
            names = [name for name in names if fnmatch.fnmatch(name.lower(), pattern)]
        item_count = len(names)
        if item_count == 1:
            print('One %s found' % category_singular_name)
        elif item_count > 1:
            print('%d %s found' % (item_count, category_plural_name))
        else:
            print('No %s found' % category_plural_name)
        no = 0
        for item in names:
            no += 1
            print('%4d: %s' % (no, item))


class CopyrightCommand(Command):
    """
    The ``cr`` command is used to display ECT's copyright information.
    """

    @classmethod
    def name_and_parser_kwargs(cls):
        help_line = 'Print copyright information.'
        return 'cr', dict(help=help_line, description=help_line)

    def execute(self, command_args):
        with open(_COPYRIGHT_INFO_PATH) as fp:
            content = fp.read()
            print(content)


class LicenseCommand(Command):
    """
    The ``lic`` command is used to display ECT's licensing information.
    """

    @classmethod
    def name_and_parser_kwargs(cls):
        help_line = 'Print license information.'
        return 'lic', dict(help=help_line, description=help_line)

    def execute(self, command_args):
        with open(_LICENSE_INFO_PATH) as fp:
            content = fp.read()
            print(content)


class DocsCommand(Command):
    """
    The ``doc`` command is used to display ECT's documentation.
    """

    @classmethod
    def name_and_parser_kwargs(cls):
        help_line = 'Display documentation in a browser window.'
        return 'doc', dict(help=help_line, description=help_line)

    def execute(self, command_args):
        import webbrowser
        webbrowser.open_new_tab(_DOCS_URL)


#: List of sub-commands supported by the CLI. Entries are classes derived from :py:class:`Command` class.
#: ECT plugins may extend this list by their commands during plugin initialisation.
COMMAND_REGISTRY = [
    ListCommand,
    RunCommand,
    DataSourceCommand,
    CopyrightCommand,
    LicenseCommand,
    DocsCommand,
]


class NoExitArgumentParser(argparse.ArgumentParser):
    """
    Special ``argparse.ArgumentParser`` that never directly exits the current process.
    It raises an ``ExitException`` instead.
    """

    def __init__(self, *args, **kwargs):
        super(NoExitArgumentParser, self).__init__(*args, **kwargs)

    def exit(self, status=0, message=None):
        """Overrides the base class method in order to raise an ``ExitException``."""
        raise NoExitArgumentParser.ExitException(status, message)

    class ExitException(Exception):
        """Raises instead of exiting the current process."""

        def __init__(self, status, message):
            self.status = status
            self.message = message

        def __str__(self):
            return '%s (%s)' % (self.message, self.status)


def main(args=None):
    """
    The CLI's entry point function.

    :param args: list of command-line arguments of type ``str``.
    :return: An exit code where ``0`` stands for success.
    """

    if not args:
        args = sys.argv[1:]

    parser = NoExitArgumentParser(prog=CLI_NAME,
                                  description='ESA CCI Toolbox command-line interface, version %s' % __version__)
    parser.add_argument('--version', action='version', version='%s %s' % (CLI_NAME, __version__))
    subparsers = parser.add_subparsers(
        dest='command_name',
        metavar='COMMAND',
        help='One of the following commands. Type "COMMAND -h" to get command-specific help.'
    )

    for command_class in COMMAND_REGISTRY:
        command_name, command_parser_kwargs = command_class.name_and_parser_kwargs()
        command_parser = subparsers.add_parser(command_name, **command_parser_kwargs)
        command_class.configure_parser(command_parser)
        command_parser.set_defaults(command_class=command_class)

    try:
        args_obj = parser.parse_args(args)

        if args_obj.command_name and args_obj.command_class:
            assert args_obj.command_name and args_obj.command_class
            status_and_message = args_obj.command_class().execute(args_obj)
            if not status_and_message:
                status_and_message = Command.STATUS_OK
            status, message = status_and_message
        else:
            parser.print_help()
            status, message = 0, None

    except NoExitArgumentParser.ExitException as e:
        status, message = e.status, e.message

    if message:
        if status:
            sys.stderr.write("%s: %s\n" % (CLI_NAME, message))
        else:
            sys.stdout.write("%s\n" % message)

    return status


if __name__ == '__main__':
    sys.exit(main())
