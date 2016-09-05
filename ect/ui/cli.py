"""
Description
===========

This module provides ECT's command-line interface (CLI) API and the CLI executable.

To use the CLI executable, invoke the module file as a script, type ``python3 cli.py [ARGS] [OPTIONS]``. Type
`python3 cli.py --help`` for usage help.

The CLI operates on sub-commands. New sub-commands can be added by inheriting from the :py:class:`Command` class
and extending the ``Command.REGISTRY`` list of known command classes.

Technical Requirements
======================

**Extensible CLI with multiple sub-commands**

:Description: The CCI Toolbox should only have a single CLI executable that comes with multiple sub-commands
    instead of maintaining a number of different executables for each purpose. Plugins shall be able to add new
    CLI sub-commands.

:URD-Source:
    * CCIT-UR-CR0001: Extensibility.
    * CCIT-UR-A0002: Offer a Command Line Interface (CLI).

----

**Run operations and workflows**

:Description: Allow for executing registered operations an workflows composed of operations.
:URD-Source:
    * CCIT-UR-CL0001: Reading and executing script files written in XML or similar

----

**List available data, operations and extensions**

:Description: Allow for listing dynamic content including available data, operations and plugin extensions.
:URD-Source:
    * CCIT-UR-E0001: Dynamic extension by the use of plug-ins

----

**Display information about available climate data sources**

:Description: Before downloading ECV datasets to the local computer, users shall be able to
    display information about them, e.g. included variables, total size, spatial and temporal resolution.

:URD-Source:
    * CCIT-UR-DM0009: Holding information of any CCI ECV type
    * CCIT-UR-DM0010: Attain meta-level status information per ECV type

----

**Synchronize locally cached climate data**

:Description: Allow for listing dynamic content including available data, operations and plugin extensions.
:URD-Source:
    * CCIT-UR-DM0006: Access to and ingestion of ESA CCI datasets

----


Verification
============

The module's unit-tests are located in `test/ui/test_cli.py <https://github.com/CCI-Tools/ect-core/blob/master/test/ui/test_cli.py>`_
and may be executed using ``$ py.test test/ui/test_cli.py --cov=ect/ui/cli.py`` for extra code coverage information.


Components
==========
"""

import argparse
import os.path
import sys
from abc import ABCMeta
from collections import OrderedDict
from typing import Tuple, Optional

from ect.core.monitor import ConsoleMonitor, Monitor
from ect.version import __version__
from ect.core.writer import find_writer

#: Name of the ECT CLI executable (= ``ect``).
CLI_NAME = 'ect'

_COPYRIGHT_INFO_PATH = os.path.dirname(__file__) + '/../../COPYRIGHT'
_LICENSE_INFO_PATH = os.path.dirname(__file__) + '/../../LICENSE'

_DOCS_URL = 'http://ect-core.readthedocs.io/en/latest/'


def _parse_write_arg(write_arg):
    """
    Parse string *write_arg* as "[NAME=]PATH[,FORMAT]" and return tuple NAME,PATH,FORMAT.

    :param write_arg: string argument expected to have format "[NAME=]PATH[,FORMAT]"
    :return: The tuple NAME,PATH,FORMAT
    """
    name_and_path = write_arg.split('=', maxsplit=2)
    name, path = name_and_path if len(name_and_path) == 2 else (None, write_arg)
    path_and_format = path.rsplit(',', maxsplit=2)
    path, format = path_and_format if len(path_and_format) == 2 else (path, None)
    return name if name else None, path if path else None, format.upper() if format else None


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
        parser.add_argument('--write', '-w', action='append', metavar='FILE', dest='write_args',
                            help='Write result to FILE where file has the format [NAME=]PATH[,FORMAT].\n'
                                 'If FORMAT is not provided file format is derived from the object\n'
                                 'type and the PATH\'s filename extensions. If OP returns multiple '
                                 'named output values, NAME is used to identify them. Multiple -w '
                                 'options may be used in this case.')
        parser.add_argument('op_name', metavar='OP', nargs='?',
                            help="Fully qualified operation name or alias")
        parser.add_argument('op_args', metavar='...', nargs=argparse.REMAINDER,
                            help="Operation arguments. Use '-h' to print operation details.")

    def execute(self, command_args):
        op_name = command_args.op_name
        if not op_name:
            return 2, "error: command '%s' requires OP argument" % self.CMD_NAME
        is_workflow = op_name.endswith('.json') and os.path.isfile(op_name)

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

        if is_workflow:
            if op_args:
                return 1, "error: command '%s': can't run workflow with arguments %s, please provide keywords only" % \
                       (RunCommand.CMD_NAME, op_args)
            from ect.core.workflow import Workflow
            op = Workflow.load(command_args.op_name)
        else:
            from ect.core.op import OP_REGISTRY as OP_REGISTRY
            op = OP_REGISTRY.get_op(command_args.op_name)
            if op is None:
                return 1, "error: command '%s': unknown operation '%s'" % (RunCommand.CMD_NAME, op_name)

        write_args = None
        if command_args.write_args:
            write_args = list(map(_parse_write_arg, command_args.write_args))
            if op.op_meta_info.has_named_outputs:
                for out_name, out_path, out_format in write_args:
                    if not out_name:
                        return 1, "error: command '%s': all --write options must have output names" % RunCommand.CMD_NAME
                    if out_name not in op.op_meta_info.output:
                        return 1, "error: command '%s': --write option with unknown output named \"%s\"" % (RunCommand.CMD_NAME, out_name)
            else:
                if len(write_args) > 1:
                    return 1, "error: command '%s': multiple --write options given for singular result" % RunCommand.CMD_NAME
                out_name, out_path, out_format = write_args[0]
                if out_name and out_name != 'return':
                    return 1, "error: command '%s': --write option with named output for singular result" % RunCommand.CMD_NAME

        if command_args.monitor:
            monitor = ConsoleMonitor()
        else:
            monitor = Monitor.NULL

        print("Running '%s' with args=%s and kwargs=%s" % (op.op_meta_info.qualified_name, op_args, dict(op_kwargs)))
        return_value = op(*op_args, monitor=monitor, **op_kwargs)
        if op.op_meta_info.has_named_outputs:
            if write_args:
                for out_name, out_path, out_format in write_args:
                    out_value = return_value[out_name].value
                    writer = find_writer(out_value, out_path, format_name=out_format)
                    if writer:
                        print("Writing output '%s' to %s using %s format..." % (out_name, out_path, writer.format_name))
                        writer.write(out_value, out_path)
                    else:
                        return 1, "error: command '%s': unknown format for --write output '%s'" % (RunCommand.CMD_NAME, out_name)
            else:
                for output in return_value:
                    print("Output '%s': %s" % (output.name, output.value))
        else:
            if write_args:
                _, out_path, out_format = write_args[0]
                writer = find_writer(return_value, out_path, format_name=out_format)
                if writer:
                    print("Writing output to %s using %s format..." % (out_path, writer.format_name))
                    writer.write(return_value, out_path)
                else:
                    return 1, "error: command '%s': unknown format for --write option" % RunCommand.CMD_NAME
            else:
                print('Output: %s' % return_value)

        return self.STATUS_OK


class OperationCommand(Command):
    """
    The ``op`` command implements various operations w.r.t. operations.
    """

    CMD_NAME = 'op'

    @classmethod
    def name_and_parser_kwargs(cls):
        help_line = 'Operations and processors.'
        return cls.CMD_NAME, dict(help=help_line, description=help_line)

    @classmethod
    def configure_parser(cls, parser):
        op_parser = parser.add_subparsers(
            dest='op_command',
            metavar='COMMAND',
            help='One of the following commands. Type "COMMAND -h" to get command-specific help.'
        )
        parser.set_defaults(op_parser=parser)

        list_parser = op_parser.add_parser('list', help='List all available operations')
        list_parser.add_argument('--pattern', '-p', nargs=1, metavar='PATTERN',
                                 help="A wildcard pattern to filter operation names. "
                                      "'*' matches zero or many characters, '?' matches a single character. "
                                      "The comparison is case insensitive.")
        # TODO mz 2016-09-05, add tags to 'op' decorator
        # list_parser.add_argument('--tag', '-t', nargs=1, metavar='TAG',
        #                          help='A tag as a category for operation')
        list_parser.set_defaults(op_command=cls.execute_list)

        info_parser = op_parser.add_parser('info', help='Show usage information about an operation')
        info_parser.add_argument('op_name', metavar='OP', nargs='?',
                                 help="Fully qualified operation name or alias")
        info_parser.set_defaults(op_command=cls.execute_info)

    @classmethod
    def execute_list(cls, command_args):
        from ect.core.op import OP_REGISTRY
        pattern = None
        if command_args.pattern:
            pattern = command_args.pattern[0]
        list_items('operation', 'operations', OP_REGISTRY.op_registrations.keys(), pattern)

    @classmethod
    def execute_info(cls, command_args):
        if not command_args.op_name:
            return 2, 'No operation name given'
        from ect.core.op import OP_REGISTRY
        op_registration = OP_REGISTRY.get_op(command_args.op_name)
        if op_registration:
            op_meta_info = op_registration.op_meta_info
            print('op: %s' % op_meta_info.qualified_name)
            if 'description' in op_meta_info.header:
                print(op_meta_info.header['description'])
        else:
            return 2, "Unknown operation '%s'" % command_args.op_name

    def execute(self, command_args):
        if hasattr(command_args, 'op_command') and command_args.op_command:
            return command_args.op_command(command_args)
        else:
            command_args.op_parser.print_help()
            return 0, None


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
        ds_parser = parser.add_subparsers(
            dest='ds_command',
            metavar='COMMAND',
            help='One of the following commands. Type "COMMAND -h" to get command-specific help.'
        )
        parser.set_defaults(ds_parser=parser)

        list_parser = ds_parser.add_parser('list', help='List all available data sources')
        list_parser.add_argument('--pattern', '-p', nargs=1, metavar='PATTERN',
                                 help="A wildcard pattern to filter data source names. "
                                      "'*' matches zero or many characters, '?' matches a single character. "
                                      "The comparison is case insensitive.")
        list_parser.set_defaults(ds_command=cls.execute_list)

        sync_parser = ds_parser.add_parser('sync', help='Synchronise a remote data source DS_NAME with its local version.')
        sync_parser.add_argument('ds_name', metavar='DS_NAME', nargs=1,
                                 help='Data source name. Type "ect ds list" to show all possible names.')
        sync_parser.add_argument('--time', '-t', nargs=1, metavar='PERIOD',
                                 help='Limit to date/time period. Format of PERIOD is DATE[,DATE] where DATE is YYYY[-MM[-DD]]')
        sync_parser.set_defaults(ds_command=cls.execute_sync)

        info_parser = ds_parser.add_parser('info', help='Display information about the data source DS_NAME.')
        info_parser.add_argument('ds_name', metavar='DS_NAME', nargs=1,
                                 help='Data source name. Type "ect ds list" to show all possible names.')
        info_parser.set_defaults(ds_command=cls.execute_info)

    @classmethod
    def execute_list(cls, command_args):
        from ect.core.io import DATA_STORE_REGISTRY
        data_store = DATA_STORE_REGISTRY.get_data_store('default')
        if data_store is None:
            return 2, "error: command 'ds list': no data_store named 'default' found"
        pattern = None
        if command_args.pattern:
            pattern = command_args.pattern[0]
        list_items('data source', 'data sources',
                               [data_source.name for data_source in data_store.query()], pattern)

    @classmethod
    def execute_info(cls, command_args):
        from ect.core.io import DATA_STORE_REGISTRY
        data_store = DATA_STORE_REGISTRY.get_data_store('default')
        if data_store is None:
            return 2, "error: command 'ds info': no data_store named 'default' found"

        data_sources = data_store.query(name=command_args.ds_name[0])
        if not data_sources or len(data_sources) == 0:
            print("Unknown 1 data source '%s'" % command_args.ds_name[0])
        else:
            for data_source in data_sources:
                print(data_source.info_string)

    @classmethod
    def execute_sync(cls, command_args):
        from ect.core.io import DATA_STORE_REGISTRY
        data_store = DATA_STORE_REGISTRY.get_data_store('default')
        if data_store is None:
            return 2, "error: command 'ds sync': no data_store named 'default' found"

        data_sources = data_store.query(name=command_args.ds_name[0])
        data_source = data_sources[0]
        if command_args.time:
            time_range = cls.parse_time_period(command_args.time[0])
            if not time_range:
                return 2, "invalid PERIOD: " + command_args.time[0]
        else:
            time_range = (None, None)
        data_source.sync(time_range=time_range, monitor=ConsoleMonitor())

    def execute(self, command_args):
        if hasattr(command_args, 'ds_command') and command_args.ds_command:
            return command_args.ds_command(command_args)
        else:
            command_args.ds_parser.print_help()
            return 0, None

    @staticmethod
    def parse_time_period(period):
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


class PluginCommand(Command):
    """
    The ``pi`` command lists the content of various plugin registry.
    """

    CMD_NAME = 'pi'

    @classmethod
    def name_and_parser_kwargs(cls):
        help_line = 'List installed plugins.'
        return cls.CMD_NAME, dict(help=help_line, description=help_line)

    @classmethod
    def configure_parser(cls, parser):
        pi_parser = parser.add_subparsers(
            dest='pi_command',
            metavar='COMMAND',
            help='One of the following commands. Type "COMMAND -h" to get command-specific help.'
        )
        parser.set_defaults(op_parser=parser)

        list_parser = pi_parser.add_parser('list', help='List all available plugins')
        list_parser.add_argument('--pattern', '-p', nargs=1, metavar='PATTERN',
                                 help="A wildcard pattern to filter plugin names. "
                                      "'*' matches zero or many characters, '?' matches a single character. "
                                      "The comparison is case insensitive.")
        list_parser.set_defaults(op_command=cls.execute_list)

    @classmethod
    def execute_list(cls, command_args):
        from ect.core.plugin import PLUGIN_REGISTRY as PLUGIN_REGISTRY
        pattern = None
        if command_args.pattern:
            pattern = command_args.pattern[0]
        list_items('plugin', 'plugins', PLUGIN_REGISTRY.keys(), pattern)

    def execute(self, command_args):
        if hasattr(command_args, 'op_command') and command_args.op_command:
            return command_args.op_command(command_args)
        else:
            command_args.op_parser.print_help()
            return 0, None


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
    RunCommand,
    DataSourceCommand,
    OperationCommand,
    PluginCommand,
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
