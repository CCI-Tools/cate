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
import sys
from abc import ABCMeta
from collections import OrderedDict
from typing import Tuple, Optional

from ect.core.monitor import ConsoleMonitor, Monitor
from ect.version import __version__

#: Name of the ECT CLI executable.
CLI_NAME = 'ect'

_COPYRIGHT_INFO = '(c) 2016 by European Space Agency (ESA). All rights reserved.'
_LICENSE_INFO_PATH = os.path.dirname(__file__) + '/../../LICENSE'
# todo nf - ECT documentation URL shall later point to ReadTheDocs
_DOCS_URL = 'https://github.com/CCI-Tools/ect-core'


class Command(metaclass=ABCMeta):
    """
    Represents (sub-)command for ECT's command-line interface.
    If a plugin wishes to extend ECT's CLI, it may append a new call derived from ``Command`` to the list
    ``REGISTRY``.
    """

    #: List of sub-commands supported by the CLI. Entries are classes derived from :py:class:`Command` class.
    #: ECT plugins may extend this list by their commands during plugin initialisation.
    REGISTRY = []

    #: Success value to be returned by :py:method:`execute`. Its value is ``(0, None)``.
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


class Run(Command):
    @classmethod
    def name_and_parser_kwargs(cls):
        help_line = 'Run an operation OP with given arguments.'
        return 'run', dict(help=help_line,
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
        from ect.core.op import REGISTRY as OP_REGISTRY

        op_name = command_args.op_name
        op = OP_REGISTRY.get_op(op_name)
        if op is None:
            return 1, "error: unknown operation '%s'" % op_name

        op_args = []
        op_kwargs = OrderedDict()
        for arg in command_args.op_args:
            kwarg = arg.split('=', maxsplit=1)
            kw = None
            if len(kwarg) == 2:
                kw, arg = kwarg
                if not kw.isidentifier():
                    return 2, "error: keyword '%s' is not a valid identifier" % kw
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
        print('Running operation %s with args=%s and kwargs=%s' % (op_name, op_args, dict(op_kwargs)))
        if command_args.monitor:
            monitor = ConsoleMonitor()
        else:
            monitor = Monitor.NULL
        return_value = op(*op_args, monitor=monitor, **op_kwargs)
        print('Output: %s' % return_value)


class List(Command):
    @classmethod
    def name_and_parser_kwargs(cls):
        help_line = 'List items of a various categories.'
        return 'list', dict(help=help_line, description=help_line)

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
            from ect.core.plugin import REGISTRY as PLUGIN_REGISTRY
            List.list_items('plugin', 'plugins', PLUGIN_REGISTRY.keys(), command_args.pattern)
        elif command_args.category == 'ds':
            data_sources_registry = []
            List.list_items('data source', 'data sources', data_sources_registry, command_args.pattern)
        elif command_args.category == 'op':
            from ect.core.op import REGISTRY as OP_REGISTRY
            List.list_items('operation', 'operations', OP_REGISTRY.op_registrations.keys(), command_args.pattern)

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


class Copyright(Command):
    @classmethod
    def name_and_parser_kwargs(cls):
        help_line = 'Print copyright information.'
        return 'copyright', dict(help=help_line, description=help_line)

    def execute(self, command_args):
        print(_COPYRIGHT_INFO)


class License(Command):
    @classmethod
    def name_and_parser_kwargs(cls):
        help_line = 'Print license information.'
        return 'license', dict(help=help_line, description=help_line)

    def execute(self, command_args):
        with open(_LICENSE_INFO_PATH) as fp:
            content = fp.read()
            print(content)


class Docs(Command):
    @classmethod
    def name_and_parser_kwargs(cls):
        help_line = 'Display documentation in a browser window.'
        return 'docs', dict(help=help_line, description=help_line)

    def execute(self, command_args):
        import webbrowser
        webbrowser.open_new_tab(_DOCS_URL)


#: List of sub-commands supported by the CLI. Entries are classes derived from :py:class:`Command` class.
#: ECT plugins may extend this list by their commands during plugin initialisation.
Command.REGISTRY.extend([
    List,
    Run,
    Copyright,
    License,
    Docs,
])


def main(args=None):
    """
    The CLI's entry point function.

    :param args: list of command-line arguments of type ``str``.
    :return: A tuple (*status*, *message*)
    """

    if not args:
        args = sys.argv[1:]

    parser = argparse.ArgumentParser(prog=CLI_NAME,
                                     description='ESA CCI Toolbox command-line interface, version %s' % __version__)
    parser.add_argument('--version', action='version', version='%s %s' % (CLI_NAME, __version__))
    subparsers = parser.add_subparsers(
        dest='command_name',
        metavar='COMMAND',
        help='One of the following commands. Type "COMMAND -h" to get command-specific help.'
    )

    for command_class in Command.REGISTRY:
        command_name, command_parser_kwargs = command_class.name_and_parser_kwargs()
        command_parser = subparsers.add_parser(command_name, **command_parser_kwargs)
        command_class.configure_parser(command_parser)
        command_parser.set_defaults(command_class=command_class)

    args_obj = parser.parse_args(args)
    assert args_obj.command_name and args_obj.command_class
    status_and_message = args_obj.command_class().execute(args_obj)
    if not status_and_message:
        status_and_message = Command.STATUS_OK

    status, message = status_and_message
    if message:
        if status:
            sys.stderr.write("%s: %s\n" % (CLI_NAME, message))
        else:
            sys.stdout.write("%s\n" % message)

    return status


if __name__ == '__main__':
    sys.exit(main())
