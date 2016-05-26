"""
Module Description
==================

This module provides ECT's command-line interface (CLI).

Module Reference
================
"""

import argparse
import sys
from abc import ABCMeta
from collections import OrderedDict

import ect.core.op
import ect.core.plugin

CLI_NAME = 'ect'

_STATUS_OK = (0, None)


class Command(metaclass=ABCMeta):
    """
    A (sub-)command for ECT's command-line interface.
    """

    def name(self):
        """
        :return: The command's name.
        """

    def parser_args(self):
        """
        :return: The keyword arguments passed as *kwargs* to an ``argparse.ArgumentParser(**kwargs)`` call.
        """
        return dict(prog='%s %s' % (CLI_NAME, self.name))

    def add_arguments(self, parser: argparse.ArgumentParser):
        """
        Make any required ``parser.add_argument(*args, **kwargs)`` calls.
        See https://docs.python.org/3.5/library/argparse.html#argparse.ArgumentParser.add_argument

        :param parser: The command parser to configure.
        """

    def execute(self, command_args: argparse.Namespace) -> int:
        """
        Executes this command.

        :param args: The command arguments in a namespace as returned by ``argparse.ArgumentParser.parse_args()``.
        :return: a tuple (*status*, *message*) of type (``int``, ``str``). *message* may be ``None``.
        """


class Run(Command):
    def name(self):
        return 'run'

    def add_arguments(self, parser: argparse.ArgumentParser):
        parser.add_argument('--silent', '-s', help='Run silently', action='store_true')
        parser.add_argument('op_name', metavar='OP', help='Operation name', nargs='?')
        parser.add_argument('op_args', metavar='...', help='Operation arguments', nargs=argparse.REMAINDER)

    def execute(self, command_args: argparse.Namespace) -> int:
        op_name = command_args.op_name
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
                arg = eval(arg)
            except (SyntaxError, NameError):
                pass
            if not kw:
                op_args.append(arg)
            else:
                op_kwargs[kw] = arg
        print('Now running operation...')
        print('  %s(*%s, **%s)' % (op_name, op_args, op_kwargs))
        return _STATUS_OK


class List(Command):
    def name(self):
        return 'list'

    def execute(self, command_args: argparse.Namespace) -> int:
        plugin_registrations = ect.core.plugin.REGISTRY
        print('-' * 80)
        print('Registered ECT plugins (%s):' % len(plugin_registrations))
        print('-' * 80)
        for entry_point_name in plugin_registrations.keys():
            print(entry_point_name)

        op_registrations = ect.core.op.REGISTRY.op_registrations
        print('-' * 80)
        print('Registered ECT operations (%d):' % len(op_registrations))
        print('-' * 80)
        for qualified_name in op_registrations.keys():
            print(qualified_name)

        print('-' * 80)
        return _STATUS_OK

#: List of sub-commands supported by the CLI. Entries are classes derived from :py:class:`Command` class.
COMMANDS = [
    List,
    Run,
]


def main(args=None):
    """
    The CLI's entry point function.

    :param args: list of command-line arguments of type ``str``.
    :return: A tuple (*status*, *message*)
    """

    if not args:
        args = sys.argv[1:]

    print('ESA CCI Toolbox command-line interface, version %s' % ect.__version__)
    parser = argparse.ArgumentParser(prog=CLI_NAME, description='Executes %s sub-commands.' % CLI_NAME)
    subparsers = parser.add_subparsers(
        help='Sub-commands. Type sub-command followed by -h to get sub-command help.')

    for command_cls in COMMANDS:
        command = command_cls()
        command_parser = subparsers.add_parser(command.name(), **command.parser_args())
        command.add_arguments(command_parser)
        command_parser.set_defaults(execute_command=command.execute)

    args_obj = parser.parse_args(args)
    return args_obj.execute_command(args_obj)


if __name__ == '__main__':
    status, message = main()
    if message:
        if status:
            sys.stderr.write("%s\n" % message)
        else:
            sys.stdout.write("%s\n" % message)
    sys.exit(status=status)
