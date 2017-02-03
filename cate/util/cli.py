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

This module provides handy utilities for creating command-line interfaces.

"""

import argparse
import sys
import traceback
from abc import ABCMeta, abstractmethod
from typing import Tuple, Optional, Sequence

from cate.util import ConsoleMonitor, Monitor


class CommandError(Exception):
    """
    An exception type signaling command-line errors.
    """

    def __init__(self, cause, *args, **kwargs):
        if isinstance(cause, Exception):
            super(CommandError, self).__init__(str(cause), *args, **kwargs)
            _, _, tb = sys.exc_info()
            self.with_traceback(tb)
        elif isinstance(cause, str):
            super(CommandError, self).__init__(cause, *args, **kwargs)
        else:
            super(CommandError, self).__init__(*args, **kwargs)
        self._cause = cause

    @property
    def cause(self):
        return self._cause


class Command(metaclass=ABCMeta):
    """
    Represents a (sub-)command of a command-line interface.
    """

    @classmethod
    def name(cls):
        """
        :return: A unique command name
        """

    @classmethod
    def parser_kwargs(cls):
        """
        Return parser keyword arguments dictionary passed to a ``argparse.ArgumentParser(**parser_kwargs)`` call.

        For the possible keywords in the returned dictionary,
        refer to https://docs.python.org/3.5/library/argparse.html#argparse.ArgumentParser.

        :return: A keyword arguments dictionary.
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

    @classmethod
    def new_monitor(cls) -> Monitor:
        """
        Create a new console progress monitor.

        :return: a new Monitor instance.
        """
        return ConsoleMonitor(stay_in_line=True, progress_bar_size=-40)


class SubCommandCommand(Command, metaclass=ABCMeta):
    @classmethod
    def configure_parser(cls, parser):
        parser.set_defaults(parser=parser)
        subparsers = parser.add_subparsers(metavar='COMMAND',
                                           help='One of the following commands. '
                                                'Type "COMMAND -h" for help.')
        cls.configure_parser_and_subparsers(parser, subparsers)

    @classmethod
    @abstractmethod
    def configure_parser_and_subparsers(cls, parser, subparsers):
        """
        Overrides must, e.g.::
            list_parser.subparsers.add_parser('list', ...)
            ...
            list_parser.set_defaults(sub_command_function=cls._execute_list)

        :param parser:
        :param subparsers:
        :return:
        """
        pass

    def execute(self, command_args):
        try:
            sub_command_function = command_args.sub_command_function
        except AttributeError:
            try:
                parser = command_args.parser
            except AttributeError:
                raise RuntimeError('internal error: '
                                   'undefined command_args.sub_command_function and command_args.parser')
            parser.print_help()
            return

        return sub_command_function(command_args)


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


def _make_parser(name: str,
                 description: str,
                 version: str,
                 command_classes: Sequence[Command],
                 license_text: str = None,
                 docs_url: str = None, ):
    parser = NoExitArgumentParser(prog=name,
                                  description='%s, version %s' % (description, version))
    parser.add_argument('--version', action='version', version='%s %s' % (name, version))
    parser.add_argument('--traceback', action='store_true', help='show (Python) stack traceback for the last error')
    if license_text:
        parser.add_argument('--license', action='store_true', help='show software license and exit')
    if docs_url:
        parser.add_argument('--docs', action='store_true', help='show software documentation in a browser window')
    subparsers = parser.add_subparsers(dest='command_name',
                                       metavar='COMMAND',
                                       help='One of the following commands. '
                                            'Type "COMMAND -h" to get command-specific help.')
    for command_class in command_classes:
        command_name = command_class.name()
        command_parser_kwargs = command_class.parser_kwargs()
        command_parser = subparsers.add_parser(command_name, **command_parser_kwargs)
        command_class.configure_parser(command_parser)
        command_parser.set_defaults(command_class=command_class)
    return parser


def run_main(name: str,
             description: str,
             version: str,
             command_classes: Sequence[Command],
             license_text: str = None,
             docs_url: str = None,
             error_message_trimmer=None,
             args: Sequence[str] = None) -> int:
    """
    A CLI's entry point function.

    To be used in your own code as follows:

    >>> if __name__ == '__main__':
    >>>    sys.exit(run_main(...))

    :param name: The program's name.
    :param description: The program's description.
    :param version: The program's version string.
    :param command_classes: The CLI commands.
    :param license_text: An optional license text.
    :param docs_url: An optional documentation URL.
    :param error_message_trimmer: An optional callable (str)->str that trims error message strings.
    :param args: list of command-line arguments. If not passed, sys.argv[1:] is used.
    :return: An exit code where ``0`` stands for success.
    """

    if not args:
        args = sys.argv[1:]

    parser = _make_parser(name, description, version, command_classes, license_text=license_text, docs_url=docs_url)

    command_name, status, message = None, 0, None
    try:
        args_obj = parser.parse_args(args)

        if license_text and args_obj.license:
            print(license_text)
            return 0

        if docs_url and args_obj.docs:
            import webbrowser
            webbrowser.open_new_tab(docs_url)
            return 0

        if args_obj.command_name and args_obj.command_class:
            command_name = args_obj.command_name
            # noinspection PyBroadException
            try:
                args_obj.command_class().execute(args_obj)
            except Exception as e:
                show_traceback = args_obj.traceback
                if show_traceback:
                    traceback.print_exc()
                status, message = 1, str(e)
                if message and not show_traceback and error_message_trimmer:
                    message = error_message_trimmer(message)
        else:
            parser.print_help()

    except NoExitArgumentParser.ExitException as e:
        status, message = e.status, e.message

    if message:
        if status:
            if command_name:
                # error from command execution
                sys.stderr.write("%s %s: error: %s\n" % (name, command_name, message))
            else:
                # error from command parser (includes "cate: error: " prefix)
                sys.stderr.write("%s\n" % message)
        else:
            sys.stdout.write("%s\n" % message)

    return status
