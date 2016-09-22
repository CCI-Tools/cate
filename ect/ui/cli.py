# The MIT License (MIT)
# Copyright (c) 2016 by the ECT Development Team and contributors
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

The module's unit-tests are located in
`test/ui/test_cli.py <https://github.com/CCI-Tools/ect-core/blob/master/test/ui/test_cli.py>`_
and may be executed using ``$ py.test test/ui/test_cli.py --cov=ect/ui/cli.py`` for extra code coverage information.


Components
==========
"""

import argparse
import os.path
import pprint
import sys
import traceback
from abc import ABCMeta, abstractmethod
from typing import Tuple, Optional

from ect.core.io import DATA_STORE_REGISTRY, open_dataset
from ect.core.monitor import ConsoleMonitor, Monitor
from ect.core.objectio import OBJECT_IO_REGISTRY, find_writer, read_object, write_object
from ect.core.op import OP_REGISTRY, parse_op_args, OpMetaInfo
from ect.core.plugin import PLUGIN_REGISTRY
from ect.core.util import to_datetime_range
from ect.core.workflow import Workflow
from ect.ui.workspace import WorkspaceManager, FSWorkspaceManager
from ect.version import __version__

# Explicitly load ECT-internal plugins.
__import__('ect.ds')
__import__('ect.ops')

#: Name of the ECT CLI executable (= ``ect``).
CLI_NAME = 'ect'

_DOCS_URL = 'http://ect-core.readthedocs.io/en/latest/'

_LICENSE = """
ECT, the ESA CCI Toolbox, version %s
Copyright (c) 2016 by ECT Development team and contributors

This program is free software: you can redistribute it and/or modify
it under the terms of the MIT License (MIT) as published by
the Open Source Initiative.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
MIT License for more details.

You should have received a copy of the MIT License along with this
program. If not, see https://opensource.org/licenses/MIT.
""" % __version__

WRITE_FORMAT_NAMES = OBJECT_IO_REGISTRY.get_format_names('w')
READ_FORMAT_NAMES = OBJECT_IO_REGISTRY.get_format_names('r')


def _new_workspace_manager() -> WorkspaceManager:
    return FSWorkspaceManager()


def _to_str_const(s: str) -> str:
    return "'%s'" % s.replace('\\', '\\\\').replace("'", "\\'")


def _parse_open_arg(load_arg: str) -> Tuple[str, str, str]:
    """
    Parse string argument ``DS := "DS_NAME=DS_ID[,DATE1[,DATE2]]"`` and return tuple DS_NAME,DS_ID,DATE1,DATE2.

    :param load_arg: The DS string argument
    :return: The tuple DS_NAME,DS_ID,DATE1,DATE2
    """
    ds_name_and_ds_id = load_arg.split('=', maxsplit=1)
    ds_name, ds_id = ds_name_and_ds_id if len(ds_name_and_ds_id) == 2 else (None, load_arg)
    ds_id_and_date_range = ds_id.rsplit(',', maxsplit=2)
    if len(ds_id_and_date_range) == 3:
        ds_id, date1, date2 = ds_id_and_date_range
    elif len(ds_id_and_date_range) == 2:
        ds_id, date1, date2 = ds_id_and_date_range[0], ds_id_and_date_range[1], None
    else:
        ds_id, date1, date2 = ds_id_and_date_range[0], None, None
    return ds_name if ds_name else None, ds_id if ds_id else None, date1 if date1 else None, date2 if date2 else None


def _parse_read_arg(read_arg: str) -> Tuple[str, str]:
    """
    Parse string argument ``FILE := "INP_NAME=PATH[,FORMAT]`` and return tuple INP_NAME,PATH,FORMAT.

    :param read_arg: The FILE string argument
    :return: The tuple INP_NAME,PATH,FORMAT
    """
    return _parse_write_arg(read_arg)


def _parse_write_arg(write_arg):
    """
    Parse string argument ``FILE := "[OUT_NAME=]PATH[,FORMAT]`` and return tuple OUT_NAME,PATH,FORMAT.

    :param write_arg: The FILE string argument
    :return: The tuple OUT_NAME,PATH,FORMAT
    """
    name_and_path = write_arg.split('=', maxsplit=1)
    name, path = name_and_path if len(name_and_path) == 2 else (None, write_arg)
    path_and_format = path.rsplit(',', maxsplit=1)
    path, format_name = path_and_format if len(path_and_format) == 2 else (path, None)
    return name if name else None, path if path else None, format_name.upper() if format_name else None


def _list_items(category_singular_name: str, category_plural_name: str, names, pattern: str):
    if pattern:
        pattern = pattern.lower()
        names = [name for name in names if pattern in name.lower()]
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


def _get_op_data_type_str(data_type: str):
    return data_type.__name__ if isinstance(data_type, type) else repr(data_type)


def _get_op_io_info_str(inputs_or_outputs: dict, title_singluar: str, title_plural: str, title_none: str) -> str:
    op_info_str = ''
    op_info_str += '\n'
    if inputs_or_outputs:
        op_info_str += '%s:' % (title_singluar if len(inputs_or_outputs) == 1 else title_plural)
        for name, properties in inputs_or_outputs.items():
            op_info_str += '\n'
            op_info_str += '  %s (%s)' % (name, _get_op_data_type_str(properties.get('data_type', object)))
            description = properties.get('description', None)
            if description:
                op_info_str += '\n'
                op_info_str += '      ' + description
            keys = sorted(properties.keys())
            for key in keys:
                if key not in ['data_type', 'description', 'position']:
                    op_info_str += '\n'
                    op_info_str += '      ' + key.replace('_', ' ') + ': ' + str(properties[key])
    else:
        op_info_str += title_none
    op_info_str += '\n'
    return op_info_str


def _get_op_info_str(op_meta_info: OpMetaInfo):
    """
    Generate an info string for the *op_meta_info*.
    :param op_meta_info: operation meta information (from e.g. workflow or operation)
    :return: an information string
    """
    op_info_str = ''

    title = 'Operation %s' % op_meta_info.qualified_name

    op_info_str += '\n'
    op_info_str += title
    op_info_str += '\n'
    op_info_str += len(title) * '='
    op_info_str += '\n'

    description = op_meta_info.header.get('description', None)
    if description:
        op_info_str += '\n'
        op_info_str += str(description)
        op_info_str += '\n'

    version = op_meta_info.header.get('version', None)
    if version:
        op_info_str += '\n'
        op_info_str += 'Version: '
        op_info_str += str(version)
        op_info_str += '\n'

    op_info_str += _get_op_io_info_str(op_meta_info.input, 'Input', 'Inputs', 'Operation does not have any inputs.')
    op_info_str += _get_op_io_info_str(op_meta_info.output, 'Output', 'Outputs', 'Operation does not have any outputs.')

    return op_info_str


class CommandError(Exception):
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
    Represents (sub-)command for ECT's command-line interface.
    If a plugin wishes to extend ECT's CLI, it may append a new call derived from ``Command`` to the list
    ``REGISTRY``.
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
        return ConsoleMonitor(stay_in_line=True, progress_bar_size=30)


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


class RunCommand(Command):
    """
    The ``run`` command is used to invoke registered operations and JSON workflows.
    """

    @classmethod
    def name(cls):
        return 'run'

    @classmethod
    def parser_kwargs(cls):
        help_line = 'Run an operation or Workflow file.'
        return dict(help=help_line,
                    description='%s Type "ect op list" to list all available operations.' % help_line)

    @classmethod
    def configure_parser(cls, parser):
        parser.add_argument('--monitor', '-m', action='store_true',
                            help='Display progress information during execution.')
        parser.add_argument('--open', '-o', action='append', metavar='DS_EXPR', dest='open_args',
                            help='Open a dataset from DS_EXPR.\n'
                                 'The DS_EXPR syntax is NAME=DS[,START[,END]]. '
                                 'DS must be a valid data source name. Type "ect ds list" to show '
                                 'all known data source names. START and END are dates and may be used to create '
                                 'temporal data subsets. The dataset loaded will be assigned to the arbitrary '
                                 'name NAME which is used to pass the datasets or its variables'
                                 'as an OP argument. To pass a variable use syntax NAME.VAR_NAME.')
        parser.add_argument('--read', '-r', action='append', metavar='FILE_EXPR', dest='read_args',
                            help='Read object from FILE_EXPR.\n'
                                 'The FILE_EXPR syntax is NAME=PATH[,FORMAT]. Possible value for FORMAT {formats}. '
                                 'If FORMAT is not provided, file format is derived from the PATH\'s '
                                 'filename extensions or file content. '
                                 'NAME may be passed as an OP argument that receives a dataset, dataset '
                                 'variable or any other data type. To pass a variable of a dataset use '
                                 'syntax NAME.VAR_NAME'
                                 ''.format(formats=', '.join(READ_FORMAT_NAMES)))
        parser.add_argument('--write', '-w', action='append', metavar='FILE_EXPR', dest='write_args',
                            help='Write result to FILE_EXPR. '
                                 'The FILE_EXPR syntax is [NAME=]PATH[,FORMAT]. Possible value for FORMAT {formats}. '
                                 'If FORMAT is not provided, file format is derived from the object '
                                 'type and the PATH\'s filename extensions. If OP returns multiple '
                                 'named output values, NAME is used to identify them. Multiple -w '
                                 'options may be used in this case. Type "ect write list" to show'
                                 'list of allowed format names.'
                                 ''.format(formats=', '.join(WRITE_FORMAT_NAMES)))
        parser.add_argument('op_name', metavar='OP',
                            help='Fully qualified operation name or Workflow file. '
                                 'Type "ect op list" to list available operators.')
        parser.add_argument('op_args', metavar='...', nargs=argparse.REMAINDER,
                            help='Operation arguments given as KEY=VALUE. KEY is any supported input by OP. VALUE '
                                 'depends on the expected data type of an OP input. It can be a True, False, '
                                 'a string, a numeric constant, one of the names specified by the --open and --read '
                                 'options, or a Python expression. Type "ect op info OP" to print information '
                                 'about the supported OP input names to be used as KEY and their data types to be '
                                 'used as VALUE.')

    def execute(self, command_args):

        op_name = command_args.op_name
        is_workflow = op_name.endswith('.json') and os.path.isfile(op_name)

        namespace = dict()

        if command_args.open_args:
            open_args = list(map(_parse_open_arg, command_args.open_args))
            for res_name, ds_name, start_date, end_date in open_args:
                if not res_name:
                    raise CommandError("missing NAME in --open option")
                if res_name in namespace:
                    raise CommandError("ambiguous NAME in --open option")
                if command_args.monitor:
                    monitor = self.new_monitor()
                else:
                    monitor = Monitor.NULL
                namespace[res_name] = open_dataset(ds_name,
                                                   start_date=start_date,
                                                   end_date=end_date,
                                                   sync=True,
                                                   monitor=monitor)

        if command_args.read_args:
            read_args = list(map(_parse_read_arg, command_args.read_args))
            for res_name, file, format_name in read_args:
                if not res_name:
                    raise CommandError('missing NAME "%s" in --read option' % res_name)
                if res_name in namespace:
                    raise CommandError('ambiguous NAME "%s" in --read option' % res_name)
                namespace[res_name], _ = read_object(file, format_name=format_name)

        op_args, op_kwargs = parse_op_args(command_args.op_args, namespace)

        if is_workflow:
            if op_args:
                raise CommandError("can't run workflow with positional arguments %s, "
                                   "please provide keyword=value pairs only" % op_args)
            op = Workflow.load(command_args.op_name)
        else:
            op = OP_REGISTRY.get_op(command_args.op_name)
            if op is None:
                raise CommandError('unknown operation "%s"' % op_name)

        write_args = None
        if command_args.write_args:
            write_args = list(map(_parse_write_arg, command_args.write_args))
            if op.op_meta_info.has_named_outputs:
                for out_name, file, format_name in write_args:
                    if not out_name:
                        raise CommandError("all --write options must have a NAME")
                    if out_name not in op.op_meta_info.output:
                        raise CommandError('NAME "%s" in --write option is not an OP output' % out_name)
            else:
                if len(write_args) > 1:
                    raise CommandError("multiple --write options given for singular result")
                out_name, file, format_name = write_args[0]
                if out_name and out_name != 'return':
                    raise CommandError('NAME "%s" in --write option is not an OP output' % out_name)

        if command_args.monitor:
            monitor = self.new_monitor()
        else:
            monitor = Monitor.NULL

        # print("Running '%s' with args=%s and kwargs=%s" % (op.op_meta_info.qualified_name, op_args, dict(op_kwargs)))
        return_value = op(monitor=monitor, **op_kwargs)
        if op.op_meta_info.has_named_outputs:
            if write_args:
                for out_name, file, format_name in write_args:
                    out_value = return_value[out_name]
                    writer = find_writer(out_value, file, format_name=format_name)
                    if writer:
                        print('Writing output "%s" to %s using %s format...' % (out_name, file, writer.format_name))
                        writer.write(out_value, file)
                    else:
                        raise CommandError('unknown format for --write output "%s"' % out_name)
            else:
                pprint.pprint(return_value)
        else:
            if write_args:
                _, file, format_name = write_args[0]
                writer = find_writer(return_value, file, format_name=format_name)
                if writer:
                    print("Writing output to %s using %s format..." % (file, writer.format_name))
                    writer.write(return_value, file)
                else:
                    raise CommandError("unknown format for --write option")
            else:
                return_type = op.op_meta_info.output['return'].get('data_type', object)
                is_void = return_type is None or issubclass(return_type, type(None))
                if not is_void:
                    pprint.pprint(return_value)


class WorkspaceCommand(SubCommandCommand):
    """
    The ``ws`` command implements various operations w.r.t. *workspaces*.
    """

    @classmethod
    def name(cls):
        return 'ws'

    @classmethod
    def parser_kwargs(cls):
        help_line = 'Manage workspaces.'
        return dict(help=help_line, description=help_line)

    @classmethod
    def configure_parser_and_subparsers(cls, parser, subparsers):
        init_parser = subparsers.add_parser('init', help='Initialize workspace.')
        init_parser.add_argument('base_dir', metavar='DIR', nargs='?',
                                 help='Base directory for the new workspace. '
                                      'Default DIR is current working directory.')
        init_parser.add_argument('--description', '-d', metavar='DESCRIPTION',
                                 help='Workspace description.')
        init_parser.set_defaults(sub_command_function=cls._execute_init)

        del_parser = subparsers.add_parser('del', help='Delete workspace.')
        del_parser.add_argument('base_dir', metavar='DIR', nargs='?',
                                help='Base directory of the workspace to be deleted. '
                                     'Default DIR is current working directory.')
        del_parser.add_argument('-y', '--yes', dest='yes', action='store_true', default=False,
                                help='Do not ask for confirmation.')
        del_parser.set_defaults(sub_command_function=cls._execute_del)

        status_parser = subparsers.add_parser('status', help='Print workspace information.')
        status_parser.add_argument('base_dir', metavar='DIR', nargs='?',
                                   help='Base directory for the new workspace. '
                                        'Default DIR is current working directory.')
        status_parser.set_defaults(sub_command_function=cls._execute_status)

    @classmethod
    def _execute_init(cls, command_args):
        workspace_manager = _new_workspace_manager()
        workspace_manager.init_workspace(base_dir=command_args.base_dir, description=command_args.description)
        print('Workspace initialized.')

    @classmethod
    def _execute_del(cls, command_args):
        base_dir = command_args.base_dir
        workspace_manager = _new_workspace_manager()
        workspace_manager.get_workspace(base_dir=base_dir)
        if command_args.yes:
            answer = 'y'
        else:
            prompt = 'Do you really want to delete workspace "%s" ([y]/n)? ' % (base_dir or '.')
            answer = input(prompt)
        if not answer or answer.lower() == 'y':
            workspace_manager.delete_workspace(base_dir=base_dir)
            print('Workspace deleted.')

    @classmethod
    def _execute_status(cls, command_args):
        workspace_manager = _new_workspace_manager()
        workspace = workspace_manager.get_workspace(base_dir=command_args.base_dir)
        workflow = workspace.workflow
        print('Workspace base directory is %s' % workspace.base_dir)
        if len(workflow.steps) > 0:
            print('Workspace resources:')
            for step in workflow.steps:
                print('  %s' % str(step))
        else:
            print('Workspace has no resources.')


class ResourceCommand(SubCommandCommand):
    """
    The ``ws`` command implements various operations w.r.t. *workspaces*.
    """

    @classmethod
    def name(cls):
        return 'res'

    @classmethod
    def parser_kwargs(cls):
        help_line = 'Manage workspace resources.'
        return dict(help=help_line, description=help_line)

    @classmethod
    def configure_parser_and_subparsers(cls, parser, subparsers):

        open_parser = subparsers.add_parser('open',
                                            help='Open a dataset from a data source and set a resource.')
        open_parser.add_argument('res_name', metavar='NAME',
                                 help='Name of the new target resource.')
        open_parser.add_argument('ds_name', metavar='DS',
                                 help='A data source named DS. Type "ect ds list" to list valid data source names.')
        open_parser.add_argument('start_date', metavar='START', nargs='?',
                                 help='Start date. Use format YYYY[-MM[-DD]].')
        open_parser.add_argument('end_date', metavar='END', nargs='?',
                                 help='End date. Use format YYYY[-MM[-DD]].')
        open_parser.set_defaults(sub_command_function=cls._execute_open)

        read_parser = subparsers.add_parser('read',
                                            help='Read an object from a file and set a resource.')
        read_parser.add_argument('res_name', metavar='NAME',
                                 help='Name of the new target resource.')
        read_parser.add_argument('file_path', metavar='FILE',
                                 help='File path.')
        read_parser.add_argument('--format', '-f', dest='format_name', metavar='FORMAT',
                                 choices=READ_FORMAT_NAMES,
                                 help='File format. Possible FORMAT values are {format}.'
                                      ''.format(format=', '.join(READ_FORMAT_NAMES)))
        # TODO (forman, 20160913): support reader-specific arguments
        # read_parser.add_argument('op_args', metavar='...', nargs=argparse.REMAINDER,
        #                           help='Specific reader arguments. '
        #                                'Type "ect res read -h" to list format-specific read arguments')
        read_parser.set_defaults(sub_command_function=cls._execute_read)

        write_parser = subparsers.add_parser('write',
                                             help='Write a resource to a file.')
        write_parser.add_argument('res_name', metavar='NAME',
                                  help='Name of an existing resource.')
        write_parser.add_argument('file_path', metavar='FILE',
                                  help='File path.')
        write_parser.add_argument('--format', '-f', dest='format_name', metavar='FORMAT',
                                  choices=WRITE_FORMAT_NAMES,
                                  help='File format. Possible FORMAT values are {format}.'
                                       ''.format(format=', '.join(WRITE_FORMAT_NAMES)))
        # TODO (forman, 20160913): support writer-specific arguments
        # read_parser.add_argument('op_args', metavar='...', nargs=argparse.REMAINDER,
        #                           help='Specific reader arguments. '
        #                                'Type "ect res write -h" to list format-specific write arguments')
        write_parser.set_defaults(sub_command_function=cls._execute_write)

        set_parser = subparsers.add_parser('set',
                                           help='Create a workflow operation and set a resource.')
        set_parser.add_argument('res_name', metavar='NAME',
                                help='Name of the new or existing target resource.')
        set_parser.add_argument('op_name', metavar='OP',
                                help='Operation name.')
        set_parser.add_argument('op_args', metavar='...', nargs=argparse.REMAINDER,
                                help='Operation arguments.')
        set_parser.set_defaults(sub_command_function=cls._execute_set)

        # TODO (forman, 20160922): implement "ect res plot"
        # plot_parser = subparsers.add_parser('plot', help='Plot a resource.')
        # plot_parser.set_defaults(sub_command_function=cls._execute_plot)

        # TODO (forman, 20160922): implement "ect res print"
        # print_parser = subparsers.add_parser('print', help='Print a resource value.')
        # print_parser.set_defaults(sub_command_function=cls._execute_print)

        # TODO (forman, 20160922): implement "ect res rename"
        # rename_parser = subparsers.add_parser('rename', help='Rename a resource.')
        # rename_parser.add_argument('res_name_old', metavar='OLD_NAME',
        #                            help='Old resource name.')
        # rename_parser.add_argument('res_name_new', metavar='NEW_NAME',
        #                            help='New resource name.')
        # rename_parser.set_defaults(sub_command_function=cls._execute_rename)

        # TODO (forman, 20160916): implement "ect res del"
        # del_parser = subparsers.add_parser('del', help='Delete a resource.')
        # del_parser.add_argument('res_name', metavar='DIR',
        #                         help='Resource name.')
        # del_parser.set_defaults(sub_command_function=cls._execute_del)

    @classmethod
    def _execute_open(cls, command_args):
        workspace_manager = _new_workspace_manager()
        ds_name = command_args.ds_name
        op_args = ['ds_name=%s' % _to_str_const(ds_name)]
        if command_args.start_date:
            op_args.append('start_date=%s' % _to_str_const(command_args.start_date))
        if command_args.end_date:
            op_args.append('end_date=%s' % _to_str_const(command_args.end_date))
        op_args.append('sync=True')
        workspace_manager.set_workspace_resource('', command_args.res_name, 'ect.ops.io.open_dataset', op_args)
        print('Resource "%s" set.' % command_args.res_name)

    @classmethod
    def _execute_read(cls, command_args):
        workspace_manager = _new_workspace_manager()
        op_args = ['file=%s' % _to_str_const(command_args.file_path)]
        if command_args.format_name:
            op_args.append('format=%s' % _to_str_const(command_args.format_name))
        workspace_manager.set_workspace_resource('', command_args.res_name, 'ect.ops.io.read_object', op_args)
        print('Resource "%s" set.' % command_args.res_name)

    @classmethod
    def _execute_write(cls, command_args):
        workspace_manager = _new_workspace_manager()
        res_name = command_args.res_name
        file_path = command_args.file_path
        format_name = command_args.format_name
        # TBD: shall we add a new step to the workflow or just execute the workflow,
        # then write the desired resource?
        workspace = workspace_manager.get_workspace('')
        monitor = cls.new_monitor()
        result = workspace.workflow(monitor=monitor)
        if res_name in result:
            obj = result[res_name]
        else:
            obj = result
        print('Writing resource "%s" to %s...' % (res_name, file_path))
        write_object(obj, file_path, format_name=format_name)
        print('Resource "%s" written to %s' % (res_name, file_path))

    @classmethod
    def _execute_set(cls, command_args):
        workspace_manager = _new_workspace_manager()
        workspace_manager.set_workspace_resource('',
                                                 command_args.res_name,
                                                 command_args.op_name, command_args.op_args)
        print('Resource "%s" set.' % command_args.res_name)


class OperationCommand(SubCommandCommand):
    """
    The ``op`` command implements various operations w.r.t. *operations*.
    """

    @classmethod
    def name(cls):
        return 'op'

    @classmethod
    def parser_kwargs(cls):
        help_line = 'Explore data operations.'
        return dict(help=help_line, description=help_line)

    @classmethod
    def configure_parser_and_subparsers(cls, parser, subparsers):
        list_parser = subparsers.add_parser('list', help='List operations.')
        list_parser.add_argument('--name', '-n', metavar='NAME',
                                 help="List only operations with name NAME or "
                                      "that have NAME in their name. "
                                      "The comparison is case insensitive.")
        list_parser.add_argument('--tag', '-t', metavar='TAG',
                                 help="List only operations tagged by TAG or "
                                      "that have TAG in one of their tags. "
                                      "The comparison is case insensitive.")
        list_parser.set_defaults(sub_command_function=cls._execute_list)

        info_parser = subparsers.add_parser('info', help='Show usage information about an operation.')
        info_parser.add_argument('op_name', metavar='OP',
                                 help="Fully qualified operation name.")
        info_parser.set_defaults(sub_command_function=cls._execute_info)

    @classmethod
    def _execute_list(cls, command_args):
        op_registrations = OP_REGISTRY.op_registrations

        def _op_has_tag(op_registration, tag_part):
            tags = op_registration.op_meta_info.header.get('tags', None)
            if tags:
                tag_part = tag_part.lower()
                if isinstance(tags, list):
                    return any(tag_part in tag.lower() for tag in tags)
                elif isinstance(tags, str):
                    return tag_part in tags.lower()
            return False

        op_names = op_registrations.keys()
        if command_args.tag:
            op_names = sorted(name for name in op_names if _op_has_tag(op_registrations.get(name), command_args.tag))
        name_pattern = None
        if command_args.name:
            name_pattern = command_args.name
        _list_items('operation', 'operations', op_names, name_pattern)

    @classmethod
    def _execute_info(cls, command_args):
        op_name = command_args.op_name
        if not op_name:
            raise CommandError('missing OP argument')
        op_registration = OP_REGISTRY.get_op(op_name)
        if not op_registration:
            raise CommandError('unknown operation "%s"' % op_name)
        print(_get_op_info_str(op_registration.op_meta_info))


class DataSourceCommand(SubCommandCommand):
    """
    The ``ds`` command implements various operations w.r.t. data sources.
    """

    @classmethod
    def name(cls):
        return 'ds'

    @classmethod
    def parser_kwargs(cls):
        help_line = 'Manage data sources.'
        return dict(help=help_line, description=help_line)

    @classmethod
    def configure_parser_and_subparsers(cls, parser, subparsers):
        list_parser = subparsers.add_parser('list', help='List all available data sources')
        list_parser.add_argument('--name', '-n', metavar='NAME',
                                 help="List only data sources named NAME or "
                                      "that have NAME in their name. "
                                      "The comparison is case insensitive.")
        # TODO (marcoz, 20160905): implement "ect ds list --var"
        # list_parser.add_argument('--var', '-v', metavar='VAR',
        #                          help="List only data sources with a variable named NAME or "
        #                               "with variables that have NAME in their name. "
        #                               "The comparison is case insensitive.")
        list_parser.set_defaults(sub_command_function=cls._execute_list)

        sync_parser = subparsers.add_parser('sync',
                                            help='Synchronise a remote data source with its local version.')
        sync_parser.add_argument('ds_name', metavar='DS',
                                 help='A data source name. '
                                      'Type "ect ds list" to show all possible data source names.')
        sync_parser.add_argument('start_date', metavar='START', nargs='?',
                                 help='Start date with format YYYY[-MM[-DD]].')
        sync_parser.add_argument('end_date', metavar='END', nargs='?',
                                 help='End date with format YYYY[-MM[-DD]]. '
                                      'END date must be greater than START date.')
        sync_parser.set_defaults(sub_command_function=cls._execute_sync)

        info_parser = subparsers.add_parser('info', help='Display information about a data source.')
        info_parser.add_argument('ds_name', metavar='DS',
                                 help='A data source name. '
                                      'Type "ect ds list" to show all possible data source names.')
        info_parser.add_argument('--var', '-v', action='store_true',
                                 help="Also display information about contained dataset variables.")
        info_parser.set_defaults(sub_command_function=cls._execute_info)

    @classmethod
    def _execute_list(cls, command_args):
        data_store = DATA_STORE_REGISTRY.get_data_store('default')
        if data_store is None:
            raise RuntimeError('internal error: no default data store found')

        ds_name = command_args.name

        _list_items('data source', 'data sources',
                    sorted(data_source.name for data_source in data_store.query()), ds_name)

    @classmethod
    def _execute_info(cls, command_args):
        data_store = DATA_STORE_REGISTRY.get_data_store('default')
        if data_store is None:
            raise RuntimeError('internal error: no default data store found')

        ds_name = command_args.ds_name
        data_sources = [data_source for data_source in data_store.query(name=ds_name) if data_source.name == ds_name]
        if not data_sources:
            raise CommandError('data source "%s" not found' % ds_name)

        data_source = data_sources[0]
        title = 'Data source %s' % data_source.name
        print()
        print(title)
        print('=' * len(title))
        print()
        print(data_source.info_string)
        if command_args.var:
            print()
            print('Variables')
            print('---------')
            print()
            print(data_source.variables_info_string)

    @classmethod
    def _execute_sync(cls, command_args):
        data_store = DATA_STORE_REGISTRY.get_data_store('default')
        if data_store is None:
            raise RuntimeError('internal error: no default data store found')

        ds_name = command_args.ds_name
        data_sources = data_store.query(name=ds_name)
        if not data_sources:
            raise CommandError('data source "%s" not found' % ds_name)

        data_source = data_sources[0]
        if command_args.start_date:
            try:
                time_range = to_datetime_range(command_args.start_date, command_args.end_date)
            except ValueError:
                raise CommandError('invalid START and/or END date')
        else:
            time_range = None

        num_sync, num_total = data_source.sync(time_range=time_range,
                                               monitor=cls.new_monitor())
        print(('%d of %d file(s) synchronized' % (num_sync, num_total)) if num_total > 0 else 'No files found')


class PluginCommand(SubCommandCommand):
    """
    The ``pi`` command lists the content of various plugin registry.
    """

    CMD_NAME = 'pi'

    @classmethod
    def name(cls):
        return 'pi'

    @classmethod
    def parser_kwargs(cls):
        help_line = 'Manage installed plugins.'
        return dict(help=help_line, description=help_line)

    @classmethod
    def configure_parser_and_subparsers(cls, parser, subparsers):
        list_parser = subparsers.add_parser('list', help='List plugins')
        list_parser.add_argument('--name', '-n', metavar='NAME_PATTERN',
                                 help="List only plugins named NAME or "
                                      "that have NAME in their name. "
                                      "The comparison is case insensitive.")
        list_parser.set_defaults(sub_command_function=cls._execute_list)

    @classmethod
    def _execute_list(cls, command_args):
        name_pattern = None
        if command_args.name:
            name_pattern = command_args.name
        _list_items('plugin', 'plugins', sorted(PLUGIN_REGISTRY.keys()), name_pattern)


class LicenseCommand(Command):
    """
    The ``lic`` command is used to display ECT's licensing information.
    """

    @classmethod
    def name(cls):
        return 'lic'

    @classmethod
    def parser_kwargs(cls):
        help_line = 'Print copyright and license information.'
        return dict(help=help_line, description=help_line)

    def execute(self, command_args):
        print(_LICENSE)


class DocsCommand(Command):
    """
    The ``doc`` command is used to display ECT's documentation.
    """

    @classmethod
    def name(cls):
        return 'doc'

    @classmethod
    def parser_kwargs(cls):
        help_line = 'Display documentation in a browser window.'
        return dict(help=help_line, description=help_line)

    def execute(self, command_args):
        import webbrowser
        webbrowser.open_new_tab(_DOCS_URL)


#: List of sub-commands supported by the CLI. Entries are classes derived from :py:class:`Command` class.
#: ECT plugins may extend this list by their commands during plugin initialisation.
COMMAND_REGISTRY = [
    RunCommand,
    WorkspaceCommand,
    ResourceCommand,
    DataSourceCommand,
    OperationCommand,
    PluginCommand,
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
    parser.add_argument('--traceback', action='store_true', help='On error, print (Python) stack traceback')
    subparsers = parser.add_subparsers(dest='command_name',
                                       metavar='COMMAND',
                                       help='One of the following commands. '
                                            'Type "COMMAND -h" to get command-specific help.')

    for command_class in COMMAND_REGISTRY:
        command_name = command_class.name()
        command_parser_kwargs = command_class.parser_kwargs()
        command_parser = subparsers.add_parser(command_name, **command_parser_kwargs)
        command_class.configure_parser(command_parser)
        command_parser.set_defaults(command_class=command_class)

    command_name, status, message = None, 0, None
    try:
        args_obj = parser.parse_args(args)

        if args_obj.command_name and args_obj.command_class:
            command_name = args_obj.command_name
            # noinspection PyBroadException
            try:
                args_obj.command_class().execute(args_obj)
            except Exception as e:
                if args_obj.traceback:
                    traceback.print_exc()
                status, message = 1, str(e)
        else:
            parser.print_help()

    except NoExitArgumentParser.ExitException as e:
        status, message = e.status, e.message

    if message:
        if status:
            if command_name:
                # error from command execution
                sys.stderr.write("%s %s: error: %s\n" % (CLI_NAME, command_name, message))
            else:
                # error from command parser (includes "ect: error: " prefix)
                sys.stderr.write("%s\n" % message)
        else:
            sys.stdout.write("%s\n" % message)

    return status


if __name__ == '__main__':
    sys.exit(main())
