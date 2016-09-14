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
import sys
from abc import ABCMeta, abstractmethod
from typing import Tuple, Optional

from ect.core.monitor import ConsoleMonitor, Monitor
from ect.core.objectio import find_writer
from ect.core.op import OP_REGISTRY, parse_op_args, OpMetaInfo
from ect.core.workflow import Workflow
from ect.ops.io import load_dataset
from ect.ui.workspace import WorkspaceManager, FSWorkspaceManager, WorkspaceError
from ect.version import __version__

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


def _new_workspace_manager() -> WorkspaceManager:
    return FSWorkspaceManager()


def _parse_load_arg(load_arg: str) -> Tuple[str, str, str]:
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


def _get_op_data_type_str(data_type: str):
    return data_type.__name__ if isinstance(data_type, type) else repr(data_type)


def _get_op_io_info_str(inputs_or_outputs: dict, title_singluar: str, title_plural: str, title_none: str) -> str:
    op_info_str = ''
    op_info_str += '\n'
    if inputs_or_outputs:
        op_info_str += '%s:' % (title_singluar if len(inputs_or_outputs) == 1 else title_plural)
        for name, properties in inputs_or_outputs.items():
            op_info_str += '\n'
            op_info_str += '  %s (%s)' % (name, _get_op_data_type_str(properties.get('data_type', 'object')))
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
                raise RuntimeError('neither command_args.sub_command_function nor command_args.parser defined')
            parser.print_help()
            return Command.STATUS_OK
        return sub_command_function(command_args)


class RunCommand(Command):
    """
    The ``run`` command is used to invoke registered operations and JSON workflows.
    """

    CMD_NAME = 'run'

    @classmethod
    def name_and_parser_kwargs(cls):
        help_line = 'Run an operation.'
        return cls.CMD_NAME, dict(help=help_line,
                                  description='%s Type "ect op list" to list all available operations.' % help_line)

    @classmethod
    def configure_parser(cls, parser):
        parser.add_argument('--monitor', '-m', action='store_true',
                            help='Display progress information during execution.')
        parser.add_argument('--load', '-l', action='append', metavar='DS', dest='load_args',
                            help='Load dataset from data source DS.\n'
                                 'The DS syntax is DS_NAME=DS_ID[,DATE1[,DATE2]]. '
                                 'DS_ID must be a valid data source ID. Type "ect ds list" to show '
                                 'all known data source IDs. DATE1 and DATE2 may be used to create '
                                 'data subsets. The dataset loaded will be assigned to the arbitrary '
                                 'name DS_NAME which is used to pass the datasets or its variables'
                                 'as an OP argument. To pass a variable use syntax DS_NAME.VAR_NAME.')
        parser.add_argument('--read', '-r', action='append', metavar='FILE', dest='read_args',
                            help='Read object from FILE.\n'
                                 'The FILE syntax is INP_NAME=PATH[,FORMAT]. '
                                 'If FORMAT is not provided, file format is derived from the PATH\'s '
                                 'filename extensions or file content. INP_NAME '
                                 'may be passed as an OP argument that receives a dataset, dataset '
                                 'variable or any other data type. To pass a variable of a dataset use '
                                 'syntax INP_NAME.VAR_NAME')
        parser.add_argument('--write', '-w', action='append', metavar='FILE', dest='write_args',
                            help='Write result to FILE. '
                                 'The FILE syntax is [OUT_NAME=]PATH[,FORMAT]. '
                                 'If FORMAT is not provided, file format is derived from the object '
                                 'type and the PATH\'s filename extensions. If OP returns multiple '
                                 'named output values, OUT_NAME is used to identify them. Multiple -w '
                                 'options may be used in this case. Type "ect write list" to show'
                                 'list of allowed format names.')
        parser.add_argument('op_name', metavar='OP', nargs='?',
                            help="Fully qualified operation name or alias")
        parser.add_argument('op_args', metavar='...', nargs=argparse.REMAINDER,
                            help="Operation arguments. Use '-h' to print operation details.")

    def execute(self, command_args):
        op_name = command_args.op_name
        if not op_name:
            return 1, "error: command '%s' requires OP argument" % self.CMD_NAME
        is_workflow = op_name.endswith('.json') and os.path.isfile(op_name)

        namespace = dict()

        if command_args.load_args:
            load_args = list(map(_parse_load_arg, command_args.load_args))
            for ds_name, ds_id, date1, date2 in load_args:
                if not ds_name:
                    return 1, "error: command '%s': missing DS_NAME in --load option" % RunCommand.CMD_NAME
                if ds_name in namespace:
                    return 1, "error: command '%s': ambiguous DS_NAME in --load option" % RunCommand.CMD_NAME
                namespace[ds_name] = load_dataset(ds_id, date1, date2)

        if command_args.read_args:
            read_args = list(map(_parse_read_arg, command_args.read_args))
            for inp_name, inp_path, inp_format in read_args:
                if not inp_name:
                    return 1, "error: command '%s': missing INP_NAME \"%s\" in --read option" % (
                        RunCommand.CMD_NAME, inp_name)
                if inp_name in namespace:
                    return 1, "error: command '%s': ambiguous INP_NAME \"%s\" in --read option" % (
                        RunCommand.CMD_NAME, inp_name)
                from ect.core.objectio import read_object
                namespace[inp_name], _ = read_object(inp_path, format_name=inp_format)

        try:
            op_args, op_kwargs = parse_op_args(command_args.op_args, namespace)
        except ValueError as e:
            return 1, "error: command '%s': %s" % (RunCommand.CMD_NAME, e)

        if is_workflow:
            if op_args:
                return 1, "error: command '%s': can't run workflow with arguments %s, please provide keywords only" % \
                       (RunCommand.CMD_NAME, op_args)
            op = Workflow.load(command_args.op_name)
        else:
            op = OP_REGISTRY.get_op(command_args.op_name)
            if op is None:
                return 1, "error: command '%s': unknown operation '%s'" % (RunCommand.CMD_NAME, op_name)

        write_args = None
        if command_args.write_args:
            write_args = list(map(_parse_write_arg, command_args.write_args))
            if op.op_meta_info.has_named_outputs:
                for out_name, out_path, out_format in write_args:
                    if not out_name:
                        return 1, "error: command '%s': all --write options must have an OUT_NAME" % RunCommand.CMD_NAME
                    if out_name not in op.op_meta_info.output:
                        return 1, "error: command '%s': OUT_NAME \"%s\" in --write option is not an OP output" % (
                            RunCommand.CMD_NAME, out_name)
            else:
                if len(write_args) > 1:
                    return 1, "error: command '%s': multiple --write options given for singular result" % \
                           RunCommand.CMD_NAME
                out_name, out_path, out_format = write_args[0]
                if out_name and out_name != 'return':
                    return 1, "error: command '%s': OUT_NAME \"%s\" in --write option is not an OP output" % (
                        RunCommand.CMD_NAME, out_name)

        if command_args.monitor:
            monitor = ConsoleMonitor()
        else:
            monitor = Monitor.NULL

        print("Running '%s' with args=%s and kwargs=%s" % (op.op_meta_info.qualified_name, op_args, dict(op_kwargs)))
        return_value = op(*op_args, monitor=monitor, **op_kwargs)
        if op.op_meta_info.has_named_outputs:
            if write_args:
                for out_name, out_path, out_format in write_args:
                    out_value = return_value[out_name]
                    writer = find_writer(out_value, out_path, format_name=out_format)
                    if writer:
                        print("Writing output '%s' to %s using %s format..." % (out_name, out_path, writer.format_name))
                        writer.write(out_value, out_path)
                    else:
                        return 1, "error: command '%s': unknown format for --write output '%s'" % (
                            RunCommand.CMD_NAME, out_name)
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


class WorkspaceCommand(SubCommandCommand):
    """
    The ``ws`` command implements various operations w.r.t. *workspaces*.
    """

    CMD_NAME = 'ws'

    @classmethod
    def name_and_parser_kwargs(cls):
        help_line = 'Manage workspaces.'
        return cls.CMD_NAME, dict(help=help_line, description=help_line)

    @classmethod
    def configure_parser_and_subparsers(cls, parser, subparsers):
        init_parser = subparsers.add_parser('init', help='Initialize workspace.')
        init_parser.add_argument('base_dir', metavar='DIR', nargs='?',
                                 help='Base directory for the new workspace. '
                                      'Default DIR is current working directory.')
        init_parser.add_argument('--description', '-d', metavar='DESCRIPTION',
                                 help='Workspace description.')
        init_parser.set_defaults(sub_command_function=cls._execute_init)

        status_parser = subparsers.add_parser('status', help='Print workspace information.')
        status_parser.add_argument('base_dir', metavar='DIR', nargs='?',
                                   help='Base directory for the new workspace. '
                                        'Default DIR is current working directory.')
        status_parser.set_defaults(sub_command_function=cls._execute_status)

    @classmethod
    def _execute_init(cls, command_args):
        workspace_manager = _new_workspace_manager()
        try:
            workspace_manager.init_workspace(base_dir=command_args.base_dir, description=command_args.description)
            print('Workspace initialized.')
        except WorkspaceError as e:
            return 1, "error: command '%s': %s" % (cls.CMD_NAME, str(e))
        return cls.STATUS_OK

    @classmethod
    def _execute_status(cls, command_args):
        workspace_manager = _new_workspace_manager()
        try:
            workspace = workspace_manager.get_workspace(base_dir=command_args.base_dir)
        except WorkspaceError as e:
            return 1, "error: command '%s': %s" % (cls.CMD_NAME, str(e))

        workflow = workspace.workflow
        if len(workflow.steps) > 0:
            print('Workspace steps:')
            for step in workflow.steps:
                print('  %s' % str(step))
        else:
            print('Empty workspace.')

        return cls.STATUS_OK


class WorkspaceResourceCommand(SubCommandCommand):
    """
    The ``ws`` command implements various operations w.r.t. *workspaces*.
    """

    CMD_NAME = 'res'

    @classmethod
    def name_and_parser_kwargs(cls):
        help_line = 'Manage workspace resources.'
        return cls.CMD_NAME, dict(help=help_line, description=help_line)

    @classmethod
    def configure_parser_and_subparsers(cls, parser, subparsers):

        load_parser = subparsers.add_parser('load',
                                            help='Load dataset from a data source and set a resource.')
        load_parser.add_argument('res_name', metavar='NAME',
                                 help='Name of the new target resource.')
        load_parser.add_argument('ds_id', metavar='DS',
                                 help='Data source ID. Type "ect ds list" to list valid data source IDs.')
        load_parser.add_argument('start_date', metavar='START', nargs='?',
                                 help='Start date. Use format YYYY[-MM[-DD]].')
        load_parser.add_argument('end_date', metavar='END', nargs='?',
                                 help='End date. Use format YYYY[-MM[-DD]].')
        load_parser.set_defaults(sub_command_function=cls._execute_load)

        read_parser = subparsers.add_parser('read',
                                            help='Read an object from a file and set a resource.')
        read_parser.add_argument('res_name', metavar='NAME',
                                 help='Name of the new target resource.')
        read_parser.add_argument('file_path', metavar='FILE',
                                 help='File path.')
        read_parser.add_argument('--format', '-f', dest='format_name', metavar='FORMAT',
                                 help='File format. Type')
        # TODO (forman, 20160913): support reader-specific arguments
        # read_parser.add_argument('op_args', metavar='...', nargs=argparse.REMAINDER,
        #                           help='Specific reader arguments. '
        #                                'Type "ect res read -h" to list format-specific read arguments')
        read_parser.set_defaults(sub_command_function=cls._execute_read)

        read_parser = subparsers.add_parser('write',
                                            help='Write a resource to a file.')
        read_parser.add_argument('res_name', metavar='NAME',
                                 help='Name of the new target resource.')
        read_parser.add_argument('file_path', metavar='FILE',
                                 help='File path.')
        read_parser.add_argument('--format', '-f', dest='format_name', metavar='FORMAT',
                                 help='File format. Type')
        # TODO (forman, 20160913): support writer-specific arguments
        # read_parser.add_argument('op_args', metavar='...', nargs=argparse.REMAINDER,
        #                           help='Specific reader arguments. '
        #                                'Type "ect res write -h" to list format-specific write arguments')
        read_parser.set_defaults(sub_command_function=cls._execute_write)

        set_parser = subparsers.add_parser('set',
                                           help='Create a workflow operation and set a resource.')
        set_parser.add_argument('res_name', metavar='NAME',
                                help='Name of the new or existing target resource.')
        set_parser.add_argument('op_name', metavar='OP',
                                help='Operation name.')
        set_parser.add_argument('op_args', metavar='...', nargs=argparse.REMAINDER,
                                help='Operation arguments.')
        set_parser.set_defaults(sub_command_function=cls._execute_set)

        del_parser = subparsers.add_parser('del', help='Delete a resource.')
        del_parser.add_argument('name', metavar='DIR',
                                help='Resource name.')
        del_parser.set_defaults(sub_command_function=cls._execute_del)

    @classmethod
    def _execute_load(cls, command_args):
        workspace_manager = _new_workspace_manager()
        try:
            op_args = ['ds_id=%s' % command_args.ds_id]
            if command_args.start_date:
                op_args.append('start_date=%s' % command_args.start_date)
            if command_args.end_date:
                op_args.append('end_date=%s' % command_args.end_date)
            workspace_manager.set_workspace_resource('', command_args.res_name, 'ect.ops.io.load_dataset', op_args)
            print("Resource '%s' set." % command_args.res_name)
        except WorkspaceError as e:
            return 1, "error: command '%s': %s" % (cls.CMD_NAME, str(e))
        return cls.STATUS_OK

    @classmethod
    def _execute_read(cls, command_args):
        workspace_manager = _new_workspace_manager()
        try:
            op_args = ['file=%s' % command_args.file_path]
            if command_args.format_name:
                op_args.append('format=%s' % command_args.format_name)
            workspace_manager.set_workspace_resource('', command_args.res_name, 'ect.ops.io.read_object', op_args)
            print("Resource '%s' set." % command_args.res_name)
        except WorkspaceError as e:
            return 1, "error: command '%s': %s" % (cls.CMD_NAME, str(e))
        return cls.STATUS_OK

    @classmethod
    def _execute_write(cls, command_args):
        workspace_manager = _new_workspace_manager()
        try:
            res_name = command_args.res_name
            file_path = command_args.file_path
            format_name = command_args.format_name
            # TBD: shall we add a new step to the workflow or just execute the workflow,
            # then write the desired resource?
            workspace = workspace_manager.get_workspace('')
            monitor = ConsoleMonitor(stay_in_line=True, progress_bar_size=80)
            try:
                result = workspace.workflow(monitor=monitor)
                if res_name in result:
                    obj = result[res_name]
                else:
                    obj = result
                from ect.ops.io import write_object
                print("Writing resource '%s' to %s..." % (res_name, file_path))
                write_object(obj, file_path, format=format_name)
                print("Resource '%s' written to %s" % (res_name, file_path))
            except Exception as e:
                return 2, "error: command '%s': %s" % (cls.CMD_NAME, str(e))
        except WorkspaceError as e:
            return 1, "error: command '%s': %s" % (cls.CMD_NAME, str(e))
        return cls.STATUS_OK

    @classmethod
    def _execute_set(cls, command_args):
        workspace_manager = _new_workspace_manager()
        try:
            workspace_manager.set_workspace_resource('', command_args.res_name,
                                                     command_args.op_name, command_args.op_args)
            print("Resource '%s' set." % command_args.res_name)
        except WorkspaceError as e:
            return 1, "error: command '%s': %s" % (cls.CMD_NAME, str(e))

        return cls.STATUS_OK

    @classmethod
    def _execute_del(cls, command_args):
        workspace_manager = _new_workspace_manager()
        try:
            # workspace.remove_resource(command_args.res_name, )
            print("Resource '%s' deleted." % command_args.res_name)
        except WorkspaceError as e:
            return 1, "error: command '%s': %s" % (cls.CMD_NAME, str(e))
        return cls.STATUS_OK


class OperationCommand(SubCommandCommand):
    """
    The ``op`` command implements various operations w.r.t. *operations*.
    """

    CMD_NAME = 'op'

    @classmethod
    def name_and_parser_kwargs(cls):
        help_line = 'Explore data operations.'
        return cls.CMD_NAME, dict(help=help_line, description=help_line)

    @classmethod
    def configure_parser_and_subparsers(cls, parser, subparsers):
        list_parser = subparsers.add_parser('list', help='List operations.')
        list_parser.add_argument('--name', '-n', metavar='NAME',
                                 help="A wildcard pattern to filter operation names. "
                                      "'*' matches zero or many characters, '?' matches a single character. "
                                      "The comparison is case insensitive.")
        list_parser.add_argument('--tag', '-t', metavar='TAG',
                                 help="A wildcard pattern to filter operation tags. "
                                      "'*' matches zero or many characters, '?' matches a single character. "
                                      "The comparison is case insensitive.")
        list_parser.set_defaults(sub_command_function=cls._execute_list)

        info_parser = subparsers.add_parser('info', help='Show usage information about an operation.')
        info_parser.add_argument('op_name', metavar='OP',
                                 help="Fully qualified operation name.")
        info_parser.set_defaults(sub_command_function=cls._execute_info)

    @classmethod
    def _execute_list(cls, command_args):
        op_registrations = OP_REGISTRY.op_registrations

        def _op_has_tag(op_registration, tag_value):
            op_meta_info_header = op_registration.op_meta_info.header
            if 'tags' in op_meta_info_header:
                import fnmatch
                tag_value_lower = tag_value.lower()
                tags_data = op_meta_info_header['tags']
                if isinstance(tags_data, list):
                    return any(fnmatch.fnmatch(single_tag.lower(), tag_value_lower) for single_tag in tags_data)
                elif isinstance(tags_data, str):
                    return fnmatch.fnmatch(tags_data.lower(), tag_value_lower)
            return False

        op_names = op_registrations.keys()
        if command_args.tag:
            op_names = [name for name in op_names if _op_has_tag(op_registrations.get(name), command_args.tag)]
        name_pattern = None
        if command_args.name:
            name_pattern = command_args.name
        _list_items('operation', 'operations', op_names, name_pattern)

    @classmethod
    def _execute_info(cls, command_args):
        if not command_args.op_name:
            return 2, "error: command 'op info': missing OP argument"
        op_registration = OP_REGISTRY.get_op(command_args.op_name)
        if op_registration:
            print(_get_op_info_str(op_registration.op_meta_info))
        else:
            return 2, "error: command 'op info': unknown operation '%s'" % command_args.op_name


class DataSourceCommand(SubCommandCommand):
    """
    The ``ds`` command implements various operations w.r.t. data sources.
    """

    CMD_NAME = 'ds'

    @classmethod
    def name_and_parser_kwargs(cls):
        help_line = 'Manage data sources.'
        return cls.CMD_NAME, dict(help=help_line, description=help_line)

    @classmethod
    def configure_parser_and_subparsers(cls, parser, subparsers):
        list_parser = subparsers.add_parser('list', help='List all available data sources')
        list_parser.add_argument('--id', '-i', metavar='ID_PATTERN',
                                 help="A wildcard pattern to filter data source IDs. "
                                      "'*' matches zero or many characters, '?' matches a single character. "
                                      "The comparison is case insensitive.")
        list_parser.add_argument('--var', '-v', metavar='VAR_PATTERN',
                                 help="A wildcard pattern to filter variable names. "
                                      "'*' matches zero or many characters, '?' matches a single character. "
                                      "The comparison is case insensitive.")
        list_parser.set_defaults(sub_command_function=cls._execute_list)

        sync_parser = subparsers.add_parser('sync',
                                            help='Synchronise a remote data source with its local version.')
        sync_parser.add_argument('ds_id', metavar='DS_ID',
                                 help='Data source ID. Type "ect ds list" to show all possible IDs.')
        sync_parser.add_argument('--time', '-t', nargs=1, metavar='PERIOD',
                                 help='Limit to date/time period. Format of PERIOD is DATE[,DATE] '
                                      'where DATE is YYYY[-MM[-DD]]')
        sync_parser.set_defaults(sub_command_function=cls._execute_sync)

        info_parser = subparsers.add_parser('info', help='Display information about a data source.')
        info_parser.add_argument('ds_id', metavar='DS_ID',
                                 help='Data source ID. Type "ect ds list" to show all possible IDs.')
        info_parser.set_defaults(sub_command_function=cls._execute_info)

    @classmethod
    def _execute_list(cls, command_args):
        from ect.core.io import DATA_STORE_REGISTRY
        data_store = DATA_STORE_REGISTRY.get_data_store('default')
        if data_store is None:
            return 2, "error: command 'ds list': no data_store named 'default' found"
        id_pattern = None
        if command_args.id:
            id_pattern = command_args.id
        var_pattern = None
        if command_args.var:
            # TODO (marcoz, 20160905): option --var not implemented yet
            return 2, "error: command 'ds list': option --var not implemented yet"
            # var_pattern = command_args.var
        _list_items('data source', 'data sources',
                    [data_source.name for data_source in data_store.query()], id_pattern)

    @classmethod
    def _execute_info(cls, command_args):
        from ect.core.io import DATA_STORE_REGISTRY
        data_store = DATA_STORE_REGISTRY.get_data_store('default')
        if data_store is None:
            return 2, "error: command 'ds info': no data_store named 'default' found"

        data_sources = data_store.query(name=command_args.ds_id)
        if not data_sources or len(data_sources) == 0:
            print("Unknown 1 data source '%s'" % command_args.ds_id)
        else:
            for data_source in data_sources:
                print(data_source.info_string)

    @classmethod
    def _execute_sync(cls, command_args):
        from ect.core.io import DATA_STORE_REGISTRY
        data_store = DATA_STORE_REGISTRY.get_data_store('default')
        if data_store is None:
            return 2, "error: command 'ds sync': no data_store named 'default' found"

        data_sources = data_store.query(name=command_args.ds_id)
        data_source = data_sources[0]
        if command_args.time:
            time_range = cls.parse_time_period(command_args.time[0])
            if not time_range:
                return 2, "invalid PERIOD: " + command_args.time[0]
        else:
            time_range = (None, None)
        data_source.sync(time_range=time_range, monitor=ConsoleMonitor())

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


class PluginCommand(SubCommandCommand):
    """
    The ``pi`` command lists the content of various plugin registry.
    """

    CMD_NAME = 'pi'

    @classmethod
    def name_and_parser_kwargs(cls):
        help_line = 'Manage installed plugins.'
        return cls.CMD_NAME, dict(help=help_line, description=help_line)

    @classmethod
    def configure_parser_and_subparsers(cls, parser, subparsers):
        list_parser = subparsers.add_parser('list', help='List plugins')
        list_parser.add_argument('--name', '-n', metavar='NAME_PATTERN',
                                 help="A wildcard pattern to filter plugin names. "
                                      "'*' matches zero or many characters, '?' matches a single character. "
                                      "The comparison is case insensitive.")
        list_parser.set_defaults(sub_command_function=cls._execute_list)

    @classmethod
    def _execute_list(cls, command_args):
        from ect.core.plugin import PLUGIN_REGISTRY as PLUGIN_REGISTRY
        name_pattern = None
        if command_args.name:
            name_pattern = command_args.name
        _list_items('plugin', 'plugins', PLUGIN_REGISTRY.keys(), name_pattern)


class LicenseCommand(Command):
    """
    The ``lic`` command is used to display ECT's licensing information.
    """

    @classmethod
    def name_and_parser_kwargs(cls):
        help_line = 'Print copyright and license information.'
        return 'lic', dict(help=help_line, description=help_line)

    def execute(self, command_args):
        print(_LICENSE)


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
    WorkspaceCommand,
    WorkspaceResourceCommand,
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
    subparsers = parser.add_subparsers(dest='command_name',
                                       metavar='COMMAND',
                                       help='One of the following commands. '
                                            'Type "COMMAND -h" to get command-specific help.')

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
