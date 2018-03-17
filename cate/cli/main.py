# The MIT License (MIT)
# Copyright (c) 2016, 2017 by the ESA CCI Toolbox development team and contributors
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

This module provides Cate's CLI executable.

To use the CLI executable, invoke the module file as a script, type ``python3 cate/cli/main.py [ARGS] [OPTIONS]``.
Type `python3 cate/cli/main.py --help`` for usage help.

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
`test/cli/test_main.py <https://github.com/CCI-Tools/cate/blob/master/test/cli/test_main.py>`_
and may be executed using ``$ py.test test/cli/test_main.py --cov=cate/cli/test_main.py``
for extra code coverage information.


Components
==========
"""

import warnings

warnings.filterwarnings("ignore")  # never print any warnings to users
import argparse
import os
import os.path
import pprint
import sys
from collections import OrderedDict
from typing import Tuple, Union, List, Dict, Any, Optional

from cate.version import __version__

from cate.util.cli import run_main, Command, SubCommandCommand, CommandError

__author__ = "Norman Fomferra (Brockmann Consult GmbH), " \
             "Marco Zühlke (Brockmann Consult GmbH)"

#: Name of the Cate CLI executable (= ``cate``).
CLI_NAME = 'cate'
CLI_DESCRIPTION = 'ESA CCI Toolbox (Cate) command-line interface'

CATE_WEBAPI_MAIN_MODULE = 'cate.webapi.main'

_DOCS_URL = 'http://cate.readthedocs.io/en/latest/'

_LICENSE = """
Cate, the ESA CCI Toolbox, version %s
Copyright (c) 2017 by Cate Development team and contributors

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

NullableStr = Union[str, None]


def _default_workspace_manager_factory() -> Any:
    from cate.conf.defaults import WEBAPI_INFO_FILE, WEBAPI_ON_INACTIVITY_AUTO_STOP_AFTER
    from cate.core.workspace import WorkspaceError
    from cate.webapi.wsmanag import WebAPIWorkspaceManager
    from cate.util.web.webapi import read_service_info, is_service_running, WebAPI

    # Read any existing '.cate/webapi.json'
    service_info = read_service_info(WEBAPI_INFO_FILE)

    if not service_info or not is_service_running(service_info.get('port'), service_info.get('address'), timeout=5.):
        WebAPI.start_subprocess(CATE_WEBAPI_MAIN_MODULE,
                                caller=CLI_NAME,
                                service_info_file=WEBAPI_INFO_FILE,
                                auto_stop_after=WEBAPI_ON_INACTIVITY_AUTO_STOP_AFTER)
        # Read new '.cate/webapi.json'
        service_info = read_service_info(WEBAPI_INFO_FILE)
        if not service_info:
            raise WorkspaceError('Cate WebAPI service could not be started')

    return WebAPIWorkspaceManager(service_info, timeout=5.)


WORKSPACE_MANAGER_FACTORY = _default_workspace_manager_factory


def _new_workspace_manager() -> Any:
    return WORKSPACE_MANAGER_FACTORY()


def _to_str_const(s: str) -> str:
    return "'%s'" % s.replace('\\', '\\\\').replace("'", "\\'")


def _parse_open_arg(load_arg: str) -> Tuple[NullableStr, NullableStr, NullableStr, NullableStr]:
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


def _parse_read_arg(read_arg: str) -> Tuple[NullableStr, NullableStr, NullableStr]:
    """
    Parse string argument ``FILE := "INP_NAME=PATH[,FORMAT]`` and return tuple INP_NAME,PATH,FORMAT.

    :param read_arg: The FILE string argument
    :return: The tuple INP_NAME,PATH,FORMAT
    """
    return _parse_write_arg(read_arg)


def _parse_write_arg(write_arg) -> Tuple[NullableStr, NullableStr, NullableStr]:
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


def _parse_op_args(raw_args: List[str],
                   input_props: Dict[str, Dict[str, Any]] = None,
                   namespace: Dict[str, Any] = None) -> Tuple[List[Dict[str, Any]], Dict[str, Dict[str, Any]]]:
    """
    Convert a raw argument list *raw_args* into a (args, kwargs) tuple.
    All elements of the raw argument list *raw_args* are expected to be textual values of either the form
    "value" (positional argument) or "name=value" (keyword argument) where value may either be

    1. "@name":  a reference by name to another step's port
    2. "<Python expression>":  a constant Python expression

    :param raw_args: raw argument list of string elements
    :param input_props: dict which maps an input name to extra properties, e.g. the "data_type" of an input
    :param namespace: the namespace to be used when converting the raw text values into Python objects.
    :return: a pair comprising the list of positional arguments and a dictionary holding the keyword arguments
    :raise ValueError: if the parsing fails
    """

    from cate.core.types import Like
    from cate.util.safe import safe_eval

    op_args = []
    op_kwargs = OrderedDict()
    for raw_arg in raw_args:
        name_and_value = raw_arg.split('=', maxsplit=1)
        if len(name_and_value) == 2:
            name, raw_value = name_and_value
            if not name:
                raise ValueError("missing input name")
            name = name.strip()
            raw_value = raw_value.strip()
            if not name.isidentifier():
                raise ValueError('"%s" is not a valid input name' % name)
        else:
            name = None
            raw_value = raw_arg

        value = None
        source = None
        props = input_props and input_props.get(name)
        data_type = props and props.get('data_type')

        if raw_value == '':
            # If we have a data type, and raw_value is empty, assume None
            value = None
        else:
            if raw_value.startswith('@'):
                if len(raw_value) > 1:
                    source = raw_value[1:]
                else:
                    value = raw_value
            else:
                # noinspection PyBroadException
                try:
                    # Eval with given namespace as locals
                    value = safe_eval(raw_value, namespace)
                except Exception:
                    value = raw_value

        if source:
            op_arg = dict(source=source)
        else:
            # For any non-None value and any data type we perform basic type validation:
            if value is not None and data_type:
                # noinspection PyTypeChecker
                if issubclass(data_type, Like):
                    # noinspection PyUnresolvedReferences
                    compatible = data_type.accepts(value)
                else:
                    # noinspection PyTypeChecker
                    compatible = isinstance(value, data_type)
                    if not compatible:
                        # noinspection PyTypeChecker
                        if issubclass(data_type, float):
                            # Allow assigning bool and int to a float
                            compatible = isinstance(value, bool) or isinstance(value, int)
                        # noinspection PyTypeChecker
                        elif issubclass(data_type, int):
                            # Allow assigning bool and float to an int
                            compatible = isinstance(value, bool) or isinstance(value, float)
                        # noinspection PyTypeChecker
                        elif issubclass(data_type, bool):
                            # Allow assigning anything to a bool
                            compatible = True
                if not compatible:
                    raise ValueError("value <%s> for input '%s' is not compatible with type %s" %
                                     (raw_value, name, data_type.__name__))
            op_arg = dict(value=value)

        if not name:
            op_args.append(op_arg)
        else:
            op_kwargs[name] = op_arg

    return op_args, op_kwargs


def _list_items(category_singular_name: str, category_plural_name: str, names: List, pattern: Optional[str]):
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
    for no, item in enumerate(names):
        print('%4d: %s' % (no, item))


def _get_op_data_type_str(data_type: str):
    return data_type.__name__ if isinstance(data_type, type) else repr(data_type)


def _get_op_io_info_str(inputs_or_outputs: dict, title_singular: str, title_plural: str, title_none: str) -> str:
    op_info_str = ''
    op_info_str += '\n'
    if inputs_or_outputs:
        inputs_or_outputs = {name: properties for name, properties in inputs_or_outputs.items()
                             if not properties.get('deprecated')}
        op_info_str += '%s:' % (title_singular if len(inputs_or_outputs) == 1 else title_plural)
        for name, properties in inputs_or_outputs.items():
            if properties.get('deprecated'):
                continue
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


def _get_op_info_str(op_meta_info: Any):
    """
    Generate an info string for the *op_meta_info*.
    :param op_meta_info: operation meta information (from e.g. workflow or operation),
           instance of cate.util.opmetainf.OpMetaInfo
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

    op_info_str += _get_op_io_info_str(op_meta_info.inputs, 'Input', 'Inputs', 'Operation does not have any inputs.')
    op_info_str += _get_op_io_info_str(op_meta_info.outputs, 'Output', 'Outputs',
                                       'Operation does not have any outputs.')

    return op_info_str


def _base_dir(base_dir: str = None):
    return os.path.abspath(base_dir or os.curdir)


class RunCommand(Command):
    """
    The ``run`` command is used to invoke registered operations and JSON workflows.
    """

    @classmethod
    def name(cls):
        return 'run'

    @classmethod
    def parser_kwargs(cls):
        return dict(help='Run an operation or Workflow file.',
                    description='Runs the given operation or Workflow file with the specified operation '
                                'arguments. Argument values may be constant values or the names of data loaded '
                                'by the --open or --read options. '
                                'Type "cate op list" to list all available operations. Type "cate op info" to find out'
                                'which arguments are supported by a given operation.')

    @classmethod
    def configure_parser(cls, parser):

        parser.add_argument('-m', '--monitor', action='store_true',
                            help='Display progress information during execution.')
        parser.add_argument('-o', '--open', action='append', metavar='DS_EXPR', dest='open_args',
                            help='Open a dataset from DS_EXPR.\n'
                                 'The DS_EXPR syntax is NAME=DS[,START[,END]]. '
                                 'DS must be a valid data source name. Type "cate ds list" to show '
                                 'all known data source names. START and END are dates and may be used to create '
                                 'temporal data subsets. The dataset loaded will be assigned to the arbitrary '
                                 'name NAME which is used to pass the datasets or its variables'
                                 'as an OP argument. To pass a variable use syntax NAME.VAR_NAME.')
        parser.add_argument('-r', '--read', action='append', metavar='FILE_EXPR', dest='read_args',
                            help='Read object from FILE_EXPR.\n'
                                 'The FILE_EXPR syntax is NAME=PATH[,FORMAT]. '
                                 'Type "cate io list -r" to see which formats are supported.'
                                 'If FORMAT is not provided, file format is derived from the PATH\'s '
                                 'filename extensions or file content. '
                                 'NAME may be passed as an OP argument that receives a dataset, dataset '
                                 'variable or any other data type. To pass a variable of a dataset use '
                                 'syntax NAME.VAR_NAME')
        parser.add_argument('-w', '--write', action='append', metavar='FILE_EXPR', dest='write_args',
                            help='Write result to FILE_EXPR. '
                                 'The FILE_EXPR syntax is [NAME=]PATH[,FORMAT]. '
                                 'Type "cate io list -w" to see which formats are supported.'
                                 'If FORMAT is not provided, file format is derived from the object '
                                 'type and the PATH\'s filename extensions. If OP returns multiple '
                                 'named output values, NAME is used to identify them. Multiple -w '
                                 'options may be used in this case.')
        parser.add_argument('op_name', metavar='OP',
                            help='Fully qualified operation name or Workflow file. '
                                 'Type "cate op list" to list available operators.')
        parser.add_argument('op_args', metavar='...', nargs=argparse.REMAINDER,
                            help='Operation arguments given as KEY=VALUE. KEY is any supported input by OP. VALUE '
                                 'depends on the expected data type of an OP input. It can be a True, False, '
                                 'a string, a numeric constant, one of the names specified by the --open and --read '
                                 'options, or a Python expression. Type "cate op info OP" to print information '
                                 'about the supported OP input names to be used as KEY and their data types to be '
                                 'used as VALUE.')

    def execute(self, command_args):
        from cate.core.objectio import find_writer, read_object
        from cate.core.op import OP_REGISTRY
        from cate.core.workflow import Workflow
        from cate.ops.io import open_dataset
        from cate.util.monitor import Monitor

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
                namespace[res_name] = open_dataset(ds_name, time_range=(start_date, end_date))

        if command_args.read_args:
            read_args = list(map(_parse_read_arg, command_args.read_args))
            for res_name, file, format_name in read_args:
                if not res_name:
                    raise CommandError('missing NAME "%s" in --read option' % res_name)
                if res_name in namespace:
                    raise CommandError('ambiguous NAME "%s" in --read option' % res_name)
                namespace[res_name], _ = read_object(file, format_name=format_name)

        if is_workflow:
            op = Workflow.load(command_args.op_name)
        else:
            op = OP_REGISTRY.get_op(command_args.op_name)
            if op is None:
                raise CommandError('unknown operation "%s"' % op_name)

        op_args, op_kwargs = _parse_op_args(command_args.op_args,
                                            input_props=op.op_meta_info.inputs, namespace=namespace)
        if op_args and is_workflow:
            raise CommandError("positional arguments are not yet supported, please provide keyword=value pairs only")

        write_args = None
        if command_args.write_args:
            write_args = list(map(_parse_write_arg, command_args.write_args))
            if op.op_meta_info.has_named_outputs:
                for out_name, file, format_name in write_args:
                    if not out_name:
                        raise CommandError("all --write options must have a NAME")
                    if out_name not in op.op_meta_info.outputs:
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
            monitor = Monitor.NONE

        op_sources = ["%s=%s" % (kw, v['source']) for kw, v in op_kwargs.items() if 'source' in v]
        if op_sources:
            raise CommandError('unresolved references: %s' % ', '.join(op_sources))

        op_kwargs = OrderedDict([(kw, v['value']) for kw, v in op_kwargs.items() if 'value' in v])

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
                return_type = op.op_meta_info.outputs['return'].get('data_type', object)
                is_void = return_type is None or issubclass(return_type, type(None))
                if not is_void:
                    pprint.pprint(return_value)


OP_ARGS_RES_HELP = 'Operation arguments given as KEY=VALUE. KEY is any supported input by OP. VALUE ' \
                   'depends on the expected data type of an OP input. It can be either a value or ' \
                   'a reference an existing resource prefixed by the add character "@". ' \
                   'The latter connects to operation steps with each other. To provide a (constant)' \
                   'value you can use boolean literals True and False, strings, or numeric values. ' \
                   'Type "cate op info OP" to print information about the supported OP ' \
                   'input names to be used as KEY and their data types to be used as VALUE. '


class WorkspaceCommand(SubCommandCommand):
    """
    The ``ws`` command implements various operations w.r.t. *workspaces*.
    """

    @classmethod
    def name(cls):
        return 'ws'

    @classmethod
    def parser_kwargs(cls):
        return dict(help='Manage workspaces.',
                    description='Used to create, open, save, modify, and delete workspaces. '
                                'Workspaces contain named workflow resources, which can be datasets read from data '
                                'stores, or any other data objects originating from applying operations to datasets '
                                'and other data objects. The origin of every resource is stored in the workspace\'s '
                                'workflow description. '
                                'Type "cate res -h" for more information about workspace resource commands.')

    @classmethod
    def configure_parser_and_subparsers(cls, parser, subparsers):
        base_dir_args = ['-d', '--dir']
        base_dir_kwargs = dict(dest='base_dir', metavar='DIR', default='.',
                               help='The workspace\'s base directory. '
                                    'If not given, the current working directory is used.')

        init_parser = subparsers.add_parser('init', help='Initialize workspace.')
        init_parser.add_argument(*base_dir_args, **base_dir_kwargs)
        init_parser.add_argument('--desc', dest='description', metavar='DESCRIPTION',
                                 help='Workspace description.')
        init_parser.set_defaults(sub_command_function=cls._execute_init)

        new_parser = subparsers.add_parser('new', help='Create new in-memory workspace.')
        new_parser.add_argument(*base_dir_args, **base_dir_kwargs)
        new_parser.add_argument('--desc', dest='description', metavar='DESCRIPTION',
                                help='Workspace description.')
        new_parser.set_defaults(sub_command_function=cls._execute_new)

        open_parser = subparsers.add_parser('open', help='Open workspace.')
        open_parser.add_argument(*base_dir_args, **base_dir_kwargs)
        open_parser.set_defaults(sub_command_function=cls._execute_open)

        close_parser = subparsers.add_parser('close', help='Close workspace.')
        close_parser.add_argument(*base_dir_args, **base_dir_kwargs)
        close_parser.add_argument('-a', '--all', dest='close_all', action='store_true',
                                  help='Close all workspaces. Ignores DIR option.')
        close_parser.add_argument('-s', '--save', dest='save', action='store_true',
                                  help='Save modified workspace before closing.')
        close_parser.set_defaults(sub_command_function=cls._execute_close)

        save_parser = subparsers.add_parser('save', help='Save workspace.')
        save_parser.add_argument(*base_dir_args, **base_dir_kwargs)
        save_parser.add_argument('-a', '--all', dest='save_all', action='store_true',
                                 help='Save all workspaces. Ignores DIR option.')
        save_parser.set_defaults(sub_command_function=cls._execute_save)

        run_parser = subparsers.add_parser('run', help='Run operation.')
        run_parser.add_argument(*base_dir_args, **base_dir_kwargs)
        run_parser.add_argument('op_name', metavar='OP',
                                help='Operation name or Workflow file path. '
                                     'Type "cate op list" to list available operations.')
        run_parser.add_argument('op_args', metavar='...', nargs=argparse.REMAINDER,
                                help=OP_ARGS_RES_HELP)
        run_parser.set_defaults(sub_command_function=cls._execute_run)

        del_parser = subparsers.add_parser('del', help='Delete workspace.')
        del_parser.add_argument(*base_dir_args, **base_dir_kwargs)
        del_parser.add_argument('-y', '--yes', dest='yes', action='store_true', default=False,
                                help='Do not ask for confirmation.')
        del_parser.set_defaults(sub_command_function=cls._execute_del)

        clean_parser = subparsers.add_parser('clean', help='Clean workspace (removes all resources).')
        clean_parser.add_argument(*base_dir_args, **base_dir_kwargs)
        clean_parser.add_argument('-y', '--yes', dest='yes', action='store_true', default=False,
                                  help='Do not ask for confirmation.')
        clean_parser.set_defaults(sub_command_function=cls._execute_clean)

        status_parser = subparsers.add_parser('status', help='Print workspace information.')
        status_parser.add_argument(*base_dir_args, **base_dir_kwargs)
        status_parser.set_defaults(sub_command_function=cls._execute_status)

        list_parser = subparsers.add_parser('list', help='List all opened workspaces.')
        list_parser.set_defaults(sub_command_function=cls._execute_list)

        exit_parser = subparsers.add_parser('exit', help='Exit interactive mode. Closes all open workspaces.')
        exit_parser.add_argument('-y', '--yes', dest='yes', action='store_true', default=False,
                                 help='Do not ask for confirmation.')
        exit_parser.add_argument('-s', '--save', dest='save_all', action='store_true', default=False,
                                 help='Save any modified workspaces before closing.')
        exit_parser.set_defaults(sub_command_function=cls._execute_exit)

    @classmethod
    def _execute_init(cls, command_args):
        workspace_manager = _new_workspace_manager()
        workspace = workspace_manager.new_workspace(_base_dir(command_args.base_dir),
                                                    description=command_args.description)
        workspace.save()
        print('Workspace initialized.')

    @classmethod
    def _execute_new(cls, command_args):
        workspace_manager = _new_workspace_manager()
        workspace_manager.new_workspace(_base_dir(command_args.base_dir),
                                        description=command_args.description)
        print('Workspace created.')

    @classmethod
    def _execute_open(cls, command_args):
        workspace_manager = _new_workspace_manager()
        workspace_manager.open_workspace(_base_dir(command_args.base_dir))
        print('Workspace opened.')

    @classmethod
    def _execute_close(cls, command_args):
        workspace_manager = _new_workspace_manager()
        if command_args.close_all:
            if command_args.save:
                workspace_manager.save_all_workspaces(monitor=cls.new_monitor())
            workspace_manager.close_all_workspaces()
            print('All workspaces closed.')
        else:
            base_dir = _base_dir(command_args.base_dir)
            if command_args.save:
                workspace_manager.save_workspace(base_dir)
            workspace_manager.close_workspace(base_dir)
            print('Workspace closed.')

    @classmethod
    def _execute_save(cls, command_args):
        workspace_manager = _new_workspace_manager()
        if command_args.save_all:
            workspace_manager.save_all_workspaces()
            print('All workspaces saved.')
        else:
            workspace_manager.save_workspace(_base_dir(command_args.base_dir))
            print('Workspace saved.')

    @classmethod
    def _execute_del(cls, command_args):
        if command_args.yes:
            answer = 'y'
        else:
            prompt = 'Do you really want to delete workspace "%s" ([y]/n)? ' % (command_args.base_dir or '.')
            answer = input(prompt)
        if not answer or answer.lower() == 'y':
            workspace_manager = _new_workspace_manager()
            workspace_manager.delete_workspace(_base_dir(command_args.base_dir))
            print('Workspace deleted.')

    @classmethod
    def _execute_clean(cls, command_args):
        if command_args.yes:
            answer = 'y'
        else:
            prompt = 'Do you really want to clean workspace "%s" ([y]/n)? ' % (command_args.base_dir or '.')
            answer = input(prompt)
        if not answer or answer.lower() == 'y':
            workspace_manager = _new_workspace_manager()
            workspace_manager.clean_workspace(_base_dir(command_args.base_dir))
            print('Workspace cleaned.')

    @classmethod
    def _execute_run(cls, command_args):
        from cate.core.op import OP_REGISTRY

        workspace_manager = _new_workspace_manager()
        op = OP_REGISTRY.get_op(command_args.op_name, True)
        op_args, op_kwargs = _parse_op_args(command_args.op_args, input_props=op.op_meta_info.inputs)
        if op_args:
            raise CommandError("positional arguments not yet supported, please provide keyword=value pairs only")
        workspace_manager.run_op_in_workspace(_base_dir(command_args.base_dir),
                                              command_args.op_name,
                                              op_kwargs,
                                              monitor=cls.new_monitor())
        print("Operation '%s' executed." % command_args.op_name)

    @classmethod
    def _execute_status(cls, command_args):
        workspace_manager = _new_workspace_manager()
        workspace = workspace_manager.get_workspace(_base_dir(command_args.base_dir))
        cls._print_workspace(workspace)

    # noinspection PyUnusedLocal
    @classmethod
    def _execute_list(cls, command_args):
        workspace_manager = _new_workspace_manager()
        workspaces = workspace_manager.get_open_workspaces()
        if workspaces:
            num_open_workspaces = len(workspaces)
            if num_open_workspaces == 1:
                print('One open workspace:')
            else:
                print('%d open workspaces:' % num_open_workspaces)
            for workspace in workspaces:
                print()
                cls._print_workspace(workspace)
        else:
            print('No open workspaces.')

    @classmethod
    def _execute_exit(cls, command_args):
        from cate.conf.defaults import WEBAPI_INFO_FILE
        from cate.util.web.webapi import read_service_info, is_service_running, WebAPI

        service_info = read_service_info(WEBAPI_INFO_FILE)
        if not service_info or \
                not is_service_running(service_info.get('port'), service_info.get('address'), timeout=5.):
            return

        if command_args.yes:
            answer = 'y'
        else:
            answer = input('Do you really want to exit interactive mode ([y]/n)? ')
        if not answer or answer.lower() == 'y':
            workspace_manager = _new_workspace_manager()
            if command_args.save_all:
                workspace_manager.save_all_workspaces(monitor=cls.new_monitor())
            workspace_manager.close_all_workspaces()
            WebAPI.stop_subprocess(CATE_WEBAPI_MAIN_MODULE, caller=CLI_NAME, service_info_file=WEBAPI_INFO_FILE)

    @classmethod
    def _print_workspace(cls, workspace):
        workflow = workspace.workflow
        print('Workspace base directory is [%s] (%s, %s)' % (workspace.base_dir,
                                                             'saved' if os.path.exists(
                                                                 workspace.workspace_dir) else 'not saved yet',
                                                             'modified' if workspace.is_modified else 'no changes'))
        if len(workflow.steps) > 0:
            print('Workspace resources:')
            for step in workflow.steps:
                print('  %s' % str(step))
        else:
            print('Workspace has no resources.')


class ResourceCommand(SubCommandCommand):
    """
    The ``res`` command implements various operations w.r.t. *workspaces*.
    """

    @classmethod
    def name(cls):
        return 'res'

    @classmethod
    def parser_kwargs(cls):
        return dict(help='Manage workspace resources.',
                    description='Used to set, run, open, read, write, plot, etc. workspace resources. '
                                'All commands expect an opened workspace. '
                                'Type "cate ws -h" for more information about workspace commands.')

    @classmethod
    def configure_parser_and_subparsers(cls, parser, subparsers):

        base_dir_args = ['-d', '--dir']
        base_dir_kwargs = dict(dest='base_dir', metavar='DIR', default='.',
                               help='The workspace\'s base directory. '
                                    'If not given, the current working directory is used.')

        open_parser = subparsers.add_parser('open',
                                            help='Open a dataset from a data source and set a resource.')
        open_parser.add_argument(*base_dir_args, **base_dir_kwargs)
        open_parser.add_argument('res_name', metavar='NAME',
                                 help='Name of the new target resource.')
        open_parser.add_argument('ds_name', metavar='DS',
                                 help='A data source named DS. Type "cate ds list" to list valid data source names.')
        open_parser.add_argument('start_date', metavar='START', nargs='?',
                                 help='Start date. Use format "YYYY[-MM[-DD]]".')
        open_parser.add_argument('end_date', metavar='END', nargs='?',
                                 help='End date. Use format "YYYY[-MM[-DD]]".')
        open_parser.add_argument('region', metavar='REGION', nargs='?',
                                 help='Region constraint. Use format "min_lon,min_lat,max_lon,max_lat".')
        open_parser.add_argument('var_names', metavar='VAR_NAMES', nargs='?',
                                 help='Names of variables to be included. Use format "pattern1,pattern2,pattern3".')
        open_parser.set_defaults(sub_command_function=cls._execute_open)

        read_parser = subparsers.add_parser('read',
                                            help='Read an object from a file and set a resource.')
        read_parser.add_argument(*base_dir_args, **base_dir_kwargs)
        read_parser.add_argument('res_name', metavar='NAME',
                                 help='Name of the new target resource.')
        read_parser.add_argument('file_path', metavar='FILE',
                                 help='File path.')
        read_parser.add_argument('-f', '--format', dest='format_name', metavar='FORMAT',
                                 help='File format. '
                                      'Type "cate io list -r" to see which formats are supported.')
        # We may support reader-specific arguments later:
        # read_parser.add_argument('op_args', metavar='...', nargs=argparse.REMAINDER,
        #                           help='Specific reader arguments. '
        #                                'Type "cate res read -h" to list format-specific read arguments')
        read_parser.set_defaults(sub_command_function=cls._execute_read)

        write_parser = subparsers.add_parser('write',
                                             help='Write a resource to a file.')
        write_parser.add_argument(*base_dir_args, **base_dir_kwargs)
        write_parser.add_argument('res_name', metavar='NAME',
                                  help='Name of an existing resource.')
        write_parser.add_argument('file_path', metavar='FILE',
                                  help='File path.')
        write_parser.add_argument('-f', '--format', dest='format_name', metavar='FORMAT',
                                  help='File format. '
                                       'Type "cate io list -w" to see which formats are supported.')
        # We may support writer-specific arguments later:
        # read_parser.add_argument('op_args', metavar='...', nargs=argparse.REMAINDER,
        #                           help='Specific reader arguments. '
        #                                'Type "cate res write -h" to list format-specific write arguments')
        write_parser.set_defaults(sub_command_function=cls._execute_write)

        set_parser = subparsers.add_parser('set',
                                           help='Set a resource from the result of an operation.')
        set_parser.add_argument(*base_dir_args, **base_dir_kwargs)
        set_parser.add_argument('-o', '--overwrite', action='store_true',
                                help='Overwrite an existing workflow step / target resource with same NAME.')
        set_parser.add_argument('res_name', metavar='NAME',
                                help='Name of the target resource to be set. Use -o to overwrite an existing NAME.')
        set_parser.add_argument('op_name', metavar='OP',
                                help='Operation name. Type "cate op list" to list available operation names.')
        set_parser.add_argument('op_args', metavar='...', nargs=argparse.REMAINDER,
                                help=OP_ARGS_RES_HELP)
        set_parser.set_defaults(sub_command_function=cls._execute_set)

        rename_parser = subparsers.add_parser('rename', help='Rename a resource.')
        rename_parser.add_argument(*base_dir_args, **base_dir_kwargs)
        rename_parser.add_argument('res_name', metavar='NAME',
                                   help='Resource name.')
        rename_parser.add_argument('res_name_new', metavar='NEW_NAME',
                                   help='New resource name.')
        rename_parser.set_defaults(sub_command_function=cls._execute_rename)

        del_parser = subparsers.add_parser('del', help='Delete a resource.')
        del_parser.add_argument(*base_dir_args, **base_dir_kwargs)
        del_parser.add_argument('res_name', metavar='NAME',
                                help='Resource name.')
        del_parser.set_defaults(sub_command_function=cls._execute_del)

        print_parser = subparsers.add_parser('print', help='If EXPR is omitted, print value of all current resources.'
                                                           'Otherwise, if EXPR identifies a resource, print its value.'
                                                           'Else print the value of a (Python) expression evaluated '
                                                           'in the context of the current workspace.')
        print_parser.add_argument(*base_dir_args, **base_dir_kwargs)
        print_parser.add_argument('res_name_or_expr', metavar='EXPR', nargs='?',
                                  help='Name of an existing resource or a valid (Python) expression.')
        print_parser.set_defaults(sub_command_function=cls._execute_print)

        plot_parser = subparsers.add_parser('plot', help='Plot a resource or the value of a (Python) expression '
                                                         'evaluated in the context of the current workspace.')
        plot_parser.add_argument(*base_dir_args, **base_dir_kwargs)
        plot_parser.add_argument('res_name_or_expr', metavar='EXPR',
                                 help='Name of an existing resource or any (Python) expression.')
        plot_parser.add_argument('-v', '--var', dest='var_name', metavar='VAR', nargs='?',
                                 help='Name of a variable to plot.')
        plot_parser.add_argument('-o', '--out', dest='file_path', metavar='FILE', nargs='?',
                                 help='Output file to write the plot figure to.')
        plot_parser.set_defaults(sub_command_function=cls._execute_plot)

    @classmethod
    def _execute_open(cls, command_args):
        from cate.core.workspace import mk_op_kwargs

        workspace_manager = _new_workspace_manager()
        op_args = dict(ds_name=command_args.ds_name)
        if command_args.var_names:
            # noinspection PyArgumentList
            op_args.update(var_names=command_args.var_names)
        if command_args.region:
            # noinspection PyArgumentList
            op_args.update(region=command_args.region)
        if command_args.start_date or command_args.end_date:
            # noinspection PyArgumentList
            op_args.update(time_range="%s,%s" % (command_args.start_date or '',
                                                 command_args.end_date or ''))
        workspace_manager.set_workspace_resource(_base_dir(command_args.base_dir),
                                                 'cate.ops.io.open_dataset',
                                                 mk_op_kwargs(**op_args),
                                                 res_name=command_args.res_name,
                                                 overwrite=False,
                                                 monitor=cls.new_monitor())
        print('Resource "%s" set.' % command_args.res_name)

    @classmethod
    def _execute_read(cls, command_args):
        from cate.core.workspace import mk_op_kwargs

        workspace_manager = _new_workspace_manager()
        op_args = dict(file=command_args.file_path)
        if command_args.format_name:
            # noinspection PyArgumentList
            op_args.update(format=command_args.format_name)
        workspace_manager.set_workspace_resource(_base_dir(command_args.base_dir),
                                                 'cate.ops.io.read_object',
                                                 mk_op_kwargs(**op_args),
                                                 res_name=command_args.res_name,
                                                 overwrite=False,
                                                 monitor=cls.new_monitor())
        print('Resource "%s" set.' % command_args.res_name)

    @classmethod
    def _execute_set(cls, command_args):
        from cate.core.op import OP_REGISTRY

        workspace_manager = _new_workspace_manager()
        op = OP_REGISTRY.get_op(command_args.op_name, True)
        op_args, op_kwargs = _parse_op_args(command_args.op_args, input_props=op.op_meta_info.inputs)
        if op_args:
            raise CommandError("positional arguments not yet supported, please provide keyword=value pairs only")
        workspace_manager.set_workspace_resource(_base_dir(command_args.base_dir),
                                                 command_args.op_name,
                                                 op_kwargs,
                                                 res_name=command_args.res_name,
                                                 overwrite=command_args.overwrite,
                                                 monitor=cls.new_monitor())
        print('Resource "%s" set.' % command_args.res_name)

    @classmethod
    def _execute_rename(cls, command_args):
        if command_args.res_name == command_args.res_name_new:
            print('Names are equal.')
            return
        workspace_manager = _new_workspace_manager()
        workspace_manager.rename_workspace_resource(_base_dir(command_args.base_dir),
                                                    command_args.res_name,
                                                    command_args.res_name_new)
        print('Resource "%s" renamed to "%s".' % (command_args.res_name, command_args.res_name_new))

    @classmethod
    def _execute_del(cls, command_args):
        workspace_manager = _new_workspace_manager()
        workspace_manager.delete_workspace_resource(_base_dir(command_args.base_dir),
                                                    command_args.res_name)
        print('Resource "%s" deleted.' % command_args.res_name)

    @classmethod
    def _execute_write(cls, command_args):
        workspace_manager = _new_workspace_manager()
        workspace_manager.write_workspace_resource(_base_dir(command_args.base_dir),
                                                   command_args.res_name,
                                                   command_args.file_path,
                                                   format_name=command_args.format_name,
                                                   monitor=cls.new_monitor())
        print('Resource "%s" written.' % command_args.res_name)

    @classmethod
    def _execute_plot(cls, command_args):
        workspace_manager = _new_workspace_manager()
        workspace_manager.plot_workspace_resource(_base_dir(command_args.base_dir),
                                                  command_args.res_name_or_expr,
                                                  var_name=command_args.var_name,
                                                  file_path=command_args.file_path,
                                                  monitor=cls.new_monitor())

    @classmethod
    def _execute_print(cls, command_args):
        workspace_manager = _new_workspace_manager()
        workspace_manager.print_workspace_resource(_base_dir(command_args.base_dir),
                                                   command_args.res_name_or_expr)


class OperationCommand(SubCommandCommand):
    """
    The ``op`` command implements various operations w.r.t. *operations*.
    """

    @classmethod
    def name(cls):
        return 'op'

    @classmethod
    def parser_kwargs(cls):
        return dict(help='Manage data operations.',
                    description='Provides a set of commands to inquire the available operations used to '
                                'analyse and process climate datasets.')

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
        list_parser.add_argument('--deprecated', '-d', action='store_true',
                                 help="List deprecated operations.")
        list_parser.add_argument('--internal', '-i', action='store_true',
                                 help='List operations tagged "internal".')
        list_parser.set_defaults(sub_command_function=cls._execute_list)

        info_parser = subparsers.add_parser('info', help='Show usage information about an operation.')
        info_parser.add_argument('op_name', metavar='OP',
                                 help="Fully qualified operation name.")
        info_parser.set_defaults(sub_command_function=cls._execute_info)

    @classmethod
    def _execute_list(cls, command_args):
        from cate.core.op import OP_REGISTRY
        from cate.util.misc import to_list

        op_regs = OP_REGISTRY.op_registrations

        def _is_op_selected(op_name: str, op_reg, tag_part: str, internal_only: bool, deprecated_only: bool):
            if op_name.startswith('_'):
                # do not list private operations
                return False
            if deprecated_only \
                    and not op_reg.op_meta_info.header.get('deprecated'):
                # do not list non-deprecated operations if user wants to see what is deprecated
                return False
            tags = to_list(op_reg.op_meta_info.header.get('tags'))
            if tags:
                # Tagged operations
                if internal_only:
                    if 'internal' not in tags:
                        return False
                else:
                    if 'internal' in tags:
                        return False
                if tag_part:
                    tag_part = tag_part.lower()
                    if isinstance(tags, list):
                        return any(tag_part in tag.lower() for tag in tags)
                    elif isinstance(tags, str):
                        return tag_part in tags.lower()
            elif internal_only or tag_part:
                # Untagged operations
                return False
            return True

        op_names = sorted([op_name for op_name, op_reg in op_regs.items() if
                           _is_op_selected(op_name, op_reg, command_args.tag, command_args.internal, command_args.deprecated)])
        name_pattern = None
        if command_args.name:
            name_pattern = command_args.name
        _list_items('operation', 'operations', op_names, name_pattern)

    @classmethod
    def _execute_info(cls, command_args):
        from cate.core.op import OP_REGISTRY

        op_name = command_args.op_name
        if not op_name:
            raise CommandError('missing OP argument')
        op_registration = OP_REGISTRY.get_op(op_name)
        if not op_registration:
            raise CommandError('unknown operation "%s"' % op_name)
        print(_get_op_info_str(op_registration.op_meta_info))


class DataSourceCommand(SubCommandCommand):
    """
    The ``ds`` command implements various operations w.r.t. datasets.
    """

    @classmethod
    def name(cls):
        return 'ds'

    @classmethod
    def parser_kwargs(cls):
        return dict(help='Manage data sources.',
                    description='Provides a set of sub-commands used to manage climate data sources. Data sources '
                                'are used to open local and remote datasets which are input to various analysis and '
                                'processing operations. Type "cate op -h" to find out more about available operations.')

    @classmethod
    def configure_parser_and_subparsers(cls, parser, subparsers):
        list_parser = subparsers.add_parser('list', help='List all available data sources')
        list_parser.add_argument('--name', '-n', metavar='NAME',
                                 help="List only data sources named NAME or "
                                      "that have NAME in their name. "
                                      "The comparison is case insensitive.")
        list_parser.add_argument('--coverage', '-c', action='store_true',
                                 help="Also display temporal coverage")
        # Improvement (marcoz, 20160905): implement "cate ds list --var"
        # list_parser.add_argument('--var', '-v', metavar='VAR',
        #                          help="List only data sources with a variable named NAME or "
        #                               "with variables that have NAME in their name. "
        #                               "The comparison is case insensitive.")
        list_parser.set_defaults(sub_command_function=cls._execute_list)

        info_parser = subparsers.add_parser('info', help='Display information about a data source.')
        info_parser.add_argument('ds_name', metavar='DS',
                                 help='A data source name. '
                                      'Type "cate ds list" to show all possible data source names.')
        info_parser.add_argument('--var', '-v', action='store_true',
                                 help="Also display information about contained dataset variables.")
        info_parser.add_argument('--local', '-l', action='store_true',
                                 help="Also display temporal coverage of cached datasets.")
        info_parser.set_defaults(sub_command_function=cls._execute_info)

        add_parser = subparsers.add_parser('add', help='Add a new local data source using a file pattern.')
        add_parser.add_argument('ds_name', metavar='DS', help='A name for the data source.')
        add_parser.add_argument('file', metavar='FILE', nargs="+",
                                help='A list of files comprising this data source. '
                                     'The files can contain the wildcard characters "*" and "?".')
        add_parser.set_defaults(sub_command_function=cls._execute_add)

        del_parser = subparsers.add_parser('del', help='Removes a data source from local data store.')
        del_parser.add_argument('ds_name', metavar='DS', help='A name for the data source.')
        del_parser.add_argument('-k', '--keep_files', dest='keep_files', action='store_true', default=False,
                                help='Do not ask for confirmation.')
        del_parser.add_argument('-y', '--yes', dest='yes', action='store_true', default=False,
                                help='Do not ask for confirmation.')
        del_parser.set_defaults(sub_command_function=cls._execute_del)

        copy_parser = subparsers.add_parser('copy', help='Makes a local copy of any other data source. '
                                                         'The copy may be limited to a subset by optional constraints.')
        copy_parser.add_argument('ref_ds', metavar='REF_DS', help='A name of origin data source.')
        copy_parser.add_argument('--name', '-n', metavar='NAME',
                                 help='A name for new data source.')
        copy_parser.add_argument('--time', '-t', metavar='TIME',
                                 help='Time range constraint. Use format "YYYY-MM-DD,YYYY-MM-DD".')
        copy_parser.add_argument('--region', '-r', metavar='REG',
                                 help='Region constraint. Use format: "min_lon,min_lat,max_lon,max_lat".')
        copy_parser.add_argument('--vars', '-v', metavar='VARS',
                                 help='Names of variables to be included. Use format "pattern1,pattern2,..."')
        copy_parser.set_defaults(sub_command_function=cls._execute_copy)

    # noinspection PyShadowingNames
    @classmethod
    def _execute_list(cls, command_args):
        from cate.core.ds import find_data_sources
        from cate.core.types import TimeRangeLike

        ds_name = command_args.name
        data_sources = sorted(find_data_sources(query_expr=ds_name), key=lambda ds: ds.id)
        if command_args.coverage:
            ds_names = []
            for ds in data_sources:
                time_range = 'None'
                temporal_coverage = ds.temporal_coverage()
                if temporal_coverage:
                    time_range = TimeRangeLike.format(temporal_coverage)
                ds_names.append('%s [%s]' % (ds.id, time_range))
        else:
            ds_names = [ds.id for ds in data_sources]
        _list_items('data source', 'data sources', ds_names, None)

    @classmethod
    def _execute_info(cls, command_args):
        from cate.core.ds import find_data_sources, format_cached_datasets_coverage_string, format_variables_info_string

        ds_name = command_args.ds_name
        data_sources = [data_source for data_source in find_data_sources(ds_id=ds_name) if data_source.id == ds_name]
        if not data_sources:
            raise CommandError('data source "%s" not found' % ds_name)

        data_source = data_sources[0]
        title = 'Data source %s' % data_source.id
        print()
        print(title)
        print('=' * len(title))
        print()
        print(data_source.info_string)
        if command_args.local:
            print('\n'
                  'Locally stored datasets:\n'
                  '------------------------\n'
                  '{info}'.format(info=format_cached_datasets_coverage_string(data_source.cache_info)))
        if command_args.var:
            print()
            print('Variables')
            print('---------')
            print()
            print(format_variables_info_string(data_source.variables_info))

    @classmethod
    def _execute_add(cls, command_args):
        from cate.core.ds import DATA_STORE_REGISTRY

        local_store = DATA_STORE_REGISTRY.get_data_store('local')
        if local_store is None:
            raise RuntimeError('internal error: no local data store found')

        ds_name = command_args.ds_name
        files = command_args.file
        ds = local_store.add_pattern(ds_name, files)
        print("Local data source with name '%s' added." % ds.id)

    @classmethod
    def _execute_del(cls, command_args):
        from cate.core.ds import DATA_STORE_REGISTRY

        local_store = DATA_STORE_REGISTRY.get_data_store('local')
        if local_store is None:
            raise RuntimeError('internal error: no local data store found')
        ds_name = command_args.ds_name
        if command_args.yes:
            answer = 'y'
        else:
            prompt = 'Do you really want to delete local data source "%s" ([y]/n)? ' % ds_name
            answer = input(prompt)
        if not answer or answer.lower() == 'y':
            keep_files = command_args.keep_files
            local_store.remove_data_source(ds_name, not keep_files)
            print("Local data source with name '%s' has been removed successfully." % ds_name)

    @classmethod
    def _execute_copy(cls, command_args):
        from cate.core.ds import DATA_STORE_REGISTRY, find_data_sources
        from cate.core.types import TimeRangeLike, PolygonLike, VarNamesLike

        local_store = DATA_STORE_REGISTRY.get_data_store('local')
        if local_store is None:
            raise RuntimeError('internal error: no local data store found')

        ds_name = command_args.ref_ds
        data_source = next(iter(find_data_sources(ds_id=ds_name)), None)
        if data_source is None:
            raise RuntimeError('internal error: no local data source found: %s' % ds_name)

        local_name = command_args.name if command_args.name else None

        time_range = TimeRangeLike.convert(command_args.time)
        region = PolygonLike.convert(command_args.region)
        var_names = VarNamesLike.convert(command_args.vars)

        ds = data_source.make_local(local_name, time_range=time_range, region=region, var_names=var_names,
                                    monitor=cls.new_monitor())
        if ds:
            print("Local data source with name '%s' has been created." % ds.id)
        else:
            print("Local data source not created. It would have been empty. Please check constraint.")


class UpdateCommand(Command):
    """
    The ``update`` command is used to update an existing cate environment to a specific or the latest cate version.
    """

    @classmethod
    def name(cls):
        return 'upd'

    @classmethod
    def parser_kwargs(cls):
        return dict(help='Update an existing cate environment to a specific or to the latest cate version',
                    description='Update an existing cate environment to a specific or to the latest cate version.')

    @classmethod
    def configure_parser(cls, parser):
        parser.add_argument('-y', '--yes', dest='yes', action='store_true', default=False,
                            help='Do not ask for confirmation.')
        parser.add_argument('-i', '--info', dest='show_info', action='store_true', default=False,
                            help='Show version information only; do not update yet.')
        parser.add_argument('--dry-run', dest='dry_run', action='store_true', default=False,
                            help='Only display what would have been done.')
        parser.add_argument('version', metavar='VERSION', nargs='?', default=None,
                            help='A cate version identifier, e.g. "1.0.3"; '
                                 'the version identifier must have the form "major.minor.micro" and may comprise '
                                 'a development release suffix, e.g. "1.2.0.dev4"')

    def execute(self, command_args):
        current_version = __version__
        desired_version = command_args.version
        show_info = command_args.show_info
        dry_run = command_args.dry_run

        from cate.util.process import run_subprocess
        if sys.platform == 'win32':
            conda_path = os.path.join(sys.prefix, 'Scripts', 'conda.bat')
        else:
            conda_path = os.path.join(sys.prefix, 'bin', 'conda')

        import subprocess

        package = 'cate-cli'
        channel = 'ccitools'
        command = [conda_path, 'search', '--channel', channel, package]
        completed_process = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout = completed_process.stdout.decode("utf-8") if completed_process.stdout else None
        stderr = completed_process.stderr.decode("utf-8") if completed_process.stderr else None
        if stderr:
            raise CommandError(stderr)

        available_versions = []
        latest_version = None
        if stdout:
            package_info = [row.split() for row in stdout.split('\n')]
            package_info.reverse()
            for entry in package_info:
                available_version = None
                if len(entry) == 4 and entry[0] == package and entry[-1] == channel:
                    available_version = entry[1]
                elif len(entry) == 3 and entry[-1] == channel:
                    available_version = entry[0]
                if available_version:
                    available_versions.append(available_version)
                    if not latest_version:
                        latest_version = available_version

        if not latest_version:
            raise CommandError('failed to retrieve latest cate version')

        if show_info:
            print('Latest version is %s' % latest_version)
            print('Current version is %s' % current_version)
            if desired_version:
                available = desired_version in available_versions
                print('Desired version is %s (%s)' % (desired_version, 'available' if available else 'not available'))
            print('Available versions:')
            for available_version in available_versions:
                print(' ', available_version)
            return

        if not desired_version:
            desired_version = latest_version

        if desired_version == current_version:
            if latest_version == current_version:
                print('Current cate version is %s and up-to-date' % current_version)
            else:
                print('Current cate version is already %s' % current_version)
            return

        if desired_version not in available_versions:
            raise CommandError('desired cate version %s is not available; '
                               'type "cate upd --info" to show available versions' % desired_version)

        if command_args.yes or dry_run:
            answer = 'y'
        else:
            prompt = 'Do you really want to change from %s to %s (y/[n])? ' % (current_version, desired_version)
            answer = input(prompt)
        if not answer or answer.lower() != 'y':
            return

        command = [conda_path, 'install', '--yes', '--channel', channel, '--channel', 'conda-forge']
        if dry_run:
            command.append('--dry-run')
        command.append('%s=%s' % (package, desired_version))

        def stdout_handler(text):
            sys.stdout.write(text)

        def stderr_handler(text):
            sys.stdout.write(text)

        run_subprocess(command, stdout_handler=stdout_handler, stderr_handler=stderr_handler)


class IOCommand(SubCommandCommand):
    """
    The ``io`` command implements various operations w.r.t. supported data and file formats.
    """

    @classmethod
    def name(cls):
        return 'io'

    @classmethod
    def parser_kwargs(cls):
        return dict(help='Manage supported data and file formats.')

    @classmethod
    def configure_parser_and_subparsers(cls, parser, subparsers):
        list_parser = subparsers.add_parser('list', help='List all supported file or data formats')
        list_parser.add_argument('--read', '-r', action='store_true',
                                 help="List only file/data formats that can be read.")
        list_parser.add_argument('--write', '-w', action='store_true',
                                 help="List only file/data formats that can be written.")
        list_parser.set_defaults(sub_command_function=cls._execute_list)

    # noinspection PyShadowingNames
    @classmethod
    def _execute_list(cls, command_args):
        from cate.core.objectio import OBJECT_IO_REGISTRY

        if command_args.read and command_args.write:
            object_io_list = OBJECT_IO_REGISTRY.get_object_io_list(mode='rw')
        elif command_args.read:
            object_io_list = OBJECT_IO_REGISTRY.get_object_io_list(mode='r')
        elif command_args.write:
            object_io_list = OBJECT_IO_REGISTRY.get_object_io_list(mode='w')
        else:
            object_io_list = OBJECT_IO_REGISTRY.get_object_io_list()

        if not object_io_list:
            print('No formats found.')
            return

        for object_io in object_io_list:
            print('{name} (*{ext}) - {desc}'.format(name=object_io.format_name,
                                                    ext=object_io.filename_ext,
                                                    desc=object_io.description))


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
        from cate.core.plugin import PLUGIN_REGISTRY

        name_pattern = None
        if command_args.name:
            name_pattern = command_args.name
        _list_items('plugin', 'plugins', sorted(PLUGIN_REGISTRY.keys()), name_pattern)


#: List of sub-commands supported by the CLI. Entries are classes derived from :py:class:`Command` class.
#: Cate plugins may extend this list by their commands during plugin initialisation.
COMMAND_REGISTRY = [
    DataSourceCommand,
    OperationCommand,
    WorkspaceCommand,
    ResourceCommand,
    RunCommand,
    IOCommand,
    UpdateCommand,
    # PluginCommand,
]


def _trim_error_message(message: str) -> str:
    from cate.webapi.wsmanag import WebAPIWorkspaceManager

    # Crop any traceback_header from message
    traceback_header = WebAPIWorkspaceManager.get_traceback_header()
    traceback_pos = message.find(traceback_header)
    if traceback_pos >= 0:
        return message[0: traceback_pos]
    else:
        return message


# use by 'sphinxarg' to generate the documentation
def _make_cate_parser():
    from cate.util.cli import _make_parser
    # noinspection PyTypeChecker
    return _make_parser(CLI_NAME, CLI_DESCRIPTION, __version__, COMMAND_REGISTRY, license_text=_LICENSE,
                        docs_url=_DOCS_URL)


def main(args=None) -> int:
    # noinspection PyTypeChecker
    return run_main(CLI_NAME,
                    CLI_DESCRIPTION,
                    __version__,
                    COMMAND_REGISTRY,
                    license_text=_LICENSE,
                    docs_url=_DOCS_URL,
                    error_message_trimmer=_trim_error_message,
                    args=args)


if __name__ == '__main__':
    sys.exit(main())
