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

This modules provides classes and functions allowing to maintain *operations*. Operations can be called from
the Cate command-line interface, may be referenced from within processing workflows, or may be called remotely
e.g. from graphical user interface or web frontend. An operation (:py:class:`Operation`) comprises a Python callable
and some additional meta-information (:py:class:`OpMetaInfo`) that allows for automatic input validation,
input value conversion, monitoring, and inter-connection of multiple operations using processing workflows and steps.

Operations are registered in operation registries (:py:class:`OpRegistry`), the default operation registry is
accessible via the global, read-only ``OP_REGISTRY`` variable.

Technical Requirements
======================

**Operation registration, lookup, and invocation**

:Description: Maintain a central place in the software that manages the available operations such as data processors,
    data converters, analysis functions, etc. Operations can be added, removed and retrieved.
    Operations are designed to be executed by the framework in a controlled way, i.e. an operation's task
    can be monitored and cancelled, it's input and out values can be validated w.r.t. the operation's meta-information.

:URD-Sources:
    * CCIT-UR-CR0001: Extensibility.
    * CCIT-UR-E0002: dynamic extension of all modules at runtime, c) The Logic Module to introduce new processors
    * CCIT-UR-LM0001: processor management allowing easy selection of tools and functionalities

----

**Exploit Python language features**

:Description: Exploit Python language to let API users express an operation in an intuitive form. For the framework API,
    stay with Python base types as far as possible instead of introducing a number of new data structures.
    Let the framework derive meta information such as names, types and documentation for the operation, its inputs,
    and its outputs from the user's Python code.
    It shall be possible to register any Python-callable of the from ``f(*args, **kwargs)`` as an operation.

----

**Add extra meta-information to operations**

:Description: Initial operation meta-information will be derived from Python code introspection. It shall include
    the user function's docstring and information about the arguments an its return values, exploiting any
    type annotations.
    For example, the following properties can be associated with input arguments: data type, default value, value set,
    valid range, if it is mandatory or optional, expected dataset schema so that operations can be ECV-specific.
    Meta-information is required to let an operation explain itself when used in a (IPython)
    REPL or when web service is requested to respond with an operations's capabilities.
    API users shall be able to extend the initial meta-information derived from Python code.

:URD-Source:
    * CCIT-UR-LM0006: offer default values for lower level users as well as selectable options for higher level users.
    * CCIT-UR-LM0002: accommodating ECV-specific processors in cases where the processing is specific to an ECV.

----

**Static annotation vs. dynamic, programmatic registration**

:Description: Operation registration and meta-information extension shall also be done by operation class /
    function *decorators*. The API shall provide a simple set of dedicated decorators that API user's attach to their
    operations. They will automatically register the user function as operation and add any extra meta-information.

----

**Operation monitoring**

:Description: Operation registration should recognise an optional *monitor* argument of a user function:
    ``f(*args, monitor=Monitor.NONE, **kwargs)``. In this case the a monitor (of type :py:class:`Monitor`)
    will be passed by the framework to the user function in order to observe the progress and to cancel an operation.

----

Verification
============

The module's unit-tests are located in
`test/test_op.py <https://github.com/CCI-Tools/cate-core/blob/master/test/test_op.py>`_ and may be executed using
``$ py.test test/test_op.py --cov=cate/core/plugin.py`` for extra code coverage information.


Components
==========
"""

import sys
from collections import OrderedDict
from typing import Union, Callable, Optional, Dict

import xarray as xr

from ..util import OpMetaInfo, object_to_qualified_name, Monitor, UNDEFINED, safe_eval
from ..util.process import run_subprocess, ProcessOutputMonitor
from ..util.tmpfile import new_temp_file, del_temp_file
from ..version import __version__

__author__ = "Norman Fomferra (Brockmann Consult GmbH)"

_MONITOR = OpMetaInfo.MONITOR_INPUT_NAME
_RETURN = OpMetaInfo.RETURN_OUTPUT_NAME


class Operation:
    """
    An Operation comprises a wrapped callable (e.g. function, constructor, lambda form)
    and additional meta-information about the wrapped operation itself and its inputs and outputs.

    :param wrapped_op: some callable object that will be wrapped.
    :param op_meta_info: operation meta information.
    """

    def __init__(self, wrapped_op: Callable, op_meta_info=None):
        if callable is None:
            raise ValueError('wrapped_op must be given')
        if not callable(wrapped_op):
            raise ValueError('wrapped_op must be callable')

        if op_meta_info is None:
            # Try unwrapping wrapped_op
            try:
                # noinspection PyUnresolvedReferences
                op_meta_info = wrapped_op.op_meta_info
                try:
                    # noinspection PyUnresolvedReferences
                    wrapped_op = wrapped_op.wrapped_op
                except AttributeError:
                    pass
            except AttributeError:
                pass

        self._wrapped_op = wrapped_op
        self._op_meta_info = op_meta_info or OpMetaInfo.introspect_operation(wrapped_op)
        for attr_name in ['__module__', '__name__', '__qualname__', '__doc__', '__file__']:
            try:
                setattr(self, attr_name, getattr(wrapped_op, attr_name))
            except AttributeError:
                pass

    @property
    def op_meta_info(self) -> OpMetaInfo:
        """
        :return: Meta-information about the operation, see :py:class:`cate.core.op.OpMetaInfo`.
        """
        return self._op_meta_info

    @property
    def wrapped_op(self) -> Callable:
        """
        :return: The actual operation object which may be any callable.
        """
        return self._wrapped_op

    def __str__(self):
        return '%s: %s' % (self._wrapped_op, self._op_meta_info)

    def __call__(self, *args, monitor: Monitor = Monitor.NONE, **kwargs):
        """
        Perform this operation.

        :param args: the arguments
        :param monitor: an optional progress monitor, which is passed to the wrapped callable, if it supports it.
        :param kwargs: the keyword arguments
        :return: the operation output.
        """

        input_values = kwargs

        # process arguments, if any
        num_args = len(args)
        if num_args:
            input_names = self.op_meta_info.input_names
            for position in range(num_args):
                if position >= len(input_names):
                    raise ValueError(
                        "too many inputs given for operation '{}'".format(self.op_meta_info.qualified_name))
                input_name = self.op_meta_info.input_names[position]
                input_values[input_name] = args[position]

        # set default_value where input values are missing
        self.op_meta_info.set_default_input_values(input_values)

        # validate the input_values using this operation's meta-info
        self.op_meta_info.validate_input_values(input_values)

        if self.op_meta_info.has_monitor:
            # set the monitor only if it is an argument
            input_values[_MONITOR] = monitor

        # call the callable
        return_value = self._wrapped_op(**input_values)

        if self.op_meta_info.has_named_outputs:
            # return_value is expected to be a dictionary-like object
            # set default_value where output values in return_value are missing
            for name, properties in self.op_meta_info.outputs.items():
                if name not in return_value or return_value[name] is None:
                    return_value[name] = properties.get('default_value')
            # validate the return_value using this operation's meta-info
            self.op_meta_info.validate_output_values(return_value)
            # Add history information to outputs
            for name, properties in self.op_meta_info.outputs.items():
                add_history = properties.get('add_history')
                if add_history:
                    return_value[name] = self._add_history(return_value[name], input_values)
        else:
            # return_value is a single value, not a dict
            # set default_value if return_value is missing
            properties = self.op_meta_info.outputs[_RETURN]
            if return_value is None:
                return_value = properties.get('default_value')
            # validate the return_value using this operation's meta-info
            self.op_meta_info.validate_output_values({_RETURN: return_value})
            # Add history information to the output
            add_history = properties.get('add_history')
            if add_history:
                return_value = self._add_history(return_value, input_values)

        return return_value

    def _add_history(self, ds: object, input_dict) -> object:
        """
        Add provenance information about cate, the operation and its inputs to
        the given output.

        :return: Dataset with history information appended
        """
        op_name = self.op_meta_info.qualified_name
        # There can potentially be different ways to stamp an output depending
        # on its type
        if not isinstance(ds, xr.Dataset):
            raise NotImplementedError('Operation "{}": Adding history information to an'
                                      ' output is currently implemented only'
                                      ' for outputs of type "xarray.Dataset".'.format(op_name))

        # Construct our own dict to stringify, otherwise the full input dataset
        # repr will be found in history.
        input_str = dict()
        for key in input_dict:
            value = input_dict[key]
            if isinstance(value, xr.Dataset):
                # We only show that 'a dataset' was provided, instead of
                # putting the full dataset repr in history
                input_str[key] = type(value)
                continue
            input_str[key] = value

        # Format the stamp
        try:
            op_version = self.op_meta_info.header['version']
        except:
            raise ValueError('Operation "{}": Could not add history information'
                             ' because the "version" property is undefined.'.format(op_name))

        stamp = '\nModified with Cate v' + __version__ + ' ' + \
                op_name + ' v' + \
                op_version + \
                ' \nDefault input values: ' + \
                str(self.op_meta_info.inputs) + '\nProvided input values: ' + \
                str(input_str) + '\n'

        # Append the stamp to existing history information or create history
        # attribute if none is found
        try:
            ds.attrs['history'] = ds.attrs['history'] + stamp
        except KeyError:
            # History doesn't yet exist
            ds.attrs['history'] = stamp
        return ds


class OpRegistry:
    """
    An operation registry allows for addition, removal, and retrieval of operations.
    """

    def __init__(self):
        self._op_registrations = OrderedDict()

    @property
    def op_registrations(self) -> OrderedDict:
        """
        Get all operation registrations of type :py:class:`cate.core.op.Operation`.

        :return: a mapping of fully qualified operation names to operation registrations
        """
        return OrderedDict(sorted(self._op_registrations.items(), key=lambda item: item[0]))

    def add_op(self, operation: Callable, fail_if_exists=True, replace_if_exists=False) -> Operation:
        """
        Add a new operation registration.

        :param operation: A operation object such as a class or any callable.
        :param fail_if_exists: raise ``ValueError`` if the operation was already registered
        :param replace_if_exists: replaces an existing operation if *fail_if_exists* is ``False``
        :return: a new or existing :py:class:`cate.core.op.Operation`
        """
        operation = self._unwrap_operation(operation)
        op_key = self.get_op_key(operation)
        if op_key in self._op_registrations:
            if fail_if_exists:
                raise ValueError("operation with name '%s' already registered" % op_key)
            elif not replace_if_exists:
                return self._op_registrations[op_key]
        op_registration = Operation(operation)
        self._op_registrations[op_key] = op_registration
        return op_registration

    def remove_op(self, operation: Callable, fail_if_not_exists=False) -> Optional[Operation]:
        """
        Remove an operation registration.

        :param operation: A fully qualified operation name or operation object such as a class or any callable.
        :param fail_if_not_exists: raise ``ValueError`` if no such operation was found
        :return: the removed :py:class:`cate.core.op.Operation` object or ``None``
                 if *fail_if_not_exists* is ``False``.
        """
        operation = self._unwrap_operation(operation)
        op_key = self.get_op_key(operation)
        if op_key not in self._op_registrations:
            if fail_if_not_exists:
                raise ValueError("operation with name '%s' not registered" % op_key)
            else:
                return None
        return self._op_registrations.pop(op_key)

    def get_op(self, operation, fail_if_not_exists=False) -> Operation:
        """
        Get an operation registration.

        :param operation: A fully qualified operation name or operation object such as a class or any callable.
        :param fail_if_not_exists: raise ``ValueError`` if no such operation was found
        :return: a :py:class:`cate.core.op.Operation` object or ``None`` if *fail_if_not_exists* is ``False``.
        """
        operation = self._unwrap_operation(operation)
        op_key = self.get_op_key(operation)
        op_registration = self._op_registrations.get(op_key, None)
        if op_registration is None and fail_if_not_exists:
            raise ValueError("operation with name '%s' not registered" % op_key)
        return op_registration

    # noinspection PyMethodMayBeStatic
    def get_op_key(self, operation: Union[str, Callable]):
        """
        Get a key under which the given operation will be registered.

        :param operation: A fully qualified operation name or a callable object
        :return: The operation key
        """
        try:
            qualified_name = operation.op_meta_info.qualified_name
        except AttributeError:
            if isinstance(operation, str):
                qualified_name = operation
            else:
                operation = self._unwrap_operation(operation)
                qualified_name = object_to_qualified_name(operation)
        if qualified_name.startswith('cate.ops.'):
            return qualified_name.rsplit('.', maxsplit=1)[1]
        else:
            return qualified_name

    @classmethod
    def _unwrap_operation(cls, operation):
        if not operation:
            raise ValueError('operation must be given')
        try:
            return operation.wrapped_op
        except AttributeError:
            return operation


class _DefaultOpRegistry(OpRegistry):
    def __repr__(self):
        return 'OP_REGISTRY'


# check (nf) - for more flexibility, REGISTRY may be configured by dependency injection
# see Python libs 'pinject' (Google), 'inject', and others

#: The default operation registry of type :py:class:`cate.core.op.OpRegistry`.
OP_REGISTRY = _DefaultOpRegistry()


def op(tags=UNDEFINED,
       version=UNDEFINED,
       res_prefix=UNDEFINED,
       deprecated=UNDEFINED,
       registry=OP_REGISTRY,
       **properties):
    """
    ``op`` is a decorator function that registers a Python function or class in the default operation registry or
    the one given by *registry*, if any.
    Any other keywords arguments in *header* are added to the operation's meta-information header.
    Classes annotated by this decorator must have callable instances.

    When a function is registered, an introspection is performed. During this process, initial operation
    the meta-information header property *description* is derived from the function's docstring.

    If any output of this operation will have its history information
    automatically updated, there should be version information found in the
    operation header. Thus it's always a good idea to add it to all operations::

    @op(version='X.x')

    :param tags: An optional list of string tags.
    :param version: An optional version string.
    :param res_prefix: An optional prefix that will be used to generate the names for "resources" that are
           used to hold a reference to the objects returned by the operation.
    :param deprecated: An optional boolean or a string. If a string is used, it should explain
           why the operation has been deprecated and which new operation to use instead.
           If set to ``True``, the operation's doc-string should explain the deprecation.
    :param registry: The operation registry.
    :param properties: Other properties (keyword arguments) that will be added to the meta-information of operation.
    """

    def decorator(op_func):
        new_properties = dict(tags=tags,
                              version=version,
                              resource_prefix=res_prefix,
                              deprecated=deprecated,
                              **properties)
        op_registration = registry.add_op(op_func, fail_if_exists=False)
        op_registration.op_meta_info.header.update({k: v for k, v in new_properties.items() if v is not UNDEFINED})
        return op_registration

    return decorator


def op_input(input_name: str,
             default_value=UNDEFINED,
             units=UNDEFINED,
             data_type=UNDEFINED,
             nullable=UNDEFINED,
             value_set_source=UNDEFINED,
             value_set=UNDEFINED,
             value_range=UNDEFINED,
             deprecated=UNDEFINED,
             position=UNDEFINED,
             context=UNDEFINED,
             registry=OP_REGISTRY,
             **properties):
    """
    ``op_input`` is a decorator function that provides meta-information for an operation input identified by
    *input_name*. If the decorated function or class is not registered as an operation yet, it is added to the default
    operation registry or the one given by *registry*, if any.

    When a function is registered, an introspection is performed. During this process, initial operation
    meta-information input properties are derived for each positional and keyword argument named *input_name*:

    ================ ==============================================================================
    Derived property Source
    ================ ==============================================================================
    *position*       The position of a positional argument, e.g. ``2`` for input ``z`` in
                     ``def f(x, y, z, c=2)``.
    *default_value*  The value of a keyword argument, e.g. ``52.3`` for input ``latitude``
                     from argument definition ``latitude:float=52.3``
    *data_type*      The type annotation type, e.g. ``float`` for input ``latitude``
                     from argument definition  ``latitude:float``
    ================ ==============================================================================

    The derived properties listed above plus any of *value_set*, *value_range*, and any key-value pairs in *properties*
    are added to the input's meta-information.
    A key-value pair in *properties* will always overwrite the derived properties listed above.

    :param input_name: The name of an input.
    :param default_value: A default value.
    :param units: The geo-physical units of the input value.
    :param data_type: The data type of the input values.
           If not given, the type of any given, non-None *default_value* is used.
    :param nullable: If ``True``, the value of the input may be ``None``.
           If not given, it will be set to ``True`` if the *default_value* is ``None``.
    :param value_set_source: The name of an input, which can be used to generate a dynamic value set.
    :param value_set: A sequence of the valid values. Note that all values in this sequence
           must be compatible with *data_type*.
    :param value_range: A sequence specifying the possible range of valid values.
    :param deprecated: An optional boolean or a string. If a string is used, it should explain
           why the input has been deprecated and which new input to use instead.
           If set to ``True``, the input's doc-string should explain the deprecation.
    :param position: The zero-based position of an input.
    :param context: If ``True``, the value of the operation input will be a dictionary representing
           the current execution context. For example,
           when the operation is executed from a workflow, the dictionary will hold at least three
           entries: ``workflow`` provides the current workflow, ``step`` is the currently executed step,
           and ``value_cache`` which is a mapping from step identifiers to step outputs. If *context* is a
           string, the value of the operation input will be the result of evaluating the string as Python expression
           with the current execution context as local environment. This means, *context* may be an expression
           such as 'workspace', 'workspace.base_dir', 'step', 'step.id'.
    :param properties: Other properties (keyword arguments) that will be added to the
           meta-information of the named output.
    :param registry: Optional operation registry.
    """

    def decorator(op_func):
        op_registration = registry.add_op(op_func, fail_if_exists=False)
        input_namespace = op_registration.op_meta_info.inputs
        if input_name not in input_namespace:
            input_namespace[input_name] = dict()
        new_properties = dict(data_type=data_type,
                              default_value=default_value,
                              units=units,
                              nullable=nullable,
                              value_set_source=value_set_source,
                              value_set=value_set,
                              value_range=value_range,
                              deprecated=deprecated,
                              position=position,
                              context=context,
                              **properties)

        input_namespace[input_name].update({k: v for k, v in new_properties.items() if v is not UNDEFINED})
        _adjust_input_properties(input_namespace[input_name])
        return op_registration

    return decorator


def op_output(output_name: str,
              data_type=UNDEFINED,
              deprecated=UNDEFINED,
              registry=OP_REGISTRY,
              **properties):
    """
    ``op_output`` is a decorator function that provides meta-information for an operation output identified by
    *output_name*. If the decorated function or class is not registered as an operation yet, it is added to the default
    operation registry or the one given by *registry*, if any.

    If your function does not return multiple named outputs, use the :py:func:`op_return` decorator function.
    Note that::

        @op_return(...)
        def my_func(...):
            ...

    if equivalent to::

        @op_output('return', ...)
        def my_func(...):
            ...

    To automatically add information about cate, its version, this operation
    and its inputs, to this output, set 'add_history' to True::

        @op_output('name', add_history=True)

    Note that the operation should have version information added to it when
    add_history is True::

        @op(version='X.x')

    :param output_name: The name of the output.
    :param data_type: The data type of the output value.
    :param deprecated: An optional boolean or a string. If a string is used, it should explain
           why the output has been deprecated and which new output to use instead.
           If set to ``True``, the output's doc-string should explain the deprecation.
    :param properties: Other properties (keyword arguments) that
           will be added to the meta-information of the named output.
    :param registry: Optional operation registry.
    """

    def decorator(op_func):
        op_registration = registry.add_op(op_func, fail_if_exists=False)
        output_namespace = op_registration.op_meta_info.outputs
        if not op_registration.op_meta_info.has_named_outputs:
            # if there is only one entry and it is the 'return' entry, rename it to value of output_name
            output_properties = output_namespace[OpMetaInfo.RETURN_OUTPUT_NAME]
            del output_namespace[OpMetaInfo.RETURN_OUTPUT_NAME]
            output_namespace[output_name] = output_properties
        elif output_name not in output_namespace:
            output_namespace[output_name] = dict()
        new_properties = dict(data_type=data_type, deprecated=deprecated, **properties)
        output_namespace[output_name].update({k: v for k, v in new_properties.items() if v is not UNDEFINED})
        return op_registration

    return decorator


def op_return(data_type=UNDEFINED,
              registry=OP_REGISTRY,
              **properties):
    """
    ``op_return`` is a decorator function that provides meta-information for a single, anonymous operation return value
    (whose output name is ``"return"``). If the decorated function or class is not registered as an operation yet,
    it is added to the default operation registry or the one given by *registry*, if any.
    Any other keywords arguments in *properties* are added to the output's meta-information.

    When a function is registered, an introspection is performed. During this process, initial operation
    meta-information output properties are derived from the function's return type annotation, that is
    *data_type* will be e.g. ``float`` if a function is annotated as ``def f(x, y) -> float: ...``.

    The derived *data_type* property and any key-value pairs in *properties* are added to the output's meta-information.
    A key-value pair in *properties* will always overwrite a derived *data_type*.

    If your function returns multiple named outputs, use the :py:func:`op_output` decorator function.
    Note that::

        @op_return(...)
        def my_func(...):
            ...

    if equivalent to::

        @op_output('return', ...)
        def my_func(...):
            ...

    To automatically add information about cate, its version, this operation
    and its inputs, to this output, set 'add_history' to True::

        @op_return(add_history=True)

    Note that the operation should have version information added to it when
    add_history is True::

        @op(version='X.x')

    :param data_type: The data type of the return value.
    :param properties: Other properties (keyword arguments)
           that will be added to the meta-information of the return value.
    :param registry: The operation registry.
    """
    return op_output(OpMetaInfo.RETURN_OUTPUT_NAME,
                     data_type=data_type,
                     registry=registry,
                     **properties)


def _adjust_input_properties(input_properties):
    """Adjust any undefined input properties that can be derived from other defined input properties."""

    default_value = input_properties.get('default_value', UNDEFINED)

    # Derive undefined 'nullable' from 'default_value'
    nullable = input_properties.get('nullable', UNDEFINED)
    if nullable is UNDEFINED and default_value is None:
        input_properties['nullable'] = True

    # Derive undefined 'data_type' from 'default_value'
    data_type = input_properties.get('data_type', UNDEFINED)
    if data_type is UNDEFINED and not (default_value is UNDEFINED or default_value is None):
        input_properties['data_type'] = type(default_value)


def new_subprocess_op(op_meta_info: OpMetaInfo,
                      command_pattern: str,
                      run_python: bool = False,
                      cwd: Optional[str] = None,
                      env: Dict[str, str] = None,
                      shell: bool = False,
                      started: Union[str, Callable] = None,
                      progress: Union[str, Callable] = None,
                      done: Union[str, Callable] = None) -> Operation:
    """
    Create an operation for a child program run in a new process.

    :param op_meta_info: Meta-information about the resulting operation and the operation's inputs and outputs.
    :param command_pattern: A pattern that will be interpolated to obtain the actual command to be executed.
           May contain "{input_name}" fields which will be replaced by the actual input value converted to text.
           *input_name* must refer to a valid operation input name in *op_meta_info.input* or it must be
           the value of either the "write_to" or "read_from" property of another input's property map.
    :param run_python: If True, *command_pattern* refers to a Python script which will be executed with
           the Python interpreter that Cate uses.
    :param cwd: Current working directory to run the command line in.
    :param env: Environment variables passed to the shell that executes the command line.
    :param shell: Whether to use the shell as the program to execute.
    :param started: Either a callable that receives a text line from the executable's stdout
           and returns a tuple (label, total_work) or a regex that must match
           in order to signal the start of progress monitoring.
           The regex must provide the group names "label" or "total_work" or both,
           e.g. "(?P<label>\w+)" or "(?P<total_work>\d+)"
    :param progress: Either a callable that receives a text line from the executable's stdout
           and returns a tuple (work, msg) or a regex that must match
           in order to signal process.
           The regex must provide group names "work" or "msg" or both,
           e.g. "(?P<msg>\w+)" or "(?P<work>\d+)"
    :param done: Either a callable that receives a text line a text line from the executable's stdout
           and returns True or False or a regex that must match
           in order to signal the end of progress monitoring.
    :return: The executable wrapped into an operation.
    """

    if started or progress and not op_meta_info.has_monitor:
        op_meta_info = OpMetaInfo(op_meta_info.qualified_name,
                                  has_monitor=True,
                                  inputs=op_meta_info.inputs,
                                  outputs=op_meta_info.outputs,
                                  header=op_meta_info.header)

    # Idea: process special input properties:
    #   - "is_cwd" - an input that provides the current working directory, must be of type str
    #   - "is_env" - an input that provides environment variables, must be of type DictLike
    #   - "is_output" - an input that provides the file path of an output, must be of type str

    def run_executable(**kwargs):

        format_kwargs = {}
        temp_input_files = {}
        temp_output_files = {}

        for name, props in op_meta_info.inputs.items():
            value = kwargs.get(name, props.get('default_value', UNDEFINED))
            if value is not UNDEFINED:
                if 'write_to' in props:
                    new_name = props['write_to']
                    _, file = new_temp_file(suffix='.nc')
                    value.to_netcdf(file)
                    format_kwargs[new_name] = file
                    temp_input_files[name] = file
                else:
                    try:
                        value = value.format()
                    except AttributeError:
                        pass
                    format_kwargs[name] = value

        for name, props in op_meta_info.outputs.items():
            if 'read_from' in props:
                new_name = props['read_from']
                _, file = new_temp_file(suffix='.nc')
                format_kwargs[new_name] = file
                temp_output_files[name] = file

        monitor = None
        if _MONITOR in format_kwargs:
            monitor = format_kwargs.pop(_MONITOR)

        command = command_pattern.format(**format_kwargs)

        stdout_handler = None
        if monitor:
            stdout_handler = ProcessOutputMonitor(monitor,
                                                  label=command,
                                                  started=started, progress=progress, done=done)

        if run_python:
            command = '"{}" {}'.format(sys.executable, command)

        exit_code = run_subprocess(command,
                                   cwd=cwd, env=env, shell=shell,
                                   stdout_handler=stdout_handler,
                                   is_cancelled=monitor.is_cancelled if monitor else None)

        for file in temp_input_files.values():
            del_temp_file(file)

        return_value = {}
        for name, file in temp_output_files.items():
            return_value[name] = xr.open_dataset(file)

        if not return_value:
            # No output specified, so we return exit code
            return exit_code

        if exit_code:
            # There is output specified, but exit code signals error
            raise ValueError('command [{}] exited with code {}'.format(command_pattern, exit_code))

        if len(return_value) == 1 and 'return' in return_value:
            # Single output
            return return_value['return']
        else:
            # Multiple outputs
            return return_value

    run_executable.__name__ = op_meta_info.qualified_name
    run_executable.__doc__ = op_meta_info.header.get('description')

    return Operation(run_executable, op_meta_info=op_meta_info)


def new_expression_op(op_meta_info: OpMetaInfo, expression: str) -> Operation:
    """
    Create an operation that wraps a Python expression.

    :param op_meta_info: Meta-information about the resulting operation and the operation's inputs and outputs.
    :param expression: The Python expression. May refer to any name given in *op_meta_info.input*.
    :return: The Python expression wrapped into an operation.
    """

    if not op_meta_info:
        raise ValueError('op_meta_info must be given')
    if not expression:
        raise ValueError('expression must be given')

    def eval_expression(**kwargs):
        return safe_eval(expression, local_namespace=kwargs)

    inputs = OrderedDict(op_meta_info.inputs)
    outputs = OrderedDict(op_meta_info.outputs)
    if len(outputs) == 0:
        outputs[_RETURN] = {}
    op_meta_info = OpMetaInfo(op_meta_info.qualified_name,
                              has_monitor=op_meta_info.has_monitor,
                              header=dict(op_meta_info.header),
                              inputs=inputs,
                              outputs=outputs)

    eval_expression.__name__ = op_meta_info.qualified_name
    eval_expression.__doc__ = op_meta_info.header.get('description')

    return Operation(eval_expression, op_meta_info=op_meta_info)
