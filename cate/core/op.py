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

__author__ = "Norman Fomferra (Brockmann Consult GmbH)"

"""
Description
===========

This modules provides classes and functions allowing to maintain *operations*. Operations can be called from
the Cate command-line interface, may be referenced from within processing workflows, or may be called remotely e.g. from
graphical user interface or web frontend. An operation (:py:class:`OpRegistration`) comprises a Python callable and
some additional meta-information (:py:class:`OpMetaInfo`) that allows for automatic input validation,
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
    the user function's docstring and information about the arguments an its return values, exploiting any type annotations.
    For example, the following properties can be associated with input arguments: data type, default value, value set,
    valid range, if it is mandatory or optional, expected dataset schema so that operations can be ECV-specific.
    Meta-information is required to let an operation explain itself when used in a (IPython) REPL or when web service is
    requested to respond with an operations's capabilities.
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
    ``f(*args, monitor=Monitor.NULL, **kwargs)``. In this case the a monitor (of type :py:class:`Monitor`) will be passed
    by the framework to the user function in order to observe the progress and to cancel an operation.

----

Verification
============

The module's unit-tests are located in `test/test_op.py <https://github.com/CCI-Tools/cate-core/blob/master/test/test_op.py>`_
and may be executed using ``$ py.test test/test_op.py --cov=cate/core/plugin.py`` for extra code coverage information.


Components
==========
"""

from collections import OrderedDict
from typing import Union, Callable

import xarray as xr

from cate import __version__
from cate.util import OpMetaInfo, object_to_qualified_name, Monitor, UNDEFINED


class OpRegistration:
    """
    A registered operation comprises the actual operation object, which may be a class or any callable, and
    meta-information about the operation.

    :param operation: the actual class or any callable object.
    """

    def __init__(self, operation):
        self._op_meta_info = OpMetaInfo.introspect_operation(operation)
        self._operation = operation
        for attr_name in ['__module__', '__name__', '__qualname__', '__doc__', '__file__']:
            try:
                setattr(self, attr_name, getattr(operation, attr_name))
            except AttributeError:
                pass

    @property
    def op_meta_info(self) -> OpMetaInfo:
        """
        :return: Meta-information about the operation, see :py:class:`cate.core.op.OpMetaInfo`.
        """
        return self._op_meta_info

    @property
    def operation(self) -> Callable:
        """
        :return: The actual operation object which may be any callable.
        """
        return self._operation

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
            raise NotImplementedError('Adding of operation signature to an'
                                      ' output is currently implemented only'
                                      ' for xarray datasets. Operation:'
                                      ' {}'.format(op_name))

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
            raise ValueError('Could not add history information for an'
                             ' operation that does not have a version defined.'
                             'Operation: {}'.format(op_name))

        stamp = '\nModified with Cate v' + __version__ + ' ' + \
                op_name + ' v' + \
                op_version + \
                ' \nDefault input values: ' + \
                str(self.op_meta_info.input) + '\nProvided input values: ' + \
                str(input_str) + '\n'

        # Append the stamp to existing history information or create history
        # attribute if none is found
        try:
            ds.attrs['history'] = ds.attrs['history'] + stamp
        except KeyError:
            # History doesn't yet exist
            ds.attrs['history'] = stamp
        return ds

    def __str__(self):
        return '%s: %s' % (self.operation, self.op_meta_info)

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
                    raise ValueError("too many inputs given for operation '{}'".format(self.op_meta_info.qualified_name))
                input_name = self.op_meta_info.input_names[position]
                input_values[input_name] = args[position]

        # set default_value where input values are missing
        self.op_meta_info.set_default_input_values(input_values)

        # validate the input_values using this operation's meta-info
        self.op_meta_info.validate_input_values(input_values)

        if self.op_meta_info.has_monitor:
            # set the monitor only if it is an argument
            input_values[self.op_meta_info.MONITOR_INPUT_NAME] = monitor

        operation = self.operation
        # call the callable
        return_value = operation(**input_values)

        if self.op_meta_info.has_named_outputs:
            # return_value is expected to be a dictionary-like object
            # set default_value where output values in return_value are missing
            for name, properties in self.op_meta_info.output.items():
                if name not in return_value or return_value[name] is None:
                    return_value[name] = properties.get('default_value')
            # validate the return_value using this operation's meta-info
            self.op_meta_info.validate_output_values(return_value)
            # Add history information to outputs
            for name, properties in self.op_meta_info.output.items():
                add_history = properties.get('add_history')
                if add_history:
                    return_value[name] = self._add_history(return_value[name], input_values)
        else:
            # return_value is a single value, not a dict
            # set default_value if return_value is missing
            properties = self.op_meta_info.output[OpMetaInfo.RETURN_OUTPUT_NAME]
            if return_value is None:
                return_value = properties.get('default_value')
            # validate the return_value using this operation's meta-info
            self.op_meta_info.validate_output_values({OpMetaInfo.RETURN_OUTPUT_NAME: return_value})
            # Add history information to the output
            add_history = properties.get('add_history')
            if add_history:
                return_value = self._add_history(return_value, input_values)

        return return_value


class OpRegistry:
    """
    An operation registry allows for addition, removal, and retrieval of operations.
    """

    def __init__(self):
        self._op_registrations = OrderedDict()

    @property
    def op_registrations(self) -> OrderedDict:
        """
        Get all operation registrations of type :py:class:`cate.core.op.OpRegistration`.

        :return: a mapping of fully qualified operation names to operation registrations
        """
        return OrderedDict(sorted(self._op_registrations.items(), key=lambda item: item[0]))

    def add_op(self, operation: Callable, fail_if_exists=True) -> OpRegistration:
        """
        Add a new operation registration.

        :param operation: A operation object such as a class or any callable.
        :param fail_if_exists: raise ``ValueError`` if the operation was already registered
        :return: a :py:class:`cate.core.op.OpRegistration` object
        """
        operation = self._unwrap_operation(operation)
        op_key = self.get_op_key(operation)
        if op_key in self._op_registrations:
            if fail_if_exists:
                raise ValueError("operation with name '%s' already registered" % op_key)
            else:
                return self._op_registrations[op_key]
        op_registration = OpRegistration(operation)
        self._op_registrations[op_key] = op_registration
        return op_registration

    def remove_op(self, operation: Callable, fail_if_not_exists=False) -> OpRegistration:
        """
        Remove an operation registration.

        :param operation: A fully qualified operation name or operation object such as a class or any callable.
        :param fail_if_not_exists: raise ``ValueError`` if no such operation was found
        :return: the removed :py:class:`cate.core.op.OpRegistration` object or ``None``
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

    def get_op(self, operation, fail_if_not_exists=False) -> OpRegistration:
        """
        Get an operation registration.

        :param operation: A fully qualified operation name or operation object such as a class or any callable.
        :param fail_if_not_exists: raise ``ValueError`` if no such operation was found
        :return: a :py:class:`cate.core.op.OpRegistration` object or ``None`` if *fail_if_not_exists* is ``False``.
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
            return operation.operation
        except AttributeError:
            return operation


class _DefaultOpRegistry(OpRegistry):
    def __repr__(self):
        return 'OP_REGISTRY'


# check (nf) - for more flexibility, REGISTRY may be configured by dependency injection
# see Python libs 'pinject' (Google), 'inject', and others

#: The default operation registry of type :py:class:`cate.core.op.OpRegistry`.
OP_REGISTRY = _DefaultOpRegistry()


def op(registry=OP_REGISTRY, **properties):
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

    :param properties: Other properties (keyword arguments) that will be added to the meta-information of operation.
    :param registry: The operation registry.
    """

    def decorator(op_func):
        op_registration = registry.add_op(op_func, fail_if_exists=False)
        try:
            default_tags = op_registration.op_meta_info.header['tags']
            if default_tags[0] not in properties['tags']:
                properties['tags'] = default_tags + properties['tags']
        except KeyError:
            # The user has not provided any tags
            pass
        op_registration.op_meta_info.header.update({k: v for k, v in properties.items() if v is not UNDEFINED})
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
        input_namespace = op_registration.op_meta_info.input
        if input_name not in input_namespace:
            input_namespace[input_name] = dict()
        new_properties = dict(data_type=data_type,
                              default_value=default_value,
                              units=units,
                              nullable=nullable,
                              value_set_source=value_set_source,
                              value_set=value_set,
                              value_range=value_range,
                              position=position,
                              context=context,
                              **properties)

        input_namespace[input_name].update({k: v for k, v in new_properties.items() if v is not UNDEFINED})
        _adjust_input_properties(input_namespace[input_name])
        return op_registration

    return decorator


def op_output(output_name: str,
              data_type=UNDEFINED,
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
    :param properties: Other properties (keyword arguments) that will be added to the meta-information of the named output.
    :param registry: Optional operation registry.
    """

    def decorator(op_func):
        op_registration = registry.add_op(op_func, fail_if_exists=False)
        output_namespace = op_registration.op_meta_info.output
        if not op_registration.op_meta_info.has_named_outputs:
            # if there is only one entry and it is the 'return' entry, rename it to value of output_name
            output_properties = output_namespace[OpMetaInfo.RETURN_OUTPUT_NAME]
            del output_namespace[OpMetaInfo.RETURN_OUTPUT_NAME]
            output_namespace[output_name] = output_properties
        elif output_name not in output_namespace:
            output_namespace[output_name] = dict()
        new_properties = dict(data_type=data_type, **properties)
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
    :param properties: Other properties (keyword arguments) that will be added to the meta-information of the return value.
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
