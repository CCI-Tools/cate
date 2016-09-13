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

This modules provides classes and functions allowing to maintain *operations*. Operations can be called from
the ECT command-line interface, may be referenced from within processing workflows, or may be called remotely e.g. from
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

The module's unit-tests are located in `test/test_op.py <https://github.com/CCI-Tools/ect-core/blob/master/test/test_op.py>`_
and may be executed using ``$ py.test test/test_op.py --cov=ect/core/plugin.py`` for extra code coverage information.


Components
==========
"""

from collections import OrderedDict
from inspect import isclass
from typing import Dict, Tuple, List

from .monitor import Monitor
from .util import object_to_qualified_name, qualified_name_to_object


class OpMetaInfo:
    """
    Meta-information about an operation:

    * :py:attr:`qualified_name`: a an ideally unique, qualified operation name
    * :py:attr:`header`: dictionary of arbitrary operation attributes
    * :py:attr:`input`: ordered dictionary of named inputs,
      each mapping to a dictionary of arbitrary input attributes
    * :py:attr:`output`: ordered dictionary of named outputs,
      each mapping to a dictionary of arbitrary output attributes

    Warning: `OpMetaInfo`` objects should be considered immutable. However, the dictionaries mentioned above
    are returned "as-is", mostly for performance reasons. Changing entries in these dictionaries directly
    may cause unwanted side-effects.

    :param op_qualified_name: The operation's qualified name.
    :param has_monitor: Whether the operation supports a :py:class:`Monitor` keyword argument named ``monitor``.
    :param header_dict: Header information dictionary.
    :param input_dict: Input information dictionary.
    :param output_dict: Output information dictionary.
    """

    def __init__(self,
                 op_qualified_name: str,
                 has_monitor: bool = False,
                 header_dict: dict = None,
                 input_dict: OrderedDict = None,
                 output_dict: OrderedDict = None):
        if not op_qualified_name:
            raise ValueError("argument 'op_qualified_name' is required")
        self._qualified_name = op_qualified_name
        self._has_monitor = True if has_monitor else False
        self._header = header_dict if header_dict else dict()
        self._input = OrderedDict(input_dict if input_dict else {})
        self._output = OrderedDict(output_dict if output_dict else {})

    #: The constant ``'monitor'``, which is the name of an operation input that will
    #: receive a :py:class:`Monitor` object as value.
    MONITOR_INPUT_NAME = 'monitor'

    #: The constant ``'return'``, which is the name of a single, unnamed operation output.
    RETURN_OUTPUT_NAME = 'return'

    @property
    def qualified_name(self) -> str:
        """
        :return: Fully qualified name of the actual operation.
        """
        return self._qualified_name

    @property
    def header(self) -> dict:
        """
        :return: Operation header attributes.
        """
        return self._header

    @property
    def input(self) -> OrderedDict:
        """
        Mapping from an input name to a dictionary of properties describing the input.

        :return: Named inputs.
        """
        return self._input

    @property
    def output(self) -> OrderedDict:
        """
        Mapping from an output name to a dictionary of properties describing the output.

        :return: Named outputs.
        """
        return self._output

    @property
    def has_monitor(self) -> bool:
        """
        :return: ``True`` if the operation supports a :py:class:`Monitor` value as additional keyword argument named
                 ``monitor``.
        """
        return self._has_monitor

    @property
    def has_named_outputs(self) -> bool:
        """
        :return: ``True`` if the output value of the operation is expected be a dictionary-like mapping of output names
                 to output values.
        """
        return not (len(self._output) == 1 and self.RETURN_OUTPUT_NAME in self._output)

    def to_json_dict(self):
        """
        Return a JSON-serializable dictionary representation of this object. E.g. values of the `data_type``
        property are converted from Python types to their string representation.

        :return: A JSON-serializable dictionary
        """

        json_dict = OrderedDict()
        json_dict['qualified_name'] = self.qualified_name
        if self.has_monitor:
            json_dict['has_monitor'] = True
        if self.header:
            json_dict['header'] = dict(self.header)
        json_dict['input'] = self.object_dict_to_json_dict(self.input)
        json_dict['output'] = self.object_dict_to_json_dict(self.output)
        return json_dict

    @classmethod
    def from_json_dict(cls, json_dict, **kwargs):
        qualified_name = json_dict.get('qualified_name', kwargs.get('qualified_name', None))
        header_obj = json_dict.get('header', kwargs.get('header', None))
        has_monitor = json_dict.get('has_monitor', kwargs.get('has_monitor', False))
        input_dict = json_dict.get('input', kwargs.get('input', None))
        output_dict = json_dict.get('output', kwargs.get('output', None))
        return OpMetaInfo(qualified_name,
                          header_dict=header_obj,
                          has_monitor=has_monitor,
                          input_dict=cls.json_dict_to_object_dict(input_dict),
                          output_dict=cls.json_dict_to_object_dict(output_dict))

    @classmethod
    def object_dict_to_json_dict(cls, obj_dict):
        json_dict = OrderedDict()
        for name, properties in obj_dict.items():
            json_dict[name] = dict(properties)
            if 'data_type' in properties:
                json_dict[name]['data_type'] = object_to_qualified_name(properties['data_type'])
        return json_dict

    @classmethod
    def json_dict_to_object_dict(cls, json_dict):
        obj_dict = OrderedDict()
        for name, properties in json_dict.items():
            obj_dict[name] = dict(properties)
            if 'data_type' in properties:
                obj_dict[name]['data_type'] = qualified_name_to_object(properties['data_type'])
        return obj_dict

    def __str__(self):
        return "OpMetaInfo('%s')" % self.qualified_name

    def __repr__(self):
        return "OpMetaInfo('%s')" % self.qualified_name

    @classmethod
    def introspect_operation(cls, operation) -> 'OpMetaInfo':
        if not operation:
            raise ValueError("'operation' argument must be given")

        op_qualified_name = object_to_qualified_name(operation, fail=True)

        header = dict()
        # Introspect the operation instance (see https://docs.python.org/3.5/library/inspect.html)
        if hasattr(operation, '__doc__'):
            # documentation string
            header['description'] = operation.__doc__

        input_dict, has_monitor = OrderedDict(), False
        if hasattr(operation, '__code__'):
            input_dict, has_monitor = cls._introspect_inputs_from_callable(operation, False)
        elif isclass(operation):
            if hasattr(operation, '__call__'):
                call_method = getattr(operation, '__call__')
                input_dict, has_monitor = cls._introspect_inputs_from_callable(call_method, True)
            else:
                raise ValueError('operations of type class must define a __call__(self, ...) method')

        output_dict = OrderedDict()
        if hasattr(operation, '__annotations__'):
            # mapping of parameters names to annotations; 'return' key is reserved for return annotations.
            annotations = operation.__annotations__
            for annotated_name, annotated_type in annotations.items():
                if annotated_name == 'return':
                    # op_meta_info.output can't be present so far -> assign new dict
                    output_dict[OpMetaInfo.RETURN_OUTPUT_NAME] = dict(data_type=annotated_type)
                elif annotated_name != cls.MONITOR_INPUT_NAME:
                    # input_dict[annotated_name] should be present through _introspect_inputs_from_callable() call
                    input_dict[annotated_name]['data_type'] = annotated_type
        if len(output_dict) == 0:
            output_dict[OpMetaInfo.RETURN_OUTPUT_NAME] = dict()

        return OpMetaInfo(op_qualified_name,
                          header_dict=header,
                          has_monitor=has_monitor,
                          input_dict=input_dict,
                          output_dict=output_dict)

    @classmethod
    def _introspect_inputs_from_callable(cls, operation, is_method: bool) -> Tuple[OrderedDict, bool]:
        input_dict = OrderedDict()
        has_monitor = False
        # code object containing compiled function bytecode
        if not hasattr(operation, '__code__'):
            # Check: throw exception here?
            return input_dict, has_monitor
        code = operation.__code__
        # number of arguments (not including * or ** args)
        arg_count = code.co_argcount
        # tuple of names of arguments and local variables
        arg_names = code.co_varnames[0:arg_count]
        if len(arg_names) > 0 and is_method and arg_names[0] == 'self':
            arg_names = arg_names[1:]
            arg_count -= 1
        # Reserve input slots for input names, but 'monitor'
        arg_pos = 0
        for arg_name in arg_names:
            if cls.MONITOR_INPUT_NAME != arg_name:
                input_dict[arg_name] = dict(position=arg_pos)
                arg_pos += 1
            else:
                has_monitor = True
        # Set 'default_value' for input names, but 'monitor'
        if operation.__defaults__:
            # tuple of any default values for positional or keyword parameters
            default_values = operation.__defaults__
            num_default_values = len(default_values)
            for i in range(num_default_values):
                arg_name = arg_names[i - num_default_values]
                if cls.MONITOR_INPUT_NAME != arg_name:
                    default_value = default_values[i]
                    input_dict[arg_name]['default_value'] = default_value
                    if 'position' in input_dict[arg_name]:
                        del input_dict[arg_name]['position']
        return input_dict, has_monitor

    def set_default_input_values(self, input_values: Dict):
        """
        If any missing input value in *input_values*, set value of "default_value" property, if it exists.

        :param input_values: The dictionary of input values that will be modified.
        """
        for name, properties in self.input.items():
            if name not in input_values and 'default_value' in properties:
                input_values[name] = properties['default_value']

    def validate_input_values(self, input_values: Dict):
        """
        Validate given *input_values* against the operation's input properties.

        :param input_values: The dictionary of input values.
        :raise ValueError: If *input_values* are invalid w.r.t. to the operation's input properties.
        """
        inputs = self.input
        # Ensure required input values have values (even None is a value).
        for name, properties in inputs.items():
            required = 'position' in properties
            if required and (name not in input_values):
                raise ValueError("input '%s' for operation '%s' required" %
                                 (name, self.qualified_name))
        # Ensure all input values are valid w.r.t. input properties
        for name, value in input_values.items():
            if name not in inputs:
                raise ValueError("'%s' is not an input of operation '%s'" % (name, self.qualified_name))
            input_properties = inputs[name]
            if value is None:
                default_is_none = input_properties.get('default_value', 1) is None
                value_set = input_properties.get('value_set', None)
                value_set_has_none = value_set and (None in value_set)
                nullable = input_properties.get('nullable', False)
                if not (default_is_none or value_set_has_none or nullable):
                    raise ValueError(
                        "input '%s' for operation '%s' is not nullable" % (name, self.qualified_name))
                continue
            data_type = input_properties.get('data_type', None)
            # TODO (forman, 20160907): perform more elaborated input type checking here
            is_float_type = data_type is float and (isinstance(value, float) or isinstance(value, int))
            if data_type and not (isinstance(value, data_type) or is_float_type):
                raise ValueError(
                    "input '%s' for operation '%s' must be of type %s, but got %s" % (
                        name, self.qualified_name, data_type, type(value)))
            value_set = input_properties.get('value_set', None)
            if value_set and (value not in value_set):
                raise ValueError(
                    "input '%s' for operation '%s' must be one of %s" % (
                        name, self.qualified_name, value_set))
            value_range = input_properties.get('value_range', None)
            if value_range and (value is None or not (value_range[0] <= value <= value_range[1])):
                raise ValueError(
                    "input '%s' for operation '%s' must be in range %s" % (
                        name, self.qualified_name, value_range))

    def validate_output_values(self, output_values: Dict):
        """
        Validate given *output_values* against the operation's output properties.

        :param output_values: The dictionary of output values.
        :raise ValueError: If *output_values* are invalid w.r.t. to the operation's output properties.
        """
        outputs = self.output
        for name, value in output_values.items():
            if name not in outputs:
                raise ValueError("'%s' is not an output of operation '%s'" % (name, self.qualified_name))
            output_properties = outputs[name]
            if value is not None:
                data_type = output_properties.get('data_type', None)
                # TODO (forman, 20160907): perform more elaborated output type checking here
                if data_type and not isinstance(value, data_type):
                    raise ValueError(
                        "output '%s' for operation '%s' must be of type %s" % (
                            name, self.qualified_name, data_type))


class OpRegistration:
    """
    A registered operation comprises the actual operation object, which may be a class or any callable, and
    meta-information about the operation.

    :param operation: the actual class or any callable object.
    """

    def __init__(self, operation):
        self._op_meta_info = OpMetaInfo.introspect_operation(operation)
        self._operation = operation

    @property
    def op_meta_info(self):
        """
        :return: Meta-information about the operation, see :py:class:`ect.core.op.OpMetaInfo`.
        """
        return self._op_meta_info

    @property
    def operation(self):
        """
        :return: The actual operation object which may be a class or any callable.
        """
        return self._operation

    def __str__(self):
        return '%s: %s' % (self.operation, self.op_meta_info)

    def __call__(self, monitor: Monitor = Monitor.NULL, **input_values):
        """
        Perform this operation.

        :param monitor: an optional progress monitor, which is passed to the wrapped callable, if it supports it.
        :param input_values: the input values
        :return: the output value(s).
        """

        # set default_value where input values are missing
        self.op_meta_info.set_default_input_values(input_values)

        # validate the input_values using this operation's meta-info
        self.op_meta_info.validate_input_values(input_values)

        if self.op_meta_info.has_monitor:
            # set the monitor only if it is an argument
            input_values[self.op_meta_info.MONITOR_INPUT_NAME] = monitor

        operation = self.operation
        if isclass(operation):
            # create object instance
            operation_instance = operation()
            # call the instance
            return_value = operation_instance(**input_values)
        else:
            # call the function/method/callable/?
            return_value = operation(**input_values)

        if self.op_meta_info.has_named_outputs:
            # return_value is expected to be a dictionary-like object
            # set default_value where output values in return_value are missing
            for name, properties in self.op_meta_info.output.items():
                if name not in return_value or return_value[name] is None:
                    return_value[name] = properties.get('default_value', None)
            # validate the return_value using this operation's meta-info
            self.op_meta_info.validate_output_values(return_value)
        else:
            # return_value is a single value, not a dict
            # set default_value if return_value is missing
            if return_value is None:
                properties = self.op_meta_info.output[OpMetaInfo.RETURN_OUTPUT_NAME]
                return_value = properties.get('default_value', None)
            # validate the return_value using this operation's meta-info
            self.op_meta_info.validate_output_values({OpMetaInfo.RETURN_OUTPUT_NAME: return_value})
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
        Get all operation registrations of type :py:class:`ect.core.op.OpRegistration`.

        :return: a mapping of fully qualified operation names to operation registrations
        """
        return OrderedDict(sorted(self._op_registrations.items(), key=lambda name: name[0]))

    def add_op(self, operation, fail_if_exists=True) -> OpRegistration:
        """
        Add a new operation registration.

        :param operation: A operation object such as a class or any callable.
        :param fail_if_exists: raise ``ValueError`` if the operation was already registered
        :return: a :py:class:`ect.core.op.OpRegistration` object
        """
        op_qualified_name = object_to_qualified_name(operation)
        if op_qualified_name in self._op_registrations:
            if fail_if_exists:
                raise ValueError("operation with name '%s' already registered" % op_qualified_name)
            else:
                return self._op_registrations[op_qualified_name]
        op_registration = OpRegistration(operation)
        self._op_registrations[op_qualified_name] = op_registration
        return op_registration

    def remove_op(self, operation, fail_if_not_exists=False) -> OpRegistration:
        """
        Remove an operation registration.

        :param operation: A fully qualified operation name or operation object such as a class or any callable.
        :param fail_if_not_exists: raise ``ValueError`` if no such operation was found
        :return: the removed :py:class:`ect.core.op.OpRegistration` object or ``None``
                 if *fail_if_not_exists* is ``False``.
        """
        op_qualified_name = operation if isinstance(operation, str) else object_to_qualified_name(operation)
        if op_qualified_name not in self._op_registrations:
            if fail_if_not_exists:
                raise ValueError("operation with name '%s' not registered" % op_qualified_name)
            else:
                return None
        return self._op_registrations.pop(op_qualified_name)

    def get_op(self, operation, fail_if_not_exists=False) -> OpRegistration:
        """
        Get an operation registration.

        :param operation: A fully qualified operation name or operation object such as a class or any callable.
        :param fail_if_not_exists: raise ``ValueError`` if no such operation was found
        :return: a :py:class:`ect.core.op.OpRegistration` object or ``None`` if *fail_if_not_exists* is ``False``.
        """
        op_qualified_name = operation if isinstance(operation, str) else object_to_qualified_name(operation)
        op_registration = self._op_registrations.get(op_qualified_name, None)
        if op_registration is None and fail_if_not_exists:
            raise ValueError("operation with name '%s' not registered" % op_qualified_name)
        return op_registration


class _DefaultOpRegistry(OpRegistry):
    def __repr__(self):
        return 'OP_REGISTRY'


# check (nf) - for more flexibility, REGISTRY may be configured by dependency injection
# see Python libs 'pinject' (Google), 'inject', and others

#: The default operation registry of type :py:class:`ect.core.op.OpRegistry`.
OP_REGISTRY = _DefaultOpRegistry()


def op(registry=OP_REGISTRY, **properties):
    """
    ``op`` is a decorator function that registers a Python function or class in the default operation registry or
    the one given by *registry*, if any.
    Any other keywords arguments in *header* are added to the operation's meta-information header.
    Classes annotated by this decorator must have callable instances.

    When a function is registered, an introspection is performed. During this process, initial operation
    the meta-information header property *description* is derived from the function's docstring.

    :param properties: Other properties (keyword arguments) that will be added to the meta-information of operation.
    :param registry: The operation registry.
    """

    def decorator(func_or_class):
        op_registration = registry.add_op(func_or_class, fail_if_exists=False)
        header = op_registration.op_meta_info.header
        new_header = dict(**properties)
        _update_properties(header, new_header)
        return func_or_class

    return decorator


def op_input(input_name: str,
             default_value=None,
             position=None,
             data_type=None,
             value_set=None,
             value_range=None,
             registry=OP_REGISTRY,
             **properties):
    """
    ``op_input`` is a decorator function that provides meta-information for an operation input identified by
    *input_name*. If the decorated function or class is not registered as an operation yet, it is added to the default
    operation registry or the one given by *registry*, if any.

    When a function is registered, an introspection is performed. During this process, initial operation
    meta-information input properties are derived for each positional and keyword argument named *input_name*:

    ---------------- ------------------------------------------------------------------------------
    Derived property Source
    ---------------- ------------------------------------------------------------------------------
    *position*       The position of a positional argument, e.g. ``2`` for input ``z`` in
                     ``def f(x, y, z, c=2)``.
    *default_value*  The value of a keyword argument, e.g. ``52.3`` for input ``latitude``
                     from argument definition ``latitude:float=52.3``
    *data_type*      The type annotation type, e.g. ``float`` for input ``latitude``
                     from argument definition  ``latitude:float``
    ---------------- ------------------------------------------------------------------------------

    The derived properties listed above plus any of *value_set*, *value_range*, and any key-value pairs in *properties*
    are added to the input's meta-information.
    A key-value pair in *properties* will always overwrite the derived properties listed above.

    :param input_name: The name of an input.
    :param position: The position of a positional input (not supported yet).
    :param default_value: A default value.
    :param data_type: The data type of the input values.
    :param value_set: A sequence of the valid values. Note that all values in this sequence
                      must be compatible with *data_type*.
    :param value_range: A sequence specifying the possible range of valid values.
    :param properties: Other properties (keyword arguments) that will be added to the meta-information of the named output.
    :param registry: Optional operation registry.
    """

    def decorator(func_or_class):
        op_registration = registry.add_op(func_or_class, fail_if_exists=False)
        input_namespace = op_registration.op_meta_info.input
        if input_name not in input_namespace:
            input_namespace[input_name] = dict()
        input_properties = input_namespace[input_name]
        new_properties = dict(data_type=data_type,
                              default_value=default_value,
                              position=position,
                              value_set=value_set,
                              value_range=value_range, **properties)
        _update_properties(input_properties, new_properties)
        return func_or_class

    return decorator


def op_output(output_name: str,
              data_type=None,
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


    :param output_name: The name of the output.
    :param data_type: The data type of the output value.
    :param properties: Other properties (keyword arguments) that will be added to the meta-information of the named output.
    :param registry: Optional operation registry.
    """

    def decorator(func_or_class):
        op_registration = registry.add_op(func_or_class, fail_if_exists=False)
        output_namespace = op_registration.op_meta_info.output
        if not op_registration.op_meta_info.has_named_outputs:
            # if there is only one entry and it is the 'return' entry, rename it to value of output_name
            output_properties = output_namespace[OpMetaInfo.RETURN_OUTPUT_NAME]
            del output_namespace[OpMetaInfo.RETURN_OUTPUT_NAME]
            output_namespace[output_name] = output_properties
        elif output_name not in output_namespace:
            output_namespace[output_name] = dict()
        output_properties = output_namespace[output_name]
        new_properties = dict(data_type=data_type, **properties)
        _update_properties(output_properties, new_properties)
        return func_or_class

    return decorator


def op_return(data_type=None,
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

    :param data_type: The data type of the return value.
    :param properties: Other properties (keyword arguments) that will be added to the meta-information of the return value.
    :param registry: The operation registry.
    """
    return op_output(OpMetaInfo.RETURN_OUTPUT_NAME,
                     data_type=data_type,
                     registry=registry,
                     **properties)


def _update_properties(old_properties: dict, new_properties: dict):
    for name, value in new_properties.items():
        if value is not None and (name not in old_properties or old_properties[name] is None):
            old_properties[name] = value


def parse_op_args(raw_args: List[str], namespace: dict = None,
                  ignore_eval_errors: bool = True) -> Tuple[List, OrderedDict]:
    """
    Convert a raw argument list *raw_args* into a (args, kwargs) tuple whose elements are converted Python objects.
    All elements of the raw argument list *raw_args* are expected to be textual values of either the form
    "value" (positional argument) or "name=value" (keyword argument).

    Each text value is converted into a Python object using the Python interpreter's ``eval`` function and using the
    provided *namespace* as local execution environment. If ``eval`` fails, *value* will be left unchanged if
    *ignore_eval_errors* is ``True``, which means *value* remains a textual value (Python type ``str``). Otherwise
    a ``ValueError`` is thrown.

    :param raw_args: raw argument list of string elements
    :param namespace: the namespace to be used when converting the raw text values into Python objects.
    :param ignore_eval_errors: if ``True``, ``eval`` failures will be ignored
    :return: a pair comprising the list of positional arguments and a dictionary holding the keyword arguments
    :raise ValueError: if the parsing fails
    """
    op_args = []
    op_kwargs = OrderedDict()
    for raw_arg in raw_args:
        name_and_value = raw_arg.split('=', maxsplit=1)
        if len(name_and_value) == 2:
            name, raw_value = name_and_value
            if not name:
                raise ValueError("missing input name")
            if not name.isidentifier():
                raise ValueError("'%s' is not a valid input name" % name)
        else:
            name = None
            raw_value = raw_arg

        # noinspection PyBroadException
        try:
            # try converting arg into a Python object using the given namespace
            value = eval(raw_value, None, namespace)
        except Exception as e:
            # import sys
            # print('Failed to convert "%s": %s' % (raw_value, e), flush=True, file=sys.stderr)
            if ignore_eval_errors:
                value = raw_value
            else:
                import sys
                _, _, traceback = sys.exc_info()
                raise ValueError('failed to evaluate expression "%s"' % raw_value).with_traceback(traceback)
        if not name:
            op_args.append(value)
        else:
            op_kwargs[name] = value

    return op_args, op_kwargs
