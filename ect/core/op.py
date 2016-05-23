"""
Module Description
==================

This modules provides classes and functions allowing to maintain *operations*.

Design targets:

* Simplicity - exploit Python language to let users express an operation in an intuitive form.
* Stay with Python base types instead og introducing a number of new data structures.
* Derive meta information such as names, types and documentation for the operation, its inputs, and its outputs from Python code
* An operation should be able to explain itself when used in a REPL in terms of its algorithms, its inputs, and its outputs.
* Three simple class annotations shall be used to decorate operations classes: an optional ``operation`` decorator, one or more ``input``, ``output`` decorators.
* Operation registration is done by operation class annotations.
* It shall be possible to register any Python-callable of the from ``op(*args, **kwargs)`` as an operation.
* Initial operation meta information will be derived from Python code introspection
* Operations should take an optional *monitor* which will be passed by the framework to observe the progress and to cancel an operation


Module Reference
================
"""

from collections import OrderedDict
from inspect import isclass
from typing import Dict

from .monitor import Monitor
from .util import object_to_qualified_name


class OpMetaInfo:
    """
    Meta-information about an operation.

    :param op_qualified_name: The operation's qualified Python name.
    """

    def __init__(self, op_qualified_name):
        self._qualified_name = op_qualified_name
        self._attributes = dict()
        self._inputs = OrderedDict()
        self._outputs = OrderedDict()

    @property
    def qualified_name(self):
        """
        :return: Fully qualified name of the actual operation.
        """
        return self._qualified_name

    @property
    def attributes(self):
        """
        :return: Operation attributes.
        """
        return self._attributes

    @property
    def inputs(self) -> OrderedDict:
        """
        Mapping from an input name to a dictionary of properties describing the input.

        :return: Named input slots.
        """
        return self._inputs

    @property
    def outputs(self):
        """
        Mapping from an output name to a dictionary of properties describing the output.

        :return: Named input slots.
        """
        return self._outputs

    def __str__(self):
        return '%s%s -> %s' % (self.qualified_name,
                               tuple([name + ':' + properties['data_type']
                                      for name, properties in self.inputs.items()]),
                               [name + ':' + properties['data_type']
                                for name, properties in self.outputs.items()])


class OpRegistration:
    """
    A registered operation comprises the actual operation object, which may be a class or any callable, and
    meta-information about the operation.

    :param operation: the actual class or any callable object.
    """

    def __init__(self, operation):
        self._meta_info = OpRegistration._introspect_operation(operation)
        self._operation = operation

    @property
    def meta_info(self):
        """
        :return: Meta-information about the operation, see :py:class:`ect.core.op.OpMetaInfo`.
        """
        return self._meta_info

    @property
    def operation(self):
        """
        :return: The actual operation object which may be a class or any callable.
        """
        return self._operation

    def __str__(self):
        return '%s: %s' % (self.operation, self.meta_info)

    @staticmethod
    def _introspect_operation(operation) -> OpMetaInfo:
        if not operation:
            raise ValueError('operation object must be given')
        op_qualified_name = object_to_qualified_name(operation, fail=True)
        meta_info = OpMetaInfo(op_qualified_name)
        # Introspect the operation instance (see https://docs.python.org/3.5/library/inspect.html)
        if hasattr(operation, '__doc__'):
            # documentation string
            meta_info.attributes['description'] = operation.__doc__
        if hasattr(operation, '__code__'):
            OpRegistration._introspect_function(meta_info, operation, False)
        if isclass(operation):
            if hasattr(operation, '__call__'):
                call_method = getattr(operation, '__call__')
                OpRegistration._introspect_function(meta_info, call_method, True)
            else:
                raise ValueError('operations of type class must define a __call__(self, ...) method')
        if hasattr(operation, '__annotations__'):
            # mapping of parameters names to annotations; "return" key is reserved for return annotations.
            annotations = operation.__annotations__
            for name, value in annotations.items():
                if name != 'return':
                    meta_info.inputs[name]['data_type'] = value
                else:
                    meta_info.outputs[name] = dict(data_type=value)
        if len(meta_info.outputs) == 0:
            meta_info.outputs['return'] = dict()
        return meta_info

    @staticmethod
    def _introspect_function(meta_info, operation, is_method):
        # code object containing compiled function bytecode
        if not hasattr(operation, '__code__'):
            # Check: throw exception here?
            return
        code = operation.__code__
        # number of arguments (not including * or ** args)
        arg_count = code.co_argcount
        # tuple of names of arguments and local variables
        arg_names = code.co_varnames[0:arg_count]
        if len(arg_names)>0 and is_method and arg_names[0] == 'self':
            arg_names = arg_names[1:]
            arg_count -= 1
        # Reserve input slots for all arguments
        for arg_name in arg_names:
            meta_info.inputs[arg_name] = dict()
        # Set 'default_value' for inputs
        if operation.__defaults__:
            # tuple of any default values for positional or keyword parameters
            default_values = operation.__defaults__
            num_default_values = len(default_values)
            for i in range(num_default_values):
                arg_name = arg_names[i - num_default_values]
                meta_info.inputs[arg_name]['default_value'] = default_values[i]

    def __call__(self, monitor: Monitor = Monitor.NULL, **input_values):
        """
        Performs this operation.

        :param monitor: an optional progress monitor, which is passed to the wrapped callable, if it supports it.
        :param input_values: the operations's input values
        :return: a dictionary that maps output names to their values.
        """

        # set default_value where input values are missing
        for name, properties in self.meta_info.inputs.items():
            if name not in input_values or input_values[name] is None:
                input_values[name] = properties.get('default_value', None)

        # validate the input_values using this operation's meta-info
        self.validate_input_values(input_values)

        if 'monitor' in self.meta_info.inputs:
            # set the monitor only if it is an argument
            input_values['monitor'] = monitor

        operation = self.operation
        if isclass(operation):
            # create object instance
            operation_instance = operation()
            # call the instance
            return_value = operation_instance(**input_values)
        else:
            # call the function/method/callable/?
            return_value = operation(**input_values)

        is_scalar_output = len(self.meta_info.outputs) == 1 and 'return' in self.meta_info.outputs
        if is_scalar_output:
            # set default_value where output values are missing
            if return_value is None:
                properties = self.meta_info.outputs['return']
                return_value = properties.get('default_value', None)
            # validate the return_value using this operation's meta-info
            self.validate_output_values({'return': return_value})
        else:
            # set default_value where output values are missing
            for name, properties in self.meta_info.outputs.items():
                if name not in return_value or return_value[name] is None:
                    return_value[name] = properties.get('default_value', None)
            # validate the return_value using this operation's meta-info
            self.validate_output_values(return_value)
        return return_value

    def validate_input_values(self, input_values: Dict):
        inputs = self.meta_info.inputs
        for name, value in input_values.items():
            if name not in inputs:
                raise ValueError("'%s' is not an input of operation '%s'" % (name, self.meta_info.qualified_name))
            input_properties = inputs[name]
            # todo - we may construct a list a validator objects from input_properties and use these instead
            if 'not_none' in input_properties and input_properties['not_none'] and value is None:
                raise ValueError(
                    "input '%s' for operation '%s' must not be None" % (name, self.meta_info.qualified_name))
                # todo - check value against other constraints given by input_properties
                # todo - check if there are mandatory inputs not given in input_values

    def validate_output_values(self, output_values: Dict):
        outputs = self.meta_info.outputs
        for name, value in output_values.items():
            if name not in outputs:
                raise ValueError("'%s' is not an output of operation '%s'" % (name, self.meta_info.qualified_name))
            output_properties = outputs[name]
            # todo - we may construct a list a validator objects from input_properties and use these instead
            if 'not_none' in output_properties and output_properties['not_none'] and value is None:
                raise ValueError(
                    "output '%s' for operation '%s' must not be None" % (name, self.meta_info.qualified_name))
                # todo - check value against other constraints given by input_properties
                # todo - check if there are mandatory inputs not given in input_values


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

        :param operation: A fully qualified operation name or registered operation object such as a class or callable.
        :param fail_if_not_exists: raise ``ValueError`` if no such operation was found
        :return: the removed :py:class:`ect.core.op.OpRegistration` object or ``None`` if *fail_if_not_exists* is ``False``.
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

        :param operation: A fully qualified operation name or registered operation object such as a class or callable.
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
        return 'REGISTRY'


#: The default operation registry of type :py:class:`ect.core.op.OpRegistry`.
REGISTRY = _DefaultOpRegistry()


def op(registry=REGISTRY):
    """
    Classes or functions annotated by this decorator are added to the given *registry*.
    Classes annotated by this decorator must have callable instances. Callable instances
    and functions must have the following signature:

        operation(**input_values) -> dict

    :param registry: The operation registry.
    """

    def _op(operation):
        registry.add_op(operation, fail_if_exists=False)
        return operation

    return _op


def op_input(input_name: str,
             default_value=None,
             not_none=None,
             data_type=None,
             value_set=None,
             value_range=None,
             registry=REGISTRY,
             **kwargs):
    """
    Classes or functions annotated by this decorator are added the given *registry* (if not already done)
    and are assigned a new input with the given *input_name*.

    :param input_name: The name of an input.
    :param not_none: If ``True``, value must not be ``None``.
    :param default_value: A default value.
    :param data_type: The data type of the input values.
    :param value_set: A sequence of the valid values. Note that all values in this sequence must be compatible with *data_type*.
    :param value_range: A sequence specifying the possible range of valid values.
    :param registry: The operation registry.
    """

    def _op_input(operation):
        op_registration = registry.add_op(operation, fail_if_exists=False)
        inputs = op_registration.meta_info.inputs
        if input_name not in inputs:
            inputs[input_name] = dict()
        input_properties = inputs[input_name]
        new_properties = dict(data_type=data_type,
                              default_value=default_value,
                              not_none=not_none,
                              value_set=value_set,
                              value_range=value_range, **kwargs)
        _update_dict(input_properties, new_properties)
        return operation

    return _op_input


def op_output(output_name: str,
              data_type=None,
              not_none=None,
              value_set=None,
              value_range=None,
              registry=REGISTRY,
              **kwargs):
    """
    Classes or functions annotated by this decorator are added the given *registry* (if not already done)
    and are assigned a new output with the given *output_name*.

    :param output_name: The name of the output.
    :param not_none: If ``True``, value must not be ``None``.
    :param data_type: The data type of the output value.
    :param value_set: A sequence of the valid values. Note that all values in this sequence must be compatible with *data_type*.
    :param value_range: A sequence specifying the possible range of valid values.
    :param registry: The operation registry.
    """

    def _op_output(operation):
        op_registration = registry.add_op(operation, fail_if_exists=False)
        outputs = op_registration.meta_info.outputs
        if len(outputs) == 1 and 'return' in outputs:
            # if there is only one entry and it is the 'return' entry, rename it to value of output_name
            output_properties = outputs.pop('return')
            outputs[output_name] = output_properties
        elif output_name not in outputs:
            outputs[output_name] = dict()
        output_properties = outputs[output_name]
        new_properties = dict(data_type=data_type,
                              not_none=not_none,
                              value_set=value_set,
                              value_range=value_range,
                              **kwargs)
        _update_dict(output_properties, new_properties)
        return operation

    return _op_output


def op_return(data_type=None,
              not_none=None,
              value_set=None,
              value_range=None,
              registry=REGISTRY,
              **kwargs):
    """
    Classes or functions annotated by this decorator are added the given *registry* (if not already done)
    and are assigned a new, single output with the name ``return``.

    :param not_none: If ``True``, value must not be ``None``.
    :param data_type: The data type of the output value.
    :param value_set: A sequence of the valid values. Note that all values in this sequence must be compatible with *data_type*.
    :param value_range: A sequence specifying the possible range of valid values.
    :param registry: The operation registry.
    """
    return op_output('return',
                     data_type=data_type,
                     not_none=not_none,
                     value_set=value_set,
                     value_range=value_range,
                     registry=registry,
                     **kwargs)


def _update_dict(old_properties, new_properties):
    for name, value in new_properties.items():
        if value is not None and (name not in old_properties or old_properties[name] is None):
            old_properties[name] = value
