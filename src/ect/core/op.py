"""

Defines the Op class which represents an arbitrary operation given some inputs and outputs.

Design targets:

* Simplicity - exploit Python language to let users express an operation in an intuitive form.
* Stay with Python base types instead og introducing a number of new data structures.
* Derive meta information such as names, types and documentation for the operation, its inputs, and its outputs
  from Python code
* An operation should be able to explain itself when used in a REPL in terms of its algorithms, its inputs, and
  its outputs.
* Three simple class annotations shall be used to decorate operations classes: an optional ``operation`` decorator,
  one or more ``input``, ``output`` decorators.
* Operation registration is done by operation class annotations.
* Meta information shall be stores in an operation's *class* definition, not in the operation *instance*.
* Any compatible Python-callable of the from op(*args, **kwargs) -> dict() shall be considered an operation.
* Initial operation meta information will be derived from Python code introspection


"""

from collections import OrderedDict
from inspect import isclass
from typing import Dict

from .monitor import Monitor


class OpMetaInfo:
    """
    Operation meta-information.

    :param op_qualified_name: The operation's qualified Python name.
    """

    def __init__(self, op_qualified_name):
        self._qualified_name = op_qualified_name
        self._attributes = dict()
        self._inputs = OrderedDict()
        self._outputs = OrderedDict()

    @property
    def qualified_name(self):
        return self._qualified_name

    @property
    def attributes(self):
        return self._attributes

    @property
    def inputs(self):
        return self._inputs

    @property
    def outputs(self):
        return self._outputs

    def __str__(self):
        return '%s%s -> %s' % (self.qualified_name,
                               tuple([name + ':' + properties['data_type']
                                      for name, properties in self.inputs.items()]),
                               [name + ':' + properties['data_type']
                                for name, properties in self.outputs.items()])


class OpRegistration:
    def __init__(self, operation):
        self._meta_info = OpRegistration._introspect_operation(operation)
        self._operation = operation

    @property
    def meta_info(self):
        return self._meta_info

    @property
    def operation(self):
        return self._operation

    def __str__(self):
        return '%s: %s' % (self.operation, self.meta_info)

    @staticmethod
    def _introspect_operation(operation) -> OpMetaInfo:
        if not operation:
            raise ValueError('operation object must be given')
        if not hasattr(operation, '__qualname__'):
            raise ValueError("missing '__qualname__' attribute in operation object")
        op_qualified_name = operation.__qualname__
        meta_info = OpMetaInfo(op_qualified_name)
        # Introspect the operation instance (see https://docs.python.org/3.5/library/inspect.html)
        if hasattr(operation, '__doc__'):
            # documentation string
            meta_info.attributes['description'] = operation.__doc__
        if hasattr(operation, '__code__'):
            # code object containing compiled function bytecode
            code = operation.__code__
            # number of arguments (not including * or ** args)
            arg_count = code.co_argcount
            # tuple of names of arguments and local variables
            var_names = code.co_varnames
            if len(var_names) > 0 and var_names[0] == 'self':
                arg_names = var_names[0:arg_count]
            else:
                arg_names = var_names
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
        if hasattr(operation, '__annotations__'):
            # mapping of parameters names to annotations; "return" key is reserved for return annotations.
            annotations = operation.__annotations__
            for name, value in annotations.items():
                if name != 'return':
                    meta_info.inputs[name]['data_type'] = value
                else:
                    meta_info.outputs[name] = dict(data_type=value)
        return meta_info

    # todo - write tests!
    def invoke(self, monitor: Monitor = Monitor.NULL, **input_values):

        # set default_value where input values are missing
        for name, properties in self.meta_info.inputs:
            if name not in input_values or input_values[name] is None:
                input_values[name] = properties.get('default_value', None)

        # validate the input_values using this operation's meta-info
        self.validate_input_values(input_values)

        operation = self.operation
        if isclass(operation):
            # create object instance
            operation_instance = operation()
            # inject input_values
            for name, value in input_values:
                setattr(operation_instance, name, value)
            # call the instance
            operation_instance(monitor=monitor)
            # extract output_values
            output_values = dict()
            for name, value in input_values:
                output_values[name] = getattr(operation_instance, name, None)
        else:
            # call the function/method/callable/?
            return_value = operation(monitor=monitor, **input_values)
            output_values = dict(return_value=return_value)

        # set default_value where output values are missing
        for name, properties in self.meta_info.outputs:
            if name not in output_values or output_values[name] is None:
                output_values[name] = properties.get('default_value', None)

        # validate the output_values using this operation's meta-info
        self.validate_output_values(output_values)

        return output_values

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
    _REGISTRATIONS = OrderedDict()

    @staticmethod
    def get_op(op_qualified_name, fail_if_not_exists=False) -> OpRegistration:
        op_registration = OpRegistry._REGISTRATIONS.get(op_qualified_name, None)
        if op_registration is None and fail_if_not_exists:
            raise ValueError("operation with name '%s' not registered" % op_qualified_name)
        return op_registration

    @staticmethod
    def add_op(operation, fail_if_exists=True) -> OpRegistration:
        op_qualified_name = operation.__qualname__
        if op_qualified_name in OpRegistry._REGISTRATIONS:
            if fail_if_exists:
                raise ValueError("operation with name '%s' already registered" % op_qualified_name)
            else:
                return OpRegistry._REGISTRATIONS[op_qualified_name]
        op_registration = OpRegistration(operation)
        OpRegistry._REGISTRATIONS[op_qualified_name] = op_registration
        return op_registration

    @staticmethod
    def remove_op(operation, fail_if_not_exists=False) -> OpRegistration:
        op_qualified_name = operation.__qualname__
        if op_qualified_name not in OpRegistry._REGISTRATIONS:
            if fail_if_not_exists:
                raise ValueError("operation with name '%s' not registered" % op_qualified_name)
            else:
                return None
        return OpRegistry._REGISTRATIONS.pop(op_qualified_name)


def add_op(operation, fail_if_exists=True):
    return OpRegistry.add_op(operation, fail_if_exists=fail_if_exists)


def remove_op(operation, fail_if_not_exists=False):
    return OpRegistry.remove_op(operation, fail_if_not_exists=fail_if_not_exists)


def get_op(op_qualified_name, fail_if_not_exists=False):
    return OpRegistry.get_op(op_qualified_name, fail_if_not_exists=fail_if_not_exists)


def op():
    """
    Classes or functions annotated by this decorator are added to the ``OpRegistry``.
    Classes annotated by this decorator must have callable instances. Callable instances
    and functions must have the following signature:

        operation(**input_values) -> dict

    """

    def _op(operation):
        OpRegistry.add_op(operation, fail_if_exists=True)
        return operation

    return _op


def op_input(input_name: str,
             default_value=None,
             not_none=None,
             data_type=None,
             value_set=None,
             value_range=None,
             **kwargs):
    """
    Classes or functions annotated by this decorator are added to the ``OpRegistry`` (if not already done)
    and are assigned a new input slot with the given name.

    :param input_name: The name of an input slot.
    :param not_none: If ``True``, value must not be ``None``.
    :param default_value: A default value.
    :param data_type: The data type of the input values.
    :param value_set: A sequence of the valid values. Note that all values in this sequence must be compatible with
                  ``value_type``.
    :param value_range: A sequence specifying the possible range of valid values.
    """

    def _op_input(operation):
        op_registration = OpRegistry.add_op(operation, fail_if_exists=False)
        inputs = op_registration.meta_info.inputs
        if input_name in inputs:
            old_properties = inputs[input_name]
        else:
            old_properties = dict()
            inputs[input_name] = old_properties
        new_properties = dict(data_type=data_type,
                              default_value=default_value,
                              not_none=not_none,
                              value_set=value_set,
                              value_range=value_range, **kwargs)
        _update_dict(old_properties, new_properties)
        return operation

    return _op_input


def op_output(output_name: str,
              data_type=None,
              not_none=None,
              value_set=None,
              value_range=None,
              **kwargs):
    """
    Class decorator that describes an 'output' slot to an operation.

    :param output_name: The name of the output slot.
    :param not_none: If ``True``, value must not be ``None``.
    :param data_type: The data type of the output value.
    :param value_set: A sequence of the valid values. Note that all values in this sequence must be compatible with
                  ``value_type``.
    :param value_range: A sequence specifying the possible range of valid values.
    """

    def _op_output(operation):
        op_registration = OpRegistry.add_op(operation, fail_if_exists=False)
        outputs = op_registration.meta_info.outputs
        if output_name in outputs:
            old_properties = outputs[output_name]
        else:
            old_properties = dict()
            outputs[output_name] = old_properties
        new_properties = dict(data_type=data_type,
                              not_none=not_none,
                              value_set=value_set,
                              value_range=value_range,
                              **kwargs)
        _update_dict(old_properties, new_properties)
        return operation

    return _op_output


def op_return(data_type=None,
              not_none=None,
              value_set=None,
              value_range=None,
              **kwargs):
    """
    Class decorator that describes the one and only 'output' slot of an operation that has a single return value.

    :param not_none: If ``True``, value must not be ``None``.
    :param data_type: The data type of the output value.
    :param value_set: A sequence of the valid values. Note that all values in this sequence must be compatible with
                  ``value_type``.
    :param value_range: A sequence specifying the possible range of valid values.
    """
    return op_output('return',
                     data_type=data_type,
                     not_none=not_none,
                     value_set=value_set,
                     value_range=value_range,
                     **kwargs)


def _update_dict(old_properties, new_properties):
    for name, value in new_properties.items():
        if value is not None and (name not in old_properties or old_properties[name] is None):
            old_properties[name] = value
