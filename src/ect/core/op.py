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
* Operation meta information


"""

from abc import ABCMeta, abstractmethod
from collections import OrderedDict
from typing import Dict

from .monitor import Monitor

class OperationMetaInfo:
    def __init__(self, callable_element):
        self._callable_element = callable_element
        self._inputs = OrderedDict()
        self._outputs = OrderedDict()

    @property
    def callable_element(self):
        return self._callable_element

    def add_input(self, name, attributes):
        self._inputs[name] = attributes


_ATTR_NAME_OP_META_INFO = '__op_meta_info__'
_ATTR_NAME_OP_INPUTS = '__op_inputs__'
_ATTR_NAME_OP_OUTPUTS = '__op_outputs__'

Type = type(str)


def operation(name=None):
    """
    Class decorator that registrates a class as an operation. Such classes should derive from **Op** or implement
    an equivalent protocol.
    """

    def _operation(op_class):
        return Op.register_op_class(op_class, name=name)

    return _operation


# noinspection PyShadowingBuiltins
def input(name: str,
          default_value=None,
          value_type: Type = object,
          value_set=None,
          value_range=None):
    """
    Class decorator that assigns the 'input' role to an attribute or property of an operation class.

    :param name: The name of an attribute or property of the decorated Operator class.
    :param default_value: A default value.
    :param value_type: The type of the input values.
    :param value_set: A sequence of the valid values. Note that all values in this sequence must be compatible with
                  ``value_type``.
    :param value_range: A sequence specifying the possible range of valid values.
    """

    def _input(cls):
        Op.register_op_class(cls)
        inputs = cls.get_inputs()
        assert inputs is not None
        inputs[name] = dict(value_type=value_type,
                            value_set=value_set,
                            value_range=value_range,
                            default_value=default_value)
        return cls

    return _input


def output(attr_name: str,
           default_value=None,
           value_type: Type = object,
           value_set=None,
           value_range=None):
    """
    Class decorator that assigns the 'output' role to an attribute or property of an operation class.

    :param attr_name: The name of an attribute or property of the decorated Operator class.
    :param default_value: A default value.
    :param value_type: The type of the input values.
    :param value_set: A sequence of the valid values. Note that all values in this sequence must be compatible with
                  ``value_type``.
    :param value_range: A sequence specifying the possible range of valid values.
    """

    def _output(cls):
        Op.register_op_class(cls)
        outputs = cls.get_outputs()
        assert outputs is not None
        outputs[attr_name] = dict(value_type=value_type,
                                  value_set=value_set,
                                  value_range=value_range,
                                  default_value=default_value)
        return cls

    return _output


class Op(metaclass=ABCMeta):
    """
    Base class for all operations.
    """

    _OP_CLASS_REGISTRY = OrderedDict()

    def __init__(self):
        inputs = type(self).get_inputs()
        assert inputs is not None
        outputs = type(self).get_outputs()
        assert outputs is not None
        for name, attributes in inputs.items():
            setattr(self, name, attributes.get('default_value', None))
        for name, attributes in outputs.items():
            setattr(self, name, attributes.get('default_value', None))

    def get_output_values(self) -> Dict:
        """
        Get the output values.
        :return: The output values as a dictionary of name/value pairs.
        """
        outputs = self.__class__.get_outputs(deep=True)
        output_values = OrderedDict()
        for name in outputs.keys():
            output_values[name] = getattr(self, name)
        return output_values

    def set_input_values(self, **input_values):
        """
        Set the input values.
        :param input_values: The input values as name/value pairs.
        """
        op_class = self.__class__
        inputs = op_class.get_inputs(deep=True)
        for name, value in input_values.items():
            if name not in inputs:
                raise ValueError('%s is not an input of operation %s' % (name, op_class.get_name()))
            attributes = inputs[name]
            # todo - check value against constraints given by attributes
            setattr(self, name, value)

    # noinspection PyMethodMayBeStatic
    def invoke(self, monitor: Monitor = Monitor.NULL, **input_values):
        """
        Invokes the operation. Three steps are performed:
        1. *set_input_values* is called using ``input_values``
        2. *perform* is called using the ``monitor``
        3. The value of *get_output_values* is retured.

        :param monitor: A progress monitor used to observe and control the performed operation.
        :param input_values: The input values as name/value pairs.
        :return: The output values as a dictionary of name/value pairs.
        """
        self.set_input_values(**input_values)
        self.perform(monitor=monitor)
        return self.get_output_values()

    # noinspection PyMethodMayBeStatic,PyUnusedLocal
    def perform(self, monitor: Monitor = Monitor.NULL):
        """
        Performs the actual operation.
        Override to perform the operation.
        The default implementation does nothing.

        :param monitor: A progress monitor used to observe and control the performed operation.
        """
        pass

    @staticmethod
    def get_op_classes(copy: bool = False):
        return OrderedDict(Op._OP_CLASS_REGISTRY) if copy else Op._OP_CLASS_REGISTRY

    @staticmethod
    def register_op_class(op_class, **kwargs):
        qualified_name = op_class.__module__ + '.' + op_class.__name__
        if qualified_name not in Op._OP_CLASS_REGISTRY:
            Op._OP_CLASS_REGISTRY[qualified_name] = op_class

        if not hasattr(op_class, _ATTR_NAME_OP_META_INFO):
            op_meta_info = OrderedDict(kwargs)
            if op_meta_info.get('name', None) is None:
                op_meta_info['name'] = op_class.__name__
            if op_meta_info.get('qualified_name', None) is None:
                op_meta_info['qualified_name'] = qualified_name
            setattr(op_class, _ATTR_NAME_OP_META_INFO, op_meta_info)
        if not hasattr(op_class, _ATTR_NAME_OP_INPUTS):
            setattr(op_class, _ATTR_NAME_OP_INPUTS, OrderedDict())
        if not hasattr(op_class, _ATTR_NAME_OP_OUTPUTS):
            setattr(op_class, _ATTR_NAME_OP_OUTPUTS, OrderedDict())

        if kwargs:
            op_meta_info = getattr(op_class, _ATTR_NAME_OP_META_INFO)
            for k, v in kwargs.items():
                op_meta_info[k] = v

        return op_class

    @staticmethod
    def unregister_op_class(op_class):
        key = op_class.__module__ + '.' + op_class.__name__
        if key in Op._OP_CLASS_REGISTRY:
            del Op._OP_CLASS_REGISTRY[key]
        return op_class

    @classmethod
    def get_name(cls):
        return cls.get_op_meta_info()['name']

    @classmethod
    def get_qualified_name(cls):
        return cls.get_op_meta_info()['qualified_name']

    @classmethod
    def get_op_meta_info(cls, copy: bool = False, deep: bool = False):
        return cls._get_ordered_dict_dict_attr(_ATTR_NAME_OP_META_INFO, copy=copy, deep=deep)

    @classmethod
    def get_inputs(cls, copy: bool = False, deep: bool = False):
        return cls._get_ordered_dict_dict_attr(_ATTR_NAME_OP_INPUTS, copy=copy, deep=deep)

    @classmethod
    def register_input(cls, attr_name: str, **kwargs):
        """
        See **input** decorator.
        """
        input(attr_name, **kwargs)(cls)

    @classmethod
    def unregister_input(cls, attr_name: str):
        inputs = cls.get_inputs()
        if inputs is not None and attr_name in inputs:
            del inputs[attr_name]

    @classmethod
    def get_outputs(cls, copy=False, deep=False):
        return cls._get_ordered_dict_dict_attr(_ATTR_NAME_OP_OUTPUTS, copy=copy, deep=deep)

    @classmethod
    def register_output(cls, attr_name: str, **kwargs):
        """
        See **output** decorator.
        """
        output(attr_name, **kwargs)(cls)

    @classmethod
    def unregister_output(cls, attr_name: str):
        outputs = cls.get_outputs()
        if outputs is not None and attr_name in outputs:
            del outputs[attr_name]

    @classmethod
    def _get_ordered_dict_dict_attr(cls, attr_name: str, copy: bool = False, deep: bool = False):
        if deep:
            pass
            # todo - collect outputs of superclass
        if hasattr(cls, attr_name):
            value = getattr(cls, attr_name)
            return OrderedDict(value) if copy else value
        return None

    def __str__(self):
        cls = self.__class__
        return '%s(%s) ==> %s' % (cls.get_name(),
                                  [name for name in cls.get_inputs().keys()],
                                  [name for name in cls.get_outputs().keys()])
