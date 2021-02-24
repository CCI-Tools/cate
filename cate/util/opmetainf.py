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

import re
from collections import OrderedDict
from typing import Tuple, Dict, List, Any, Optional

from .misc import object_to_qualified_name, qualified_name_to_object

Props = Dict[str, Any]

_SPHINX_PARAM_DIRECTIVE_PATTERN = re.compile(":param (?P<name>[^:]+): (?P<desc>[^:]+)")
_SPHINX_RETURN_DIRECTIVE_PATTERN = re.compile(":returns?: (?P<desc>[^:]+)")

_TYPING_PREFIX = 'typing.'


class OpMetaInfo:
    """
    Represents meta-information about an operation:

    * :py:attr:`qualified_name`: a an ideally unique, qualified operation name
    * :py:attr:`header`: dictionary of arbitrary operation attributes
    * :py:attr:`input`: ordered dictionary of named inputs,
      each mapping to a dictionary of arbitrary input attributes
    * :py:attr:`output`: ordered dictionary of named outputs,
      each mapping to a dictionary of arbitrary output attributes

    Warning: `OpMetaInfo`` objects should be considered immutable. However, the dictionaries mentioned above
    are returned "as-is", mostly for performance reasons. Changing entries in these dictionaries directly
    may cause unwanted side-effects.

    :param qualified_name: The operation's qualified name.
    :param has_monitor: Whether the operation supports a :py:class:`Monitor` keyword argument named ``monitor``.
    :param header: Header information dictionary.
    :param input_names: Input information dictionary.
    :param inputs: Input information dictionary.
    :param outputs: Output information dictionary.
    """

    def __init__(self,
                 qualified_name: str,
                 has_monitor: bool = False,
                 header: dict = None,
                 input_names: List[str] = None,
                 inputs: Dict[str, Props] = None,
                 outputs: Dict[str, Props] = None):
        if not qualified_name:
            raise ValueError("qualified_name must be given")
        self._qualified_name = qualified_name
        self._has_monitor = True if has_monitor else False
        self._header = header if header else dict()
        self._inputs = OrderedDict(inputs if inputs else {})
        self._outputs = OrderedDict(outputs if outputs else {})
        self._input_names = input_names or self._get_input_names(self._inputs)

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
    def header(self) -> Dict[str, Any]:
        """
        :return: Operation header attributes.
        """
        return self._header

    @property
    def input_names(self) -> List[str]:
        """
        The input names in the order they have been declared.

        :return: List of input names.
        """
        return self._input_names

    @property
    def inputs(self) -> Dict[str, Props]:
        """
        Mapping from an input name to a dictionary of properties describing the input.

        :return: Named inputs.
        """
        return self._inputs

    @property
    def outputs(self) -> Dict[str, Props]:
        """
        Mapping from an output name to a dictionary of properties describing the output.

        :return: Named outputs.
        """
        return self._outputs

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
        return not (len(self._outputs) == 1 and self.RETURN_OUTPUT_NAME in self._outputs)

    @property
    def can_cache(self) -> bool:
        return not self._header.get('no_cache', False)

    def to_json_dict(self, data_type_to_json=None) -> Dict[str, Any]:
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
        json_dict['inputs'] = self.object_dict_to_json_dict(self.inputs, data_type_to_json)
        json_dict['outputs'] = self.object_dict_to_json_dict(self.outputs, data_type_to_json)
        return json_dict

    @classmethod
    def from_json_dict(cls, json_dict: Dict[str, Any], json_to_data_type=None, **kwargs) -> 'OpMetaInfo':
        qualified_name = json_dict.get('qualified_name', kwargs.get('qualified_name', None))
        header_obj = json_dict.get('header', kwargs.get('header', None))
        has_monitor = json_dict.get('has_monitor', kwargs.get('has_monitor', False))
        input_dict = json_dict.get('inputs', kwargs.get('inputs', None))
        output_dict = json_dict.get('outputs', kwargs.get('outputs', None))
        return OpMetaInfo(qualified_name,
                          header=header_obj,
                          has_monitor=has_monitor,
                          inputs=cls.json_dict_to_object_dict(input_dict, json_to_data_type),
                          outputs=cls.json_dict_to_object_dict(output_dict, json_to_data_type))

    @classmethod
    def object_dict_to_json_dict(cls, obj_dict, data_type_to_json=None):
        if not data_type_to_json:
            data_type_to_json = object_to_qualified_name
        json_dict = OrderedDict()
        for name, properties in obj_dict.items():
            json_dict[name] = dict(properties)
            if 'data_type' in properties:
                json_dict[name]['data_type'] = data_type_to_json(properties['data_type'])
        return json_dict

    @classmethod
    def json_dict_to_object_dict(cls, json_dict, json_to_data_type=None):
        if not json_to_data_type:
            json_to_data_type = qualified_name_to_object
        obj_dict = OrderedDict()
        for name, properties in json_dict.items():
            obj_dict[name] = dict(properties)
            if 'data_type' in properties:
                obj_dict[name]['data_type'] = json_to_data_type(properties['data_type'])
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

        arg_inputs, input_dict, has_monitor = [], OrderedDict(), False
        if hasattr(operation, '__code__'):
            arg_inputs, input_dict, has_monitor = cls._introspect_inputs_from_callable(operation, False)

        return_name = OpMetaInfo.RETURN_OUTPUT_NAME

        output_dict = OrderedDict()
        if hasattr(operation, '__annotations__'):
            # mapping of parameters names to annotations; 'return' key is reserved for return annotations.
            annotations = operation.__annotations__
            for annotated_name, annotated_type in annotations.items():
                if annotated_name == 'return':
                    # op_meta_info.output can't be present so far -> assign new dict
                    output_dict[return_name] = dict(data_type=annotated_type)
                elif annotated_name != cls.MONITOR_INPUT_NAME:
                    # input_dict[annotated_name] should be present through _introspect_inputs_from_callable() call
                    input_dict[annotated_name]['data_type'] = annotated_type
        if len(output_dict) == 0:
            output_dict[return_name] = dict()

        header = dict()
        # Introspect the operation instance (see https://docs.python.org/3.5/library/inspect.html)
        if hasattr(operation, '__doc__'):
            # documentation string
            docstring = operation.__doc__
            if docstring:
                description, param_descriptions, return_description = cls._parse_docstring(docstring)
                if description:
                    header['description'] = description
                if param_descriptions:
                    for param_name, param_description in param_descriptions.items():
                        if param_name in input_dict:
                            input_dict[param_name]['description'] = param_descriptions[param_name]
                if return_description and return_name in output_dict:
                    output_dict[return_name]['description'] = return_description

        return OpMetaInfo(op_qualified_name,
                          header=header,
                          has_monitor=has_monitor,
                          inputs=input_dict,
                          outputs=output_dict)

    @classmethod
    def _introspect_inputs_from_callable(cls, operation, is_method: bool) -> \
            Tuple[List[Tuple[str, Props]], Props, bool]:
        arg_inputs = []
        input_dict = OrderedDict()
        has_monitor = False
        # code object containing compiled function bytecode
        if not hasattr(operation, '__code__'):
            # Check: throw exception here?
            return arg_inputs, input_dict, has_monitor
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
                input_props = dict(position=arg_pos)
                arg_inputs.append((arg_name, input_props))
                input_dict[arg_name] = input_props
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
                    if default_value is None:
                        input_dict[arg_name]['nullable'] = True
                    else:
                        input_dict[arg_name]['data_type'] = type(default_value)
        return arg_inputs, input_dict, has_monitor

    def set_default_input_values(self, input_values: Dict):
        """
        If any missing input value in *input_values*, set value of "default_value" property, if it exists.

        :param input_values: The dictionary of input values that will be modified.
        """
        for name, properties in self.inputs.items():
            if name not in input_values and 'default_value' in properties:
                input_values[name] = properties['default_value']

    def validate_input_values(self, input_values: Dict, except_types=None, validation_exception_class=ValueError):
        """
        Validate given *input_values* against the operation's input properties.

        :param input_values: The dictionary of input values.
        :param except_types: A set of types or ``None``. If an input value's type is in this set,
               it will not be validated against the various input properties,
               such as ``data_type``, ``nullable``, ``value_set``, ``value_range``.
        :param validation_exception_class: The exception class to be used to raise exceptions if
               validation fails. Must derive from ``BaseException``. Defaults to ``ValueError``.
        :raise validation_error_class: If *input_values* are invalid w.r.t. to the operation's input properties.
        """
        inputs = self.inputs
        # Ensure required input values have values (even None is a value).
        for name, properties in inputs.items():
            has_no_default = 'default_value' not in properties
            is_auto = 'context' in properties
            required = has_no_default and not is_auto
            if required and (name not in input_values):
                raise validation_exception_class("Input '%s' for operation '%s' must be given." %
                                                 (name, self.qualified_name))
        # Ensure all input values are valid w.r.t. input properties
        for name, value in input_values.items():
            if name not in inputs:
                raise validation_exception_class(
                    "'%s' is not an input of operation '%s'." % (name, self.qualified_name))
            if except_types and type(value) in except_types:
                continue
            input_properties = inputs[name]
            if input_properties.get('context'):
                # Context values will be set by framework
                continue
            data_type = input_properties.get('data_type')
            if value is None:
                default_is_none = input_properties.get('default_value', 1) is None
                value_set = input_properties.get('value_set')
                value_set_has_none = value_set and (None in value_set)
                nullable = input_properties.get('nullable', False)
                if not (default_is_none or value_set_has_none or nullable):
                    raise validation_exception_class(
                        "Input '%s' for operation '%s' must be given." % (name, self.qualified_name))
                continue
            if data_type:
                self._validate_value_against_data_type(data_type, value, self.qualified_name, "Input", name,
                                                       validation_exception_class)
            value_set = input_properties.get('value_set', None)
            if value_set and (value not in value_set):
                raise validation_exception_class(
                    "Input '%s' for operation '%s' must be one of %s." % (
                        name, self.qualified_name, value_set))
            value_range = input_properties.get('value_range', None)
            if value_range and (value is None or not (value_range[0] <= value <= value_range[1])):
                raise validation_exception_class(
                    "Input '%s' for operation '%s' must be in range %s." % (
                        name, self.qualified_name, value_range))

    def validate_output_values(self, output_values: Dict, validation_exception_class: type = ValueError):
        """
        Validate given *output_values* against the operation's output properties.

        :param output_values: The dictionary of output values.
        :param validation_exception_class: The exception class to be used to raise exceptions if
               validation fails. Must derive from ``BaseException``. Defaults to ``ValueError``.
        :raise validation_error_class: If *output_values* are invalid w.r.t. to the operation's output properties.
        """
        outputs = self.outputs
        for name, value in output_values.items():
            if name not in outputs:
                raise validation_exception_class(
                    "'%s' is not an output of operation '%s'." % (name, self.qualified_name))
            output_properties = outputs[name]
            if value is not None:
                data_type = output_properties.get('data_type', None)
                if data_type:
                    self._validate_value_against_data_type(data_type, value, self.qualified_name, "Output", name,
                                                           validation_exception_class)

    @classmethod
    def _validate_value_against_data_type(cls,
                                          data_type,
                                          value,
                                          op_name: str,
                                          port_type: str,
                                          port_name: str,
                                          validation_exception_class: type):
        try:
            value, can_convert = cls._convert_value(data_type, value)
        except (ValueError, validation_exception_class) as e:
            raise validation_exception_class(
                "%s '%s' for operation '%s': %s" % (port_type, port_name, op_name, str(e)))
        if not can_convert and value is not None:
            is_float_type = data_type is float and (isinstance(value, float) or isinstance(value, int))
            if not is_float_type and not is_instance_of(value, data_type):
                raise validation_exception_class(
                    "%s '%s' for operation '%s' must be of type '%s', but got type '%s'." % (
                        port_type, port_name, op_name, data_type.__name__, type(value).__name__))

    @classmethod
    def _convert_value(cls, data_type: Any, value: Optional[Any]) -> Tuple[Any, bool]:
        """Check if the given type has an "convert(value)" method, i.e. our XXXLike types, if so return its result."""
        # noinspection PyBroadException
        try:
            return data_type.convert(value), True
        except AttributeError:
            return value, False

    @classmethod
    def _parse_docstring(cls, docstring):
        lines = docstring.split('\n')
        description_lines = []
        directive_lines = []
        in_description = False
        in_directive = False
        param_descriptions = OrderedDict()
        return_descriptions = OrderedDict()
        for line in lines:
            line = line.strip()
            if line.startswith(':'):
                if in_description:
                    in_description = False
                elif in_directive:
                    cls._process_sphinx_directive_lines(directive_lines, param_descriptions, return_descriptions)
                directive_lines = [line]
                in_directive = True
            elif line:
                if in_description:
                    description_lines.append(line)
                elif in_directive:
                    directive_lines.append(line)
                else:
                    description_lines = [line]
                    in_description = True
            else:
                if in_description:
                    description_lines.append('')
                elif in_directive:
                    cls._process_sphinx_directive_lines(directive_lines, param_descriptions, return_descriptions)
                    directive_lines = []
                    in_directive = False

        if in_directive:
            cls._process_sphinx_directive_lines(directive_lines, param_descriptions, return_descriptions)

        return ('\n'.join(description_lines).strip(' \n\t') if description_lines else None,
                param_descriptions,
                return_descriptions.get('return', None))

    @classmethod
    def _process_sphinx_directive_lines(cls,
                                        directive_lines: List[str],
                                        param_descriptions: Dict[str, str],
                                        return_description: Dict[str, str]) -> None:
        # print("consume", annotation_lines)
        text = ' '.join(directive_lines).strip()
        matcher = _SPHINX_PARAM_DIRECTIVE_PATTERN.match(text)
        if matcher:
            name = matcher.group('name')
            text = cls._strip_sphinx_directive_text(matcher.group('desc'))
            param_descriptions[name] = text
            return

        matcher = _SPHINX_RETURN_DIRECTIVE_PATTERN.match(text)
        if matcher:
            text = cls._strip_sphinx_directive_text(matcher.group('desc'))
            return_description['return'] = text

    @classmethod
    def _strip_sphinx_directive_text(cls, description: str) -> str:
        return description.strip().replace('``', '"') \
            .replace(':py:class:', '') \
            .replace(':py:func:', '') \
            .replace(':py:attr:', '') \
            .replace('`', '')

    @classmethod
    def _get_input_names(cls, inputs: Dict[str, Props]) -> List[str]:
        num_inputs = len(inputs)
        if not num_inputs:
            return []
        input_names = num_inputs * ['']
        index = 0
        for name, props in inputs.items():
            position = props.get('position', index)
            if 0 <= position < num_inputs:
                if input_names[position]:
                    raise ValueError("illegal input property, position={} used twice".format(position))
            else:
                raise ValueError(
                    "illegal input property, expected position={} to {}, but was {}".format(0, num_inputs - 1,
                                                                                            position))
            input_names[position] = name
            index += 1
        for position in range(num_inputs):
            if not input_names[position]:
                raise ValueError("illegal input properties, position={} is undefined".format(position))
        return input_names


def is_instance_of(value, data_type) -> bool:
    typing_name = repr(data_type)
    if typing_name.startswith(_TYPING_PREFIX):
        typing_name = typing_name[len(_TYPING_PREFIX):]
        bracket_pos = typing_name.find('[', 1)
        if bracket_pos != -1:
            typing_name = typing_name[0:bracket_pos]
        if typing_name == 'Union':
            union_args = data_type.__args__ if hasattr(data_type, '__args__') else None
            if union_args is not None:
                for union_arg in union_args:
                    if is_instance_of(value, union_arg):
                        return True
            return False
        elif typing_name == 'Callable':
            # Don't go into details of return value and parameters types
            return callable(value)
        else:
            typing_origin = data_type.__origin__ if hasattr(data_type, '__origin__') else None
            if typing_origin is not None:
                return is_instance_of(value, typing_origin)
            return False

    return isinstance(value, data_type)
