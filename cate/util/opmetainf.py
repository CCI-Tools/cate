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
from inspect import isclass
from typing import Tuple, Dict, List, Any

from cate.util import object_to_qualified_name, qualified_name_to_object


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

    @property
    def can_cache(self) -> bool:
        return not self._header.get('no_cache', False)

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

        input_dict, has_monitor = OrderedDict(), False
        if hasattr(operation, '__code__'):
            input_dict, has_monitor = cls._introspect_inputs_from_callable(operation, False)
        elif isclass(operation):
            if hasattr(operation, '__call__'):
                call_method = getattr(operation, '__call__')
                input_dict, has_monitor = cls._introspect_inputs_from_callable(call_method, True)
            else:
                raise ValueError('operations of type class must define a __call__(self, ...) method')

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

    def validate_input_values(self, input_values: Dict, except_types: set = None):
        """
        Validate given *input_values* against the operation's input properties.

        :param input_values: The dictionary of input values.
        :param except_types: A set of types or ``None``. If an input value's type is in this set,
               it will not be validated against the various input properties,
               such as ``data_type``, ``nullable``, ``value_set``, ``value_range``.
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
            if except_types and type(value) in except_types:
                continue
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
            if self.has_accepts(data_type):
                accepted = data_type.accepts(value)
                if not accepted:
                    raise ValueError(
                        "input '%s' for operation '%s' must be of type '%s', but got type '%s'" % (
                            name, self.qualified_name, data_type.__name__, type(value).__name__))
            else:
                is_float_type = data_type is float and (isinstance(value, float) or isinstance(value, int))
                if data_type and not (isinstance(value, data_type) or is_float_type):
                    raise ValueError(
                        "input '%s' for operation '%s' must be of type '%s', but got type '%s'" % (
                            name, self.qualified_name, data_type.__name__, type(value).__name__))
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
                if data_type:
                    if self.has_accepts(data_type):
                        accepted = data_type.accepts(value)
                        if not accepted:
                            raise ValueError(
                                "output '%s' for operation '%s' must be of type %s" % (
                                    name, self.qualified_name, data_type))
                    elif not isinstance(value, data_type):
                        raise ValueError(
                            "output '%s' for operation '%s' must be of type %s" % (
                                name, self.qualified_name, data_type))

    @classmethod
    def has_accepts(cls, data_type: Any) -> bool:
        return hasattr(data_type, 'accepts') and callable(data_type.accepts)

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


_SPHINX_PARAM_DIRECTIVE_PATTERN = re.compile(":param (?P<name>[^:]+): (?P<desc>[^:]+)")
_SPHINX_RETURN_DIRECTIVE_PATTERN = re.compile(":returns?: (?P<desc>[^:]+)")
