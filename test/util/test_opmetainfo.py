import json
from collections import OrderedDict
from unittest import TestCase

from cate.util.opmetainf import OpMetaInfo
from cate.util.misc import object_to_qualified_name
from cate.util.monitor import Monitor

MONITOR = OpMetaInfo.MONITOR_INPUT_NAME
RETURN = OpMetaInfo.RETURN_OUTPUT_NAME


class OpMetaInfoTest(TestCase):
    def test_init(self):
        op_meta_info = OpMetaInfo('x.y.Z')
        op_meta_info.header['description'] = 'Hello!'
        op_meta_info.inputs['x'] = {'data_type': str}
        op_meta_info.inputs['y'] = {'data_type': int}
        op_meta_info.outputs[RETURN] = {'data_type': str}

        self.assertEqual(str(op_meta_info), "OpMetaInfo('x.y.Z')")
        self.assertEqual(repr(op_meta_info), "OpMetaInfo('x.y.Z')")
        self.assertEqual(op_meta_info.qualified_name, 'x.y.Z')
        self.assertEqual(op_meta_info.has_monitor, False)
        self.assertEqual(op_meta_info.has_named_outputs, False)
        self.assertEqual(op_meta_info.can_cache, True)
        self.assertEqual(op_meta_info.header, {'description': 'Hello!'})
        self.assertEqual(OrderedDict(op_meta_info.inputs),
                         OrderedDict([('x', {'data_type': str}), ('y', {'data_type': int})]))
        self.assertEqual(OrderedDict(op_meta_info.outputs),
                         OrderedDict([(RETURN, {'data_type': str})]))

    def test_introspect_operation(self):
        # noinspection PyUnusedLocal
        def f(a: str, b: int = None, c: float = 1, d='A') -> float:
            """
            The doc.

            :param a: the a str
            :param b: the
              b int
            :param c: the c float
            :param d:
                         the d 'A'

            :return: a float
            """

        op_meta_info = OpMetaInfo.introspect_operation(f)
        self.assertEqual(op_meta_info.qualified_name, object_to_qualified_name(f))
        self.assertEqual(op_meta_info.header, dict(description='The doc.'))
        self.assertEqual(len(op_meta_info.inputs), 4)
        self.assertEqual(len(op_meta_info.outputs), 1)
        self.assertIn('a', op_meta_info.inputs)
        self.assertIn('b', op_meta_info.inputs)
        self.assertIn('c', op_meta_info.inputs)
        self.assertIn('d', op_meta_info.inputs)
        self.assertIn(RETURN, op_meta_info.outputs)
        self.assertEqual(op_meta_info.inputs['a'], dict(position=0, data_type=str, description='the a str'))
        self.assertEqual(op_meta_info.inputs['b'], dict(position=1, data_type=int, nullable=True, default_value=None, description='the b int'))
        self.assertEqual(op_meta_info.inputs['c'], dict(position=2, data_type=float, default_value=1, description='the c float'))
        self.assertEqual(op_meta_info.inputs['d'], dict(position=3, data_type=str, default_value='A', description="the d 'A'"))
        self.assertEqual(op_meta_info.outputs[RETURN], dict(data_type=float, description='a float'))
        self.assertEqual(op_meta_info.has_monitor, False)
        self.assertEqual(op_meta_info.has_named_outputs, False)
        self.assertEqual(op_meta_info.can_cache, True)

    def test_introspect_operation_with_monitor(self):
        # noinspection PyUnusedLocal
        def g(x: float, monitor: Monitor) -> float:
            """The doc."""

        op_meta_info = OpMetaInfo.introspect_operation(g)
        self.assertEqual(op_meta_info.qualified_name, object_to_qualified_name(g))
        self.assertEqual(op_meta_info.header, dict(description='The doc.'))
        self.assertEqual(len(op_meta_info.inputs), 1)
        self.assertEqual(len(op_meta_info.outputs), 1)
        self.assertIn('x', op_meta_info.inputs)
        self.assertNotIn(MONITOR, op_meta_info.inputs)
        self.assertIn(RETURN, op_meta_info.outputs)
        self.assertEqual(op_meta_info.inputs['x'], dict(data_type=float, position=0))
        self.assertEqual(op_meta_info.outputs[RETURN], dict(data_type=float))
        self.assertEqual(op_meta_info.has_monitor, True)
        self.assertEqual(op_meta_info.has_named_outputs, False)

    def test_validate_input_values(self):
        op_meta_info = OpMetaInfo('some_op')
        op_meta_info.inputs['file'] = dict(data_type=str)
        op_meta_info.inputs['count'] = dict(data_type=int, default_value=2, nullable=True)
        op_meta_info.inputs['ctx'] = dict(context=True)
        op_meta_info.outputs[RETURN] = dict(data_type=int)

        self.assertEqual(op_meta_info.validate_input_values(dict(file='a/b/c')), None)
        self.assertEqual(op_meta_info.validate_input_values(dict(file='a/b/c', count=3)), None)
        self.assertEqual(op_meta_info.validate_input_values(dict(file='a/b/c', count=None)), None)

        with self.assertRaises(ValueError) as cm:
            op_meta_info.validate_input_values(dict())
        self.assertEqual(str(cm.exception),
                         "Input 'file' for operation 'some_op' must be given.")

        with self.assertRaises(ValueError) as cm:
            op_meta_info.validate_input_values(dict(file=None))
        self.assertEqual(str(cm.exception),
                         "Input 'file' for operation 'some_op' must be given.")

        with self.assertRaises(ValueError) as cm:
            op_meta_info.validate_input_values(dict(file='a/b/c', count='bibo'))
        self.assertEqual(str(cm.exception),
                         "Input 'count' for operation 'some_op' must be of type 'int', but got type 'str'.")

    def test_to_json_dict(self):
        op_meta_info = OpMetaInfo('x.y.Z')
        op_meta_info.header['description'] = 'Hello!'
        op_meta_info.inputs['x'] = {'data_type': str}
        op_meta_info.inputs['y'] = {'data_type': int}
        op_meta_info.outputs[RETURN] = {'data_type': float}
        actual_json_dict = op_meta_info.to_json_dict()
        actual_json_text = json.dumps(actual_json_dict, indent=4)

        expected_json_text = """
        {
            "qualified_name": "x.y.Z",
            "header": {
                "description": "Hello!"
            },
            "inputs": {
                "x": {
                    "data_type": "str"
                },
                "y": {
                    "data_type": "int"
                }
            },
            "outputs": {
                "return": {
                    "data_type": "float"
                }
            }
        }
        """
        expected_json_dict = json.loads(expected_json_text)

        self.assertEqual(actual_json_dict, expected_json_dict,
                         msg='\n%sexpected:\n%s\n%s\nbut got:\n%s\n' %
                             (120 * '-', expected_json_text,
                              120 * '-', actual_json_text))

    def test_from_json_dict(self):
        json_text = """
        {
            "qualified_name": "x.y.Z",
            "has_monitor": true,
            "header": {
                "description": "Hello!"
            },
            "inputs": {
                "x": {
                    "data_type": "str"
                },
                "y": {
                    "data_type": "int"
                }
            },
            "outputs": {
                "return": {
                    "data_type": "float"
                }
            }
        }
        """
        json_dict = json.loads(json_text)

        op_meta_info = OpMetaInfo.from_json_dict(json_dict)
        self.assertEqual(op_meta_info.qualified_name, 'x.y.Z')
        self.assertEqual(op_meta_info.header, dict(description='Hello!'))
        self.assertTrue(op_meta_info.has_monitor)
        self.assertEqual(len(op_meta_info.inputs), 2)
        self.assertIn('x', op_meta_info.inputs)
        self.assertIn('y', op_meta_info.inputs)
        self.assertEqual(op_meta_info.inputs['x'], OrderedDict([('data_type', str)]))
        self.assertEqual(op_meta_info.inputs['y'], OrderedDict([('data_type', int)]))
        self.assertEqual(len(op_meta_info.outputs), 1)
        self.assertEqual(op_meta_info.outputs[RETURN], OrderedDict([('data_type', float)]))
