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
        op_meta_info.input['x'] = {'data_type': str}
        op_meta_info.input['y'] = {'data_type': int}
        op_meta_info.output[RETURN] = {'data_type': str}

        self.assertEqual(str(op_meta_info), "OpMetaInfo('x.y.Z')")
        self.assertEqual(repr(op_meta_info), "OpMetaInfo('x.y.Z')")
        self.assertEqual(op_meta_info.qualified_name, 'x.y.Z')
        self.assertEqual(op_meta_info.has_monitor, False)
        self.assertEqual(op_meta_info.has_named_outputs, False)
        self.assertEqual(op_meta_info.can_cache, True)
        self.assertEqual(op_meta_info.header, {'description': 'Hello!'})
        self.assertEqual(OrderedDict(op_meta_info.input),
                         OrderedDict([('x', {'data_type': str}), ('y', {'data_type': int})]))
        self.assertEqual(OrderedDict(op_meta_info.output),
                         OrderedDict([(RETURN, {'data_type': str})]))

    def test_introspect_operation(self):
        # noinspection PyUnusedLocal
        def f(a: str, b: int, c: float = 1, d='A') -> float:
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
        self.assertEqual(len(op_meta_info.input), 4)
        self.assertEqual(len(op_meta_info.output), 1)
        self.assertIn('a', op_meta_info.input)
        self.assertIn('b', op_meta_info.input)
        self.assertIn('c', op_meta_info.input)
        self.assertIn('d', op_meta_info.input)
        self.assertIn(RETURN, op_meta_info.output)
        self.assertEqual(op_meta_info.input['a'], dict(data_type=str, position=0, description='the a str'))
        self.assertEqual(op_meta_info.input['b'], dict(data_type=int, position=1, description='the b int'))
        self.assertEqual(op_meta_info.input['c'], dict(data_type=float, default_value=1, description='the c float'))
        self.assertEqual(op_meta_info.input['d'], dict(default_value='A', description="the d 'A'"))
        self.assertEqual(op_meta_info.output[RETURN], dict(data_type=float, description='a float'))
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
        self.assertEqual(len(op_meta_info.input), 1)
        self.assertEqual(len(op_meta_info.output), 1)
        self.assertIn('x', op_meta_info.input)
        self.assertNotIn(MONITOR, op_meta_info.input)
        self.assertIn(RETURN, op_meta_info.output)
        self.assertEqual(op_meta_info.input['x'], dict(data_type=float, position=0))
        self.assertEqual(op_meta_info.output[RETURN], dict(data_type=float))
        self.assertEqual(op_meta_info.has_monitor, True)
        self.assertEqual(op_meta_info.has_named_outputs, False)

    def test_to_json_dict(self):
        op_meta_info = OpMetaInfo('x.y.Z')
        op_meta_info.header['description'] = 'Hello!'
        op_meta_info.input['x'] = {'data_type': str}
        op_meta_info.input['y'] = {'data_type': int}
        op_meta_info.output[RETURN] = {'data_type': float}
        actual_json_dict = op_meta_info.to_json_dict()
        actual_json_text = json.dumps(actual_json_dict, indent=4)

        expected_json_text = """
        {
            "qualified_name": "x.y.Z",
            "header": {
                "description": "Hello!"
            },
            "input": {
                "x": {
                    "data_type": "str"
                },
                "y": {
                    "data_type": "int"
                }
            },
            "output": {
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
            "input": {
                "x": {
                    "data_type": "str"
                },
                "y": {
                    "data_type": "int"
                }
            },
            "output": {
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
        self.assertEqual(len(op_meta_info.input), 2)
        self.assertIn('x', op_meta_info.input)
        self.assertIn('y', op_meta_info.input)
        self.assertEqual(op_meta_info.input['x'], OrderedDict([('data_type', str)]))
        self.assertEqual(op_meta_info.input['y'], OrderedDict([('data_type', int)]))
        self.assertEqual(len(op_meta_info.output), 1)
        self.assertEqual(op_meta_info.output[RETURN], OrderedDict([('data_type', float)]))
