from collections import OrderedDict
from unittest import TestCase

from ect.core.op import get_op, add_op, remove_op, op, op_input, op_return, op_output


def f(a: float, b, c, u=3, v='A', w=4.9) -> str:
    """Hi, I am f!"""
    return str(a + b + c + u + len(v) + w)


@op()
def f_op(a: float, b, c, u=3, v='A', w=4.9) -> str:
    """Hi, I am f_op!"""
    return str(a + b + c + u + len(v) + w)


@op_input('a', value_range=[0., 1.])
@op_input('v', value_set=['A', 'B', 'C'])
@op_return(not_none=True)
def f_op_inp_ret(a: float, b, c, u=3, v='A', w=4.9) -> str:
    """Hi, I am f_op_inp_ret!"""
    return str(a + b + c + u + len(v) + w)


class OpIsFunctionTest(TestCase):
    def test_f(self):
        added_op_reg = add_op(f)
        self.assertIsNotNone(added_op_reg)

        with self.assertRaises(ValueError):
            add_op(f, fail_if_exists=True)

        self.assertIs(add_op(f, fail_if_exists=False), added_op_reg)

        op_reg = get_op(f.__qualname__)
        self.assertIs(op_reg, added_op_reg)
        self.assertIs(op_reg.operation, f)
        self.assertIsNotNone(op_reg.meta_info)
        self.assertEqual(op_reg.meta_info.qualified_name, f.__qualname__)
        self.assertEqual(op_reg.meta_info.attributes, dict(description='Hi, I am f!'))
        expected_inputs = OrderedDict()
        expected_inputs['a'] = dict(data_type=float)
        expected_inputs['b'] = dict()
        expected_inputs['c'] = dict()
        expected_inputs['u'] = dict(default_value=3)
        expected_inputs['v'] = dict(default_value='A')
        expected_inputs['w'] = dict(default_value=4.9)
        self.assertEqual(op_reg.meta_info.inputs, expected_inputs)
        expected_outputs = OrderedDict()
        expected_outputs['return'] = dict(data_type=str)
        self.assertEqual(op_reg.meta_info.outputs, expected_outputs)

        removed_op_reg = remove_op(f)
        self.assertIs(removed_op_reg, op_reg)
        op_reg = get_op(f.__qualname__)
        self.assertIsNone(op_reg)

        with self.assertRaises(ValueError):
            remove_op(f, fail_if_not_exists=True)

    def test_f_op(self):
        with self.assertRaises(ValueError):
            # must exist
            add_op(f_op, fail_if_exists=True)

        op_reg = get_op(f_op.__qualname__)
        self.assertIs(op_reg.operation, f_op)
        self.assertIsNotNone(op_reg.meta_info)
        self.assertEqual(op_reg.meta_info.qualified_name, f_op.__qualname__)
        self.assertEqual(op_reg.meta_info.attributes, dict(description='Hi, I am f_op!'))
        expected_inputs = OrderedDict()
        expected_inputs['a'] = dict(data_type=float)
        expected_inputs['b'] = dict()
        expected_inputs['c'] = dict()
        expected_inputs['u'] = dict(default_value=3)
        expected_inputs['v'] = dict(default_value='A')
        expected_inputs['w'] = dict(default_value=4.9)
        self.assertEqual(op_reg.meta_info.inputs, expected_inputs)
        expected_outputs = OrderedDict()
        expected_outputs['return'] = dict(data_type=str)
        self.assertEqual(op_reg.meta_info.outputs, expected_outputs)

    def test_f_op_inp_ret(self):
        with self.assertRaises(ValueError):
            # must exist
            add_op(f_op_inp_ret, fail_if_exists=True)

        op_reg = get_op(f_op_inp_ret.__qualname__)
        self.assertIs(op_reg.operation, f_op_inp_ret)
        self.assertIsNotNone(op_reg.meta_info)
        self.assertEqual(op_reg.meta_info.qualified_name, f_op_inp_ret.__qualname__)
        self.assertEqual(op_reg.meta_info.attributes, dict(description='Hi, I am f_op_inp_ret!'))
        expected_inputs = OrderedDict()
        expected_inputs['a'] = dict(data_type=float, value_range=[0., 1.])
        expected_inputs['b'] = dict()
        expected_inputs['c'] = dict()
        expected_inputs['u'] = dict(default_value=3)
        expected_inputs['v'] = dict(default_value='A', value_set=['A', 'B', 'C'])
        expected_inputs['w'] = dict(default_value=4.9)
        self.assertEqual(op_reg.meta_info.inputs, expected_inputs)
        expected_outputs = OrderedDict()
        expected_outputs['return'] = dict(data_type=str, not_none=True)
        self.assertEqual(op_reg.meta_info.outputs, expected_outputs)


class C:
    """Hi, I am C!"""

    def __call__(self):
        return None


@op()
class C_op:
    """Hi, I am C_op!"""

    def __call__(self):
        return None


@op_input('a', data_type=float, default_value=0.5, value_range=[0., 1.])
@op_input('b', data_type=str, default_value='A', value_set=['A', 'B', 'C'])
@op_output('x', data_type=float)
@op_output('y', data_type=list)
class C_op_inp_out:
    """Hi, I am C_op_inp_out!"""

    def __init__(self):
        self.a = None
        self.b = None
        self.x = None
        self.y = None

    def __call__(self):
        self.x = 2.5 * self.a
        self.y = [self.a, self.b]


class OpIsClassTest(TestCase):
    def test_C(self):
        added_op_reg = add_op(C)
        self.assertIsNotNone(added_op_reg)

        with self.assertRaises(ValueError):
            add_op(C, fail_if_exists=True)

        self.assertIs(add_op(C, fail_if_exists=False), added_op_reg)

        op_reg = get_op(C.__qualname__)
        self.assertIs(op_reg, added_op_reg)
        self.assertIs(op_reg.operation, C)
        self.assertIsNotNone(op_reg.meta_info)
        self.assertEqual(op_reg.meta_info.qualified_name, C.__qualname__)
        self.assertEqual(op_reg.meta_info.attributes, dict(description='Hi, I am C!'))
        self.assertEqual(op_reg.meta_info.inputs, OrderedDict())
        self.assertEqual(op_reg.meta_info.outputs, OrderedDict())

        removed_op_reg = remove_op(C)
        self.assertIs(removed_op_reg, op_reg)
        op_reg = get_op(C.__qualname__)
        self.assertIsNone(op_reg)

        with self.assertRaises(ValueError):
            remove_op(C, fail_if_not_exists=True)

    def test_C_op(self):
        with self.assertRaises(ValueError):
            # must exist
            add_op(C_op, fail_if_exists=True)

        op_reg = get_op(C_op.__qualname__)
        self.assertIs(op_reg.operation, C_op)
        self.assertIsNotNone(op_reg.meta_info)
        self.assertEqual(op_reg.meta_info.qualified_name, C_op.__qualname__)
        self.assertEqual(op_reg.meta_info.attributes, dict(description='Hi, I am C_op!'))
        self.assertEqual(op_reg.meta_info.inputs, OrderedDict())
        self.assertEqual(op_reg.meta_info.outputs, OrderedDict())

    def test_C_op_inp_out(self):
        with self.assertRaises(ValueError):
            # must exist
            add_op(C_op_inp_out, fail_if_exists=True)

        op_reg = get_op(C_op_inp_out.__qualname__)
        self.assertIs(op_reg.operation, C_op_inp_out)
        self.assertIsNotNone(op_reg.meta_info)
        self.assertEqual(op_reg.meta_info.qualified_name, C_op_inp_out.__qualname__)
        self.assertEqual(op_reg.meta_info.attributes, dict(description='Hi, I am C_op_inp_out!'))
        expected_inputs = OrderedDict()
        expected_inputs['b'] = dict(data_type=str, default_value='A', value_set=['A', 'B', 'C'])
        expected_inputs['a'] = dict(data_type=float, default_value=0.5, value_range=[0., 1.])
        self.assertEqual(op_reg.meta_info.inputs, expected_inputs)
        expected_outputs = OrderedDict()
        expected_outputs['y'] = dict(data_type=list)
        expected_outputs['x'] = dict(data_type=float)
        self.assertEqual(op_reg.meta_info.outputs, expected_outputs)
