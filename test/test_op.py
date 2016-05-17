from collections import OrderedDict
from unittest import TestCase

from ect.core.op import Op, operation, input, output


class NotAnOp:
    pass


class OpWithoutOperationDeco(Op):
    def __init__(self):
        super(OpWithoutOperationDeco, self).__init__()


@operation(name='PeterPaulAndMary')
class OpWithOperationDeco(Op):
    def __init__(self):
        super(OpWithOperationDeco, self).__init__()


@input('x')
@output('a', default_value=[1, 2, 3])
class OpWithSomeAttr(Op):
    def __init__(self, x=3):
        super(OpWithSomeAttr, self).__init__()
        self._x = x

    @property
    def x(self):
        return self._x

    @x.setter
    def x(self, value):
        self._x = value


OpWithSomeAttr.register_input('y', default_value='A')
OpWithSomeAttr.register_output('b', default_value=('U', 'V', 'W'))


class TestOp(TestCase):
    def test_OpWithoutOperationDeco_registration(self):
        self.assertNotIn('test_op.OpWithoutOperationDeco', Op.get_op_classes())

    def test_OpWithOperationDeco_registration(self):
        self.assertIn('test_op.OpWithOperationDeco', Op.get_op_classes())
        self.assertEqual(OpWithOperationDeco.get_name(), 'PeterPaulAndMary')
        self.assertEqual(OpWithOperationDeco.get_qualified_name(), 'test_op.OpWithOperationDeco')
        self.assertEqual(OpWithOperationDeco.get_inputs(), OrderedDict())
        self.assertEqual(OpWithOperationDeco.get_outputs(), OrderedDict())

    def test_OpWithSomeAttr_registration(self):
        self.assertIn('test_op.OpWithSomeAttr', Op.get_op_classes())
        self.assertEqual(OpWithSomeAttr.get_name(), 'OpWithSomeAttr')
        self.assertEqual(OpWithSomeAttr.get_qualified_name(), 'test_op.OpWithSomeAttr')
        self.assertIn('x', OpWithSomeAttr.get_inputs())
        self.assertIn('y', OpWithSomeAttr.get_inputs())
        self.assertIn('a', OpWithSomeAttr.get_outputs())
        self.assertIn('b', OpWithSomeAttr.get_outputs())

    def test_OpWithSomeAttr_instantiation(self):
        op = OpWithSomeAttr()
        self.assertEqual(op.x, 3)
        self.assertEqual(op.y, 'A')

        self.assertEqual(op.a, [1, 2, 3])
        self.assertEqual(op.b, ('U', 'V', 'W'))

        op = OpWithSomeAttr(x=4)
        self.assertEqual(op.x, 4)
        self.assertEqual(op.y, 'A')
