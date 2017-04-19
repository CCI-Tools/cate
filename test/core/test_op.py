from collections import OrderedDict
from unittest import TestCase

from shapely.geometry import Point

from cate.core.op import OpRegistry, op, op_input, op_return, op_output, OP_REGISTRY, parse_op_args
from cate.core.types import PointLike, TimeRangeLike
from cate.util.opmetainf import OpMetaInfo
from cate.util.misc import object_to_qualified_name, to_datetime_range
from cate.util.monitor import Monitor

MONITOR = OpMetaInfo.MONITOR_INPUT_NAME
RETURN = OpMetaInfo.RETURN_OUTPUT_NAME


class OpTest(TestCase):
    def setUp(self):
        self.registry = OpRegistry()

    def tearDown(self):
        self.registry = None

    def test_f(self):
        def f(a: float, b, c, u=3, v='A', w=4.9) -> str:
            """Hi, I am f!"""
            return str(a + b + c + u + len(v) + w)

        registry = self.registry
        added_op_reg = registry.add_op(f)
        self.assertIsNotNone(added_op_reg)

        with self.assertRaises(ValueError):
            registry.add_op(f, fail_if_exists=True)

        self.assertIs(registry.add_op(f, fail_if_exists=False), added_op_reg)

        op_reg = registry.get_op(object_to_qualified_name(f))
        self.assertIs(op_reg, added_op_reg)
        self.assertIs(op_reg.operation, f)
        expected_inputs = OrderedDict()
        expected_inputs['a'] = dict(data_type=float, position=0)
        expected_inputs['b'] = dict(position=1)
        expected_inputs['c'] = dict(position=2)
        expected_inputs['u'] = dict(default_value=3)
        expected_inputs['v'] = dict(default_value='A')
        expected_inputs['w'] = dict(default_value=4.9)
        expected_outputs = OrderedDict()
        expected_outputs[RETURN] = dict(data_type=str)
        self._assertMetaInfo(op_reg.op_meta_info,
                             object_to_qualified_name(f),
                             dict(description='Hi, I am f!'),
                             expected_inputs,
                             expected_outputs)

        removed_op_reg = registry.remove_op(f)
        self.assertIs(removed_op_reg, op_reg)
        op_reg = registry.get_op(object_to_qualified_name(f))
        self.assertIsNone(op_reg)

        with self.assertRaises(ValueError):
            registry.remove_op(f, fail_if_not_exists=True)

    def test_f_op(self):
        @op(registry=self.registry)
        def f_op(a: float, b, c, u=3, v='A', w=4.9) -> str:
            """Hi, I am f_op!"""
            return str(a + b + c + u + len(v) + w)

        with self.assertRaises(ValueError):
            # must exist
            self.registry.add_op(f_op, fail_if_exists=True)

        op_reg = self.registry.get_op(object_to_qualified_name(f_op))
        self.assertIs(op_reg.operation, f_op)
        expected_inputs = OrderedDict()
        expected_inputs['a'] = dict(position=0, data_type=float)
        expected_inputs['b'] = dict(position=1)
        expected_inputs['c'] = dict(position=2)
        expected_inputs['u'] = dict(default_value=3)
        expected_inputs['v'] = dict(default_value='A')
        expected_inputs['w'] = dict(default_value=4.9)
        expected_outputs = OrderedDict()
        expected_outputs[RETURN] = dict(data_type=str)
        self._assertMetaInfo(op_reg.op_meta_info,
                             object_to_qualified_name(f_op),
                             dict(description='Hi, I am f_op!'),
                             expected_inputs,
                             expected_outputs)

    def test_f_op_inp_ret(self):
        @op_input('a', value_range=[0., 1.], registry=self.registry)
        @op_input('v', value_set=['A', 'B', 'C'], registry=self.registry)
        @op_return(registry=self.registry)
        def f_op_inp_ret(a: float, b, c, u=3, v='A', w=4.9) -> str:
            """Hi, I am f_op_inp_ret!"""
            return str(a + b + c + u + len(v) + w)

        with self.assertRaises(ValueError):
            # must exist
            self.registry.add_op(f_op_inp_ret, fail_if_exists=True)

        op_reg = self.registry.get_op(object_to_qualified_name(f_op_inp_ret))
        self.assertIs(op_reg.operation, f_op_inp_ret)
        expected_inputs = OrderedDict()
        expected_inputs['a'] = dict(position=0, data_type=float, value_range=[0., 1.])
        expected_inputs['b'] = dict(position=1)
        expected_inputs['c'] = dict(position=2)
        expected_inputs['u'] = dict(default_value=3)
        expected_inputs['v'] = dict(default_value='A', value_set=['A', 'B', 'C'])
        expected_inputs['w'] = dict(default_value=4.9)
        expected_outputs = OrderedDict()
        expected_outputs[RETURN] = dict(data_type=str)
        self._assertMetaInfo(op_reg.op_meta_info,
                             object_to_qualified_name(f_op_inp_ret),
                             dict(description='Hi, I am f_op_inp_ret!'),
                             expected_inputs,
                             expected_outputs)

    def test_C(self):
        class C:
            """Hi, I am C!"""

            def __call__(self):
                return None

        registry = self.registry
        added_op_reg = registry.add_op(C)
        self.assertIsNotNone(added_op_reg)

        with self.assertRaises(ValueError):
            registry.add_op(C, fail_if_exists=True)

        self.assertIs(registry.add_op(C, fail_if_exists=False), added_op_reg)

        op_reg = registry.get_op(object_to_qualified_name(C))
        self.assertIs(op_reg, added_op_reg)
        self.assertIs(op_reg.operation, C)
        self._assertMetaInfo(op_reg.op_meta_info,
                             object_to_qualified_name(C),
                             dict(description='Hi, I am C!'),
                             OrderedDict(),
                             OrderedDict({RETURN: {}}))

        removed_op_reg = registry.remove_op(C)
        self.assertIs(removed_op_reg, op_reg)
        op_reg = registry.get_op(object_to_qualified_name(C))
        self.assertIsNone(op_reg)

        with self.assertRaises(ValueError):
            registry.remove_op(C, fail_if_not_exists=True)

    def test_C_op(self):
        @op(author='Ernie and Bert', registry=self.registry)
        class C_op:
            """Hi, I am C_op!"""

            def __call__(self):
                return None

        with self.assertRaises(ValueError):
            # must exist
            self.registry.add_op(C_op, fail_if_exists=True)

        op_reg = self.registry.get_op(object_to_qualified_name(C_op))
        self.assertIs(op_reg.operation, C_op)
        self._assertMetaInfo(op_reg.op_meta_info,
                             object_to_qualified_name(C_op),
                             dict(description='Hi, I am C_op!', author='Ernie and Bert'),
                             OrderedDict(),
                             OrderedDict({RETURN: {}}))

    def _assertMetaInfo(self, op_meta_info: OpMetaInfo,
                        expected_name: str,
                        expected_header: dict,
                        expected_input: OrderedDict,
                        expected_output: OrderedDict):
        self.assertIsNotNone(op_meta_info)
        self.assertEqual(op_meta_info.qualified_name, expected_name)
        self.assertEqual(op_meta_info.header, expected_header)
        self.assertEqual(OrderedDict(op_meta_info.input), expected_input)
        self.assertEqual(OrderedDict(op_meta_info.output), expected_output)

    def test_function_validation(self):
        @op_input('x', registry=self.registry, data_type=float, value_range=[0.1, 0.9], default_value=0.5)
        @op_input('y', registry=self.registry)
        @op_input('a', registry=self.registry, data_type=int, value_set=[1, 4, 5])
        @op_return(registry=self.registry, data_type=float)
        def f(x, y: float, a=4):
            return a * x + y if a != 5 else 'foo'

        self.assertEqual(f(y=1, x=8), 33)
        self.assertEqual(f(**dict(a=5, x=8, y=1)), 'foo')

        op_reg = self.registry.get_op(f)

        self.assertEqual(op_reg.op_meta_info.input['x'].get('data_type', None), float)
        self.assertEqual(op_reg.op_meta_info.input['x'].get('value_range', None), [0.1, 0.9])
        self.assertEqual(op_reg.op_meta_info.input['x'].get('default_value', None), 0.5)
        self.assertEqual(op_reg.op_meta_info.input['x'].get('position', None), 0)
        self.assertEqual(op_reg.op_meta_info.input['y'].get('data_type', None), float)
        self.assertEqual(op_reg.op_meta_info.input['y'].get('position', None), 1)
        self.assertEqual(op_reg.op_meta_info.input['a'].get('data_type', None), int)
        self.assertEqual(op_reg.op_meta_info.input['a'].get('value_set', None), [1, 4, 5])
        self.assertEqual(op_reg.op_meta_info.input['a'].get('default_value', None), 4)
        self.assertEqual(op_reg.op_meta_info.input['a'].get('position', None), None)
        self.assertEqual(op_reg.op_meta_info.output[RETURN].get('data_type', None), float)

        with self.assertRaises(ValueError) as cm:
            result = op_reg(x=0, y=3.)
        self.assertEqual(str(cm.exception), "input 'x' for operation 'test.core.test_op.f' must be in range [0.1, 0.9]")

        with self.assertRaises(ValueError) as cm:
            result = op_reg(x='A', y=3.)
        self.assertEqual(str(cm.exception), "input 'x' for operation 'test.core.test_op.f' must be of type 'float', "
                                            "but got type 'str'")

        with self.assertRaises(ValueError) as cm:
            result = op_reg(x=0.4)
        self.assertEqual(str(cm.exception), "input 'y' for operation 'test.core.test_op.f' required")

        with self.assertRaises(ValueError) as cm:
            result = op_reg(x=0.6, y=0.1, a=2)
        self.assertEqual(str(cm.exception), "input 'a' for operation 'test.core.test_op.f' must be one of [1, 4, 5]")

        with self.assertRaises(ValueError) as cm:
            result = op_reg(x=0.6, y=0.1, a=5)
        self.assertEqual(str(cm.exception),
                         "output '%s' for operation 'test.core.test_op.f' must be of type <class 'float'>" % RETURN)

        result = op_reg(y=3)
        self.assertEqual(result, 4 * 0.5 + 3)

    def test_function_invocation(self):
        def f(x, a=4):
            return a * x

        op_reg = self.registry.add_op(f)
        result = op_reg(x=2.5)
        self.assertEqual(result, 4 * 2.5)

    def test_function_invocation_with_monitor(self):
        def f(monitor: Monitor, x, a=4):
            monitor.start('f', 23)
            return_value = a * x
            monitor.done()
            return return_value

        op_reg = self.registry.add_op(f)
        monitor = MyMonitor()
        result = op_reg(x=2.5, monitor=monitor)
        self.assertEqual(result, 4 * 2.5)
        self.assertEqual(monitor.total_work, 23)
        self.assertEqual(monitor.is_done, True)

    def test_class_invocation(self):
        @op_input('x', registry=self.registry)
        @op_input('a', default_value=4, registry=self.registry)
        @op_output('y', registry=self.registry)
        class C:
            def __call__(self, x, a):
                return {'y': x * a}

        op_reg = self.registry.get_op(C)
        result = op_reg(x=2.5)
        self.assertEqual(result, {'y': 4 * 2.5})

    def test_class_invocation_with_monitor(self):
        @op_input('x', registry=self.registry)
        @op_input('a', default_value=4, registry=self.registry)
        @op_output('y', registry=self.registry)
        class C:
            def __call__(self, x, a, monitor: Monitor):
                monitor.start('C', 19)
                output = {'y': x * a}
                monitor.done()
                return output

        op_reg = self.registry.get_op(C)
        monitor = MyMonitor()
        result = op_reg(x=2.5, monitor=monitor)
        self.assertEqual(result, {'y': 4 * 2.5})
        self.assertEqual(monitor.total_work, 19)
        self.assertEqual(monitor.is_done, True)

    def test_class_invocation_with_start_up(self):
        @op_input('x', registry=self.registry)
        @op_input('a', default_value=4, registry=self.registry)
        @op_output('y', registry=self.registry)
        class C:
            b = None

            @classmethod
            def start_up(cls):
                C.b = 1.5

            @classmethod
            def tear_down(cls):
                C.b = None

            def __call__(self, x, a):
                return {'y': x * a + C.b}

        op_reg = self.registry.get_op(C)
        with self.assertRaisesRegex(TypeError, "unsupported operand type\\(s\\) for \\+\\: 'float' and 'NoneType'"):
            # because C.b is None, C.start_up has not been called yet
            op_reg(x=2.5)

        # Note: this is exemplary code how the framework could call special class methods start_up/tear_down if it
        # finds them declared in a given op-class.
        # - 'start_up' may be called a single time before instances are created.
        # - 'tear_down' may be called and an operation is deregistered and it's 'start_up' has been called.
        C.start_up()
        result = op_reg(x=2.5)
        C.tear_down()
        self.assertEqual(result, {'y': 4 * 2.5 + 1.5})

    def test_C_op_inp_out(self):
        @op_input('a', data_type=float, default_value=0.5, value_range=[0., 1.], registry=self.registry)
        @op_input('b', data_type=str, default_value='A', value_set=['A', 'B', 'C'], registry=self.registry)
        @op_output('x', data_type=float, registry=self.registry)
        @op_output('y', data_type=list, registry=self.registry)
        class C_op_inp_out:
            """Hi, I am C_op_inp_out!"""

            def __call__(self, a, b):
                x = 2.5 * a
                y = [a, b]
                return {'x': x, 'y': y}

        with self.assertRaises(ValueError):
            # must exist
            self.registry.add_op(C_op_inp_out, fail_if_exists=True)

        op_reg = self.registry.get_op(object_to_qualified_name(C_op_inp_out))
        self.assertIs(op_reg.operation, C_op_inp_out)
        expected_inputs = OrderedDict()
        expected_inputs['a'] = dict(position=0, data_type=float, default_value=0.5, value_range=[0., 1.])
        expected_inputs['b'] = dict(position=1, data_type=str, default_value='A', value_set=['A', 'B', 'C'])
        expected_outputs = OrderedDict()
        expected_outputs['y'] = dict(data_type=list)
        expected_outputs['x'] = dict(data_type=float)
        self._assertMetaInfo(op_reg.op_meta_info,
                             object_to_qualified_name(C_op_inp_out),
                             dict(description='Hi, I am C_op_inp_out!'),
                             expected_inputs,
                             expected_outputs)

    def test_history_op(self):
        """
        Test adding operation signature to output history information.
        """
        import xarray as xr
        from cate import __version__

        # Test @op_return
        @op(version='0.9', registry=self.registry)
        @op_return(add_history=True, registry=self.registry)
        def history_op(ds: xr.Dataset, a=1, b='bilinear'):
            ret = ds.copy()
            return ret

        ds = xr.Dataset()

        op_reg = self.registry.get_op(object_to_qualified_name(history_op))
        op_meta_info = op_reg.op_meta_info

        # This is a partial stamp, as the way a dict is stringified is not
        # always the same
        stamp = '\nModified with Cate v' + __version__ + ' ' + \
                op_meta_info.qualified_name + ' v' + \
                op_meta_info.header['version'] + \
                ' \nDefault input values: ' + \
                str(op_meta_info.input) + '\nProvided input values: '

        ret_ds = op_reg(ds=ds, a=2, b='trilinear')
        self.assertTrue(stamp in ret_ds.attrs['history'])
        # Check that a passed value is found in the stamp
        self.assertTrue('trilinear' in ret_ds.attrs['history'])

        # Double line-break indicates that this is a subsequent stamp entry
        stamp2 = '\n\nModified with Cate v' + __version__

        ret_ds = op_reg(ds=ret_ds, a=4, b='quadrilinear')
        self.assertTrue(stamp2 in ret_ds.attrs['history'])
        # Check that a passed value is found in the stamp
        self.assertTrue('quadrilinear' in ret_ds.attrs['history'])
        # Check that a previous passed value is found in the stamp
        self.assertTrue('trilinear' in ret_ds.attrs['history'])

        # Test @op_output
        @op(version='1.9', registry=self.registry)
        @op_output('name1', add_history=True, registry=self.registry)
        @op_output('name2', add_history=False, registry=self.registry)
        @op_output('name3', registry=self.registry)
        def history_named_op(ds: xr.Dataset, a=1, b='bilinear'):
            ds1 = ds.copy()
            ds2 = ds.copy()
            ds3 = ds.copy()
            return {'name1': ds1, 'name2': ds2, 'name3': ds3}

        ds = xr.Dataset()

        op_reg = self.registry.get_op(object_to_qualified_name(history_named_op))
        op_meta_info = op_reg.op_meta_info

        # This is a partial stamp, as the way a dict is stringified is not
        # always the same
        stamp = '\nModified with Cate v' + __version__ + ' ' + \
                op_meta_info.qualified_name + ' v' + \
                op_meta_info.header['version'] + \
                ' \nDefault input values: ' + \
                str(op_meta_info.input) + '\nProvided input values: '

        ret = op_reg(ds=ds, a=2, b='trilinear')
        # Check that the dataset was stamped
        self.assertTrue(stamp in ret['name1'].attrs['history'])
        # Check that a passed value is found in the stamp
        self.assertTrue('trilinear' in ret['name1'].attrs['history'])
        # Check that none of the other two datasets have been stamped
        with self.assertRaises(KeyError):
            ret['name2'].attrs['history']
        with self.assertRaises(KeyError):
            ret['name3'].attrs['history']

        # Double line-break indicates that this is a subsequent stamp entry
        stamp2 = '\n\nModified with Cate v' + __version__

        ret = op_reg(ds=ret_ds, a=4, b='quadrilinear')
        self.assertTrue(stamp2 in ret['name1'].attrs['history'])
        # Check that a passed value is found in the stamp
        self.assertTrue('quadrilinear' in ret['name1'].attrs['history'])
        # Check that a previous passed value is found in the stamp
        self.assertTrue('trilinear' in ret['name1'].attrs['history'])
        # Other datasets should have the old history, while 'name1' should be
        # updated
        self.assertTrue(ret['name1'].attrs['history'] !=
                        ret['name2'].attrs['history'])
        self.assertTrue(ret['name1'].attrs['history'] !=
                        ret['name3'].attrs['history'])
        self.assertTrue(ret['name2'].attrs['history'] ==
                        ret['name3'].attrs['history'])

        # Test missing version
        @op(registry=self.registry)
        @op_return(add_history=True, registry=self.registry)
        def history_no_version(ds: xr.Dataset, a=1, b='bilinear'):
            ds1 = ds.copy()
            return ds1

        ds = xr.Dataset()

        op_reg = \
            self.registry.get_op(object_to_qualified_name(history_no_version))
        with self.assertRaises(ValueError) as err:
            ret = op_reg(ds=ds, a=2, b='trilinear')
        self.assertTrue('Could not add history' in str(err.exception))

        # Test not implemented output type stamping
        @op(version='1.1', registry=self.registry)
        @op_return(add_history=True, registry=self.registry)
        def history_wrong_type(ds: xr.Dataset, a=1, b='bilinear'):
            return "Joke's on you"

        ds = xr.Dataset()
        op_reg = \
            self.registry.get_op(object_to_qualified_name(history_wrong_type))
        with self.assertRaises(NotImplementedError) as err:
            ret = op_reg(ds=ds, a=2, b='abc')
        self.assertTrue('Adding of operation signature' in str(err.exception))


class ParseOpArgsTest(TestCase):
    def test_no_namespace(self):
        self.assertEqual(parse_op_args(['']), ([''], OrderedDict()))
        self.assertEqual(parse_op_args(['a=b']), ([], OrderedDict(a='b')))
        self.assertEqual(parse_op_args(['a="b"']), ([], OrderedDict(a='b')))
        self.assertEqual(parse_op_args(['a="C:\\\\Users"']), ([], OrderedDict(a='C:\\Users')))
        self.assertEqual(parse_op_args(['a=2']), ([], OrderedDict(a=2)))
        self.assertEqual(parse_op_args(['a="c"']), ([], OrderedDict(a='c')))
        self.assertEqual(parse_op_args(['a=True']), ([], OrderedDict(a=True)))
        self.assertEqual(parse_op_args(['z=4.6', 'y=1', 'x=2.+6j']),
                         ([], OrderedDict([('z', 4.6), ('y', 1), ('x', (2 + 6j))])))

    def test_with_namespace(self):
        class Dataset:
            pass

        ds = Dataset()
        ds.sst = 237.8

        import math as m
        namespace = dict(ds=ds, m=m)

        self.assertEqual(parse_op_args(['ds', 'm.pi', 'b=ds.sst + 0.2', 'u=m.cos(m.pi)'], namespace=namespace),
                         ([ds, m.pi], OrderedDict([('b', 238.0), ('u', m.cos(m.pi))])))

    def test_with_input_props(self):
        class Dataset:
            pass

        ds = Dataset()
        ds.sst = 237.8

        input_props = dict(a=dict(data_type=PointLike),
                           b=dict(data_type=TimeRangeLike),
                           c=dict(data_type=int))

        self.assertEqual(parse_op_args(['a = 11.3, 52.9',
                                        'b = 2001-01-01, 2004-05-06',
                                        'c=8.3',
                                        'd="Bibo"',
                                        'e=ds.sst'],
                                       input_props=input_props,
                                       namespace=dict(ds=ds)),
                         ([], OrderedDict([('a', Point(11.3, 52.9)),
                                           ('b', to_datetime_range('2001-01-01', '2004-05-06')),
                                           ('c', 8.3),
                                           ('d', 'Bibo'),
                                           ('e', 237.8)]))
                         )

    def test_errors(self):
        with self.assertRaises(ValueError) as cm:
            parse_op_args(['=9'])
        self.assertEqual(str(cm.exception), "missing input name")

        with self.assertRaises(ValueError) as cm:
            parse_op_args(['8=9'])
        self.assertEqual(str(cm.exception), '"8" is not a valid input name')

        with self.assertRaises(ValueError) as cm:
            parse_op_args(["fp=open('info.txt')"], ignore_parse_errors=False)
        self.assertEqual(str(cm.exception), 'failed to convert "open(\'info.txt\')" into a value for input "fp"')


class DefaultOpRegistryTest(TestCase):
    def test_it(self):
        self.assertIsNotNone(OP_REGISTRY)
        self.assertEqual(repr(OP_REGISTRY), 'OP_REGISTRY')


class MyMonitor(Monitor):
    def __init__(self):
        self.total_work = 0
        self.worked = 0
        self.is_done = False

    def start(self, label: str, total_work: float = None):
        self.total_work = total_work

    def progress(self, work: float = None, msg: str = None):
        self.worked += work

    def done(self):
        self.is_done = True
