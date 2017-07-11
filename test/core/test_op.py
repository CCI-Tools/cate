import os.path
from collections import OrderedDict
from unittest import TestCase

import xarray as xr

from cate.core.op import OpRegistry, op, op_input, op_return, op_output, OP_REGISTRY
from cate.core.types import FileLike, VarName
from cate.util.misc import object_to_qualified_name
from cate.util.monitor import Monitor
from cate.util.opmetainf import OpMetaInfo

MONITOR = OpMetaInfo.MONITOR_INPUT_NAME
RETURN = OpMetaInfo.RETURN_OUTPUT_NAME

DIR = os.path.dirname(__file__)
SOILMOISTURE_NC = os.path.join(DIR, 'test_data', 'small',
                               'ESACCI-SOILMOISTURE-L3S-SSMV-COMBINED-20000101000000-fv02.2.nc')


class OpTest(TestCase):
    def setUp(self):
        self.registry = OpRegistry()

    def tearDown(self):
        self.registry = None

    def test_executable_no_ds(self):
        import os.path
        import sys
        exe = sys.executable + " " + os.path.join(DIR, 'executables', 'mkentropy.py')
        commandline_pattern = exe + " {num_steps} {period}"
        op_reg = self.registry.add_op_from_executable(OpMetaInfo('make_entropy',
                                                                 input_dict={
                                                                     'num_steps': {'data_type': int},
                                                                     'period': {'data_type': float},
                                                                 },
                                                                 output_dict={
                                                                     'return': {'data_type': int}
                                                                 }),
                                                      commandline_pattern)
        exit_code = op_reg(num_steps=10, period=0.1)
        self.assertEqual(exit_code, 0)

    def test_executable_ds_file(self):
        import os.path
        import sys
        exe = sys.executable + " " + os.path.join(DIR, 'executables', 'filterds.py')
        commandline_pattern = exe + " {ifile} {ofile} {var}"
        op_reg = self.registry.add_op_from_executable(OpMetaInfo('filter_ds',
                                                                 input_dict={
                                                                     'ifile': {'data_type': FileLike},
                                                                     'ofile': {'data_type': FileLike},
                                                                     'var': {'data_type': VarName},
                                                                 },
                                                                 output_dict={
                                                                     'return': {'data_type': int}
                                                                 }),
                                                      commandline_pattern)
        ofile = os.path.join(DIR, 'test_data', 'filter_ds.nc')
        if os.path.isfile(ofile):
            os.remove(ofile)
        exit_code = op_reg(ifile=SOILMOISTURE_NC, ofile=ofile, var='sm')
        self.assertEqual(exit_code, 0)
        self.assertTrue(os.path.isfile(ofile))
        os.remove(ofile)

    def test_executable_ds_in_mem(self):
        import os.path
        import sys
        exe = sys.executable + " " + os.path.join(DIR, 'executables', 'filterds.py')
        commandline_pattern = exe + " {ifile} {ofile} {var}"
        op_reg = self.registry.add_op_from_executable(OpMetaInfo('filter_ds',
                                                                 input_dict={
                                                                     'ds': {
                                                                         'data_type': xr.Dataset,
                                                                         'write_to': 'ifile'
                                                                     },
                                                                     'var': {
                                                                         'data_type': VarName
                                                                     },
                                                                 },
                                                                 output_dict={
                                                                     'return': {
                                                                         'data_type': xr.Dataset,
                                                                         'read_from': 'ofile'
                                                                     }
                                                                 }),
                                                      commandline_pattern)
        ds = xr.open_dataset(SOILMOISTURE_NC)
        ds_out = op_reg(ds=ds, var='sm')
        self.assertIsNotNone(ds_out)
        self.assertIsNotNone('sm' in ds_out)

    def test_expression(self):
        op_reg = self.registry.add_op_from_expression(OpMetaInfo('add_xy',
                                                                 input_dict={
                                                                     'x': {'data_type': float},
                                                                     'y': {'data_type': float},
                                                                 },
                                                                 output_dict={
                                                                     'return': {'data_type': float}
                                                                 }),
                                                      'x + y')
        z = op_reg(x=1.2, y=2.4)
        self.assertEqual(z, 1.2 + 2.4)

    def test_plain_function(self):
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
        expected_inputs['a'] = dict(position=0, data_type=float)
        expected_inputs['b'] = dict(position=1)
        expected_inputs['c'] = dict(position=2)
        expected_inputs['u'] = dict(position=3, default_value=3, data_type=int)
        expected_inputs['v'] = dict(position=4, default_value='A', data_type=str)
        expected_inputs['w'] = dict(position=5, default_value=4.9, data_type=float)
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

    def test_decorated_function(self):
        @op(registry=self.registry)
        def f_op(a: float, b, c, u=3, v='A', w=4.9) -> str:
            """Hi, I am f_op!"""
            return str(a + b + c + u + len(v) + w)

        with self.assertRaises(ValueError):
            # must exist
            self.registry.add_op(f_op, fail_if_exists=True)

        op_reg = self.registry.get_op(object_to_qualified_name(f_op))
        expected_inputs = OrderedDict()
        expected_inputs['a'] = dict(position=0, data_type=float)
        expected_inputs['b'] = dict(position=1)
        expected_inputs['c'] = dict(position=2)
        expected_inputs['u'] = dict(position=3, default_value=3, data_type=int)
        expected_inputs['v'] = dict(position=4, default_value='A', data_type=str)
        expected_inputs['w'] = dict(position=5, default_value=4.9, data_type=float)
        expected_outputs = OrderedDict()
        expected_outputs[RETURN] = dict(data_type=str)
        self._assertMetaInfo(op_reg.op_meta_info,
                             object_to_qualified_name(f_op),
                             dict(description='Hi, I am f_op!'),
                             expected_inputs,
                             expected_outputs)

    def test_decorated_function_with_inputs_and_outputs(self):
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
        expected_inputs = OrderedDict()
        expected_inputs['a'] = dict(position=0, data_type=float, value_range=[0., 1.])
        expected_inputs['b'] = dict(position=1)
        expected_inputs['c'] = dict(position=2)
        expected_inputs['u'] = dict(position=3, default_value=3, data_type=int)
        expected_inputs['v'] = dict(position=4, default_value='A', data_type=str, value_set=['A', 'B', 'C'])
        expected_inputs['w'] = dict(position=5, default_value=4.9, data_type=float)
        expected_outputs = OrderedDict()
        expected_outputs[RETURN] = dict(data_type=str)
        self._assertMetaInfo(op_reg.op_meta_info,
                             object_to_qualified_name(f_op_inp_ret),
                             dict(description='Hi, I am f_op_inp_ret!'),
                             expected_inputs,
                             expected_outputs)

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

        self.assertIs(f, self.registry.get_op(f))
        self.assertEqual(f.op_meta_info.input['x'].get('data_type', None), float)
        self.assertEqual(f.op_meta_info.input['x'].get('value_range', None), [0.1, 0.9])
        self.assertEqual(f.op_meta_info.input['x'].get('default_value', None), 0.5)
        self.assertEqual(f.op_meta_info.input['x'].get('position', None), 0)
        self.assertEqual(f.op_meta_info.input['y'].get('data_type', None), float)
        self.assertEqual(f.op_meta_info.input['y'].get('position', None), 1)
        self.assertEqual(f.op_meta_info.input['a'].get('data_type', None), int)
        self.assertEqual(f.op_meta_info.input['a'].get('value_set', None), [1, 4, 5])
        self.assertEqual(f.op_meta_info.input['a'].get('default_value', None), 4)
        self.assertEqual(f.op_meta_info.input['a'].get('position', None), 2)
        self.assertEqual(f.op_meta_info.output[RETURN].get('data_type', None), float)

        self.assertEqual(f(y=1, x=0.2), 4 * 0.2 + 1)
        self.assertEqual(f(y=3), 4 * 0.5 + 3)
        self.assertEqual(f(0.6, y=3, a=1), 1 * 0.6 + 3.0)

        with self.assertRaises(ValueError) as cm:
            f(y=1, x=8)
        self.assertEqual(str(cm.exception),
                         "input 'x' for operation 'test.core.test_op.f' must be in range [0.1, 0.9]")

        with self.assertRaises(ValueError) as cm:
            f(y=None, x=0.2)
        self.assertEqual(str(cm.exception),
                         "input 'y' for operation 'test.core.test_op.f' is not nullable")

        with self.assertRaises(ValueError) as cm:
            f(y=0.5, x=0.2, a=2)
        self.assertEqual(str(cm.exception),
                         "input 'a' for operation 'test.core.test_op.f' must be one of [1, 4, 5]")

        with self.assertRaises(ValueError) as cm:
            result = f(x=0, y=3.)
        self.assertEqual(str(cm.exception),
                         "input 'x' for operation 'test.core.test_op.f' must be in range [0.1, 0.9]")

        with self.assertRaises(ValueError) as cm:
            result = f(x='A', y=3.)
        self.assertEqual(str(cm.exception),
                         "input 'x' for operation 'test.core.test_op.f' must be of type 'float', "
                         "but got type 'str'")

        with self.assertRaises(ValueError) as cm:
            result = f(x=0.4)
        self.assertEqual(str(cm.exception),
                         "input 'y' for operation 'test.core.test_op.f' required")

        with self.assertRaises(ValueError) as cm:
            result = f(x=0.6, y=0.1, a=2)
        self.assertEqual(str(cm.exception),
                         "input 'a' for operation 'test.core.test_op.f' must be one of [1, 4, 5]")

        with self.assertRaises(ValueError) as cm:
            f(y=3, a=5)
        self.assertEqual(str(cm.exception),
                         "output 'return' for operation 'test.core.test_op.f' must be of type 'float', "
                         "but got type 'str'")

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
