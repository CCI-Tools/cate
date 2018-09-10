import json
import os
import os.path
import shutil
import sys
import unittest
import datetime
from collections import OrderedDict
from time import sleep
from typing import Union, List
from unittest import TestCase

from cate.cli import main
from cate.core.ds import DATA_STORE_REGISTRY
from cate.core.op import OP_REGISTRY
from cate.core.types import PointLike, TimeRangeLike
from cate.core.wsmanag import FSWorkspaceManager
from cate.ds.esa_cci_odp import EsaCciOdpDataStore
from cate.util.misc import fetch_std_streams
from cate.util.monitor import Monitor

NETCDF_TEST_FILE = os.path.join(os.path.dirname(__file__), '..', 'data', 'precip_and_temp.nc')


def _create_test_data_store():
    with open(os.path.join(os.path.dirname(__file__), '..', 'ds', 'esgf-index-cache.json')) as fp:
        json_text = fp.read()
    json_dict = json.loads(json_text)

    # The EsaCciOdpDataStore created with an initial json_dict avoids fetching it from remote
    DS = EsaCciOdpDataStore('test-odp', index_cache_json_dict=json_dict, index_cache_update_tag='test2')
    return DS


class CliTestCase(unittest.TestCase):
    _orig_stores = None

    @classmethod
    def setUpClass(cls):
        cls._orig_stores = list(DATA_STORE_REGISTRY.get_data_stores())
        DATA_STORE_REGISTRY._data_stores.clear()
        DATA_STORE_REGISTRY.add_data_store(_create_test_data_store())

    @classmethod
    def tearDownClass(cls):
        # clean up frozen files
        for d in DATA_STORE_REGISTRY.get_data_stores():
            d.get_updates(reset=True)

        DATA_STORE_REGISTRY._data_stores.clear()
        for data_store in cls._orig_stores:
            DATA_STORE_REGISTRY.add_data_store(data_store)

    def assert_main(self,
                    args: Union[None, List[str]],
                    expected_status: int = 0,
                    expected_stdout: Union[None, str, List[str]] = None,
                    expected_stderr: Union[None, str, List[str]] = '') -> None:
        with fetch_std_streams() as (stdout, stderr):
            actual_status = main.main(args=args)
            self.assertEqual(actual_status, expected_status,
                             msg='args = %s\n'
                                 'status = %s\n'
                                 'stdout = [%s]\n'
                                 'stderr = [%s]' % (args, actual_status, stdout.getvalue(), stderr.getvalue()))
        print(stdout.getvalue())
        if isinstance(expected_stdout, str):
            self.assertEqual(expected_stdout, stdout.getvalue())
        elif expected_stdout:
            for item in expected_stdout:
                self.assertIn(item, stdout.getvalue())

        if isinstance(expected_stderr, str):
            self.assertEqual(expected_stderr, stderr.getvalue())
        elif expected_stderr:
            for item in expected_stderr:
                self.assertIn(item, stderr.getvalue())

    def remove_file(self, file_path, ignore_errors=True):
        if os.path.exists(file_path):
            os.remove(file_path)
        if ignore_errors and os.path.isfile(file_path):
            self.fail("Can't remove file %s" % file_path)

    def remove_tree(self, dir_path, ignore_errors=True):
        if os.path.exists(dir_path):
            shutil.rmtree(dir_path, ignore_errors=ignore_errors)
        if ignore_errors and os.path.isdir(dir_path):
            self.fail("Can't remove dir %s" % dir_path)

    def create_catalog_differences(self, new_ds):
        for d in DATA_STORE_REGISTRY.get_data_stores():
            diff_file = os.path.join(d.data_store_path, d._get_update_tag() + '-diff.json')
            if os.path.isfile(diff_file):
                with open(diff_file, 'r') as json_in:
                    report = json.load(json_in)
                report['new'].append(new_ds)
            else:
                generated = datetime.datetime.now()
                report = {"generated": str(generated),
                          "source_ref_time": str(generated),
                          "new": [new_ds],
                          "del": list()}
            with open(diff_file, 'w') as json_out:
                json.dump(report, json_out)


class CliTest(CliTestCase):
    def test_noargs(self):
        sys.argv = []
        self.assert_main(None)

    def test_invalid_command(self):
        self.assert_main(['pipo'], expected_status=2, expected_stderr=None)

    def test_option_version(self):
        self.assert_main(['--version'], expected_stdout=['cate '])

    def test_option_license(self):
        self.assert_main(['--license'], expected_stdout=['MIT License'])

    def test_option_traceback(self):
        self.assert_main(['--traceback'])

    def test_option_help(self):
        self.assert_main(['-h'])
        self.assert_main(['--help'])

    def test_parse_open_arg(self):
        self.assertEqual(main._parse_open_arg('sst2011=SST_LT_ATSR_L3U_V01.0_ATSR1'),
                         ('sst2011', 'SST_LT_ATSR_L3U_V01.0_ATSR1', None, None))
        self.assertEqual(main._parse_open_arg('sst2011=SST_LT_ATSR_L3U_V01.0_ATSR1,2011'),
                         ('sst2011', 'SST_LT_ATSR_L3U_V01.0_ATSR1', '2011', None))
        self.assertEqual(main._parse_open_arg('SST_LT_ATSR_L3U_V01.0_ATSR1,,2012'),
                         (None, 'SST_LT_ATSR_L3U_V01.0_ATSR1', None, '2012'))
        self.assertEqual(main._parse_open_arg('=SST_LT_ATSR_L3U_V01.0_ATSR1'),
                         (None, 'SST_LT_ATSR_L3U_V01.0_ATSR1', None, None))
        self.assertEqual(main._parse_open_arg('sst2011='),
                         ('sst2011', None, None, None))

    def test_parse_write_arg(self):
        self.assertEqual(main._parse_write_arg('/home/norman/data'), (None, '/home/norman/data', None))
        self.assertEqual(main._parse_write_arg('/home/norman/.git'), (None, '/home/norman/.git', None))
        self.assertEqual(main._parse_write_arg('/home/norman/im.png'), (None, '/home/norman/im.png', None))
        self.assertEqual(main._parse_write_arg('/home/norman/im.png,PNG'), (None, '/home/norman/im.png', 'PNG'))
        self.assertEqual(main._parse_write_arg('ds=/home/norman/data.nc,netcdf4'),
                         ('ds', '/home/norman/data.nc', 'NETCDF4'))


class WorkspaceCommandTest(CliTestCase):
    def setUp(self):
        self.remove_tree('.cate-workspace', ignore_errors=False)

        # NOTE: We use the same workspace manager instance in between cli.main() calls to simulate a stateful-service
        self.cli_workspace_manager_factory = main.WORKSPACE_MANAGER_FACTORY
        self.workspace_manager = FSWorkspaceManager()
        main.WORKSPACE_MANAGER_FACTORY = lambda: self.workspace_manager

    def tearDown(self):
        main.WORKSPACE_MANAGER_FACTORY = self.cli_workspace_manager_factory
        self.remove_tree('.cate-workspace', ignore_errors=False)

    def assert_workspace_base_dir(self, base_dir):
        self.assertTrue(os.path.isdir(base_dir))
        self.assertTrue(os.path.isdir(os.path.join(base_dir, '.cate-workspace')))
        self.assertTrue(os.path.isfile(os.path.join(base_dir, '.cate-workspace', 'workflow.json')))

    def test_ws_init_arg(self):
        base_dir = 'my_workspace'
        self.assert_main(['ws', 'init', '-d', base_dir], expected_stdout=['Workspace initialized'])
        self.assert_workspace_base_dir(base_dir)
        self.assert_main(['ws', 'init', '-d', base_dir],
                         expected_stderr=['Workspace already opened: '],
                         expected_status=1)
        self.assert_main(['ws', 'del', '-y', '-d', base_dir], expected_stdout=['Workspace deleted'])
        self.remove_tree('my_workspace')

    def test_ws_init(self):
        self.assert_main(['ws', 'init'], expected_stdout=['Workspace initialized'])
        self.assert_workspace_base_dir('.')
        self.assert_main(['ws', 'init'],
                         expected_stderr=['Workspace already opened: '],
                         expected_status=1)

    def test_ws_del(self):
        base_dir = 'my_workspace'
        self.assert_main(['ws', 'init', '-d', base_dir], expected_stdout=['Workspace initialized'])
        self.assert_main(['ws', 'del', '-y', '-d', base_dir], expected_stdout=['Workspace deleted'])
        self.assert_main(['ws', 'del', '-y', '-d', base_dir],
                         expected_stderr=['cate ws: error: Not a workspace: '],
                         expected_status=1)
        self.remove_tree('my_workspace')

    def test_ws_clean(self):
        self.assert_main(['ws', 'init'], expected_stdout=['Workspace initialized'])
        self.assert_main(['res', 'read', 'ds', NETCDF_TEST_FILE], expected_stdout=['Resource "ds" set.'])
        self.assert_main(['ws', 'clean', '-y'], expected_stdout=['Workspace cleaned'])


class ResourceCommandTest(CliTestCase):
    def setUp(self):
        # NOTE: We use the same workspace manager instance in between cli.main() calls to simulate a stateful-service
        self.cli_workspace_manager_factory = main.WORKSPACE_MANAGER_FACTORY
        self.workspace_manager = FSWorkspaceManager()
        main.WORKSPACE_MANAGER_FACTORY = lambda: self.workspace_manager

    def tearDown(self):
        main.WORKSPACE_MANAGER_FACTORY = self.cli_workspace_manager_factory

    def test_res_read_set_write(self):
        input_file = NETCDF_TEST_FILE
        output_file = '_timeseries_.nc'

        self.assert_main(['ws', 'new'],
                         expected_stdout=['Workspace created'])
        self.assert_main(['res', 'read', 'ds', input_file],
                         expected_stdout=['Resource "ds" set.'])
        self.assert_main(['res', 'set', 'ts', 'cate.ops.timeseries.tseries_mean', 'ds=@ds', 'var=temperature'],
                         expected_stdout=['Resource "ts" set.'])
        self.assert_main(['res', 'write', 'ts', output_file],
                         expected_stdout=['Writing resource "ts"'])
        self.assert_main(['ws', 'close'],
                         expected_stdout=['Workspace closed.'])

        self.remove_file(output_file)

    def test_res_read_rename(self):
        input_file = NETCDF_TEST_FILE

        self.assert_main(['ws', 'new'],
                         expected_stdout=['Workspace created'])
        self.assert_main(['res', 'read', 'ds', input_file],
                         expected_stdout=['Resource "ds" set.'])
        self.assert_main(['res', 'rename', 'ds', 'myDS'],
                         expected_stdout=['Resource "ds" renamed to "myDS".'])

    def test_res_read_rename_unique(self):
        input_file = NETCDF_TEST_FILE

        self.assert_main(['ws', 'new'],
                         expected_stdout=['Workspace created'])
        self.assert_main(['res', 'read', 'ds1', input_file],
                         expected_stdout=['Resource "ds1" set.'])
        self.assert_main(['res', 'read', 'ds2', input_file],
                         expected_stdout=['Resource "ds2" set.'])
        self.assert_main(['res', 'rename', 'ds1', 'ds2'],
                         expected_status=1,
                         expected_stderr=['Resource "ds1" cannot be renamed to "ds2", '
                                          'because "ds2" is already in use.'])

    def test_res_open_read_set_set(self):
        self.assert_main(['ws', 'new'],
                         expected_stdout=['Workspace created'])
        self.assert_main(['res', 'read', 'ds1', NETCDF_TEST_FILE],
                         expected_stdout=['Resource "ds1" set.'])
        self.assert_main(['res', 'read', 'ds2', NETCDF_TEST_FILE],
                         expected_stdout=['Resource "ds2" set.'])
        self.assert_main(['res', 'set', 'ts', 'cate.ops.timeseries.tseries_mean', 'ds=@ds2', 'var=temperature'],
                         expected_stdout=['Resource "ts" set.'])
        self.assert_main(['ws', 'status'],
                         expected_stdout=[
                             'Workspace resources:',
                             '  ds1 = cate.ops.io.read_object('
                             'file=%s, format=None) [OpStep]' % NETCDF_TEST_FILE,
                             '  ds2 = cate.ops.io.read_object('
                             'file=%s, format=None) [OpStep]' % NETCDF_TEST_FILE,
                             '  ts = cate.ops.timeseries.tseries_mean('
                             'ds=@ds2, var=temperature, std_suffix=_std, calculate_std=True) [OpStep]'])

        self.assert_main(['res', 'set', 'ts', 'cate.ops.timeseries.tseries_mean', 'ds=@ds2', 'var=temperature'],
                         expected_status=1,
                         expected_stderr=[
                             'cate res: error: A resource named "ts" already exists'])

        self.assert_main(['res', 'set', '-o', 'ts', 'cate.ops.timeseries.tseries_mean', 'ds=@ds2', 'var=temperature'],
                         expected_stdout=['Resource "ts" set.'])
        self.assert_main(['ws', 'status'],
                         expected_stdout=[
                             'Workspace resources:',
                             '  ds1 = cate.ops.io.read_object('
                             'file=%s, format=None) [OpStep]' % NETCDF_TEST_FILE,
                             '  ds2 = cate.ops.io.read_object('
                             'file=%s, format=None) [OpStep]' % NETCDF_TEST_FILE,
                             '  ts = cate.ops.timeseries.tseries_mean('
                             'ds=@ds2, var=temperature, std_suffix=_std, calculate_std=True) [OpStep]'])

        self.assert_main(['res', 'set', 'ts',
                          'cate.ops.timeseries.tseries_point', 'ds=@ds2', 'point=XYZ',
                          'var=temperature'],
                         expected_status=1,
                         expected_stderr=[
                             "cate res: error: value <XYZ> for input 'point' is not compatible with type PointLike"])

        self.assert_main(['ws', 'close'], expected_stdout=['Workspace closed.'])


class OperationCommandTest(CliTestCase):
    def test_op_info(self):
        self.assert_main(['op', 'info', 'cate.ops.timeseries.tseries_point'],
                         expected_stdout=['Extract time-series'])
        self.assert_main(['op', 'info', 'foobarbaz'],
                         expected_status=1,
                         expected_stdout='',
                         expected_stderr=['cate op: error: unknown operation "foobarbaz"'])
        self.assert_main(['op', 'info'],
                         expected_status=2,
                         expected_stdout='',
                         expected_stderr=["cate op info: error: the following arguments are required: OP"])

    def test_op_list(self):
        self.assert_main(['op', 'list'], expected_stdout=['operations found'])
        self.assert_main(['op', 'list', '-n', 'read'], expected_stdout=['operations found'])
        self.assert_main(['op', 'list', '-n', 'nevermatch'], expected_stdout=['No operations found'])
        self.assert_main(['op', 'list', '--internal'], expected_stdout=['2 operations found'])
        self.assert_main(['op', 'list', '--tag', 'input'], expected_stdout=['9 operations found'])
        self.assert_main(['op', 'list', '--tag', 'output'], expected_stdout=['6 operations found'])
        self.assert_main(['op', 'list', '--deprecated'], expected_stdout=['2 operations found'])


'''
@unittest.skip(reason='Hardcoded values from remote service, contains outdated assumptions')
'''


class DataSourceCommandTest(CliTestCase):
    def test_ds_info(self):
        self.assert_main(['ds', 'info', 'esacci.OZONE.mon.L3.NP.multi-sensor.multi-platform.MERGED.fv0002.r1'],
                         expected_status=0,
                         expected_stdout=['Data source esacci.OZONE.mon.L3.',
                                          'project:                  esacci'])
        self.assert_main(['ds', 'info', 'esacci.OZONE.mon.L3.NP.multi-sensor.multi-platform.MERGED.fv0002.r1', '--var'],
                         expected_status=0,
                         expected_stdout=['Data source esacci.OZONE.mon.L3.',
                                          'project:                  esacci',
                                          'air_pressure (hPa):'])
        self.assert_main(['ds', 'info', 'SOIL_MOISTURE_DAILY_FILES_ACTIVE_V02.2'],
                         expected_status=1,
                         expected_stderr=['data source "SOIL_MOISTURE_DAILY_FILES_ACTIVE_V02.2" not found'])

    def test_ds_list(self):
        self.assert_main(['ds', 'list'],
                         expected_stdout=['61 data sources found'])
        self.assert_main(['ds', 'list', '--name', 'CLOUD'],
                         expected_stdout=['14 data sources found'])

    def test_ds_update(self):
        self.assert_main(['ds', 'list', '-u'],
                         expected_stdout=['All datastores are up to date.'])
        self.create_catalog_differences('test_diff_new')
        self.assert_main(['ds', 'list', '-u'],
                         expected_stdout=['test_diff_new'])

    @unittest.skip(reason="skipped unless you want to debug data source synchronisation")
    def test_ds_copy(self):
        self.assert_main(['ds', 'copy', 'esacci.OZONE.mon.L3.NP.multi-sensor.multi-platform.MERGED.fv0002.r1'])

    @unittest.skip(reason="skipped unless you want to debug data source synchronisation")
    def test_ds_copy_with_period(self):
        self.assert_main(
            ['ds', 'copy', 'esacci.OZONE.mon.L3.NP.multi-sensor.multi-platform.MERGED.fv0002.r1',
             't=2007-12-01,2007-12-31'])

    def test_ds(self):
        self.assert_main(['ds'],
                         expected_stdout=["usage: cate ds [-h] COMMAND ..."])


class RunCommandTest(CliTestCase):
    def test_run(self):
        self.assert_main(['run'],
                         expected_status=2,
                         expected_stdout='',
                         expected_stderr=["cate run: error: the following arguments are required: OP, ..."])

    def test_run_foobar(self):
        self.assert_main(['run', 'foobar', 'lat=13.2', 'lon=52.9'],
                         expected_status=1,
                         expected_stdout='',
                         expected_stderr='cate run: error: unknown operation "foobar"\n')

    def test_run_op(self):
        op_reg = OP_REGISTRY.add_op(timeseries, fail_if_exists=True)

        try:
            # Run without --monitor and --write
            self.assert_main(['run', op_reg.op_meta_info.qualified_name, 'lat=13.2', 'lon=52.9'],
                             expected_stdout=['[0.3, 0.25, 0.05, 0.4, 0.2, 0.1, 0.5]'])

            # Run with --monitor and without --write
            self.assert_main(['run', '--monitor', op_reg.op_meta_info.qualified_name, 'lat=13.2', 'lon=52.9'],
                             expected_stdout=['[0.3, 0.25, 0.05, 0.4, 0.2, 0.1, 0.5]'])

            # Run with --monitor and --write
            self.assert_main(['run', '--monitor', '--write', 'timeseries_data.txt',
                              op_reg.op_meta_info.qualified_name, 'lat=13.2', 'lon=52.9'],
                             expected_stdout=['Writing output to timeseries_data.txt using TEXT format...'])
            self.assertTrue(os.path.isfile('timeseries_data.txt'))
            os.remove('timeseries_data.txt')

            # Run with invalid keyword
            self.assert_main(['run', op_reg.op_meta_info.qualified_name, 'l*t=13.2', 'lon=52.9'],
                             expected_status=1,
                             expected_stderr=['cate run: error: "l*t" is not a valid input name'],
                             expected_stdout='')

        finally:
            OP_REGISTRY.remove_op(op_reg, fail_if_not_exists=True)

    def test_run_workflow(self):

        op_reg = OP_REGISTRY.add_op(timeseries, fail_if_exists=True)

        workflow_file = os.path.join(os.path.dirname(__file__), 'timeseries.json')
        self.assertTrue(os.path.exists(workflow_file), msg='missing test file %s' % workflow_file)

        try:
            # Run without --monitor and --write
            self.assert_main(['run', workflow_file, 'lat=13.2', 'lon=52.9'],
                             expected_stdout=['[0.3, 0.25, 0.05, 0.4, 0.2, 0.1, 0.5]'])

            # Run with --monitor and without --write
            self.assert_main(['run', '--monitor', workflow_file, 'lat=13.2', 'lon=52.9'],
                             expected_stdout=['[0.3, 0.25, 0.05, 0.4, 0.2, 0.1, 0.5]'])

            # Run with --monitor and --write
            self.assert_main(['run', '--monitor', '--write', 'timeseries_data.json',
                              workflow_file, 'lat=13.2', 'lon=52.9'],
                             expected_stdout=['Writing output to timeseries_data.json using JSON format...'])
            self.assertTrue(os.path.isfile('timeseries_data.json'))
            os.remove('timeseries_data.json')

        finally:
            OP_REGISTRY.remove_op(op_reg, fail_if_not_exists=True)

    def test_main_options(self):
        self.assert_main(['run', '-h'])
        self.assert_main(['run', '--help'])


# Tests for "cate upd" may be skipped because they can be very slow

@unittest.skipIf(os.environ.get('CATE_DISABLE_CLI_UPDATE_TESTS', None) == '1', 'CATE_DISABLE_CLI_UPDATE_TESTS = 1')
class UpdateCommandTest(CliTestCase):
    def test_upd_info(self):
        self.assert_main(['upd', '--info'],
                         expected_status=0,
                         expected_stdout=['Latest version is ', 'Current version is',
                                          'Available versions'],
                         expected_stderr='')
        self.assert_main(['upd', '--info', '1.0.0'],
                         expected_status=0,
                         expected_stdout=['Latest version is ', 'Current version is',
                                          'Desired version is 1.0.0 (available)', 'Available versions'],
                         expected_stderr='')

    def test_upd(self):
        self.assert_main(['upd', '--dry-run'],
                         expected_status=0,
                         expected_stdout=['Current cate version is'],
                         expected_stderr='')
        self.assert_main(['upd', '--dry-run', '1.0.0'],
                         expected_status=0,
                         expected_stdout=['The following NEW packages will be INSTALLED:', 'cate-cli:'],
                         expected_stderr='')
        self.assert_main(['upd', '--dry-run', '282.2.1'],
                         expected_status=1,
                         expected_stdout='',
                         expected_stderr=['cate upd: error: desired cate version 282.2.1 is not available;',
                                          'type "cate upd --info" to show available versions'])


class IOCommandTest(CliTestCase):
    IO_LIST_OUTPUT = "JSON (*.json) - JSON format (plain text, UTF8)\n" \
                     "NETCDF3 (*.nc) - netCDF 3 file format, which fully supports 2+ GB files.\n" \
                     "NETCDF4 (*.nc) - netCDF 4 file format (HDF5 file format, using netCDF 4 API features)\n" \
                     "TEXT (*.txt) - Plain text format\n"

    def test_io_list(self):
        self.assert_main(['io', 'list', '-r', '-w'],
                         expected_stdout=[IOCommandTest.IO_LIST_OUTPUT])
        self.assert_main(['io', 'list', '-r'],
                         expected_stdout=[IOCommandTest.IO_LIST_OUTPUT])
        self.assert_main(['io', 'list', '-w'],
                         expected_stdout=[IOCommandTest.IO_LIST_OUTPUT])
        self.assert_main(['io', 'list'],
                         expected_stdout=[IOCommandTest.IO_LIST_OUTPUT])


class ParseOpArgsTest(TestCase):
    def test_existing_method(self):
        op = OP_REGISTRY.get_op('cate.ops.timeseries.tseries_point', True)
        op_args, op_kwargs = main._parse_op_args(['ds=@ds', 'point=12.2,54.3', 'var=temperature', 'method=bfill'],
                                                 input_props=op.op_meta_info.inputs)
        self.assertEqual(op_args, [])
        self.assertEqual(op_kwargs, OrderedDict([('ds', dict(source='ds')),
                                                 ('point', dict(value=(12.2, 54.3))),
                                                 ('var', dict(value='temperature')),
                                                 ('method', dict(value='bfill'))]))

    def test_no_namespace(self):
        self.assertEqual(main._parse_op_args([]), ([], OrderedDict()))
        self.assertEqual(main._parse_op_args(['']), ([dict(value=None)], OrderedDict()))
        self.assertEqual(main._parse_op_args(['a=@b']), ([], OrderedDict(a=dict(source='b'))))
        self.assertEqual(main._parse_op_args(['a=@b.x']), ([], OrderedDict(a=dict(source='b.x'))))
        self.assertEqual(main._parse_op_args(['a=b']), ([], OrderedDict(a=dict(value='b'))))
        self.assertEqual(main._parse_op_args(['a="b"']), ([], OrderedDict(a=dict(value='b'))))
        self.assertEqual(main._parse_op_args(['a="C:\\\\Users"']), ([], OrderedDict(a=dict(value='C:\\Users'))))
        self.assertEqual(main._parse_op_args(['a=2', 'b=']), ([], OrderedDict([('a', dict(value=2)),
                                                                               ('b', dict(value=None))])))
        self.assertEqual(main._parse_op_args(['a="c"']), ([], OrderedDict(a=dict(value='c'))))
        self.assertEqual(main._parse_op_args(['a=True']), ([], OrderedDict(a=dict(value=True))))
        self.assertEqual(main._parse_op_args(['z=4.6', 'y=1', 'x=2.+6j']), ([], OrderedDict([('z', dict(value=4.6)),
                                                                                             ('y', dict(value=1)),
                                                                                             ('x',
                                                                                              dict(value=(2 + 6j)))])))

    def test_with_namespace(self):
        class Dataset:
            pass

        ds = Dataset()
        ds.sst = 237.8

        import math as m
        namespace = dict(ds=ds, m=m)

        self.assertEqual(main._parse_op_args(['ds', 'm.pi', 'b=ds.sst + 0.2', 'u=m.cos(m.pi)'], namespace=namespace),
                         ([dict(value=ds),
                           dict(value=m.pi)],
                          OrderedDict([('b', dict(value=238.0)),
                                       ('u', dict(value=m.cos(m.pi)))])))

    def test_with_input_props(self):
        class Dataset:
            pass

        ds = Dataset()
        ds.sst = 237.8

        input_props = dict(a=dict(data_type=PointLike),
                           b=dict(data_type=TimeRangeLike),
                           c=dict(data_type=int))

        self.assertEqual(main._parse_op_args(['a = 11.3, 52.9',
                                              'b = 2001-01-01, 2004-05-06',
                                              'c=8.3',
                                              'd="Bibo"',
                                              'e=ds.sst'],
                                             input_props=input_props,
                                             namespace=dict(ds=ds)),
                         ([], OrderedDict([('a', dict(value=(11.3, 52.9))),
                                           ('b', dict(value='2001-01-01, 2004-05-06')),
                                           ('c', dict(value=8.3)),
                                           ('d', dict(value='Bibo')),
                                           ('e', dict(value=237.8))]))
                         )

    def test_errors(self):
        with self.assertRaises(ValueError) as cm:
            main._parse_op_args(['=9'])
        self.assertEqual(str(cm.exception), "missing input name")

        with self.assertRaises(ValueError) as cm:
            main._parse_op_args(['8=9'])
        self.assertEqual(str(cm.exception), '"8" is not a valid input name')

        with self.assertRaises(ValueError) as cm:
            main._parse_op_args(['thres="x"'], input_props=dict(thres=dict(data_type=float)))
        self.assertEqual(str(cm.exception), "value <\"x\"> for input 'thres' is not compatible with type float")

        with self.assertRaises(ValueError) as cm:
            main._parse_op_args(['thres="x"'], input_props=dict(thres=dict(data_type=PointLike)))
        self.assertEqual(str(cm.exception), "value <\"x\"> for input 'thres' is not compatible with type PointLike")


# class PluginCommandTest(CliTestCase):
#     def test_pi_list(self):
#         self.assert_main(['pi', 'list'], expected_stdout=['plugins found'])


def timeseries(lat: float, lon: float, method: str = 'nearest', monitor=Monitor.NONE) -> list:
    """Timeseries dummy function for testing."""
    print('lat=%s lon=%s method=%s' % (lat, lon, method))
    work_units = [0.3, 0.25, 0.05, 0.4, 0.2, 0.1, 0.5]
    with monitor.starting('Extracting timeseries data', sum(work_units)):
        for work_unit in work_units:
            sleep(work_unit / 10.)
            monitor.progress(work_unit)
    return work_units


def timeseries2(var, lat: float, lon: float, method: str = 'nearest', monitor=Monitor.NONE) -> list:
    """Timeseries dummy function for testing."""
    print('lat=%s lon=%s method=%s' % (lat, lon, method))
    work_units = [0.3, 0.25, 0.05, 0.4, 0.2, 0.1, 0.5]
    with monitor.starting('Extracting timeseries data', sum(work_units)):
        for work_unit in work_units:
            sleep(work_unit / 10.)
            monitor.progress(work_unit)
    ts = var[0, 0]
    return ts
