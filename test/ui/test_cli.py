import json
import os
import os.path
import shutil
import sys
import unittest
from time import sleep
from typing import Union, List

from ect.core.io import DATA_STORE_REGISTRY
from ect.core.monitor import Monitor
from ect.core.op import OP_REGISTRY
from ect.core.util import fetch_std_streams
from ect.ds.esa_cci_odp import EsaCciOdpDataStore
from ect.ui import cli
from ect.ui.workspace import FSWorkspaceManager


def _create_test_data_store():
    with open(os.path.join(os.path.dirname(__file__), '..', 'ds', 'esgf-index-cache.json')) as fp:
        json_text = fp.read()
    json_dict = json.loads(json_text)
    # The EsaCciOdpDataStore created with an initial json_dict avoids fetching it from remote
    return EsaCciOdpDataStore(index_cache_json_dict=json_dict)


class CliTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        DATA_STORE_REGISTRY.add_data_store("default", _create_test_data_store())

    def assert_main(self,
                    args: Union[None, List[str]],
                    expected_status: int = 0,
                    expected_stdout: Union[None, str, List[str]] = None,
                    expected_stderr: Union[None, str, List[str]] = '') -> None:
        with fetch_std_streams() as (stdout, stderr):
            actual_status = cli.main(args=args)
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


class CliTest(CliTestCase):
    def test_noargs(self):
        sys.argv = []
        self.assert_main(None)

    def test_invalid_command(self):
        self.assert_main(['pipo'], expected_status=2, expected_stderr=None)

    def test_option_version(self):
        self.assert_main(['--version'])

    def test_option_traceback(self):
        self.assert_main(['--traceback'])

    def test_option_help(self):
        self.assert_main(['-h'])
        self.assert_main(['--help'])

    def test_parse_open_arg(self):
        self.assertEqual(cli._parse_open_arg('sst2011=SST_LT_ATSR_L3U_V01.0_ATSR1'),
                         ('sst2011', 'SST_LT_ATSR_L3U_V01.0_ATSR1', None, None))
        self.assertEqual(cli._parse_open_arg('sst2011=SST_LT_ATSR_L3U_V01.0_ATSR1,2011'),
                         ('sst2011', 'SST_LT_ATSR_L3U_V01.0_ATSR1', '2011', None))
        self.assertEqual(cli._parse_open_arg('SST_LT_ATSR_L3U_V01.0_ATSR1,,2012'),
                         (None, 'SST_LT_ATSR_L3U_V01.0_ATSR1', None, '2012'))
        self.assertEqual(cli._parse_open_arg('=SST_LT_ATSR_L3U_V01.0_ATSR1'),
                         (None, 'SST_LT_ATSR_L3U_V01.0_ATSR1', None, None))
        self.assertEqual(cli._parse_open_arg('sst2011='),
                         ('sst2011', None, None, None))

    def test_parse_write_arg(self):
        self.assertEqual(cli._parse_write_arg('/home/norman/data'), (None, '/home/norman/data', None))
        self.assertEqual(cli._parse_write_arg('/home/norman/.git'), (None, '/home/norman/.git', None))
        self.assertEqual(cli._parse_write_arg('/home/norman/im.png'), (None, '/home/norman/im.png', None))
        self.assertEqual(cli._parse_write_arg('/home/norman/im.png,PNG'), (None, '/home/norman/im.png', 'PNG'))
        self.assertEqual(cli._parse_write_arg('ds=/home/norman/data.nc,netcdf4'),
                         ('ds', '/home/norman/data.nc', 'NETCDF4'))


class WorkspaceCommandTest(CliTestCase):
    def setUp(self):
        self.remove_tree('.ect-workspace', ignore_errors=False)

        # NOTE: We use the same workspace manager instance in between cli.main() calls to simulate a stateful-service
        self.cli_workspace_manager_factory = cli.WORKSPACE_MANAGER_FACTORY
        self.workspace_manager = FSWorkspaceManager()
        cli.WORKSPACE_MANAGER_FACTORY = lambda: self.workspace_manager

    def tearDown(self):
        cli.WORKSPACE_MANAGER_FACTORY = self.cli_workspace_manager_factory
        self.remove_tree('.ect-workspace', ignore_errors=False)

    def assert_workspace_base_dir(self, base_dir):
        self.assertTrue(os.path.isdir(base_dir))
        self.assertTrue(os.path.isdir(os.path.join(base_dir, '.ect-workspace')))
        self.assertTrue(os.path.isfile(os.path.join(base_dir, '.ect-workspace', 'workflow.json')))

    def test_ws_init_arg(self):
        base_dir = 'my_workspace'
        self.assert_main(['ws', 'init', base_dir], expected_stdout=['Workspace initialized'])
        self.assert_workspace_base_dir(base_dir)
        self.assert_main(['ws', 'init', base_dir], expected_stderr=['workspace exists: '], expected_status=1)
        self.assert_main(['ws', 'del', '-y', base_dir], expected_stdout=['Workspace deleted'])
        self.remove_tree('my_workspace')

    def test_ws_init(self):
        self.assert_main(['ws', 'init'], expected_stdout=['Workspace initialized'])
        self.assert_workspace_base_dir('.')
        self.assert_main(['ws', 'init'],
                         expected_stderr=['workspace exists: '],
                         expected_status=1)

    def test_ws_del(self):
        base_dir = 'my_workspace'
        self.assert_main(['ws', 'init', base_dir], expected_stdout=['Workspace initialized'])
        self.assert_main(['ws', 'del', '-y', base_dir], expected_stdout=['Workspace deleted'])
        self.assert_main(['ws', 'del', '-y', base_dir],
                         expected_stderr=['ect ws: error: not a workspace: '],
                         expected_status=1)
        self.remove_tree('my_workspace')

    def test_ws_clean(self):
        self.assert_main(['ws', 'init'], expected_stdout=['Workspace initialized'])
        self.assert_main(['res', 'read', 'ds', 'test.nc'], expected_stdout=['Resource "ds" set.'])
        self.assert_main(['ws', 'clean', '-y'], expected_stdout=['Workspace cleaned'])


class ResourceCommandTest(CliTestCase):
    def setUp(self):
        # NOTE: We use the same workspace manager instance in between cli.main() calls to simulate a stateful-service
        self.cli_workspace_manager_factory = cli.WORKSPACE_MANAGER_FACTORY
        self.workspace_manager = FSWorkspaceManager()
        cli.WORKSPACE_MANAGER_FACTORY = lambda: self.workspace_manager

    def tearDown(self):
        cli.WORKSPACE_MANAGER_FACTORY = self.cli_workspace_manager_factory

    def test_res_read_set_write(self):
        input_file = os.path.join(os.path.dirname(__file__), 'precip_and_temp.nc')
        output_file = '_timeseries_.nc'

        self.assert_main(['ws', 'new'],
                         expected_stdout=['Workspace created'])
        self.assert_main(['res', 'read', 'ds', input_file],
                         expected_stdout=['Resource "ds" set.'])
        self.assert_main(['res', 'set', 'ts', 'ect.ops.timeseries.timeseries', 'ds=ds', 'lat=0', 'lon=0'],
                         expected_stdout=['Resource "ts" set.'])
        self.assert_main(['res', 'write', 'ts', output_file],
                         expected_stdout=['Writing resource "ts"'])
        self.assert_main(['ws', 'close'],
                         expected_stdout=['Workspace closed.'])

        self.remove_file(output_file)

    def test_res_open_read_set_set(self):
        self.assert_main(['ws', 'new'],
                         expected_stdout=['Workspace created'])
        self.assert_main(['res', 'open', 'ds1', 'SOIL_MOISTURE_DAILY_FILES_ACTIVE_V02.2', '2010'],
                         expected_stdout=['Resource "ds1" set.'])
        self.assert_main(['res', 'read', 'ds2', 'precip_and_temp.nc'],
                         expected_stdout=['Resource "ds2" set.'])
        self.assert_main(['res', 'set', 'ts', 'ect.ops.timeseries.timeseries', 'ds=ds2', 'lat=13.2', 'lon=52.9'],
                         expected_stdout=['Resource "ts" set.'])
        self.assert_main(['ws', 'status'],
                         expected_stdout=
                         ['Workspace resources:',
                          '  ds1 = ect.ops.io.open_dataset(ds_name=\'SOIL_MOISTURE_DAILY_FILES_ACTIVE_V02.2\', '
                          'start_date=\'2010\', end_date=None, sync=True) [OpStep]',
                          '  ds2 = ect.ops.io.read_object(file=\'precip_and_temp.nc\', format=None) [OpStep]\n',
                          '  ts = ect.ops.timeseries.timeseries(ds=ds2, lat=13.2, lon=52.9, method=\'nearest\') [OpStep]'])

        self.assert_main(['res', 'set', 'ts', 'ect.ops.timeseries.timeseries', 'ds=ds2', 'lat=-10.4', 'lon=176'],
                         expected_stdout=['Resource "ts" set.'])
        self.assert_main(['ws', 'status'],
                         expected_stdout=
                         ['Workspace resources:',
                          '  ds1 = ect.ops.io.open_dataset(ds_name=\'SOIL_MOISTURE_DAILY_FILES_ACTIVE_V02.2\', '
                          'start_date=\'2010\', end_date=None, sync=True) [OpStep]',
                          '  ds2 = ect.ops.io.read_object(file=\'precip_and_temp.nc\', format=None) [OpStep]\n',
                          '  ts = ect.ops.timeseries.timeseries(ds=ds2, lat=-10.4, lon=176, method=\'nearest\') [OpStep]'])

        self.assert_main(['res', 'set', 'ts', 'ect.ops.timeseries.timeseries', 'ds=ds2', 'lat="XYZ"', 'lon=50.1'],
                         expected_status=1,
                         expected_stderr=["ect res: error: input 'lat' for operation 'ect.ops.timeseries.timeseries' "
                                          "must be of type 'float', but got type 'str'"])

        self.assert_main(['ws', 'close'], expected_stdout=['Workspace closed.'])


class OperationCommandTest(CliTestCase):
    def test_op_info(self):
        self.assert_main(['op', 'info', 'ect.ops.timeseries.timeseries'],
                         expected_stdout=['Extract time-series'])
        self.assert_main(['op', 'info', 'foobarbaz'],
                         expected_status=1,
                         expected_stdout='',
                         expected_stderr=['ect op: error: unknown operation "foobarbaz"'])
        self.assert_main(['op', 'info'],
                         expected_status=2,
                         expected_stdout='',
                         expected_stderr=["ect op info: error: the following arguments are required: OP"])

    def test_op_list(self):
        self.assert_main(['op', 'list'], expected_stdout=['operations found'])
        self.assert_main(['op', 'list', '-n', 'read'], expected_stdout=['operations found'])
        self.assert_main(['op', 'list', '-n', 'nevermatch'], expected_stdout=['No operations found'])
        self.assert_main(['op', 'list', '--tag', 'io'], expected_stdout=['14 operations found'])


class DataSourceCommandTest(CliTestCase):
    def test_ds_info(self):
        self.assert_main(['ds', 'info', 'esacci.OZONE.mon.L3.NP.multi-sensor.multi-platform.MERGED.fv0002.r1'],
                         expected_status=0,
                         expected_stdout=['Data source esacci.OZONE.mon.L3.',
                                          'cci_project:            OZONE'])
        self.assert_main(['ds', 'info', 'esacci.OZONE.mon.L3.NP.multi-sensor.multi-platform.MERGED.fv0002.r1', '--var'],
                         expected_status=0,
                         expected_stdout=['Data source esacci.OZONE.mon.L3.',
                                          'cci_project:            OZONE',
                                          'air_pressure (hPa):'])
        self.assert_main(['ds', 'info', 'SOIL_MOISTURE_DAILY_FILES_ACTIVE_V02.2'],
                         expected_status=1,
                         expected_stderr=['data source "SOIL_MOISTURE_DAILY_FILES_ACTIVE_V02.2" not found'])

    def test_ds_list(self):
        self.assert_main(['ds', 'list'],
                         expected_stdout=['61 data sources found'])
        self.assert_main(['ds', 'list', '--name', 'CLOUD'],
                         expected_stdout=['14 data sources found'])

    @unittest.skip(reason="skipped unless you want to debug data source synchronisation")
    def test_ds_sync(self):
        self.assert_main(['ds', 'sync', 'esacci.OZONE.mon.L3.NP.multi-sensor.multi-platform.MERGED.fv0002.r1'])

    @unittest.skip(reason="skipped unless you want to debug data source synchronisation")
    def test_ds_sync_with_period(self):
        self.assert_main(
            ['ds', 'sync', 'esacci.OZONE.mon.L3.NP.multi-sensor.multi-platform.MERGED.fv0002.r1', '--time', '2010-12'])

    def test_ds(self):
        self.assert_main(['ds'],
                         expected_stdout="usage: ect ds [-h] COMMAND ...\n"
                                         "\n"
                                         "Manage data sources.\n"
                                         "\n"
                                         "positional arguments:\n"
                                         "  COMMAND     One of the following commands. Type \"COMMAND -h\" for help.\n"
                                         "    list      List all available data sources\n"
                                         "    sync      Synchronise a remote data source with its local version.\n"
                                         "    info      Display information about a data source.\n"
                                         "\n"
                                         "optional arguments:\n"
                                         "  -h, --help  show this help message and exit\n")


class RunCommandTest(CliTestCase):
    def test_run(self):
        self.assert_main(['run'],
                         expected_status=2,
                         expected_stdout='',
                         expected_stderr=["ect run: error: the following arguments are required: OP, ..."])

    def test_run_foobar(self):
        self.assert_main(['run', 'foobar', 'lat=13.2', 'lon=52.9'],
                         expected_status=1,
                         expected_stdout='',
                         expected_stderr='ect run: error: unknown operation "foobar"\n')

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
                             expected_stderr=["ect run: error: 'l*t' is not a valid input name"],
                             expected_stdout='')

        finally:
            OP_REGISTRY.remove_op(op_reg.operation, fail_if_not_exists=True)

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
            OP_REGISTRY.remove_op(op_reg.operation, fail_if_not_exists=True)

    def test_run_help(self):
        self.assert_main(['run', '-h'])
        self.assert_main(['run', '--help'])


class PluginCommandTest(CliTestCase):
    def test_pi_list(self):
        self.assert_main(['pi', 'list'], expected_stdout=['plugins found'])


class LicenseCommandTest(CliTestCase):
    def test_lic(self):
        self.assert_main(['lic'], expected_stdout=['MIT License'])


def timeseries(lat: float, lon: float, method: str = 'nearest', monitor=Monitor.NULL) -> list:
    """Timeseries dummy function for testing."""
    print('lat=%s lon=%s method=%s' % (lat, lon, method))
    work_units = [0.3, 0.25, 0.05, 0.4, 0.2, 0.1, 0.5]
    with monitor.starting('Extracting timeseries data', sum(work_units)):
        for work_unit in work_units:
            sleep(work_unit / 10.)
            monitor.progress(work_unit)
    return work_units


def timeseries2(var, lat: float, lon: float, method: str = 'nearest', monitor=Monitor.NULL) -> list:
    """Timeseries dummy function for testing."""
    print('lat=%s lon=%s method=%s' % (lat, lon, method))
    work_units = [0.3, 0.25, 0.05, 0.4, 0.2, 0.1, 0.5]
    with monitor.starting('Extracting timeseries data', sum(work_units)):
        for work_unit in work_units:
            sleep(work_unit / 10.)
            monitor.progress(work_unit)
    ts = var[0, 0]
    return ts
