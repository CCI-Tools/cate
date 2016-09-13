import os
import os.path
import shutil
import sys
import unittest
from time import sleep
from typing import Union, List

from ect.core.monitor import Monitor
from ect.core.op import OP_REGISTRY
from ect.core.util import fetch_std_streams
from ect.ui import cli
from ect.ui.workspace import WORKSPACE_DATA_DIR_NAME


class CliTestCase(unittest.TestCase):
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

    def test_option_help(self):
        self.assert_main(['-h'])
        self.assert_main(['--help'])

    def test_parse_load_arg(self):
        self.assertEqual(cli._parse_load_arg('sst2011=SST_LT_ATSR_L3U_V01.0_ATSR1'),
                         ('sst2011', 'SST_LT_ATSR_L3U_V01.0_ATSR1', None, None))
        self.assertEqual(cli._parse_load_arg('sst2011=SST_LT_ATSR_L3U_V01.0_ATSR1,2011'),
                         ('sst2011', 'SST_LT_ATSR_L3U_V01.0_ATSR1', '2011', None))
        self.assertEqual(cli._parse_load_arg('SST_LT_ATSR_L3U_V01.0_ATSR1,,2012'),
                         (None, 'SST_LT_ATSR_L3U_V01.0_ATSR1', None, '2012'))
        self.assertEqual(cli._parse_load_arg('=SST_LT_ATSR_L3U_V01.0_ATSR1'),
                         (None, 'SST_LT_ATSR_L3U_V01.0_ATSR1', None, None))
        self.assertEqual(cli._parse_load_arg('sst2011='),
                         ('sst2011', None, None, None))

    def test_parse_write_arg(self):
        self.assertEqual(cli._parse_write_arg('/home/norman/data'), (None, '/home/norman/data', None))
        self.assertEqual(cli._parse_write_arg('/home/norman/.git'), (None, '/home/norman/.git', None))
        self.assertEqual(cli._parse_write_arg('/home/norman/im.png'), (None, '/home/norman/im.png', None))
        self.assertEqual(cli._parse_write_arg('/home/norman/im.png,PNG'), (None, '/home/norman/im.png', 'PNG'))
        self.assertEqual(cli._parse_write_arg('ds=/home/norman/data.nc,netcdf4'),
                         ('ds', '/home/norman/data.nc', 'NETCDF4'))


class CliWorkspaceCommandTest(CliTestCase):
    def assert_workspace_base_dir(self, base_dir):
        self.assertTrue(os.path.isdir(base_dir))
        self.assertTrue(os.path.isdir(os.path.join(base_dir, WORKSPACE_DATA_DIR_NAME)))
        self.assertTrue(os.path.isfile(os.path.join(base_dir, WORKSPACE_DATA_DIR_NAME, 'workflow.json')))

    def test_command_ws_init_with_arg(self):
        base_dir = '_bibo_workspace'
        self.remove_tree(base_dir, ignore_errors=False)
        self.assert_main(['ws', 'init', base_dir], expected_stdout=['Workspace initialized'])
        self.assert_workspace_base_dir(base_dir)
        self.assert_main(['ws', 'init', base_dir], expected_stderr=['workspace exists: '], expected_status=1)
        self.remove_tree(base_dir)

    def test_command_ws_init_no_arg(self):
        self.remove_tree(WORKSPACE_DATA_DIR_NAME, ignore_errors=False)
        self.assert_main(['ws', 'init'], expected_stdout=['Workspace initialized'])
        self.assert_workspace_base_dir('.')
        self.assert_main(['ws', 'init'], expected_stderr=['workspace exists: '], expected_status=1)
        self.remove_tree(WORKSPACE_DATA_DIR_NAME)


class CliWorkspaceResourceCommandTest(CliTestCase):
    def setUp(self):
        self.remove_tree(WORKSPACE_DATA_DIR_NAME, ignore_errors=False)

    def tearDown(self):
        self.remove_tree(WORKSPACE_DATA_DIR_NAME)

    def test_command_res(self):
        self.assert_main(['ws', 'init'],
                         expected_stdout=['Workspace initialized'])
        self.assert_main(['res', 'load', 'ds1', 'SOIL_MOISTURE_DAILY_FILES_ACTIVE_V02.2', '2010'],
                         expected_stdout=["Resource 'ds1' set."])
        self.assert_main(['res', 'read', 'ds2', 'precipitation.nc'],
                         expected_stdout=["Resource 'ds2' set."])
        self.assert_main(['res', 'set', 'ts', 'ect.ops.timeseries.timeseries', 'ds=ds2', 'lat=13.2', 'lon=52.9'],
                         expected_stdout=["Resource 'ts' set."])
        self.assert_main(['ws', 'status'],
                         expected_stdout='Workspace steps:\n'
                                         '  ds1 = ect.ops.io.load_dataset(ds_id,start_date,end_date) (OpStep)\n'
                                         '  ds2 = ect.ops.io.read_object(file,format,obj) (OpStep)\n'
                                         '  ts = ect.ops.timeseries.timeseries(ds,lat,lon,method) (OpStep)\n')


class CliOperationCommandTest(CliTestCase):
    def test_command_op_info(self):
        self.assert_main(['op', 'info', 'ect.ops.timeseries.timeseries'],
                         expected_stdout=['Extract time-series'])
        self.assert_main(['op', 'info', 'foobarbaz'],
                         expected_status=2,
                         expected_stdout='',
                         expected_stderr="ect: error: command 'op info': unknown operation 'foobarbaz'\n")
        self.assert_main(['op', 'info'],
                         expected_status=2,
                         expected_stdout='',
                         expected_stderr=["usage: ect op info [-h] OP\n"])

    def test_command_op_list(self):
        self.assert_main(['op', 'list'], expected_stdout=['operations found'])
        self.assert_main(['op', 'list', '-n', '*data*'], expected_stdout=['operations found'])
        self.assert_main(['op', 'list', '-n', 'nevermatch'], expected_stdout=['No operations found'])
        self.assert_main(['op', 'list', '--tag', 'io'], expected_stdout=['11 operations found'])


class CliDataSourceCommandTest(CliTestCase):
    def test_command_ds_info(self):
        self.assert_main(['ds', 'info', 'SOIL_MOISTURE_DAILY_FILES_ACTIVE_V02.2'],
                         expected_stdout=['Base directory'])

    def test_command_ds_list(self):
        self.assert_main(['ds', 'list'],
                         expected_stdout=['98 data sources found'])
        self.assert_main(['ds', 'list', '--id', 'CLOUD*'],
                         expected_stdout=['19 data sources found'])

    @unittest.skip(reason="skipped unless you want to debug data source synchronisation")
    def test_command_ds_sync(self):
        self.assert_main(['ds', 'sync', 'SOIL_MOISTURE_DAILY_FILES_ACTIVE_V02.2'])

    @unittest.skip(reason="skipped unless you want to debug data source synchronisation")
    def test_command_ds_sync_with_period(self):
        self.assert_main(['ds', 'sync', 'SOIL_MOISTURE_DAILY_FILES_ACTIVE_V02.2', '--time', '2010-12'])

    def test_command_ds_parse_time_period(self):
        from ect.ui.cli import DataSourceCommand
        from datetime import date

        self.assertEqual(DataSourceCommand.parse_time_period('2010'), (date(2010, 1, 1), date(2010, 12, 31)))
        self.assertEqual(DataSourceCommand.parse_time_period('2010-02'), (date(2010, 2, 1), date(2010, 2, 28)))
        self.assertEqual(DataSourceCommand.parse_time_period('2010-12'), (date(2010, 12, 1), date(2010, 12, 31)))
        self.assertEqual(DataSourceCommand.parse_time_period('2010-02-04'), (date(2010, 2, 4), date(2010, 2, 4)))
        self.assertEqual(DataSourceCommand.parse_time_period('2010-12-31'), (date(2010, 12, 31), date(2010, 12, 31)))

        self.assertEqual(DataSourceCommand.parse_time_period('2010,2014'), (date(2010, 1, 1), date(2014, 12, 31)))
        self.assertEqual(DataSourceCommand.parse_time_period('2010-02,2010-09'), (date(2010, 2, 1), date(2010, 9, 30)))
        self.assertEqual(DataSourceCommand.parse_time_period('2010-12,2011-12'),
                         (date(2010, 12, 1), date(2011, 12, 31)))
        self.assertEqual(DataSourceCommand.parse_time_period('2010-02-04,2019-02-04'),
                         (date(2010, 2, 4), date(2019, 2, 4)))
        self.assertEqual(DataSourceCommand.parse_time_period('2010-12-31,2010-01-06'),
                         (date(2010, 12, 31), date(2010, 1, 6)))

        # errors
        self.assertEqual(DataSourceCommand.parse_time_period('2010-12-31,2010-01'), None)
        self.assertEqual(DataSourceCommand.parse_time_period('2010,2010-01'), None)
        self.assertEqual(DataSourceCommand.parse_time_period('2010-01,2010-76'), None)
        self.assertEqual(DataSourceCommand.parse_time_period('2010-1-3-83,2010-01'), None)
        self.assertEqual(DataSourceCommand.parse_time_period('20L0-1-3-83,2010-01'), None)

    def test_command_run_no_args(self):
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


class CliRunCommandTest(CliTestCase):
    def test_command_run_with_unknown_op(self):
        self.assert_main(['run', 'pipapo', 'lat=13.2', 'lon=52.9'],
                         expected_status=1,
                         expected_stdout='',
                         expected_stderr="ect: error: command 'run': unknown operation 'pipapo'\n")

    def test_command_run_noargs(self):
        self.assert_main(['run'],
                         expected_status=1,
                         expected_stdout='',
                         expected_stderr="ect: error: command 'run' requires OP argument\n")

    def test_command_run_with_op(self):
        op_reg = OP_REGISTRY.add_op(timeseries, fail_if_exists=True)

        try:
            # Run without --monitor and --write
            self.assert_main(['run', op_reg.op_meta_info.qualified_name, 'lat=13.2', 'lon=52.9'],
                             expected_stdout=["Running '",
                                              'lat=13.2 lon=52.9 method=nearest',
                                              'Output: [0.3, 0.25, 0.05, 0.4, 0.2, 0.1, 0.5]'])

            # Run with --monitor and without --write
            self.assert_main(['run', '--monitor', op_reg.op_meta_info.qualified_name, 'lat=13.2', 'lon=52.9'],
                             expected_stdout=["Running '",
                                              'lat=13.2 lon=52.9 method=nearest',
                                              'Extracting timeseries data: started',
                                              'Extracting timeseries data:  33%',
                                              'Extracting timeseries data: done',
                                              'Output: [0.3, 0.25, 0.05, 0.4, 0.2, 0.1, 0.5]'])

            # Run with --monitor and --write
            self.assert_main(['run', '--monitor', '--write', 'timeseries_data.txt',
                              op_reg.op_meta_info.qualified_name, 'lat=13.2', 'lon=52.9'],
                             expected_stdout=["Running '",
                                              'lat=13.2 lon=52.9 method=nearest',
                                              'Extracting timeseries data: started',
                                              'Extracting timeseries data:  33%',
                                              'Extracting timeseries data: done',
                                              'Writing output to timeseries_data.txt using TEXT format...'])
            self.assertTrue(os.path.isfile('timeseries_data.txt'))
            os.remove('timeseries_data.txt')

            # Run with invalid keyword
            with fetch_std_streams() as (stdout, stderr):
                status = cli.main(args=['run', op_reg.op_meta_info.qualified_name, 'l*t=13.2', 'lon=52.9'])
                self.assertEqual(status, 1, msg=stderr.getvalue())
            self.assertEqual(stdout.getvalue(), '')
            self.assertEqual(stderr.getvalue(), "ect: error: command 'run': 'l*t' is not a valid input name\n")

        finally:
            OP_REGISTRY.remove_op(op_reg.operation, fail_if_not_exists=True)

    def test_command_run_with_workflow(self):

        op_reg = OP_REGISTRY.add_op(timeseries, fail_if_exists=True)

        workflow_file = os.path.join(os.path.dirname(__file__), 'timeseries.json')
        self.assertTrue(os.path.exists(workflow_file), msg='missing test file %s' % workflow_file)

        try:
            # Run without --monitor and --write
            self.assert_main(['run', workflow_file, 'lat=13.2', 'lon=52.9'],
                             expected_stdout=["Running '",
                                              'lat=13.2 lon=52.9',
                                              'Output: [0.3, 0.25, 0.05, 0.4, 0.2, 0.1, 0.5]'])

            # Run with --monitor and without --write
            self.assert_main(['run', '--monitor', workflow_file, 'lat=13.2', 'lon=52.9'],
                             expected_stdout=["Running '",
                                              'lat=13.2 lon=52.9',
                                              'Extracting timeseries data: started',
                                              'Extracting timeseries data:  33%',
                                              'Extracting timeseries data: done',
                                              'Output: [0.3, 0.25, 0.05, 0.4, 0.2, 0.1, 0.5]'])

            # Run with --monitor and --write
            self.assert_main(['run', '--monitor', '--write', 'timeseries_data.json',
                              workflow_file, 'lat=13.2', 'lon=52.9'],
                             expected_stdout=["Running '",
                                              'lat=13.2 lon=52.9',
                                              'Extracting timeseries data: started',
                                              'Extracting timeseries data:  33%',
                                              'Extracting timeseries data: done',
                                              'Writing output to timeseries_data.json using JSON format...'])
            self.assertTrue(os.path.isfile('timeseries_data.json'))
            os.remove('timeseries_data.json')

        finally:
            OP_REGISTRY.remove_op(op_reg.operation, fail_if_not_exists=True)

    def test_command_run_help(self):
        self.assert_main(['run', '-h'])
        self.assert_main(['run', '--help'])


class CliPluginCommandTest(CliTestCase):
    def test_command_list(self):
        self.assert_main(['pi', 'list'], expected_stdout=['plugins found'])


class CliLicenseCommandTest(CliTestCase):
    def test_command_license(self):
        self.assert_main(['lic'], expected_stdout=['GNU General Public License'])


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
