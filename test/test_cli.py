import sys
import unittest
from contextlib import contextmanager
from io import StringIO
from time import sleep

from ect.core import cli
from ect.core.monitor import Monitor


@contextmanager
def fetch_std_streams():
    sys.stdout.flush()
    sys.stderr.flush()

    old_stdout = sys.stdout
    old_stderr = sys.stderr

    sys.stdout = StringIO()
    sys.stderr = StringIO()

    try:
        yield sys.stdout, sys.stderr
    finally:
        sys.stdout.flush()
        sys.stderr.flush()

        sys.stdout = old_stdout
        sys.stderr = old_stderr


class CliTest(unittest.TestCase):
    def test_noargs(self):
        import sys
        sys.argv = []
        status = cli.main()
        self.assertEqual(status, 0)

    def test_invalid_command(self):
        status = cli.main(['pipo'])
        self.assertEqual(status, 2)

    def test_option_version(self):
        status = cli.main(args=['--version'])
        self.assertEqual(status, 0)

    def test_option_help(self):
        status = cli.main(args=['--h'])
        self.assertEqual(status, 0)
        status = cli.main(args=['--help'])
        self.assertEqual(status, 0)


class CliDataSourceCommandTest(unittest.TestCase):
    def test_command_ds_info(self):
        with fetch_std_streams() as (sout, serr):
            status = cli.main(args=['ds', 'SOIL_MOISTURE_DAILY_FILES_ACTIVE_V02.2'])
            self.assertEqual(status, 0)
        out1 = sout.getvalue()
        self.assertTrue('Base directory' in out1)
        self.assertEqual(serr.getvalue(), '')

        with fetch_std_streams() as (sout, serr):
            status = cli.main(args=['ds', 'SOIL_MOISTURE_DAILY_FILES_ACTIVE_V02.2', '--info'])
            self.assertEqual(status, 0)
        out2 = sout.getvalue()

        self.assertEqual(out1, out2)

    @unittest.skip(reason="skipped unless you want to debug data source synchronisation")
    def test_command_ds_sync(self):
        with fetch_std_streams() as (sout, serr):
            status = cli.main(args=['ds', 'SOIL_MOISTURE_DAILY_FILES_ACTIVE_V02.2', '--sync'])
            self.assertEqual(status, 0)

    @unittest.skip(reason="skipped unless you want to debug data source synchronisation")
    def test_command_ds_sync_with_period(self):
        with fetch_std_streams() as (sout, serr):
            status = cli.main(args=['ds', 'SOIL_MOISTURE_DAILY_FILES_ACTIVE_V02.2', '--sync', '--period', '2010-12'])
            self.assertEqual(status, 0)

    def test_command_ds_parse_period(self):
        from ect.core.cli import DataSourceCommand
        from datetime import date

        self.assertEqual(DataSourceCommand.parse_period('2010'), (date(2010, 1, 1), date(2010, 12, 31)))
        self.assertEqual(DataSourceCommand.parse_period('2010-02'), (date(2010, 2, 1), date(2010, 2, 28)))
        self.assertEqual(DataSourceCommand.parse_period('2010-12'), (date(2010, 12, 1), date(2010, 12, 31)))
        self.assertEqual(DataSourceCommand.parse_period('2010-02-04'), (date(2010, 2, 4), date(2010, 2, 4)))
        self.assertEqual(DataSourceCommand.parse_period('2010-12-31'), (date(2010, 12, 31), date(2010, 12, 31)))

        self.assertEqual(DataSourceCommand.parse_period('2010,2014'), (date(2010, 1, 1), date(2014, 12, 31)))
        self.assertEqual(DataSourceCommand.parse_period('2010-02,2010-09'), (date(2010, 2, 1), date(2010, 9, 30)))
        self.assertEqual(DataSourceCommand.parse_period('2010-12,2011-12'), (date(2010, 12, 1), date(2011, 12, 31)))
        self.assertEqual(DataSourceCommand.parse_period('2010-02-04,2019-02-04'), (date(2010, 2, 4), date(2019, 2, 4)))
        self.assertEqual(DataSourceCommand.parse_period('2010-12-31,2010-01-06'), (date(2010, 12, 31), date(2010, 1, 6)))

        # errors
        self.assertEqual(DataSourceCommand.parse_period('2010-12-31,2010-01'), None)
        self.assertEqual(DataSourceCommand.parse_period('2010,2010-01'), None)
        self.assertEqual(DataSourceCommand.parse_period('2010-01,2010-76'), None)
        self.assertEqual(DataSourceCommand.parse_period('2010-1-3-83,2010-01'), None)
        self.assertEqual(DataSourceCommand.parse_period('20L0-1-3-83,2010-01'), None)

    def test_command_run_no_args(self):
        with fetch_std_streams() as (sout, serr):
            status = cli.main(args=['ds'])
            self.assertEqual(status, 2)
        self.assertEqual(sout.getvalue(), '')
        self.assertEqual(serr.getvalue(), "usage: ect ds [-h] [--period PERIOD] [--info] [--sync] DS_NAME [DS_NAME ...]\n"
                                          "ect: ect ds: error: the following arguments are required: DS_NAME\n\n")


class CliRunCommandTest(unittest.TestCase):
    def test_command_run_with_unknown_op(self):
        with fetch_std_streams() as (sout, serr):
            status = cli.main(args=['run', 'pipapo', 'lat=13.2', 'lon=52.9'])
            self.assertEqual(status, 1)
        self.assertEqual(sout.getvalue(), '')
        self.assertEqual(serr.getvalue(), "ect: error: command 'run': unknown operation 'pipapo'\n")

    def test_command_run_noargs(self):
        with fetch_std_streams() as (sout, serr):
            status = cli.main(args=['run'])
            self.assertEqual(status, 2)
        self.assertEqual(sout.getvalue(), '')
        self.assertEqual(serr.getvalue(), "ect: error: command 'run' requires OP argument\n")

    def test_command_run_with_op(self):
        from ect.core.op import OP_REGISTRY as OP_REGISTRY

        op_reg = OP_REGISTRY.add_op(timeseries, fail_if_exists=True)

        try:
            # Run without progress monitor
            with fetch_std_streams() as (sout, serr):
                status = cli.main(args=['run', op_reg.meta_info.qualified_name, 'lat=13.2', 'lon=52.9'])
                self.assertEqual(status, 0)
            soutv = sout.getvalue()
            self.assertTrue('Running operation ' in soutv)
            self.assertTrue('lat=13.2 lon=52.9 method=nearest' in soutv)
            self.assertTrue('Output: [0.3, 0.25, 0.05, 0.4, 0.2, 0.1, 0.5]' in soutv)
            self.assertEqual(serr.getvalue(), '')

            # Run with progress monitor
            with fetch_std_streams() as (sout, serr):
                status = cli.main(args=['run', '--monitor', op_reg.meta_info.qualified_name, 'lat=13.2', 'lon=52.9'])
                self.assertEqual(status, 0)
            soutv = sout.getvalue()
            self.assertTrue('Running operation ' in soutv)
            self.assertTrue('lat=13.2 lon=52.9 method=nearest' in soutv)
            self.assertTrue('Extracting timeseries data: started' in soutv)
            self.assertTrue('Extracting timeseries data:  33%' in soutv)
            self.assertTrue('Extracting timeseries data: done' in soutv)
            self.assertTrue('Output: [0.3, 0.25, 0.05, 0.4, 0.2, 0.1, 0.5]' in soutv)
            self.assertEqual(serr.getvalue(), '')

            # Run with invalid keyword
            with fetch_std_streams() as (sout, serr):
                status = cli.main(args=['run', op_reg.meta_info.qualified_name, 'l*t=13.2', 'lon=52.9'])
                self.assertEqual(status, 2)
            self.assertEqual(sout.getvalue(), '')
            self.assertEqual(serr.getvalue(), "ect: error: command 'run': keyword 'l*t' is not a valid identifier\n")

        finally:
            OP_REGISTRY.remove_op(op_reg.operation, fail_if_not_exists=True)

    def test_command_run_with_graph(self):
        from ect.core.op import OP_REGISTRY as OP_REGISTRY
        import os.path

        op_reg = OP_REGISTRY.add_op(timeseries, fail_if_exists=True)

        graph_file = os.path.join(os.path.dirname(__file__), 'graphs/test_cli_timeseries_graph.json')
        self.assertTrue(os.path.exists(graph_file), msg='missing test file %s' % graph_file)

        try:
            # Run without progress monitor
            with fetch_std_streams() as (sout, serr):
                status = cli.main(args=['run', graph_file, 'lat=13.2', 'lon=52.9'])
                self.assertEqual(status, 0)
            soutv = sout.getvalue()
            self.assertTrue('Running graph ' in soutv)
            self.assertTrue('lat=13.2 lon=52.9' in soutv)
            self.assertTrue('Output: return = [0.3, 0.25, 0.05, 0.4, 0.2, 0.1, 0.5]' in soutv)
            self.assertEqual(serr.getvalue(), '')

            # Run with progress monitor
            with fetch_std_streams() as (sout, serr):
                status = cli.main(args=['run', '--monitor', graph_file, 'lat=13.2', 'lon=52.9'])
                self.assertEqual(status, 0)
            soutv = sout.getvalue()
            self.assertTrue('Running graph ' in soutv)
            self.assertTrue('lat=13.2 lon=52.9' in soutv)
            self.assertTrue('Extracting timeseries data: started' in soutv)
            self.assertTrue('Extracting timeseries data:  33%' in soutv)
            self.assertTrue('Extracting timeseries data: done' in soutv)
            self.assertTrue('Output: return = [0.3, 0.25, 0.05, 0.4, 0.2, 0.1, 0.5]' in soutv)
            self.assertEqual(serr.getvalue(), '')

        finally:
            OP_REGISTRY.remove_op(op_reg.operation, fail_if_not_exists=True)

    def test_command_run_help(self):
        status = cli.main(args=['run', '-h'])
        self.assertEqual(status, 0)

        status = cli.main(args=['run', '--help'])
        self.assertEqual(status, 0)


class CliListCommandTest(unittest.TestCase):
    def test_command_list(self):
        with fetch_std_streams() as (sout, serr):
            status = cli.main(args=['list'])
            self.assertEqual(status, 0)
        self.assertIn('operation', sout.getvalue())
        self.assertIn('found', sout.getvalue())
        self.assertEqual(serr.getvalue(), '')

        with fetch_std_streams() as (sout, serr):
            status = cli.main(args=['list', 'op'])
            self.assertEqual(status, 0)
        self.assertIn('operation', sout.getvalue())
        self.assertIn('found', sout.getvalue())
        self.assertEqual(serr.getvalue(), '')

        with fetch_std_streams() as (sout, serr):
            status = cli.main(args=['list', 'pi'])
            self.assertEqual(status, 0)
        self.assertIn('plugin', sout.getvalue())
        self.assertIn('found', sout.getvalue())
        self.assertEqual(serr.getvalue(), '')

        with fetch_std_streams() as (sout, serr):
            status = cli.main(args=['list', 'ds'])
            self.assertEqual(status, 0)
        self.assertIn('data source', sout.getvalue())
        self.assertIn('found', sout.getvalue())
        self.assertEqual(serr.getvalue(), '')

        with fetch_std_streams() as (sout, serr):
            status = cli.main(args=['list', '--pattern', 'sst*', 'ds'])
            self.assertEqual(status, 0)
        self.assertIn('data source', sout.getvalue())
        self.assertIn('found', sout.getvalue())
        self.assertEqual(serr.getvalue(), '')


class CliLicenseCommandTest(unittest.TestCase):
    def test_command_license(self):
        with fetch_std_streams() as (sout, serr):
            status = cli.main(args=['license'])
            self.assertEqual(status, 0)
        self.assertIn('GNU GENERAL PUBLIC LICENSE', sout.getvalue())
        self.assertEqual(serr.getvalue(), '')


class CliCopyrightCommandTest(unittest.TestCase):
    def test_command_copyright(self):
        with fetch_std_streams() as (sout, serr):
            status = cli.main(args=['copyright'])
            self.assertEqual(status, 0)
        self.assertIn('European Space Agency', sout.getvalue())
        self.assertEqual(serr.getvalue(), '')


def timeseries(lat: float, lon: float, method: str = 'nearest', monitor=Monitor.NULL) -> list:
    """Timeseries dummy function for testing."""
    print('lat=%s lon=%s method=%s' % (lat, lon, method))
    work_units = [0.3, 0.25, 0.05, 0.4, 0.2, 0.1, 0.5]
    with monitor.starting('Extracting timeseries data', sum(work_units)):
        for work_unit in work_units:
            sleep(work_unit / 10.)
            monitor.progress(work_unit)
    return work_units
