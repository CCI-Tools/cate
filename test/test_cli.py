import sys
from contextlib import contextmanager
from io import StringIO
from unittest import TestCase

from ect.core import cli


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


class CliTest(TestCase):
    def test_noargs(self):
        with self.assertRaises(SystemExit):
            cli.main()

    def test_invalid_command(self):
        with self.assertRaises(SystemExit):
            cli.main(['pipo'])

    def test_option_help(self):
        with self.assertRaises(SystemExit):
            cli.main(args=['--h'])
        with self.assertRaises(SystemExit):
            cli.main(args=['--help'])

    def test_option_version(self):
        with self.assertRaises(SystemExit):
            cli.main(args=['--version'])

    def test_command_license(self):
        with fetch_std_streams() as (sout, serr):
            status = cli.main(args=['license'])
            self.assertEqual(status, 0)
        self.assertIn('GNU GENERAL PUBLIC LICENSE', sout.getvalue())
        self.assertEqual(serr.getvalue(), '')

    def test_command_copyright(self):
        with fetch_std_streams() as (sout, serr):
            status = cli.main(args=['copyright'])
            self.assertEqual(status, 0)
        self.assertIn('European Space Agency', sout.getvalue())
        self.assertEqual(serr.getvalue(), '')

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

    def test_command_run_help(self):
        with self.assertRaises(SystemExit):
            cli.main(args=['run', '-h'])

        with self.assertRaises(SystemExit):
            cli.main(args=['run', '-help'])

    def test_command_run_with_unknown_op(self):
        with fetch_std_streams() as (sout, serr):
            status = cli.main(args=['run', 'pipapo', 'lat=13.2', 'lon=52.9'])
            self.assertEqual(status, 1)
        self.assertEqual(sout.getvalue(), '')
        self.assertEqual(serr.getvalue(), "ect: error: unknown operation 'pipapo'\n")

    def test_command_run_with_op(self):
        from ect.core.op import REGISTRY as OP_REGISTRY
        from ect.core.monitor import starting, Monitor
        from time import sleep

        def timeseries(lat: float, lon: float, method: str = 'nearest', monitor=Monitor.NULL) -> list:
            print('lat=%s lon=%s method=%s' % (lat, lon, method))
            work_units = [0.3, 0.25, 0.05, 0.4, 0.2, 0.1, 0.5]
            with starting(monitor, 'Extracting timeseries data', sum(work_units)):
                for work_unit in work_units:
                    sleep(work_unit / 10.)
                    monitor.progress(work_unit)
            return work_units

        op_reg = OP_REGISTRY.add_op(timeseries, fail_if_exists=True)

        try:
            # Run without progress monitor
            with fetch_std_streams() as (sout, serr):
                status = cli.main(args=['run', op_reg.meta_info.qualified_name, 'lat=13.2', 'lon=52.9'])
                self.assertEqual(status, 0)
            self.assertTrue('Running operation ' in sout.getvalue())
            self.assertTrue('lat=13.2 lon=52.9 method=nearest' in sout.getvalue())
            self.assertTrue('Output: [0.3, 0.25, 0.05, 0.4, 0.2, 0.1, 0.5]' in sout.getvalue())
            self.assertEqual(serr.getvalue(), '')

            # Run with progress monitor
            with fetch_std_streams() as (sout, serr):
                status = cli.main(args=['run', '--monitor', op_reg.meta_info.qualified_name, 'lat=13.2', 'lon=52.9'])
                self.assertEqual(status, 0)
            self.assertTrue('Running operation ' in sout.getvalue())
            self.assertTrue('lat=13.2 lon=52.9 method=nearest' in sout.getvalue())
            self.assertTrue('Extracting timeseries data: start' in sout.getvalue())
            self.assertTrue('Extracting timeseries data: 33%' in sout.getvalue())
            self.assertTrue('Extracting timeseries data: done' in sout.getvalue())
            self.assertTrue('Output: [0.3, 0.25, 0.05, 0.4, 0.2, 0.1, 0.5]' in sout.getvalue())
            self.assertEqual(serr.getvalue(), '')

            # Run with invalid keyword
            with fetch_std_streams() as (sout, serr):
                status = cli.main(args=['run', op_reg.meta_info.qualified_name, 'l*t=13.2', 'lon=52.9'])
                self.assertEqual(status, 2)
            self.assertEqual(sout.getvalue(), '')
            self.assertEqual(serr.getvalue(), "ect: error: keyword 'l*t' is not a valid identifier\n")

        finally:
            OP_REGISTRY.remove_op(op_reg.operation, fail_if_not_exists=True)

    def test_command_run_with_graph(self):
        from ect.core.graph import Graph
        from ect.core.op import REGISTRY as OP_REGISTRY
        from ect.core.monitor import starting, Monitor
        import os.path
        from time import sleep

        def timeseries(lat: float, lon: float, method: str = 'nearest', monitor=Monitor.NULL) -> list:
            print('lat=%s lon=%s method=%s' % (lat, lon, method))
            work_units = [0.3, 0.25, 0.05, 0.4, 0.2, 0.1, 0.5]
            with starting(monitor, 'Extracting time series data', sum(work_units)):
                for work_unit in work_units:
                    sleep(work_unit / 10.)
                    monitor.progress(work_unit)
            return work_units

        op_reg = OP_REGISTRY.add_op(timeseries, fail_if_exists=True)

        graph_file = os.path.join(os.path.dirname(__file__), 'test_cli_timeseries_graph.json')
        self.assertTrue(os.path.exists(graph_file), msg='missing test file %s' % graph_file)

        try:
            # Run without progress monitor
            with fetch_std_streams() as (sout, serr):
                status = cli.main(args=['run', graph_file, 'lat=13.2', 'lon=52.9'])
                self.assertEqual(status, 0)
            self.assertTrue('Running operation ' in sout.getvalue())
            self.assertTrue('lat=13.2 lon=52.9 method=nearest' in sout.getvalue())
            self.assertTrue('Output: [0.3, 0.25, 0.05, 0.4, 0.2, 0.1, 0.5]' in sout.getvalue())
            self.assertEqual(serr.getvalue(), '')

            # Run with progress monitor
            with fetch_std_streams() as (sout, serr):
                status = cli.main(args=['run', '--monitor', graph_file, 'lat=13.2', 'lon=52.9'])
                self.assertEqual(status, 0)
            self.assertTrue('Running operation ' in sout.getvalue())
            self.assertTrue('lat=13.2 lon=52.9 method=nearest' in sout.getvalue())
            self.assertTrue('Extracting timeseries data: start' in sout.getvalue())
            self.assertTrue('Extracting timeseries data: 33%' in sout.getvalue())
            self.assertTrue('Extracting timeseries data: done' in sout.getvalue())
            self.assertTrue('Output: [0.3, 0.25, 0.05, 0.4, 0.2, 0.1, 0.5]' in sout.getvalue())
            self.assertEqual(serr.getvalue(), '')

        finally:
            OP_REGISTRY.remove_op(op_reg.operation, fail_if_not_exists=True)
