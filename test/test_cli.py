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
    def test_help(self):
        with self.assertRaises(SystemExit):
            cli.main(args=['--help'])

    def test_subcmd_list(self):
        with fetch_std_streams() as (sout, serr):
            status, message = cli.main(args=['list'])
            self.assertEqual(status, 0)
            self.assertEqual(message, None)
        self.assertIn('ESA CCI Toolbox command-line interface, version ', sout.getvalue())
        self.assertIn('Registered ECT plugins', sout.getvalue())
        self.assertIn('Registered ECT operations', sout.getvalue())
        self.assertEqual(serr.getvalue(), '')


    def test_subcmd_run(self):
        with fetch_std_streams() as (sout, serr):
            status, message = cli.main(args=['run', 'time_series', 'myds', 'lat=13.2', 'lon=52.9'])
            self.assertEqual(status, 0)
            self.assertEqual(message, None)
        self.assertIn('ESA CCI Toolbox command-line interface, version ', sout.getvalue())
        self.assertIn('Now running operation...', sout.getvalue())
        self.assertEqual(serr.getvalue(), '')

        with fetch_std_streams() as (sout, serr):
            status, message = cli.main(args=['run', 'time_series', 'myds', 'l+t=13.2', 'lon=52.9'])
            self.assertEqual(status, 2)
            self.assertEqual(message, "error: keyword 'l+t' is not a valid identifier")
        self.assertIn('ESA CCI Toolbox command-line interface, version ', sout.getvalue())
        self.assertEqual(serr.getvalue(), '')