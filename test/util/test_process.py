import os.path
import sys
from unittest import TestCase

from cate.util.process import run_subprocess, ProcessOutputMonitor
from .test_monitor import RecordingMonitor

DIR = os.path.dirname(__file__)
MAKE_ENTROPY = os.path.join(DIR, '..', 'core', 'executables', 'mkentropy.py')


class ProcessTest(TestCase):
    def setUp(self):
        self.monitor = RecordingMonitor()
        self.stdout_lines = []
        self.stderr_lines = []
        self.line_count = 0
        self.started = False

    def on_started(self, process):
        self.assertEqual(process.returncode, None)
        self.started = True

    def store_stderr_line(self, line):
        self.stderr_lines.append(line.strip())

    def store_stdout_line(self, line):
        self.stdout_lines.append(line.strip())

    def store_stdout_line_and_cancel(self, line):
        if self.line_count == 3:
            self.monitor.cancel()
        self.store_stdout_line(line)
        self.line_count += 1

    def test_execute_with_handler(self):
        exit_code = run_subprocess([sys.executable, MAKE_ENTROPY, '5', '0.1'],
                                   stdout_handler=self.store_stdout_line,
                                   stderr_handler=self.store_stderr_line)
        self.assertEqual(exit_code, 0)
        self.assertEqual(self.stdout_lines, ['mkentropy: Running 5 steps',
                                             'mkentropy: Did 1 of 5 steps: 20.0%',
                                             'mkentropy: Did 2 of 5 steps: 40.0%',
                                             'mkentropy: Did 3 of 5 steps: 60.0%',
                                             'mkentropy: Did 4 of 5 steps: 80.0%',
                                             'mkentropy: Did 5 of 5 steps: 100.0%',
                                             'mkentropy: Done making some entropy',
                                             ''])
        self.assertEqual(self.stderr_lines, [''])

    def test_execute_with_handler_failing(self):
        exit_code = run_subprocess([sys.executable, MAKE_ENTROPY, '5', '0.1', '3'],
                                   stdout_handler=self.store_stdout_line,
                                   stderr_handler=self.store_stderr_line)
        self.assertEqual(exit_code, 1)
        self.assertEqual(self.stdout_lines, ['mkentropy: Running 5 steps',
                                             'mkentropy: Did 1 of 5 steps: 20.0%',
                                             'mkentropy: Did 2 of 5 steps: 40.0%',
                                             'mkentropy: Did 3 of 5 steps: 60.0%',
                                             ''])
        self.assertTrue(len(self.stderr_lines) > 2)
        self.assertEqual(self.stderr_lines[-2], 'RuntimeError: An intended error occurred!')
        self.assertEqual(self.stderr_lines[-1], '')

    def test_execute_with_handler_and_cancellation(self):
        exit_code = run_subprocess([sys.executable, MAKE_ENTROPY, '5', '0.2'],
                                   started_handler=self.on_started,
                                   stdout_handler=self.store_stdout_line_and_cancel,
                                   stderr_handler=self.store_stderr_line,
                                   is_cancelled=self.monitor.is_cancelled,
                                   cancelled_check_period=0.02,
                                   kill_on_cancel=False)
        self.assertTrue(self.started)
        self.assertTrue(exit_code != 0)
        self.assertTrue(len(self.stdout_lines) > 1)
        self.assertEqual(self.stderr_lines, [''])

    def test_execute_with_handler_and_kill_cancellation(self):
        exit_code = run_subprocess([sys.executable, MAKE_ENTROPY, '5', '0.2'],
                                   started_handler=self.on_started,
                                   stdout_handler=self.store_stdout_line_and_cancel,
                                   stderr_handler=self.store_stderr_line,
                                   is_cancelled=self.monitor.is_cancelled,
                                   cancelled_check_period=0.02,
                                   kill_on_cancel=True)
        self.assertTrue(self.started)
        self.assertTrue(exit_code != 0)
        self.assertTrue(len(self.stdout_lines) > 1)
        self.assertTrue(len(self.stderr_lines) > 0)

    def test_execute_with_monitor(self):
        handler = ProcessOutputMonitor(self.monitor,
                                       started=r'mkentropy: Running (?P<total_work>\d+)',
                                       progress=r'mkentropy: Did (?P<work>\d+)',
                                       done=lambda line: 'Done' in line)
        exit_code = run_subprocess([sys.executable, MAKE_ENTROPY, '5', '0.1'], stdout_handler=handler)
        self.assertEqual(exit_code, 0)
        self.assertEqual(self.monitor.records, [('start', 'Process running', 5.0),
                                                ('progress', 1.0, None, 20),
                                                ('progress', 1.0, None, 40),
                                                ('progress', 1.0, None, 60),
                                                ('progress', 1.0, None, 80),
                                                ('progress', 1.0, None, 100),
                                                ('done',)])
