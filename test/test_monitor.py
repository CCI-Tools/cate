from unittest import TestCase

from ect.core.monitor import Monitor, ChildMonitor, ConsoleMonitor


class NullMonitorTest(TestCase):
    def test_NULL(self):
        self.assertIsNotNone(Monitor.NULL)
        self.assertEqual(repr(Monitor.NULL), 'Monitor.NULL')

    def test_child_monitor(self):
        sub_monitor = Monitor.NULL.child(10)
        self.assertIs(sub_monitor, Monitor.NULL)


class ConsoleMonitorTest(TestCase):
    def test_console_monitor(self):
        monitor = ConsoleMonitor()
        monitor.start('task A', total_work=10)
        monitor.progress(work=1)
        monitor.progress(work=5, msg='phase 1')
        monitor.progress(msg='phase 2')
        monitor.progress(work=4)
        monitor.progress()
        monitor.done()
        self.assertTrue(True)

    def test_label_req(self):
        monitor = ConsoleMonitor()
        with self.assertRaises(ValueError):
            # "ValueError: label must be given"
            monitor.start('', total_work=10)

    def test_child_monitor(self):
        monitor = ConsoleMonitor()
        child_monitor = monitor.child(7.5)
        self.assertIsInstance(child_monitor, ChildMonitor)


class RecordingMonitorTest(TestCase):
    def test_recording_monitor(self):
        # test the RecordingMonitor used for testing
        monitor = RecordingMonitor()
        monitor.start('task A', total_work=10)
        monitor.progress(work=1)
        monitor.progress(work=5, msg='phase 1')
        monitor.progress(msg='phase 2')
        monitor.progress(work=4)
        monitor.done()
        self.assertEqual(monitor.records, [('start', 'task A', 10),
                                           ('progress', 1, None, 10),
                                           ('progress', 5, 'phase 1', 60),
                                           ('progress', None, 'phase 2', None),
                                           ('progress', 4, None, 100),
                                           ('done',)])

    def test_context_manager(self):
        monitor = RecordingMonitor()
        with monitor.starting('task A', total_work=10):
            monitor.progress(work=1)
            monitor.progress(work=5, msg='phase 1')
            monitor.progress(msg='phase 2')
            monitor.progress(work=4)

        self.assertEqual(monitor.records, [('start', 'task A', 10),
                                           ('progress', 1, None, 10),
                                           ('progress', 5, 'phase 1', 60),
                                           ('progress', None, 'phase 2', None),
                                           ('progress', 4, None, 100),
                                           ('done',)])


class ChildMonitorTest(TestCase):
    def test_child_monitor(self):
        monitor = RecordingMonitor()
        sub_monitor = monitor.child(10)
        self.assertIsInstance(sub_monitor, ChildMonitor)

        monitor.start('task A', total_work=10)

        sm1 = monitor.child(work=1)
        sm1.start('sub-task A.1', 100)
        sm1.progress(work=30)
        sm1.progress(work=20)
        sm1.progress(work=50)
        sm1.done()

        sm2 = monitor.child(work=5)
        sm2.start('sub-task A.2', 100)
        sm2.progress(work=30)
        sm2.progress(work=20)
        sm2.progress(work=50)
        sm2.done()

        sm3 = monitor.child(work=4)
        sm3.start('sub-task A.3', 100)
        sm3.progress(work=30)
        sm3.progress(work=20)
        sm3.progress(work=50)
        sm3.done()

        monitor.done()

        # import pprint
        # pprint.pprint(monitor.records)

        self.assertEqual(monitor.records, [('start', 'task A', 10),
                                           ('progress', 0.0, 'sub-task A.1', 0),
                                           ('progress', 0.3, None, 3),
                                           ('progress', 0.2, None, 5),
                                           ('progress', 0.5, None, 10),
                                           ('progress', 0.0, 'sub-task A.1', 10),
                                           ('progress', 0.0, 'sub-task A.2', 10),
                                           ('progress', 1.5, None, 25),
                                           ('progress', 1.0, None, 35),
                                           ('progress', 2.5, None, 60),
                                           ('progress', 0.0, 'sub-task A.2', 60),
                                           ('progress', 0.0, 'sub-task A.3', 60),
                                           ('progress', 1.2, None, 72),
                                           ('progress', 0.8, None, 80),
                                           ('progress', 2.0, None, 100),
                                           ('progress', 0.0, 'sub-task A.3', 100),
                                           ('done',)])

    def test_label_req(self):
        monitor = RecordingMonitor()
        sub_monitor = monitor.child(10)
        with self.assertRaises(ValueError):
            # "ValueError: label must be given"
            sub_monitor.start('', total_work=10)


class RecordingMonitor(Monitor):
    """A monitor that buffers progress output as a string so that e.g. a remote service can pick it up."""

    def __init__(self):
        self._records = []
        self._label = None
        self._worked = None
        self._total_work = None

    @property
    def records(self):
        return self._records

    def start(self, label: str, total_work: float = None):
        self._label = label
        self._worked = 0.
        self._total_work = total_work
        self._records.append(('start', label, total_work))

    def progress(self, work: float = None, msg: str = None):
        percentage = None
        if work is not None:
            self._worked += work
            percentage = int(100. * self._worked / self._total_work + 0.5)
        self._records.append(('progress', work, msg, percentage))

    def done(self):
        self._records.append(('done',))
