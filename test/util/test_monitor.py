from unittest import TestCase

from cate.util.misc import fetch_std_streams
from cate.util.monitor import Monitor, ChildMonitor, ConsoleMonitor


class NullMonitorTest(TestCase):
    def test_NONE(self):
        self.assertIsNotNone(Monitor.NONE)
        self.assertEqual(repr(Monitor.NONE), 'Monitor.NONE')

    def test_child_monitor(self):
        self.assertIs(Monitor.NONE.child(work=10), Monitor.NONE)

    def test_cancel(self):
        m = Monitor.NONE
        m.start('task A', total_work=10)
        self.assertFalse(m.is_cancelled())
        m.cancel()
        self.assertFalse(m.is_cancelled())


class ConsoleMonitorTest(TestCase):
    def test_console_monitor_wo_progress_bar(self):
        m = ConsoleMonitor()
        with fetch_std_streams() as (stdout, stderr):
            m.start('task A', total_work=10)
            m.progress(work=1)
            m.progress(work=5, msg='phase 1')
            m.progress(msg='phase 2')
            m.progress(work=4)
            m.progress()
            m.done()
        actual_stdout = stdout.getvalue()
        expected_stdout = 'task A: started\n' + \
                          'task A:  10% \n' + \
                          'task A:  60% phase 1\n' + \
                          'task A: phase 2\n' + \
                          'task A: 100% \n' + \
                          'task A: in progress...\n' + \
                          'task A: done\n'

        self.assertEqual(actual_stdout, expected_stdout)
        self.assertEqual(stderr.getvalue(), '')

    def test_console_monitor_with_progress_bar(self):
        m = ConsoleMonitor(progress_bar_size=10)
        with fetch_std_streams() as (stdout, stderr):
            m.start('task A', total_work=10)
            m.progress(work=1)
            m.progress(work=5, msg='phase 1')
            m.progress(msg='phase 2')
            m.progress(work=4)
            m.progress()
            m.done()
        actual_stdout = stdout.getvalue()
        expected_stdout = 'task A: started\n' + \
                          'task A: [#---------]  10% \n' + \
                          'task A: [######----]  60% phase 1\n' + \
                          'task A: phase 2\n' + \
                          'task A: [##########] 100% \n' + \
                          'task A: in progress...\n' + \
                          'task A: done\n'

        self.assertEqual(actual_stdout, expected_stdout)
        self.assertEqual(stderr.getvalue(), '')

    def test_label_required(self):
        monitor = ConsoleMonitor()
        with fetch_std_streams():
            with self.assertRaises(ValueError):
                # "ValueError: label must be given"
                monitor.start('', total_work=10)

    def test_child_monitor(self):
        monitor = ConsoleMonitor()
        child_monitor = monitor.child(work=7.5)
        self.assertIsInstance(child_monitor, ChildMonitor)

    def test_cancel(self):
        m = ConsoleMonitor()
        with fetch_std_streams():
            m.start('task A', total_work=10)
            self.assertFalse(m.is_cancelled())
            m.cancel()
            self.assertTrue(m.is_cancelled())


class RecordingMonitorTest(TestCase):
    def test_recording_monitor(self):
        # test the RecordingMonitor used for testing
        m = RecordingMonitor()
        m.start('task A', total_work=10)
        m.progress(work=1)
        m.progress(work=5, msg='phase 1')
        m.progress(msg='phase 2')
        m.progress(work=4)
        m.done()
        self.assertEqual(m.records, [('start', 'task A', 10),
                                     ('progress', 1, None, 10),
                                     ('progress', 5, 'phase 1', 60),
                                     ('progress', None, 'phase 2', None),
                                     ('progress', 4, None, 100),
                                     ('done',)])

    def test_context_manager(self):
        m = RecordingMonitor()
        with m.starting('task A', total_work=10):
            m.progress(work=1)
            m.progress(work=5, msg='phase 1')
            m.progress(msg='phase 2')
            m.progress(work=4)

        self.assertEqual(m.records, [('start', 'task A', 10),
                                     ('progress', 1, None, 10),
                                     ('progress', 5, 'phase 1', 60),
                                     ('progress', None, 'phase 2', None),
                                     ('progress', 4, None, 100),
                                     ('done',)])


class ChildMonitorTest(TestCase):
    def test_child_monitor(self):
        m = RecordingMonitor()
        sub_monitor = m.child(work=10)
        self.assertIsInstance(sub_monitor, ChildMonitor)

        m.start('task A', total_work=10)

        cm1 = m.child(work=1)
        cm1.start('sub-task A.1', 100)
        cm1.progress(work=30)
        cm1.progress(work=20)
        cm1.progress(work=50)
        cm1.done()

        cm2 = m.child(work=5)
        cm2.start('sub-task A.2', 100)
        cm2.progress(work=30)
        cm2.progress(work=20)
        cm2.progress(work=50)
        cm2.done()

        cm3 = m.child(work=4)
        cm3.start('sub-task A.3', 100)
        cm3.progress(work=30)
        cm3.progress(work=20)
        cm3.progress(work=50)
        cm3.done()

        m.done()

        # import pprint
        # pprint.pprint(monitor.records)

        self.assertEqual(m.records, [('start', 'task A', 10),
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

    def test_cancel(self):
        m = RecordingMonitor()
        sub_monitor = m.child(work=10)
        self.assertIsInstance(sub_monitor, ChildMonitor)

        m.start('task A', total_work=10)

        cm = m.child(work=1)
        cm.start('sub-task A.1', 100)

        self.assertFalse(cm.is_cancelled())
        self.assertFalse(m.is_cancelled())

        cm.cancel()

        self.assertTrue(cm.is_cancelled())
        self.assertTrue(m.is_cancelled())

    def test_no_label(self):
        m = RecordingMonitor()
        m.start('xxx', 20)
        sm = m.child(work=10)
        sm.start('', total_work=5)
        sm.done()
        sm.start(None, total_work=5)
        sm.done()
        m.done()


class RecordingMonitor(Monitor):
    """A monitor that buffers progress output as a string so that e.g. a remote service can pick it up."""

    def __init__(self):
        self._records = []
        self._label = None
        self._worked = None
        self._total_work = None
        self._cancelled = None

    @property
    def records(self):
        return self._records

    def start(self, label: str, total_work: float = None):
        self._label = label
        self._worked = 0.
        self._total_work = total_work
        self._cancelled = False
        self._records.append(('start', label, total_work))

    def progress(self, work: float = None, msg: str = None):
        self.check_for_cancellation()
        percentage = None
        if work is not None:
            self._worked += work
            percentage = int(100. * self._worked / self._total_work + 0.5)
        self._records.append(('progress', work, msg, percentage))

    def done(self):
        self._records.append(('done',))

    def cancel(self):
        self._records.append(('cancel',))
        self._cancelled = True

    def is_cancelled(self) -> bool:
        return self._cancelled

    def __str__(self):
        return "\n".join([str(r) for r in self._records])
