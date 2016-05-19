from unittest import TestCase

from ect.core import Monitor


class MonitorTest(TestCase):
    def test_NULL(self):
        self.assertIsNotNone(Monitor.NULL)
        self.assertEqual(repr(Monitor.NULL), 'Monitor.NULL')


