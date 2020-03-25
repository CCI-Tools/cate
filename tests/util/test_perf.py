import time
from unittest import TestCase

from cate.util.perf import measure_time_cm


class MeasureTimeTest(TestCase):
    def test_enabled(self):
        measure_time = measure_time_cm(disabled=False)
        with measure_time("hello") as cm:
            time.sleep(0.06)
        self.assertTrue(hasattr(cm, "duration"))
        self.assertTrue(cm.duration > 0.05)
        self.assertIsNotNone(cm.logger)

    def test_disabled(self):
        measure_time = measure_time_cm(disabled=True)
        with measure_time("hello") as cm:
            time.sleep(0.05)
        self.assertTrue(hasattr(cm, "duration"))
        self.assertIsNone(cm.duration)
        self.assertIsNone(cm.logger)
