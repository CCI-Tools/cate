from cate.core.op import op
from cate.util.monitor import Monitor, Cancellation

import time


@op()
def test_monitor_works(monitor: Monitor = Monitor.NONE):
    """
    test monitor
    """
    with monitor.starting('Test monitor', total_work=100):
        monitor.progress(work=0)

        for i in range(0, 10):
            time.sleep(1)
            try:
                monitor.progress(10, "Some text")
            except Cancellation as c:
                print('Test monitor operation cancelled')
                raise c
        monitor.done()


@op()
def test_monitor_fails(monitor: Monitor = Monitor.NONE):
    """
    test monitor
    """
    with monitor.starting('Test monitor', total_work=100):
        monitor.progress(work=0)

        for i in range(0, 10):
            time.sleep(1)
            try:
                monitor.progress(10)
            except Cancellation as c:
                print('Test monitor operation cancelled')
                raise c
        monitor.done()
