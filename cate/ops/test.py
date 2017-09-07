import xarray as xr

from cate.core.op import op
from cate.util.monitor import Monitor, Cancellation

import time


@op()
def test_monitor(ds: xr.Dataset, monitor: Monitor = Monitor.NONE) -> xr.Dataset:
    """
    test monitor
    """
    with monitor.starting('Test monitor', total_work=100):
        monitor.progress(work=0)

        for i in range(0, 10):
            time.sleep(2)
            print('alive')
            try:
                monitor.progress(work=10)
            except Cancellation as c:
                print('Test monitor operation cancelled')
                raise c
        monitor.done()

    return ds
