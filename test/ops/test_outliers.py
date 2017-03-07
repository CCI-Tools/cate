"""
Tests for outlier detection operations
"""

from unittest import TestCase
import xarray as xr
import numpy as np

from cate.ops import outliers


class TestOutliers(TestCase):
    def test_outliers(self):
        ds = xr.Dataset({
            'first': xr.DataArray(np.arange(16, dtype=float).reshape(4, 4),
                                  dims=('x', 'y'),
                                  attrs={'a1': 'Dummy attribute'}),
            'second': xr.DataArray(np.arange(16).reshape(4, 4),
                                   dims=('x', 'y'),
                                   attrs={'a1': 'Dummy attribute'})
        })

        ret_ds = outliers.detect_outliers(ds, 'first')
        test = ds['first'].copy()
        test[0][0] = np.nan
        test[3][3] = np.nan
        self.assertTrue(test.identical(ret_ds['first']))
        self.assertTrue(ret_ds['second'].identical(ds['second']))

        ret_ds = outliers.detect_outliers(ds, 'fir*', threshold_low=2,
                                          threshold_high=13, quantiles=False)
        test = ds['first'].copy()
        test[0] = [np.nan, np.nan, np.nan, 3.0]
        test[3] = [12.0, np.nan, np.nan, np.nan]
        self.assertTrue(test.identical(ret_ds['first']))
        self.assertTrue(ret_ds['second'].identical(ds['second']))
