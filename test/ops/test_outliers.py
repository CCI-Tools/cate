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
        }, attrs={'dummy': 'dummy_attr'})

        # Test nominal execution, outliers -> nan
        ret_ds = outliers.detect_outliers(ds, 'first')
        test = ds['first'].copy()
        test[0][0] = np.nan
        test[3][3] = np.nan
        self.assertTrue(test.identical(ret_ds['first']))
        self.assertTrue(ret_ds['second'].identical(ds['second']))

        # Test nominal execution with a wildcard
        ret_ds = outliers.detect_outliers(ds, 'fir*', threshold_low=2,
                                          threshold_high=13, quantiles=False)
        test = ds['first'].copy()
        test[0] = [np.nan, np.nan, np.nan, 3.0]
        test[3] = [12.0, np.nan, np.nan, np.nan]
        self.assertTrue(test.identical(ret_ds['first']))
        self.assertTrue(ret_ds['second'].identical(ds['second']))

        # Test adding a mask data array to a dataset that doesn't abide
        # strictly by CF conventions
        ret_ds = outliers.detect_outliers(ds, 'first', mask=True)
        self.assertTrue(ds['first'].equals(ret_ds['first']))
        test_mask = np.array(np.zeros(16, dtype='i1').reshape(4, 4))
        test_mask[0][0] = 1
        test_mask[3][3] = 1
        ret_mask = ret_ds['first_outlier_mask']
        ret_first = ret_ds['first']
        self.assertTrue(np.array_equal(test_mask, ret_mask.values))
        self.assertTrue('status_flag' in ret_mask.attrs['standard_name'])
        self.assertTrue('mask' in ret_mask.attrs['long_name'])
        self.assertTrue(('first_outlier_mask' in
                         ret_first.attrs['ancillary_variables']))

        # Test adding a mask data array to a dataset that somewhat abides by CF
        # conventions
        ds = xr.Dataset({
            'first': xr.DataArray(np.arange(16, dtype=float).reshape(4, 4),
                                  dims=('x', 'y'),
                                  attrs={'a1': 'Dummy attribute',
                                         'ancillary_variables': 'second',
                                         'standard_name': 'dummy',
                                         'long_name': 'Dummy'}),
            'second': xr.DataArray(np.arange(16).reshape(4, 4),
                                   dims=('x', 'y'),
                                   attrs={'a1': 'Dummy attribute'})
        }, attrs={'dummy': 'dummy_attr'})

        ret_ds = outliers.detect_outliers(ds, 'first', mask=True)
        self.assertTrue(ds['first'].equals(ret_ds['first']))
        test_mask = np.array(np.zeros(16, dtype='i1').reshape(4, 4))
        test_mask[0][0] = 1
        test_mask[3][3] = 1
        ret_mask = ret_ds['first_outlier_mask']
        ret_first = ret_ds['first']
        self.assertTrue(np.array_equal(test_mask, ret_mask.values))
        self.assertTrue('status_flag' in ret_mask.attrs['standard_name'])
        self.assertTrue('dummy ' in ret_mask.attrs['standard_name'])
        self.assertTrue('mask' in ret_mask.attrs['long_name'])
        self.assertTrue('Dummy ' in ret_mask.attrs['long_name'])
        self.assertTrue(('first_outlier_mask' in
                         ret_first.attrs['ancillary_variables']))
        self.assertTrue(('second ' in
                         ret_first.attrs['ancillary_variables']))
