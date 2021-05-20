"""
Test for the harmonization operation
"""

import calendar
from datetime import datetime
from unittest import TestCase

import cftime
import numpy as np
import xarray as xr

from cate.core.op import OP_REGISTRY
from cate.ops.normalize import adjust_temporal_attrs
from cate.ops.normalize import normalize
from cate.util.misc import object_to_qualified_name


# noinspection PyPep8Naming
def assertDatasetEqual(expected, actual):
    # this method is functionally equivalent to
    # `assert expected == actual`, but it
    # checks each aspect of equality separately for easier debugging
    assert expected.equals(actual), (expected, actual)


class TestNormalize(TestCase):

    def test_normalize_inverted_lat(self):
        first = np.zeros([3, 45, 90])
        first[0, :, :] = np.eye(45, 90)
        ds = xr.Dataset({
            'first': (['time', 'lat', 'lon'], first),
            'second': (['time', 'lat', 'lon'], np.zeros([3, 45, 90])),
            'lat': np.linspace(88, -88, 45),
            'lon': np.linspace(-178, 178, 90),
            'time': [datetime(2000, x, 1) for x in range(1, 4)]}).chunk(chunks={'time': 1})

        first = np.zeros([3, 45, 90])
        first[0, :, :] = np.flip(np.eye(45, 90), axis=0)
        expected = xr.Dataset({
            'first': (['time', 'lat', 'lon'], first),
            'second': (['time', 'lat', 'lon'], np.zeros([3, 45, 90])),
            'lat': np.linspace(-88, 88, 45),
            'lon': np.linspace(-178, 178, 90),
            'time': [datetime(2000, x, 1) for x in range(1, 4)]}).chunk(chunks={'time': 1})

        actual = normalize(ds)
        xr.testing.assert_equal(actual, expected)

    def test_registered(self):
        """
        Test as a registered operation
        """
        reg_op = OP_REGISTRY.get_op(object_to_qualified_name(normalize))
        dataset = xr.Dataset({'first': (['latitude',
                                         'longitude'], [[1, 2, 3],
                                                        [2, 3, 4]])})
        expected = xr.Dataset({'first': (['lat', 'lon'], [[1, 2, 3],
                                                          [2, 3, 4]])})
        actual = reg_op(ds=dataset)
        assertDatasetEqual(actual, expected)


class AdjustTemporalAttrsTest(TestCase):
    def test_nominal(self):

        time_datas = [
            [datetime(2000, x, 1) for x in range(1, 13)],
            [np.datetime64(datetime(2000, x, 1)) for x in range(1, 13)],
            [cftime.DatetimeGregorian(2000, x, 1) for x in range(1, 13)],
        ]

        for time_data in time_datas:
            print(time_data)
            ds = xr.Dataset({
                'first': (['lat', 'lon', 'time'], np.zeros([45, 90, 12])),
                'second': (['lat', 'lon', 'time'], np.zeros([45, 90, 12])),
                'lat': np.linspace(-88, 88, 45),
                'lon': np.linspace(-178, 178, 90),
                'time': time_data
            })

            ds1 = adjust_temporal_attrs(ds)

            # Make sure original dataset is not altered
            with self.assertRaises(KeyError):
                # noinspection PyStatementEffect
                ds.attrs['time_coverage_start']

            # Make sure expected values are in the new dataset
            self.assertEqual(ds1.attrs['time_coverage_start'],
                             '2000-01-01T00:00:00')
            self.assertEqual(ds1.attrs['time_coverage_end'],
                             '2000-12-01T00:00:00')
            self.assertEqual(ds1.attrs['time_coverage_resolution'],
                             'P1M')
            self.assertEqual(ds1.attrs['time_coverage_duration'],
                             'P336D')

            # Test existing attributes update
            # noinspection PyTypeChecker
            indexers = {'time': slice(datetime(2000, 2, 15), datetime(2000, 6, 15))}
            ds2 = ds1.sel(**indexers)
            ds2 = adjust_temporal_attrs(ds2)

            self.assertEqual(ds2.attrs['time_coverage_start'],
                             '2000-03-01T00:00:00')
            self.assertEqual(ds2.attrs['time_coverage_end'],
                             '2000-06-01T00:00:00')
            self.assertEqual(ds2.attrs['time_coverage_resolution'],
                             'P1M')
            self.assertEqual(ds2.attrs['time_coverage_duration'],
                             'P93D')

    def test_wrong_type(self):
        ds = xr.Dataset({
            'first': (['time', 'lat', 'lon'], np.zeros([12, 45, 90])),
            'second': (['time', 'lat', 'lon'], np.zeros([12, 45, 90])),
            'lon': (['lon'], np.linspace(-178, 178, 90)),
            'lat': (['lat'], np.linspace(-88, 88, 45)),
            'time': (['time'], np.linspace(0, 1, 12))})

        ds1 = adjust_temporal_attrs(ds)

        self.assertIs(ds1, ds)
        self.assertNotIn('time_coverage_start', ds1)
        self.assertNotIn('time_coverage_end', ds1)
        self.assertNotIn('time_coverage_resolution', ds1)
        self.assertNotIn('time_coverage_duration', ds1)

    def test_bnds(self):
        """Test a case when time_bnds is available"""
        time = [datetime(2000, x, 1) for x in range(1, 13)]
        ds = xr.Dataset({
            'first': (['lat', 'lon', 'time'], np.zeros([45, 90, 12])),
            'second': (['lat', 'lon', 'time'], np.zeros([45, 90, 12])),
            'lat': np.linspace(-88, 88, 45),
            'lon': np.linspace(-178, 178, 90),
            'nv': [0, 1],
            'time': time})

        month_ends = list()
        for x in ds.time.values:
            year = int(str(x)[0:4])
            month = int(str(x)[5:7])
            day = calendar.monthrange(year, month)[1]
            month_ends.append(datetime(year, month, day))

        ds['time_bnds'] = (['time', 'nv'], list(zip(time, month_ends)))
        ds.time.attrs['bounds'] = 'time_bnds'

        ds1 = adjust_temporal_attrs(ds)

        # Make sure original dataset is not altered
        with self.assertRaises(KeyError):
            # noinspection PyStatementEffect
            ds.attrs['time_coverage_start']

        # Make sure expected values are in the new dataset
        self.assertEqual(ds1.attrs['time_coverage_start'],
                         '2000-01-01T00:00:00')
        self.assertEqual(ds1.attrs['time_coverage_end'],
                         '2000-12-31T00:00:00')
        self.assertEqual(ds1.attrs['time_coverage_resolution'],
                         'P1M')
        self.assertEqual(ds1.attrs['time_coverage_duration'],
                         'P366D')

    def test_single_slice(self):
        """Test a case when the dataset is a single time slice"""
        # With bnds
        ds = xr.Dataset({
            'first': (['lat', 'lon', 'time'], np.zeros([45, 90, 1])),
            'second': (['lat', 'lon', 'time'], np.zeros([45, 90, 1])),
            'lat': np.linspace(-88, 88, 45),
            'lon': np.linspace(-178, 178, 90),
            'nv': [0, 1],
            'time': [datetime(2000, 1, 1)]})
        ds.time.attrs['bounds'] = 'time_bnds'
        ds['time_bnds'] = (['time', 'nv'],
                           [(datetime(2000, 1, 1), datetime(2000, 1, 31))])

        ds1 = adjust_temporal_attrs(ds)

        # Make sure original dataset is not altered
        with self.assertRaises(KeyError):
            # noinspection PyStatementEffect
            ds.attrs['time_coverage_start']

        # Make sure expected values are in the new dataset
        self.assertEqual(ds1.attrs['time_coverage_start'],
                         '2000-01-01T00:00:00')
        self.assertEqual(ds1.attrs['time_coverage_end'],
                         '2000-01-31T00:00:00')
        self.assertEqual(ds1.attrs['time_coverage_duration'],
                         'P31D')
        with self.assertRaises(KeyError):
            # Resolution is not defined for a single slice
            # noinspection PyStatementEffect
            ds.attrs['time_coverage_resolution']

        # Without bnds
        ds = xr.Dataset({
            'first': (['lat', 'lon', 'time'], np.zeros([45, 90, 1])),
            'second': (['lat', 'lon', 'time'], np.zeros([45, 90, 1])),
            'lat': np.linspace(-88, 88, 45),
            'lon': np.linspace(-178, 178, 90),
            'time': [datetime(2000, 1, 1)]})

        ds1 = adjust_temporal_attrs(ds)

        self.assertEqual(ds1.attrs['time_coverage_start'],
                         '2000-01-01T00:00:00')
        self.assertEqual(ds1.attrs['time_coverage_end'],
                         '2000-01-01T00:00:00')
        with self.assertRaises(KeyError):
            # noinspection PyStatementEffect
            ds.attrs['time_coverage_resolution']
        with self.assertRaises(KeyError):
            # noinspection PyStatementEffect
            ds.attrs['time_coverage_duration']
