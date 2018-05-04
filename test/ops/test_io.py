"""
Test the IO operations
"""

import os
import unittest
from io import StringIO
from unittest import TestCase

import geopandas as gpd

from cate.ops.io import open_dataset, save_dataset, read_csv, read_geo_data_frame, write_csv


class TestIO(TestCase):
    @unittest.skip(reason="skipped unless you want to debug data source access")
    def test_open_dataset(self):
        # Test normal functionality
        dataset = open_dataset(ds_id='AEROSOL_AATSR_SU_L3_V4.21_MONTHLY',
                               time_range='2008-01-01, 2008-03-01')
        self.assertIsNotNone(dataset)

        # Test swapped dates
        with self.assertRaises(ValueError):
            open_dataset(ds_id='AEROSOL_AATSR_SU_L3_V4.21_MONTHLY', time_range='2008-03-01, 2008-01-01')

        # Test required arguments
        with self.assertRaises(TypeError):
            open_dataset(ds_id='AEROSOL_AATSR_SU_L3_V4.21_MONTHLY', time_range='2008-03-01')

    @unittest.skip(reason="skipped unless you want to debug data source access")
    def test_save_dataset(self):
        # Test normal functionality
        dataset = open_dataset(ds_id='AEROSOL_AATSR_SU_L3_V4.21_MONTHLY',
                               time_range='2008-01-01, 2008-03-01')
        save_dataset(dataset, 'remove_me.nc')
        self.assertTrue(os.path.isfile('remove_me.nc'))
        os.remove('remove_me.nc')

        # Test required arguments
        with self.assertRaises(TypeError):
            save_dataset(dataset)

        # Test behavior when passing unexpected type
        with self.assertRaises(NotImplementedError):
            dataset = ('a', 1, 3, 5)
            save_dataset(dataset, 'remove_me.nc')

        self.assertFalse(os.path.isfile('remove_me.nc'))

    def test_read_csv(self):
        raw_data = "id,first_name,last_name,age,preTestScore,postTestScore\n0,Jason,Miller,42,4,\"25,000\"\n"
        file_out = StringIO(raw_data)
        file_in = StringIO()

        df = read_csv(file_out, index_col='id')
        df.to_csv(file_in)

        self.assertEqual(file_in.getvalue(), raw_data)

        raw_data = "time,first_name,last_name,age,preTestScore,postTestScore\n1981-01-01,Jason,Miller,42,4,\"25,000\"\n"
        file_out = StringIO(raw_data)
        file_in = StringIO()

        df = read_csv(file_out, index_col='time')
        df.to_csv(file_in)

        self.assertEqual(file_in.getvalue(), raw_data)

    def test_read_geo_data_frame(self):
        file = os.path.join(os.path.dirname(__file__), '..', '..', 'cate', 'ds', 'data', 'countries',
                            'countries.geojson')

        data_frame = read_geo_data_frame(file)
        self.assertIsInstance(data_frame, gpd.GeoDataFrame)
        self.assertEqual(len(data_frame), 179)
        data_frame.close()

        # Now with crs
        data_frame = read_geo_data_frame(file, crs="EPSG:4326")
        self.assertIsInstance(data_frame, gpd.GeoDataFrame)
        self.assertEqual(len(data_frame), 179)
        data_frame.close()

    def test_write_csv(self):
        import io
        import xarray as xr
        import numpy as np

        ds = xr.Dataset(
            data_vars=dict(delta=xr.DataArray(np.linspace(-12, 13, 3 * 2 * 2, dtype=np.int16).reshape((3, 2, 2)),
                                              dims=['time', 'lat', 'lon']),
                           mean=xr.DataArray(np.linspace(2, 13, 3 * 2 * 2, dtype=np.uint16).reshape((3, 2, 2)),
                                             dims=['time', 'lat', 'lon'])),
            coords=dict(time=[1, 2, 3],
                        lat=[51, 51.2],
                        lon=[10.2, 11.4]))

        file = io.StringIO()
        write_csv(ds, file=file)
        self.assertEqual(file.getvalue(), 'lat,lon,time,delta,mean\n'
                                          '51.0,10.2,1,-12,2\n'
                                          '51.0,10.2,2,-2,6\n'
                                          '51.0,10.2,3,6,10\n'
                                          '51.0,11.4,1,-9,3\n'
                                          '51.0,11.4,2,0,7\n'
                                          '51.0,11.4,3,8,11\n'
                                          '51.2,10.2,1,-7,4\n'
                                          '51.2,10.2,2,1,8\n'
                                          '51.2,10.2,3,10,12\n'
                                          '51.2,11.4,1,-5,5\n'
                                          '51.2,11.4,2,3,9\n'
                                          '51.2,11.4,3,13,13\n')

        file = io.StringIO()
        write_csv(ds, file=file, columns=['mean'], delimiter=';')
        self.assertEqual(file.getvalue(), 'lat;lon;time;mean\n'
                                          '51.0;10.2;1;2\n'
                                          '51.0;10.2;2;6\n'
                                          '51.0;10.2;3;10\n'
                                          '51.0;11.4;1;3\n'
                                          '51.0;11.4;2;7\n'
                                          '51.0;11.4;3;11\n'
                                          '51.2;10.2;1;4\n'
                                          '51.2;10.2;2;8\n'
                                          '51.2;10.2;3;12\n'
                                          '51.2;11.4;1;5\n'
                                          '51.2;11.4;2;9\n'
                                          '51.2;11.4;3;13\n')
