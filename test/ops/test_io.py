"""
Test the IO operations
"""

import os
import unittest
from unittest import TestCase
from io import StringIO

from cate.ops.io import open_dataset, save_dataset, read_csv, read_geo_data


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

    def test_read_geo_data(self):
        file = os.path.join('cate', 'ds', 'data', 'countries', 'countries.geojson')
        data_frame = read_geo_data(file)
        self.assertIsNotNone(data_frame)
        data_frame.close()




