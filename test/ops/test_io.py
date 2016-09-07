"""
Test the IO operations
"""

import os
import unittest
from unittest import TestCase

from ect.ops.io import load_dataset, store_dataset


class TestIO(TestCase):
    @unittest.skip(reason="skipped unless you want to debug data source access")
    def test_load_dataset(self):
        # Test normal functionality
        dset = None
        dset = load_dataset('AEROSOL_AATSR_SU_L3_V4.21_MONTHLY', '2008-01-01', '2008-03-01')
        self.assertIsNotNone(dset)

        # Test swapped dates
        with self.assertRaises(ValueError):
            dset = load_dataset('AEROSOL_AATSR_SU_L3_V4.21_MONTHLY', '2008-03-01', '2008-01-01')

        # Test required arguments
        with self.assertRaises(TypeError):
            dset = load_dataset('AEROSOL_AATSR_SU_L3_V4.21_MONTHLY', '2008-03-01')

    @unittest.skip(reason="skipped unless you want to debug data source access")
    def test_save_dataset(self):
        # Test normal functionality
        dset = None
        dset = load_dataset('AEROSOL_AATSR_SU_L3_V4.21_MONTHLY', '2008-01-01', '2008-03-01')
        store_dataset(dset, 'remove_me.nc')
        self.assertTrue(os.path.isfile('remove_me.nc'))
        os.remove('remove_me.nc')

        # Test required arguments
        with self.assertRaises(TypeError):
            store_dataset(dset)

        # Test behavior when passing unexpected type
        with self.assertRaises(NotImplementedError):
            dset = ('a', 1, 3, 5)
            store_dataset(dset, 'remove_me.nc')

        self.assertFalse(os.path.isfile('remove_me.nc'))
