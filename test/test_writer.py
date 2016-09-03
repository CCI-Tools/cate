import os.path
from unittest import TestCase

import numpy as np
import pandas as pd
import xarray as xr
from ect.core.writer import WRITER_REGISTRY, write_obj


class WriterRegistryTest(TestCase):
    def test_writers(self):
        self.assertIsNotNone(WRITER_REGISTRY.writers)
        self.assertTrue(len(WRITER_REGISTRY.writers) >= 4)

    def test_find_writer(self):
        writer = WRITER_REGISTRY.find_writer(obj=xr.Dataset())
        self.assertIsNotNone(writer)
        self.assertEquals(writer.format_name, 'NETCDF4')
        self.assertEquals(writer.filename_ext, '.nc')

        writer = WRITER_REGISTRY.find_writer(format_name='NETCDF3', filename_ext='.nc')
        self.assertIsNotNone(writer)
        self.assertEquals(writer.format_name, 'NETCDF3')
        self.assertEquals(writer.filename_ext, '.nc')

        writer = WRITER_REGISTRY.find_writer(filename_ext='.txt')
        self.assertIsNotNone(writer)
        self.assertEquals(writer.format_name, 'TEXT')
        self.assertEquals(writer.filename_ext, '.txt')

        writer = WRITER_REGISTRY.find_writer(obj=dict(a=3), format_name='JSON')
        self.assertIsNotNone(writer)
        self.assertEquals(writer.format_name, 'JSON')
        self.assertEquals(writer.filename_ext, '.json')

        writer = WRITER_REGISTRY.find_writer(format_name='BEAM-DIMAP',
                                             default_writer=WRITER_REGISTRY.find_writer(format_name='NETCDF3'))
        self.assertIsNotNone(writer)
        self.assertEquals(writer.format_name, 'NETCDF3')
        self.assertEquals(writer.filename_ext, '.nc')

        writer = WRITER_REGISTRY.find_writer(format_name='BEAM-DIMAP')
        self.assertIsNone(writer)


class WriteObjTest(TestCase):
    def test_write_obj_TEXT(self):
        self._test_write_obj('Hallo!', 'write_obj_TEXT.txt', 'TEXT')

    def test_write_obj_JSON(self):
        self._test_write_obj(dict(a=1, b=2), 'write_obj_JSON.json', 'JSON')

    def test_write_obj_NETCDF4(self):
        self._test_write_obj(self.new_ds(), 'write_obj_NETCDF4.nc', 'NETCDF4')

    def test_write_obj_NETCDF3(self):
        self._test_write_obj(self.new_ds(), 'write_obj_NETCDF3.nc', 'NETCDF3')

    def _test_write_obj(self, obj, file_path, format_name):
        if os.path.exists(file_path):
            os.remove(file_path)
        self.assertFalse(os.path.exists(file_path))
        write_obj(obj, file_path, format_name=format_name)
        self.assertTrue(os.path.isfile(file_path))
        os.remove(file_path)

    def new_ds(self):
        temperature = 15 + 8 * np.random.randn(2, 2, 3)
        precipitation = 10 * np.random.rand(2, 2, 3)
        lon = [[-99.83, -99.32], [-99.79, -99.23]]
        lat = [[42.25, 42.21], [42.63, 42.59]]
        ds = xr.Dataset({'temperature': (['x', 'y', 'time'], temperature),
                         'precipitation': (['x', 'y', 'time'], precipitation)
                         },
                        coords={'lon': (['x', 'y'], lon),
                                'lat': (['x', 'y'], lat),
                                'time': pd.date_range('2014-09-06', periods=3),
                                'reference_time': pd.Timestamp('2014-09-05')
                                })
        return ds
