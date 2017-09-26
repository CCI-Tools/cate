import os.path
from contextlib import contextmanager
import tempfile
import shutil
import itertools
import sys

from unittest import TestCase

import numpy as np
import pandas as pd
import xarray as xr

from cate.core.objectio import OBJECT_IO_REGISTRY, read_object, write_object

__import__('cate.ops.io')


_counter = itertools.count()
ON_WIN = sys.platform == 'win32'


@contextmanager
def create_tmp_file():
    tmp_dir = tempfile.mkdtemp()
    path = os.path.join(tmp_dir, 'tmp_file_{}.nc'.format(next(_counter)))
    try:
        yield path
    finally:
        try:
            shutil.rmtree(tmp_dir)
        except OSError:
            if not ON_WIN:
                raise


class WriterRegistryTest(TestCase):
    def test_writers(self):
        self.assertIsNotNone(OBJECT_IO_REGISTRY.object_io_list)
        self.assertTrue(len(OBJECT_IO_REGISTRY.object_io_list) >= 4)

    def test_format_names(self):
        format_names = OBJECT_IO_REGISTRY.get_format_names()
        self.assertEqual(format_names, ['JSON', 'NETCDF3', 'NETCDF4', 'TEXT'])

    def test_find_reader(self):
        reader = OBJECT_IO_REGISTRY.find_reader(file='test.nc')
        self.assertIsNone(reader)

        reader = OBJECT_IO_REGISTRY.find_reader(format_name='NETCDF3', filename_ext='.nc')
        self.assertIsNotNone(reader)
        self.assertEqual(reader.format_name, 'NETCDF3')
        self.assertEqual(reader.filename_ext, '.nc')

        reader = OBJECT_IO_REGISTRY.find_reader(file='hello.txt')
        self.assertIsNotNone(reader)
        self.assertEqual(reader.format_name, 'TEXT')
        self.assertEqual(reader.filename_ext, '.txt')

        reader = OBJECT_IO_REGISTRY.find_reader(format_name='JSON')
        self.assertIsNotNone(reader)
        self.assertEqual(reader.format_name, 'JSON')
        self.assertEqual(reader.filename_ext, '.json')

        reader = OBJECT_IO_REGISTRY.find_reader(file='meris_l1b.dim',
                                                default_reader=OBJECT_IO_REGISTRY.find_reader(format_name='NETCDF4'))
        self.assertIsNotNone(reader)
        self.assertEqual(reader.format_name, 'NETCDF4')
        self.assertEqual(reader.filename_ext, '.nc')

        reader = OBJECT_IO_REGISTRY.find_reader(format_name='BEAM-DIMAP')
        self.assertIsNone(reader)

    def test_find_writer(self):
        writer = OBJECT_IO_REGISTRY.find_writer(filename_ext='.nc')
        self.assertIsNotNone(writer)
        self.assertIn(writer.format_name, {'NETCDF3', 'NETCDF4'})
        self.assertEqual(writer.filename_ext, '.nc')

        writer = OBJECT_IO_REGISTRY.find_writer(obj=xr.Dataset(), format_name='NETCDF4')
        self.assertIsNotNone(writer)
        self.assertTrue(writer.format_name == 'NETCDF4')
        self.assertEqual(writer.filename_ext, '.nc')

        writer = OBJECT_IO_REGISTRY.find_writer(format_name='NETCDF3', filename_ext='.nc')
        self.assertIsNotNone(writer)
        self.assertEqual(writer.format_name, 'NETCDF3')
        self.assertEqual(writer.filename_ext, '.nc')

        writer = OBJECT_IO_REGISTRY.find_writer(filename_ext='.txt')
        self.assertIsNotNone(writer)
        self.assertEqual(writer.format_name, 'TEXT')
        self.assertEqual(writer.filename_ext, '.txt')

        writer = OBJECT_IO_REGISTRY.find_writer(obj=dict(a=3), format_name='JSON')
        self.assertIsNotNone(writer)
        self.assertEqual(writer.format_name, 'JSON')
        self.assertEqual(writer.filename_ext, '.json')

        writer = OBJECT_IO_REGISTRY.find_writer(format_name='BEAM-DIMAP',
                                                default_writer=OBJECT_IO_REGISTRY.find_writer(format_name='NETCDF3'))
        self.assertIsNotNone(writer)
        self.assertEqual(writer.format_name, 'NETCDF3')
        self.assertEqual(writer.filename_ext, '.nc')

        writer = OBJECT_IO_REGISTRY.find_writer(format_name='BEAM-DIMAP')
        self.assertIsNone(writer)


class WriteObjectTest(TestCase):
    def test_write_read_object_TEXT(self):
        self._test_write_read_object('Hallo!', 'write_obj_TEXT.txt', 'TEXT')

    def test_write_read_object_JSON(self):
        self._test_write_read_object(dict(a=1, b=2), 'write_obj_JSON.json', 'JSON')

    def test_write_read_object_NETCDF4(self):
        with create_tmp_file() as tmp_file:
            self._test_write_read_object_2(self.new_ds(), tmp_file, 'NETCDF4')

    def test_write_read_object_NETCDF3(self):
        with create_tmp_file() as tmp_file:
            self._test_write_read_object_2(self.new_ds(), tmp_file, 'NETCDF3')

    def _test_write_read_object(self, obj, file_path, format_name):
        if os.path.exists(file_path):
            os.remove(file_path)
        self.assertFalse(os.path.exists(file_path))
        writer = write_object(obj, file_path, format_name=format_name)
        self.assertIsNotNone(writer)
        self.assertTrue(os.path.isfile(file_path))
        obj, reader = read_object(file_path)
        self.assertIsNotNone(obj)
        self.assertIsNotNone(reader)
        if hasattr(obj, 'close'):
            obj.close()
        os.remove(file_path)

    def _test_write_read_object_2(self, obj, file_path, format_name):
        self.assertFalse(os.path.exists(file_path))
        writer = write_object(obj, file_path, format_name=format_name)
        self.assertIsNotNone(writer)
        self.assertTrue(os.path.isfile(file_path))
        obj, reader = read_object(file_path)
        self.assertIsNotNone(obj)
        self.assertIsNotNone(reader)

    def new_ds(self):
        periods = 5
        temperature = (15 + 8 * np.random.randn(2, 2, periods)).round(decimals=1)
        precipitation = (10 * np.random.rand(2, 2, periods)).round(decimals=1)
        lon = [[-99.83, -99.32], [-99.79, -99.23]]
        lat = [[42.25, 42.21], [42.63, 42.59]]
        ds = xr.Dataset({'temperature': (['x', 'y', 'time'], temperature),
                         'precipitation': (['x', 'y', 'time'], precipitation)
                         },
                        coords={'lon': (['x', 'y'], lon),
                                'lat': (['x', 'y'], lat),
                                'time': pd.date_range('2014-09-06', periods=periods),
                                'reference_time': pd.Timestamp('2014-09-05')
                                })
        return ds
