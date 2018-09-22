"""
Test the IO operations
"""
import os
import shutil

import shapely.wkt
import unittest
from io import StringIO
from unittest import TestCase

import geopandas as gpd

from cate.core.types import ValidationError
from cate.ops.io import open_dataset, save_dataset, read_csv, read_geo_data_frame, write_csv, write_geo_data_frame


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
            # noinspection PyTypeChecker
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

        data_frame = read_geo_data_frame(file=file)
        self.assertIsInstance(data_frame, gpd.GeoDataFrame)
        self.assertEqual(len(data_frame), 179)
        data_frame.close()

    def test_write_geo_data_frame(self):
        gdf = gpd.GeoDataFrame({'coli': [1, 2, 3, 4, 5, 6],
                                'cols': ['a', 'b', 'c', 'x', 'y', 'z'],
                                'colf': [0.4, 0.5, 0.3, 0.3, 0.1, 0.4],
                                'geometry': gpd.GeoSeries([
                                    shapely.wkt.loads('POINT(10 10)'),
                                    shapely.wkt.loads('POINT(10 20)'),
                                    shapely.wkt.loads('POINT(10 30)'),
                                    shapely.wkt.loads('POINT(20 30)'),
                                    shapely.wkt.loads('POINT(20 20)'),
                                    shapely.wkt.loads('POINT(20 10)'),
                                ])})

        out_dir = os.path.join(os.path.dirname(__file__), '..', '..', "_test_outputs")
        shutil.rmtree(out_dir, ignore_errors=True)
        os.makedirs(out_dir, exist_ok=True)

        file = os.path.join(out_dir, 'test1.geojson')
        write_geo_data_frame(gdf=gdf, file=file)
        self.assertTrue(os.path.isfile(file))

        file = os.path.join(out_dir, 'test2.js')
        write_geo_data_frame(gdf=gdf, file=file, more_args=dict(driver='GeoJSON'))
        self.assertTrue(os.path.isfile(file))

        file = os.path.join(out_dir, 'test3')
        write_geo_data_frame(gdf=gdf, file=file)
        self.assertTrue(os.path.isfile(file + ".shp"))

        file = os.path.join(out_dir, 'test4.shp')
        write_geo_data_frame(gdf=gdf, file=file)
        self.assertTrue(os.path.isfile(file))

        # noinspection PyBroadException
        try:
            file = os.path.join(out_dir, 'test5.gpkg')
            write_geo_data_frame(gdf=gdf, file=file)
            self.assertTrue(os.path.isfile(file))
        except BaseException as e:
            # Success of writing to GPKG is platform dependent, so we don't care here about errors
            print(f'ignoring test failure: {e}')

        # noinspection PyBroadException
        try:
            file = os.path.join(out_dir, 'test6.gpx')
            write_geo_data_frame(gdf=gdf, file=file)
            self.assertTrue(os.path.isfile(file))
        except BaseException as e:
            # Success of writing to GPX is platform dependent, so we don't care here about errors
            print(f'ignoring test failure: {e}')

        file = os.path.join(out_dir, 'test7.bibo')
        with self.assertRaises(ValidationError) as cm:
            write_geo_data_frame(gdf=gdf, file=file)
        self.assertEquals(f'{cm.exception}', 'Cannot detect supported format from file extension ".bibo"')

        shutil.rmtree(out_dir, ignore_errors=True)

    def test_write_csv_with_dataset(self):
        import io
        import xarray as xr
        import numpy as np

        time = [1, 2, 3]
        lat = [51, 51.2]
        lon = [10.2, 11.4]
        ds = xr.Dataset(
            data_vars=dict(delta=xr.DataArray(np.linspace(-12, 13, 3 * 2 * 2, dtype=np.int16).reshape((3, 2, 2)),
                                              dims=['time', 'lat', 'lon']),
                           mean=xr.DataArray(np.linspace(2, 13, 3 * 2 * 2, dtype=np.uint16).reshape((3, 2, 2)),
                                             dims=['time', 'lat', 'lon'])),
            coords=dict(time=time, lat=lat, lon=lon))

        file = io.StringIO()
        write_csv(ds, file=file)
        self.assertEqual(file.getvalue(), 'index,time,lat,lon,delta,mean\n'
                                          '0,1,51.0,10.2,-12,2\n'
                                          '1,1,51.0,11.4,-9,3\n'
                                          '2,1,51.2,10.2,-7,4\n'
                                          '3,1,51.2,11.4,-5,5\n'
                                          '4,2,51.0,10.2,-2,6\n'
                                          '5,2,51.0,11.4,0,7\n'
                                          '6,2,51.2,10.2,1,8\n'
                                          '7,2,51.2,11.4,3,9\n'
                                          '8,3,51.0,10.2,6,10\n'
                                          '9,3,51.0,11.4,8,11\n'
                                          '10,3,51.2,10.2,10,12\n'
                                          '11,3,51.2,11.4,13,13\n')

        file = io.StringIO()
        write_csv(ds, file=file, columns=['mean'], delimiter=';')
        self.assertEqual(file.getvalue(), 'index;time;lat;lon;mean\n'
                                          '0;1;51.0;10.2;2\n'
                                          '1;1;51.0;11.4;3\n'
                                          '2;1;51.2;10.2;4\n'
                                          '3;1;51.2;11.4;5\n'
                                          '4;2;51.0;10.2;6\n'
                                          '5;2;51.0;11.4;7\n'
                                          '6;2;51.2;10.2;8\n'
                                          '7;2;51.2;11.4;9\n'
                                          '8;3;51.0;10.2;10\n'
                                          '9;3;51.0;11.4;11\n'
                                          '10;3;51.2;10.2;12\n'
                                          '11;3;51.2;11.4;13\n')
        file = io.StringIO()
        with self.assertRaises(ValidationError) as cm:
            write_csv(None, file=file)
        self.assertEqual(str(cm.exception),
                         "Input 'obj' for operation 'cate.ops.io.write_csv' must be given.")

        with self.assertRaises(ValidationError) as cm:
            write_csv(ds, file=None)
        self.assertEqual(str(cm.exception),
                         "Input 'file' for operation 'cate.ops.io.write_csv' must be given.")

        ds_mixed_dims = ds.copy()
        ds_mixed_dims['error'] = xr.DataArray(np.linspace(1, 2, len(time)), dims=['time'])

        with self.assertRaises(ValidationError) as cm:
            write_csv(ds_mixed_dims, file=file)
        self.assertEqual(str(cm.exception),
                         'Not all variables have the same dimensions. '
                         'Please select variables so that their dimensions are equal.')

        file = io.StringIO()
        write_csv(ds_mixed_dims, file=file, columns=['error'], delimiter=';')
        self.assertEqual(file.getvalue(), 'index;time;error\n'
                                          '0;1;1.0\n'
                                          '1;2;1.5\n'
                                          '2;3;2.0\n')

    def test_write_csv_with_data_frame(self):
        import io
        import pandas as pd

        df = pd.DataFrame(data=dict(time=[1, 2, 3],
                                    lat=[51.0, 51.1, 51.2],
                                    lon=[10.2, 11.4, 11.8],
                                    delta=[-1, 0, -1],
                                    mean=[0.8, 0.5, 0.3]),
                          columns=['time', 'lat', 'lon', 'delta', 'mean'],
                          index=None)

        file = io.StringIO()
        write_csv(df, file=file)
        self.assertEqual(file.getvalue(), 'index,time,lat,lon,delta,mean\n'
                                          '0,1,51.0,10.2,-1,0.8\n'
                                          '1,2,51.1,11.4,0,0.5\n'
                                          '2,3,51.2,11.8,-1,0.3\n')
