import os
import unittest
from typing import Sequence, Any, Optional, Tuple
from unittest import TestCase

import numpy as np
import xarray as xr
import xcube.core.store as xcube_store

from cate.core.ds import DataStore, DataAccessError, open_xarray_dataset, open_dataset, DATA_STORE_REGISTRY, \
    get_spatial_ext_chunk_sizes, get_ext_chunk_sizes, NetworkError, get_metadata_from_descriptor, find_data_store, \
    get_data_descriptor
from cate.core.types import ValidationError
from tests.util.test_monitor import RecordingMonitor

_TEST_DATA_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'test_data')


class SimpleDataStore(DataStore):

    def __init__(self, ds_id: str, data_ids: Sequence[Tuple[str, Optional[str]]]):
        super().__init__(ds_id, title='Simple Test Store', is_local=True)
        self._data_ids = data_ids

    def get_data_ids(self) -> Sequence[Tuple[str, Optional[str]]]:
        return self._data_ids

    def describe_data(self, data_id: str) -> xcube_store.DataDescriptor:
        if not self.has_data(data_id):
            raise DataAccessError(f'{data_id} is not contained in data store {self.id}.')
        return xcube_store.DatasetDescriptor(data_id=data_id)

    def open_data(self, data_id: str, **open_params) -> Any:
        if not self.has_data(data_id):
            raise DataAccessError(f'{data_id} is not contained in data store {self.id}.')
        return xr.Dataset({'a': 42})

    def write_data(self, data: Any, data_id: str = None) -> str:
        raise xcube_store.DataStoreError('Not supported')


class DataStoreTest(TestCase):
    def setUp(self):
        self.TEST_DATA_STORE = SimpleDataStore('test_aero_ozone',
                                               [('aerosol', 'Particles in air'),
                                                ('ozone', 'Oxygen molecules')])
        self.TEST_DATA_STORE_SST = SimpleDataStore('test_sst',
                                                   [('sst', 'Sea surface temperature')])

    def test_id(self):
        self.assertEqual('test_aero_ozone', self.TEST_DATA_STORE.id)
        self.assertEqual('test_sst', self.TEST_DATA_STORE_SST.id)

    def test_title(self):
        self.assertEqual('Simple Test Store', self.TEST_DATA_STORE.title)
        self.assertEqual('Simple Test Store', self.TEST_DATA_STORE_SST.title)

    def test_is_local(self):
        self.assertTrue(self.TEST_DATA_STORE.is_local)
        self.assertTrue(self.TEST_DATA_STORE_SST.is_local)

    def test_get_data_ids(self):
        self.assertEqual([('aerosol', 'Particles in air'), ('ozone', 'Oxygen molecules')],
                         self.TEST_DATA_STORE.get_data_ids())
        self.assertEqual([('sst', 'Sea surface temperature')],
                         self.TEST_DATA_STORE_SST.get_data_ids())

    def test_has_data(self):
        self.assertTrue(self.TEST_DATA_STORE.has_data('aerosol'))
        self.assertTrue(self.TEST_DATA_STORE.has_data('ozone'))
        self.assertFalse(self.TEST_DATA_STORE.has_data('sst'))

        self.assertFalse(self.TEST_DATA_STORE_SST.has_data('aerosol'))
        self.assertFalse(self.TEST_DATA_STORE_SST.has_data('ozone'))
        self.assertTrue(self.TEST_DATA_STORE_SST.has_data('sst'))

    def test_describe_data(self):
        aerosol_descriptor = self.TEST_DATA_STORE.describe_data('aerosol')
        self.assertEqual('aerosol', aerosol_descriptor.data_id)
        self.assertEqual('dataset', aerosol_descriptor.type_id)

        ozone_descriptor = self.TEST_DATA_STORE.describe_data('ozone')
        self.assertEqual('ozone', ozone_descriptor.data_id)
        self.assertEqual('dataset', ozone_descriptor.type_id)

        sst_descriptor = self.TEST_DATA_STORE_SST.describe_data('sst')
        self.assertEqual('sst', sst_descriptor.data_id)
        self.assertEqual('dataset', sst_descriptor.type_id)

    def test_open_data(self):
        aerosol_data = self.TEST_DATA_STORE.open_data('aerosol')
        self.assertEqual([42], aerosol_data.a.data)


class IOTest(TestCase):

    def setUp(self):
        self.TEST_DATA_STORE = SimpleDataStore('test_aero_ozone',
                                               [('aerosol', 'Particles in air'),
                                                ('ozone', 'Oxygen molecules'),
                                                ('oc', 'ocean colour')])
        self.TEST_DATA_STORE_SST = SimpleDataStore('test_sst',
                                                   [('sst', 'Sea surface temperature'),
                                                    ('oc', 'ocean colour')])
        self._orig_stores = list(DATA_STORE_REGISTRY.get_data_stores())
        DATA_STORE_REGISTRY._data_stores.clear()
        DATA_STORE_REGISTRY.add_data_store(self.TEST_DATA_STORE)
        DATA_STORE_REGISTRY.add_data_store(self.TEST_DATA_STORE_SST)

    def tearDown(self):
        DATA_STORE_REGISTRY._data_stores.clear()
        for data_store in self._orig_stores:
            DATA_STORE_REGISTRY.add_data_store(data_store)

    def test_find_data_store(self):
        aerosol_store = find_data_store('aerosol')
        self.assertEqual('test_aero_ozone', aerosol_store.id)

        ozone_store = find_data_store('ozone')
        self.assertEqual('test_aero_ozone', ozone_store.id)

        sst_store = find_data_store('sst')
        self.assertEqual('test_sst', sst_store.id)

        permafrost_store = find_data_store('permafrost')
        self.assertIsNone(permafrost_store)

        with self.assertRaises(ValidationError):
            find_data_store('oc')
            self.fail('ValidationError expected')

    def test_find_data_store_use_store_param(self):
        aerosol_store = find_data_store('aerosol', data_stores=[self.TEST_DATA_STORE])
        self.assertEqual('test_aero_ozone', aerosol_store.id)

        aerosol_store = find_data_store('aerosol', data_stores=[self.TEST_DATA_STORE_SST])
        self.assertIsNone(aerosol_store)

        sst_store = find_data_store('sst', data_stores=[self.TEST_DATA_STORE_SST])
        self.assertEqual('test_sst', sst_store.id)

        sst_store = find_data_store('sst', data_stores=[self.TEST_DATA_STORE])
        self.assertIsNone(sst_store)

    def test_get_data_descriptor(self):
        aerosol_descriptor = get_data_descriptor('aerosol')
        self.assertIsInstance(aerosol_descriptor, xcube_store.DatasetDescriptor)
        self.assertEqual('aerosol', aerosol_descriptor.data_id)

        sst_descriptor = get_data_descriptor('sst')
        self.assertIsInstance(sst_descriptor, xcube_store.DatasetDescriptor)
        self.assertEqual('sst', sst_descriptor.data_id)

        permafrost_descriptor = get_data_descriptor('permafrost')
        self.assertIsNone(permafrost_descriptor)

        with self.assertRaises(ValidationError):
            get_data_descriptor('oc')

    def test_get_metadata_from_descriptor(self):
        descriptor = xcube_store.DatasetDescriptor(
            data_id='xyz',
            type_id='abc',
            crs='EPSG:9346',
            bbox=(10., 20., 30., 40.),
            spatial_res=20.,
            time_range=('2017-06-05', '2017-06-27'),
            time_period='daily',
            data_vars=[xcube_store.VariableDescriptor(name='surface_pressure',
                                                      dtype='rj',
                                                      dims=('dfjhrt', 'sg'),
                                                      attrs=dict(units='hPa',
                                                                 long_name='dgfrf',
                                                                 standard_name='dhgydf'
                                                                 )
                                                      )],
            attrs=dict(
                title= 'ESA Ozone Climate Change Initiative (Ozone CCI): Level 3 Nadir Ozone Profile Merged Data Product, version 2',
                institution= 'Royal Netherlands Meteorological Institute, KNMI',
                source= 'This dataset contains L2 profiles from GOME, SCIAMACHY, OMI and GOME-2 gridded onto a global grid.',
                history= 'L2 data gridded to global grid.',
                references= 'http://www.esa-ozone-cci.org/',
                tracking_id= '32CF0EE6-1F21-4FAE-B0BE-A8C6FD88A775',
                Conventions= 'CF-1.6',
                product_version= 'fv0002',
                summary= 'This dataset contains L2 profiles from GOME, SCIAMACHY, OMI and GOME-2 gridded onto a global grid.',
                keywords= 'satellite, observation, atmosphere, ozone',
                id= '32CF0EE6-1F21-4FAE-B0BE-A8C6FD88A775',
                naming_authority= 'KNMI, http://www.knmi.nl/',
                comment= 'These data were produced at KNMI as part of the ESA OZONE CCI project.',
                date_created= '2014-01-08T12:50:21.908',
                creator_name= 'J.C.A. van Peet',
                creator_url= 'KNMI, http://www.knmi.nl/',
                creator_email= 'peet@knmi.nl',
                project= 'Climate Change Initiative - European Space Agency',
                geospatial_lat_min= -90.0,
                geospatial_lat_max= 90.0,
                geospatial_lat_units= 'degree_north',
                geospatial_lat_resolution= 1.0,
                geospatial_lon_min= -180.0,
                geospatial_lon_max= 180.0,
                geospatial_lon_units= 'degree_east',
                geospatial_lon_resolution= 1.0,
                geospatial_vertical_min= 0.01,
                geospatial_vertical_max= 1013.0,
                time_coverage_start= '19970104T102333Z',
                time_coverage_end= '19970131T233849Z',
                time_coverage_duration= 'P1M',
                time_coverage_resolution= 'P1M',
                standard_name_vocabulary= 'NetCDF Climate and Forecast(CF) Metadata Convention version 20, 11 September 2012',
                license= 'data use is free and open',
                platform= 'merged: ERS-2, ENVISAT, EOS-AURA, METOP-A',
                sensor= 'merged: GOME, SCIAMACHY, OMI and GOME-2.',
                spatial_resolution= 'see geospatial_lat_resolution and geospatial_lat_resolution',
                Note= 'netCDF compression applied.',
                ecv= 'OZONE',
                time_frequency= 'month',
                institute= 'Royal Netherlands Meteorological Institute',
                processing_level= 'L3',
                product_string= 'MERGED',
                data_type= 'NP',
                file_formats= ['.nc', '.txt']
            )
        )
        descriptor_metadata = get_metadata_from_descriptor(descriptor)
        expected_metadata = dict(
            data_id='xyz',
            type_id='abc',
            crs='EPSG:9346',
            bbox=(10., 20., 30., 40.),
            spatial_res=20.,
            time_range=('2017-06-05', '2017-06-27'),
            time_period='daily',
            title='ESA Ozone Climate Change Initiative (Ozone CCI): Level 3 Nadir Ozone Profile Merged Data Product, version 2',
            product_version='fv0002',
            ecv='OZONE',
            time_frequency='month',
            institute='Royal Netherlands Meteorological Institute',
            processing_level='L3',
            product_string='MERGED',
            data_type='NP',
            file_formats=['.nc', '.txt'],
            variables=[
                dict(name='surface_pressure',
                     units='hPa',
                     long_name='dgfrf',
                     standard_name='dhgydf'
                     )
                ]
        )
        self.assertEqual(expected_metadata, descriptor_metadata)

    def test_open_dataset(self):
        with self.assertRaises(ValueError) as cm:
            # noinspection PyTypeChecker
            open_dataset(None)
        self.assertTupleEqual(('No data source given',), cm.exception.args)

        with self.assertRaises(ValueError) as cm:
            open_dataset('foo')
        self.assertEqual(("No data store found that contains the ID 'foo'",), cm.exception.args)

        aerosol_dataset, aerosol_dataset_name = open_dataset('aerosol')
        self.assertIsNotNone(aerosol_dataset)
        self.assertEqual('aerosol', aerosol_dataset_name)
        self.assertIsInstance(aerosol_dataset, xr.Dataset)
        self.assertEqual(42, aerosol_dataset.a.values)

    def test_open_xarray_dataset(self):
        path_large = os.path.join(_TEST_DATA_PATH, 'large', '*.nc')
        path_small = os.path.join(_TEST_DATA_PATH, 'small', '*.nc')

        ds_large_mon = RecordingMonitor()
        ds_small_mon = RecordingMonitor()
        ds_large = open_xarray_dataset(path_large, monitor=ds_large_mon)
        ds_small = open_xarray_dataset(path_small, monitor=ds_small_mon)

        # Test monitors
        self.assertEqual(ds_large_mon.records, [('start', 'Opening dataset', 1), ('progress', 1, None, 100), ('done',)])
        self.assertEqual(ds_small_mon.records, [('start', 'Opening dataset', 1), ('progress', 1, None, 100), ('done',)])

        # Test chunking
        self.assertEqual(ds_small.chunks, {'lon': (1440,), 'lat': (720,), 'time': (1,)})
        self.assertEqual(ds_large.chunks, {'lon': (7200,), 'lat': (3600,), 'time': (1,), 'bnds': (2,)})

class ChunkUtilsTest(unittest.TestCase):
    def test_get_spatial_ext_chunk_sizes(self):
        ds = xr.Dataset({
            'v1': (['lat', 'lon'], np.zeros([45, 90])),
            'v2': (['lat', 'lon'], np.zeros([45, 90])),
            'v3': (['lon'], np.zeros(90)),
            'lon': (['lon'], np.linspace(-178, 178, 90)),
            'lat': (['lat'], np.linspace(-88, 88, 45))})
        ds.v1.encoding['chunksizes'] = (5, 10)
        ds.v2.encoding['chunksizes'] = (15, 30)
        chunk_sizes = get_spatial_ext_chunk_sizes(ds)
        self.assertIsNotNone(chunk_sizes)
        self.assertEqual(chunk_sizes, dict(lat=15, lon=30))

    def test_get_spatial_ext_chunk_sizes_wo_lat_lon(self):
        ds = xr.Dataset({
            'v1': (['lon'], np.zeros([90])),
            'v2': (['lat'], np.zeros([45]))})
        ds.v1.encoding['chunksizes'] = (90,)
        ds.v2.encoding['chunksizes'] = (45,)
        chunk_sizes = get_spatial_ext_chunk_sizes(ds)
        self.assertIsNone(chunk_sizes)

    def test_get_spatial_ext_chunk_sizes_with_time(self):
        ds = xr.Dataset({
            'v1': (['time', 'lat', 'lon'], np.zeros([12, 45, 90])),
            'v2': (['time', 'lat', 'lon'], np.zeros([12, 45, 90])),
            'v3': (['lon'], np.zeros(90)),
            'lon': (['lon'], np.linspace(-178, 178, 90)),
            'lat': (['lat'], np.linspace(-88, 88, 45)),
            'time': (['time'], np.linspace(0, 1, 12))})
        ds.v1.encoding['chunksizes'] = (1, 5, 10)
        ds.v2.encoding['chunksizes'] = (1, 15, 30)
        ds.v3.encoding['chunksizes'] = (90,)
        chunk_sizes = get_spatial_ext_chunk_sizes(ds)
        self.assertIsNotNone(chunk_sizes)
        self.assertEqual(chunk_sizes, dict(lat=15, lon=30))

    def test_get_ext_chunk_sizes(self):
        ds = xr.Dataset({
            'v1': (['time', 'lat', 'lon'], np.zeros([12, 45, 90])),
            'v2': (['time', 'lat', 'lon'], np.zeros([12, 45, 90])),
            'v3': (['time', 'lat', 'lon'], np.zeros([12, 45, 90])),
            'v4': (['lat', 'lon'], np.zeros([45, 90])),
            'v5': (['time'], np.zeros(12)),
            'lon': (['lon'], np.linspace(-178, 178, 90)),
            'lat': (['lat'], np.linspace(-88, 88, 45)),
            'time': (['time'], np.linspace(0, 1, 12))})
        ds.v1.encoding['chunksizes'] = (12, 5, 10)
        ds.v2.encoding['chunksizes'] = (12, 15, 30)
        ds.v3.encoding['chunksizes'] = (12, 5, 10)
        ds.v4.encoding['chunksizes'] = (5, 10)
        ds.v5.encoding['chunksizes'] = (1,)

        def map_fn(size, prev_value):
            """Collect all sizes."""
            return [size] + prev_value

        def reduce_fn(values):
            """Median."""
            values = sorted(values)
            return values[len(values) // 2]

        chunk_sizes = get_ext_chunk_sizes(ds, init_value=[], map_fn=map_fn, reduce_fn=reduce_fn)
        self.assertIsNotNone(chunk_sizes)
        self.assertEqual(chunk_sizes, dict(time=12, lat=5, lon=10))

    def test_open_xarray(self):
        wrong_path = os.path.join(_TEST_DATA_PATH, 'small', '*.nck')
        wrong_url = 'httpz://www.acme.com'
        path = [wrong_path, wrong_url]
        try:
            open_xarray_dataset(path)
        except IOError as e:
            self.assertEqual(str(e), 'File {} not found'.format(path))

        right_path = os.path.join(_TEST_DATA_PATH, 'small', '*.nc')
        wrong_url = 'httpz://www.acme.com'
        path = [right_path, wrong_url]
        dsa = open_xarray_dataset(path)
        self.assertIsNotNone(dsa)


class DataAccessErrorTest(unittest.TestCase):
    def test_plain(self):
        try:
            raise DataAccessError("haha")
        except DataAccessError as e:
            self.assertEqual(str(e), "haha")
            self.assertIsInstance(e, Exception)


class NetworkErrorTest(unittest.TestCase):
    def test_plain(self):
        try:
            raise NetworkError("hoho")
        except NetworkError as e:
            self.assertEqual(str(e), "hoho")
            self.assertIsInstance(e, ConnectionError)
