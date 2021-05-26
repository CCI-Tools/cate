import os
import unittest

import numpy as np
import xarray as xr

import xcube.core.store as xcube_store
from cate.core.ds import DataAccessError
from cate.core.ds import NetworkError
from cate.core.ds import find_data_store
from cate.core.ds import get_data_descriptor
from cate.core.ds import get_ext_chunk_sizes
from cate.core.ds import get_metadata_from_descriptor
from cate.core.ds import get_spatial_ext_chunk_sizes
from cate.core.ds import open_dataset
from cate.core.types import ValidationError
from xcube.core.store import DataStoreError
from ..storetest import StoreTest

_TEST_DATA_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'test_data')


class IOTest(StoreTest):

    def test_find_data_store(self):
        aerosol_store_id, aerosol_store = find_data_store(
            '20000302-ESACCI-L3C_AEROSOL-AER_PRODUCTS-ATSR2-ERS2-ADV_DAILY-v2.30.nc')
        self.assertEqual('local_test_store_1', aerosol_store_id)
        self.assertIsNotNone(aerosol_store)

        ozone_store_id, ozone_store = \
            find_data_store('ESACCI-OZONE-L3S-TC-MERGED-DLR_1M-20050501-fv0100.nc')
        self.assertEqual('local_test_store_2', ozone_store_id)
        self.assertIsNotNone(ozone_store)

        permafrost_store_id, permafrost_store = find_data_store('permafrost')
        self.assertIsNone(permafrost_store_id)
        self.assertIsNone(permafrost_store)

        with self.assertRaises(ValidationError):
            find_data_store('ESACCI-OC-L3S-IOP-MERGED-1M_MONTHLY_4km_GEO_PML_OCx_QAA-200505'
                            '-fv4.2.nc')

    def test_get_data_descriptor(self):
        aerosol_descriptor = get_data_descriptor(
            '20000302-ESACCI-L3C_AEROSOL-AER_PRODUCTS-ATSR2-ERS2-ADV_DAILY-v2.30.nc')
        self.assertIsInstance(aerosol_descriptor, xcube_store.DatasetDescriptor)
        self.assertEqual('20000302-ESACCI-L3C_AEROSOL-AER_PRODUCTS-ATSR2-ERS2-ADV_DAILY-v2.30.nc',
                         aerosol_descriptor.data_id)

        sst_descriptor = get_data_descriptor(
            '19910916120000-ESACCI-L3C_GHRSST-SSTskin-AVHRR12_G-CDR2.1_night-v02.0-fv01.0.nc')
        self.assertIsInstance(sst_descriptor, xcube_store.DatasetDescriptor)
        self.assertEqual(
            '19910916120000-ESACCI-L3C_GHRSST-SSTskin-AVHRR12_G-CDR2.1_night-v02.0-fv01.0.nc',
            sst_descriptor.data_id)

        permafrost_descriptor = get_data_descriptor('permafrost')
        self.assertIsNone(permafrost_descriptor)

        with self.assertRaises(ValidationError):
            get_data_descriptor(
                'ESACCI-OC-L3S-IOP-MERGED-1M_MONTHLY_4km_GEO_PML_OCx_QAA-200505-fv4.2.nc')

    def test_get_metadata_from_descriptor(self):
        descriptor = xcube_store.DatasetDescriptor(
            data_id='xyz',
            crs='EPSG:9346',
            bbox=(10., 20., 30., 40.),
            spatial_res=20.,
            time_range=('2017-06-05', '2017-06-27'),
            time_period='daily',
            coords={
                'lon': xcube_store.VariableDescriptor(
                    name='lon',
                    dtype='float32',
                    dims=('lon',),
                    attrs=dict(units='degrees',
                               long_name='longitude',
                               standard_name='longitude')),
                'lat': xcube_store.VariableDescriptor(
                    name='lat',
                    dtype='float32',
                    dims=('lat',),
                    attrs=dict(units='degrees',
                               long_name='latitude',
                               standard_name='latitude')),
                'time': xcube_store.VariableDescriptor(
                    name='time',
                    dtype='datetime64[ms]',
                    dims=('time',),
                    attrs=dict(units='milliseconds since 1970-01-01T00:00:00',
                               long_name='time',
                               standard_name='time'))
            },
            data_vars={
                'surface_pressure': xcube_store.VariableDescriptor(
                    name='surface_pressure',
                    dtype='float32',
                    dims=('time', 'lat', 'lon'),
                    attrs=dict(units='hPa',
                               long_name='surface_pressure',
                               standard_name='surface_pressure'))
            },
            attrs=dict(
                title='ESA Ozone Climate Change Initiative (Ozone CCI): '
                      'Level 3 Nadir Ozone Profile Merged Data Product, version 2',
                institution='Royal Netherlands Meteorological Institute, KNMI',
                source='This dataset contains L2 profiles from GOME, SCIAMACHY, OMI and GOME-2 '
                       'gridded onto a global grid.',
                history='L2 data gridded to global grid.',
                references='http://www.esa-ozone-cci.org/',
                tracking_id='32CF0EE6-1F21-4FAE-B0BE-A8C6FD88A775',
                Conventions='CF-1.6',
                product_version='fv0002',
                summary='This dataset contains L2 profiles from GOME, SCIAMACHY, OMI and GOME-2 '
                        'gridded onto a global grid.',
                keywords='satellite, observation, atmosphere, ozone',
                id='32CF0EE6-1F21-4FAE-B0BE-A8C6FD88A775',
                naming_authority='KNMI, http://www.knmi.nl/',
                comment='These data were produced at KNMI as part of the ESA OZONE CCI project.',
                date_created='2014-01-08T12:50:21.908',
                creator_name='J.C.A. van Peet',
                creator_url='KNMI, http://www.knmi.nl/',
                creator_email='peet@knmi.nl',
                project='Climate Change Initiative - European Space Agency',
                geospatial_lat_min=-90.0,
                geospatial_lat_max=90.0,
                geospatial_lat_units='degree_north',
                geospatial_lat_resolution=1.0,
                geospatial_lon_min=-180.0,
                geospatial_lon_max=180.0,
                geospatial_lon_units='degree_east',
                geospatial_lon_resolution=1.0,
                geospatial_vertical_min=0.01,
                geospatial_vertical_max=1013.0,
                time_coverage_start='19970104T102333Z',
                time_coverage_end='19970131T233849Z',
                time_coverage_duration='P1M',
                time_coverage_resolution='P1M',
                standard_name_vocabulary='NetCDF Climate and Forecast(CF) Metadata Convention '
                                         'version 20, 11 September 2012',
                license='data use is free and open',
                platform='merged: ERS-2, ENVISAT, EOS-AURA, METOP-A',
                sensor='merged: GOME, SCIAMACHY, OMI and GOME-2.',
                spatial_resolution='see geospatial_lat_resolution and geospatial_lat_resolution',
                Note='netCDF compression applied.',
                ecv='OZONE',
                time_frequency='month',
                institute='Royal Netherlands Meteorological Institute',
                processing_level='L3',
                product_string='MERGED',
                data_type='NP',
                file_formats=['.nc', '.txt']
            )
        )
        descriptor_metadata = get_metadata_from_descriptor(descriptor)
        expected_metadata = dict(
            data_id='xyz',
            type_specifier='dataset',
            crs='EPSG:9346',
            bbox=(10., 20., 30., 40.),
            spatial_res=20.,
            time_range=('2017-06-05', '2017-06-27'),
            time_period='daily',
            title='ESA Ozone Climate Change Initiative (Ozone CCI): '
                  'Level 3 Nadir Ozone Profile Merged Data Product, version 2',
            product_version='fv0002',
            ecv='OZONE',
            time_frequency='month',
            institute='Royal Netherlands Meteorological Institute',
            processing_level='L3',
            product_string='MERGED',
            data_type='NP',
            file_formats=['.nc', '.txt'],
            data_vars=[
                {'name': 'surface_pressure',
                 'dtype': 'float32',
                 'dims': ('time', 'lat', 'lon'),
                 'long_name': 'surface_pressure',
                 'standard_name': 'surface_pressure',
                 'units': 'hPa'}
            ],
            coords=[
                {'name': 'lon',
                 'dtype': 'float32',
                 'dims': ('lon',),
                 'long_name': 'longitude',
                 'standard_name': 'longitude',
                 'units': 'degrees'},
                {'name': 'lat',
                 'dtype': 'float32',
                 'dims': ('lat',),
                 'long_name': 'latitude',
                 'standard_name': 'latitude',
                 'units': 'degrees'},
                {'name': 'time',
                 'dtype': 'datetime64[ms]',
                 'dims': ('time',),
                 'long_name': 'time',
                 'standard_name': 'time',
                 'units': 'milliseconds since 1970-01-01T00:00:00'}
            ],
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

        aerosol_dataset, aerosol_dataset_name = \
            open_dataset('20000302-ESACCI-L3C_AEROSOL-AER_PRODUCTS-ATSR2-ERS2-ADV_DAILY-v2.30.nc')
        self.assertIsNotNone(aerosol_dataset)
        self.assertEqual('20000302-ESACCI-L3C_AEROSOL-AER_PRODUCTS-ATSR2-ERS2-ADV_DAILY-v2.30.nc',
                         aerosol_dataset_name)
        self.assertIsInstance(aerosol_dataset, xr.Dataset)
        self.assertEqual({'ANG550-670_mean', 'AOD550_uncertainty_mean'},
                         set(aerosol_dataset.data_vars))

        with self.assertRaises(DataStoreError) as cm:
            open_dataset('20000302-ESACCI-L3C_AEROSOL-AER_PRODUCTS-ATSR2-ERS2-ADV_DAILY-v2.30.nc',
                         data_store_id='unknown_store')
        self.assertEqual(('Configured data store instance "unknown_store" not found.',),
                         cm.exception.args)

        aerosol_dataset, aerosol_dataset_name = \
            open_dataset('20000302-ESACCI-L3C_AEROSOL-AER_PRODUCTS-ATSR2-ERS2-ADV_DAILY-v2.30.nc',
                         data_store_id='local_test_store_1')
        self.assertIsNotNone(aerosol_dataset)
        self.assertEqual('20000302-ESACCI-L3C_AEROSOL-AER_PRODUCTS-ATSR2-ERS2-ADV_DAILY-v2.30.nc',
                         aerosol_dataset_name)
        self.assertIsInstance(aerosol_dataset, xr.Dataset)
        self.assertEqual({'ANG550-670_mean', 'AOD550_uncertainty_mean'},
                         set(aerosol_dataset.data_vars))


class ChunkUtilsTest(unittest.TestCase):
    def test_get_spatial_ext_chunk_sizes(self):
        ds = xr.Dataset({
            'v1': (['lat', 'lon'], np.zeros([45, 90])),
            'v2': (['lat', 'lon'], np.zeros([45, 90])),
            'v3': (['lon'], np.zeros(90)),
            'lon': (['lon'], np.linspace(-178, 178, 90)),
            'lat': (['lat'], np.linspace(-88, 88, 45))})
        np.linspace
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
