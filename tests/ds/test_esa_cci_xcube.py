import json
import os
import shapely.wkt
import unittest

from datetime import datetime

from cate.core.ds import DATA_STORE_REGISTRY
from cate.core.ds import DataStoreNotice
from cate.ds.esa_cci_odp_xcube import EsaCciOdpDataStore
from cate.ds.esa_cci_odp_xcube import EsaCciOdpDataSource
from xcube_cci.dataaccess import CciOdpDataStore

def _create_test_data_store():
    for d in DATA_STORE_REGISTRY.get_data_stores():
        d.get_updates(reset=True)
    metadata_path = os.path.join(os.path.dirname(__file__), 'resources/datasources/metadata')
    # The EsaCciOdpDataStore created with a test metadata dir avoids fetching from remote
    data_store = EsaCciOdpDataStore('test-odp', index_cache_update_tag='test1',
                                    meta_data_store_path=metadata_path)
    DATA_STORE_REGISTRY.add_data_store(data_store)
    return data_store


class EsaCciOdpDataStoreTest(unittest.TestCase):

    def setUp(self):
        self._store = _create_test_data_store()

    def test_get_data_ids(self):
        data_ids = self._store._get_data_ids()
        self.assertIsNotNone(data_ids)
        self.assertEqual(5, len(data_ids))
        self.assertIn('esacci.AEROSOL.mon.L3C.AER_PRODUCTS.AATSR.Envisat.SU.4-21.r1', data_ids)
        self.assertIn('esacci.CLOUD.mon.L3C.CLD_PRODUCTS.MODIS.Terra.MODIS_TERRA.2-0.r1', data_ids)
        self.assertIn('esacci.OC.mon.L3S.K_490.multi-sensor.multi-platform.MERGED.3-1.geographic', data_ids)
        self.assertIn('esacci.OZONE.mon.L3.NP.multi-sensor.multi-platform.MERGED.fv0002.r1', data_ids)
        self.assertIn('esacci.SST.day.L4.SSTdepth.multi-sensor.multi-platform.OSTIA.1-1.r1', data_ids)

    def test_query(self):
        data_sources = self._store.query(query_expr='mon')
        self.assertIsNotNone(data_sources)
        self.assertEqual(4, len(data_sources))
        self.assertEqual('esacci.AEROSOL.mon.L3C.AER_PRODUCTS.AATSR.Envisat.SU.4-21.r1', data_sources[0].id)
        self.assertEqual('esacci.CLOUD.mon.L3C.CLD_PRODUCTS.MODIS.Terra.MODIS_TERRA.2-0.r1', data_sources[1].id)
        self.assertEqual('esacci.OC.mon.L3S.K_490.multi-sensor.multi-platform.MERGED.3-1.geographic', data_sources[2].id)
        self.assertEqual('esacci.OZONE.mon.L3.NP.multi-sensor.multi-platform.MERGED.fv0002.r1', data_sources[3].id)

    def test_id_title_and_is_local(self):
        self.assertEqual(self._store.id, 'test-odp')
        self.assertEqual(self._store.title, 'ESA CCI Open Data Portal (xcube access)')
        self.assertEqual(self._store.is_local, False)

    def test_description(self):
        self.assertIsNotNone(self._store.description)
        self.assertTrue(len(self._store.description) > 40)

    def test_notices(self):
        self.assertIsInstance(self._store.notices, list)
        self.assertEqual(2, len(self._store.notices))

        notice0 = self._store.notices[0]
        self.assertIsInstance(notice0, DataStoreNotice)
        self.assertEqual(notice0.id, "terminologyClarification")
        self.assertEqual(notice0.title, "Terminology Clarification")
        self.assertEqual(notice0.icon, "info-sign")
        self.assertEqual(notice0.intent, "primary")
        self.assertTrue(len(notice0.content) > 20)

        notice1 = self._store.notices[1]
        self.assertIsInstance(notice0, DataStoreNotice)
        self.assertEqual(notice1.id, "dataCompleteness")
        self.assertEqual(notice1.title, "Data Completeness")
        self.assertEqual(notice1.icon, "warning-sign")
        self.assertEqual(notice1.intent, "warning")
        self.assertTrue(len(notice1.content) > 20)

    @unittest.skipIf(os.environ.get('CATE_DISABLE_WEB_TESTS', None) == '1', 'CATE_DISABLE_WEB_TESTS = 1')
    def test_query_web_access(self):
        store = EsaCciOdpDataStore()
        all_data_sources = store.query()
        self.assertIsNotNone(all_data_sources)


class EsaCciOdpDataSourceTest(unittest.TestCase):
    def setUp(self) -> None:
        self._store = _create_test_data_store()
        self._source = EsaCciOdpDataSource(data_store=self._store,
                                           cci_store=CciOdpDataStore(normalize_data=True),
                                           data_id='esacci.AEROSOL.mon.L3C.AER_PRODUCTS.AATSR.Envisat.SU.4-21.r1')

    def test_id(self):
        self.assertEqual('esacci.AEROSOL.mon.L3C.AER_PRODUCTS.AATSR.Envisat.SU.4-21.r1', self._source.id)

    def test_data_store(self):
        self.assertEqual(self._store, self._source.data_store)

    def test_spatial_coverage(self):
        spatial_coverage = self._source.spatial_coverage
        expected_polygon = shapely.wkt.loads('POLYGON ((180 -90, 180 90, -180 90, -180 -90, 180 -90))')
        self.assertTrue(spatial_coverage.almost_equals(expected_polygon))

    def test_temporal_coverage(self):
        temporal_coverage = self._source.temporal_coverage()
        self.assertIsNotNone(temporal_coverage)
        self.assertEqual(datetime(2002, 5, 19, 23), temporal_coverage[0])
        self.assertEqual(datetime(2012, 4, 30, 22, 59, 59), temporal_coverage[1])

    def test_variables_info(self):
        variables_info = self._source.variables_info
        names = [var_info['name'] for var_info in variables_info]
        self.assertEqual(["pixel_count", "AOD550_mean", "AOD550_sdev", "AOD670_mean", "AOD670_sdev", "AOD870_mean",
                          "AOD870_sdev", "AOD1600_mean", "AOD1600_sdev", "ANG550_870_mean", "ANG550_870_sdev",
                          "FM_AOD550_mean", "FM_AOD550_sdev", "D_AOD550_mean", "D_AOD550_sdev", "AAOD550_mean",
                          "AAOD550_sdev", "SSA550_mean", "SSA550_sdev", "surface_reflectance550_mean",
                          "surface_reflectance550_sdev", "surface_reflectance670_mean", "surface_reflectance670_sdev",
                          "surface_reflectance870_mean", "surface_reflectance870_sdev", "surface_reflectance1600_mean",
                          "surface_reflectance1600_sdev", "AOD550_uncertainty_mean", "AOD550_uncertainty_sdev",
                          "AOD550_uncertainty", "AOD550_uncertainty_min", "AOD550_uncertainty_max",
                          "AOD670_uncertainty_mean", "AOD670_uncertainty_sdev", "AOD670_uncertainty",
                          "AOD670_uncertainty_min", "AOD670_uncertainty_max", "AOD870_uncertainty_mean",
                          "AOD870_uncertainty_sdev", "AOD870_uncertainty", "AOD870_uncertainty_min",
                          "AOD870_uncertainty_max", "AOD1600_uncertainty_mean", "AOD1600_uncertainty_sdev",
                          "AOD1600_uncertainty", "AOD1600_uncertainty_min", "AOD1600_uncertainty_max",
                          "cloud_fraction_mean", "cloud_fraction_sdev", "surface_type_number_mean",
                          "surface_type_number_sdev"],
                         names)

    def test_title(self):
        self.assertEqual('ESA Aerosol Climate Change Initiative (Aerosol CCI): Level 3 aerosol products '
                         'from AATSR (SU algorithm), Version 4.21',
                         self._source.title)

    def test_meta_info(self):
        meta_info = self._source.meta_info
        self.assertIsNotNone(meta_info)
        self.assertEqual('180.0', meta_info.get('bbox_maxx', None))
        self.assertEqual('-180.0', meta_info.get('bbox_minx', None))
        self.assertEqual('-90.0', meta_info.get('bbox_miny', None))
        self.assertEqual('90.0', meta_info.get('bbox_maxy', None))
        self.assertEqual('AEROSOL', meta_info.get('cci_project', None))
        self.assertEqual('2016-05-07T12:14:30', meta_info.get('creation_date', None))
        self.assertEqual('AER_PRODUCTS', meta_info.get('data_type', None))
        self.assertEqual('AEROSOL', meta_info.get('ecv', None))
        self.assertEqual('.nc', meta_info.get('file_formats', ['', ''])[0])
        self.assertEqual('.txt', meta_info.get('file_formats', ['', ''])[1])
        self.assertEqual('Swansea University', meta_info.get('institute', None))
        self.assertEqual('Envisat', meta_info.get('platform_id', None))
        self.assertEqual('L3C', meta_info.get('processing_level', None))
        self.assertEqual('SU', meta_info.get('product_string', None))
        self.assertEqual('4-21', meta_info.get('product_version', None))
        self.assertEqual('2016-05-07T12:14:30', meta_info.get('publication_date', None))
        self.assertEqual('AATSR', meta_info.get('sensor_id', None))
        self.assertEqual('2012-04-30T22:59:59', meta_info.get('temporal_coverage_end', None))
        self.assertEqual('2002-05-19T23:00:00', meta_info.get('temporal_coverage_start', None))
        self.assertEqual('month', meta_info.get('time_frequency', None))

    def test_schema(self):
        self.assertEqual(self._source.schema, None)


@unittest.skipIf(os.environ.get('CATE_DISABLE_WEB_TESTS', None) == '1', 'CATE_DISABLE_WEB_TESTS = 1')
class SpatialSubsetTest(unittest.TestCase):

    def test_make_local_spatial_5_days_frequency(self):
        data_store = EsaCciOdpDataStore()
        # The following reproduces Cate issue #904:
        cci_dataset_collection = 'esacci.AEROSOL.5-days.L3C.AEX.GOMOS.Envisat.AERGOM.2-19.r1'
        data_source = data_store.query(cci_dataset_collection)[0]
        ds_from_remote_source = data_source.open_dataset(time_range=['2002-04-01', '2002-04-06'],
                                                         var_names=['S_AOD550', 'Tropopause_height'],
                                                         region='-113.9, 40.0,-113.8, 40.1')
        self.assertIsNotNone(ds_from_remote_source)
        ds = data_source.make_local('local_name_xcube',
                                    time_range=['2002-04-01', '2002-04-06'],
                                    var_names=['S_AOD550', 'Tropopause_height'],
                                    region='-113.9, 40.0,-113.8, 40.1')
        self.assertIsNotNone(ds)
