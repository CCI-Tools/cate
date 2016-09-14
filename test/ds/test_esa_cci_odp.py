import json
import os.path
import unittest

from ect.ds.esa_cci_odp import EsaCciOdpDataStore


@unittest.skip(reason="skipped unless you want to debug index cache management")
class EsaCciOdpDataStoreIndexCacheTest(unittest.TestCase):
    def test_index_cache(self):
        self.data_store = EsaCciOdpDataStore(index_cache_used=True, index_cache_expiration_days=1.0e-6)


def _create_test_data_store():
    with open(os.path.join(os.path.dirname(__file__), 'esgf-index-cache.json')) as fp:
        json_text = fp.read()
    json_dict = json.loads(json_text)
    return EsaCciOdpDataStore(index_cache_json_dict=json_dict)


class EsaCciOdpDataStoreTest(unittest.TestCase):
    def setUp(self):
        self.data_store = _create_test_data_store()

    def test_query(self):
        data_sources = self.data_store.query()
        self.assertIsNotNone(data_sources)
        self.assertEqual(len(data_sources), 61)

    def test_query_with_string(self):
        data_sources = self.data_store.query('OC')
        self.assertIsNotNone(data_sources)
        self.assertEqual(len(data_sources), 20)


class EsaCciOdpDataSourceTest(unittest.TestCase):
    def setUp(self):
        self.data_store = _create_test_data_store()
        data_sources = self.data_store.query('OC')
        self.assertIsNotNone(data_sources)
        self.assertIsNotNone(data_sources[0])
        self.data_source = data_sources[0]

    def test_data_store(self):
        self.assertIs(self.data_source.data_store,
                      self.data_store)

    def test_id(self):
        self.assertEqual(self.data_source.name,
                         'esacci.OC.day.L3S.K_490.multi-sensor.multi-platform.MERGED.1-0.r2')

    def test_schema(self):
        self.assertEqual(self.data_source.schema,
                         None)

    def test_info_string(self):
        self.maxDiff = None
        #print(self.data_source.info_string)
        self.assertEqual(self.data_source.info_string,
                         'Data source "esacci.OC.day.L3S.K_490.multi-sensor.multi-platform.MERGED.1-0.r2"\n'
                         '===============================================================================\n'
                         '\n'
                         'cci_project:            OC\n'
                         'data_type:              K_490\n'
                         'number_of_aggregations: 0\n'
                         'number_of_files:        1000\n'
                         'platform_id:            multi-platform\n'
                         'processing_level:       L3S\n'
                         'product_string:         MERGED\n'
                         'product_version:        1-0\n'
                         'project:                esacci\n'
                         'realization:            r2\n'
                         'sensor_id:              multi-sensor\n'
                         'size:                   35338081094\n'
                         'time_frequency:         day\n'
                         'version:                20160704')
