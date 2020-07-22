import json
import os
import shapely.wkt
import unittest

from datetime import datetime

from cate.core.ds import DATA_STORE_REGISTRY
from cate.ds.esa_cci_odp_xcube import EsaCciOdpDataStore
from cate.ds.esa_cci_odp_xcube import EsaCciOdpDataSource
# from xcube.core.store import DataDescriptor
from xcube.core.store import DatasetDescriptor
from xcube.core.store import TYPE_ID_DATASET

def _create_test_data_store():
    with open(os.path.join(os.path.dirname(__file__), 'resources/os-data-list.json')) as fp:
        json_text = fp.read()
    json_dict = json.loads(json_text)
    for d in DATA_STORE_REGISTRY.get_data_stores():
        d.get_updates(reset=True)
    metadata_path = os.path.join(os.path.dirname(__file__), 'resources/datasources/metadata')
    # The EsaCciOdpDataStore created with an initial json_dict and a metadata dir avoids fetching from remote
    data_store = EsaCciOdpDataStore('test-odp', index_cache_json_dict=json_dict, index_cache_update_tag='test1',
                                    meta_data_store_path=metadata_path)
    DATA_STORE_REGISTRY.add_data_store(data_store)
    return data_store


class EsaCciOdpDataSourceTest(unittest.TestCase):
    def setUp(self) -> None:
        self._store = EsaCciOdpDataStore()
        descriptor = DatasetDescriptor(data_id='my_first_data_id',
                                       type_id=TYPE_ID_DATASET,
                                       bbox=(-10., 20., 10., 40.),
                                       time_range=('2017-01-15', '2019-10-30'),
                                       attrs=dict(title='dataset_title'))
        self._source = EsaCciOdpDataSource(data_store=self._store,
                                           descriptor=descriptor,
                                           json_dict={},
                                           raw_datasource_id='',
                                           datasource_id=descriptor.data_id)


    def test_id(self):
        self.assertEqual('my_first_data_id', self._source.id)

    def test_data_store(self):
        self.assertEqual(self._store, self._source.data_store)

    def test_spatial_coverage(self):
        spatial_coverage = self._source.spatial_coverage
        expected_polygon = shapely.wkt.loads('POLYGON ((10 20, 10 40, -10 40, -10 20, 10 20))')
        self.assertTrue(spatial_coverage.almost_equals(expected_polygon))

    def test_temporal_coverage(self):
        temporal_coverage = self._source.temporal_coverage()
        self.assertIsNotNone(temporal_coverage)
        self.assertEqual(datetime(2017, 1, 15), temporal_coverage[0])
        self.assertEqual(datetime(2019, 10, 30, 23, 59, 59), temporal_coverage[1])

    # def test_variables_info(self):
    #     self._source.variables_info

    def test_title(self):
        self.assertEqual('dataset_title', self._source.title)

    # def test_open_dataset(self):





