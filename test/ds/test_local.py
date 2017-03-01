import os
import os.path
import tempfile
import unittest
from collections import OrderedDict

from cate.ds.local import LocalFilePatternDataStore, LocalFilePatternDataSource


class LocalFilePatternDataStoreTest(unittest.TestCase):
    def setUp(self):
        self.tmp_dir = tempfile.mkdtemp()
        self.data_store = LocalFilePatternDataStore('test', self.tmp_dir)
        self.assertTrue(os.path.isdir(self.tmp_dir))
        self.assertEqual(0, len(os.listdir(self.tmp_dir)))
        self.data_store.add_pattern("ozone", "/DATA/ozone/*/*.nc")
        self.data_store.add_pattern("aerosol", ["/DATA/aerosol/*/*/AERO_V1*.nc", "/DATA/aerosol/*/*/AERO_V2*.nc"])
        self.assertEqual(2, len(os.listdir(self.tmp_dir)))

    def test_add_pattern(self):
        data_sources = self.data_store.query()
        self.assertIsNotNone(data_sources)
        self.assertEqual(len(data_sources), 2)

        inserted_name = self.data_store.add_pattern("a_name", "a_pat")
        self.assertEqual('test.a_name', inserted_name)

        data_sources = self.data_store.query()
        self.assertEqual(len(data_sources), 3)

        with self.assertRaises(ValueError) as cm:
            self.data_store.add_pattern("a_name", "a_pat2")
        self.assertEqual("Local data store 'test' already contains a data source named 'test.a_name'",
                         str(cm.exception))

        data_sources = self.data_store.query()
        self.assertEqual(len(data_sources), 3)

    def test__repr_html(self):
        html = self.data_store._repr_html_()
        self.assertEqual(524, len(html))

    def test_init(self):
        data_store2 = LocalFilePatternDataStore('test', self.tmp_dir)
        data_sources = data_store2.query()
        self.assertIsNotNone(data_sources)
        self.assertEqual(len(data_sources), 2)


class LocalFilePatternSourceTest(unittest.TestCase):
    def setUp(self):
        self._dummy_store = LocalFilePatternDataStore('dummy', 'dummy')
        self.ds1 = LocalFilePatternDataSource("ozone",
                                              ["/DATA/ozone/*/*.nc"],
                                              self._dummy_store)
        self.ds2 = LocalFilePatternDataSource("aerosol",
                                              ["/DATA/aerosol/*/A*.nc", "/DATA/aerosol/*/B*.nc"],
                                              self._dummy_store)

        self.empty_ds = LocalFilePatternDataSource("empty",
                                              [],
                                              self._dummy_store)

        self.assertIsNotNone(self.ds1)
        self.assertIsNotNone(self.ds2)
        self.assertIsNotNone(self.empty_ds)

    def test_data_store(self):
        self.assertIs(self.ds1.data_store, self._dummy_store)
        self.assertIs(self.ds2.data_store, self._dummy_store)
        self.assertIs(self.empty_ds.data_store, self._dummy_store)

    def test_id(self):
        self.assertEqual(self.ds1.name, 'ozone')
        self.assertEqual(self.ds2.name, 'aerosol')
        self.assertEqual(self.empty_ds.name, 'empty')

    def test_schema(self):
        self.assertEqual(self.ds1.schema, None)
        self.assertEqual(self.ds2.schema, None)
        self.assertEqual(self.empty_ds.schema, None)

    def test_info_string(self):
        self.assertEqual('Files: /DATA/ozone/*/*.nc', self.ds1.info_string)
        self.assertEqual('Files: /DATA/aerosol/*/A*.nc /DATA/aerosol/*/B*.nc', self.ds2.info_string)
        self.assertEqual('Files: ', self.empty_ds.info_string)

    def test_temporal_coverage(self):
        self.assertEqual(self.ds1.temporal_coverage(), None)
        self.assertEqual(self.ds2.temporal_coverage(), None)
        self.assertEqual(self.empty_ds.temporal_coverage(), None)

    def test_to_json_dict(self):
        self.assertEqual(self.ds1.to_json_dict().get('name'), 'ozone')
        self.assertEqual(self.ds1.to_json_dict().get('files'),
                         [('/DATA/ozone/*/*.nc', None)])

        self.assertEqual(self.ds2.to_json_dict().get('name'), 'aerosol')
        self.assertEqual(self.ds2.to_json_dict().get('files'),
                         [("/DATA/aerosol/*/A*.nc", None), ("/DATA/aerosol/*/B*.nc", None)])

        self.assertEqual(self.empty_ds.to_json_dict().get('name'), 'empty')
        self.assertEqual(self.empty_ds.to_json_dict().get('files'), [])
