import os
import os.path
import tempfile
import unittest

from ect.ds.local import LocalFilePatternDataStore, LocalFilePatternDataSource

TMP_STORE_JSON = os.path.join(tempfile.gettempdir(), 'LocalFilePatternDataStoreTest.json')


class LocalFilePatternDataStoreTest(unittest.TestCase):
    def setUp(self):
        self.data_store = LocalFilePatternDataStore('test', TMP_STORE_JSON)
        self.assertFalse(os.path.isfile(TMP_STORE_JSON))
        self.data_store.add_pattern("ozone", "/DATA/ozone/*/*.nc")
        self.data_store.add_pattern("aerosol", "/DATA/aerosol/*/*/AERO*.nc")
        self.assertTrue(os.path.isfile(TMP_STORE_JSON))

    def tearDown(self):
        if os.path.isfile(TMP_STORE_JSON):
            os.remove(TMP_STORE_JSON)

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
        self.assertEqual("The data_store already contains a data_source with the name 'test.a_name'", str(cm.exception))

        data_sources = self.data_store.query()
        self.assertEqual(len(data_sources), 3)

    def test__repr_html(self):
        html = self.data_store._repr_html_()
        self.assertEqual(499, len(html))


class LocalFilePatternSourceTest(unittest.TestCase):
    def setUp(self):
        self.data_source = LocalFilePatternDataSource("ozone", "/DATA/ozone/*/*.nc", self)
        self.assertIsNotNone(self.data_source)

    def test_data_store(self):
        self.assertIs(self.data_source.data_store, self)

    def test_id(self):
        self.assertEqual(self.data_source.name, 'ozone')

    def test_schema(self):
        self.assertEqual(self.data_source.schema, None)

    def test_info_string(self):
        self.assertEqual('Name: ozone\nFile pattern: /DATA/ozone/*/*.nc', self.data_source.info_string)

    def test_temporal_coverage(self):
        self.assertEqual(self.data_source.temporal_coverage, None)
