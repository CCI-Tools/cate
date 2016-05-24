from unittest import TestCase
import ect.core.io as io

class IOTest(TestCase):

    def test_query_data_sources(self):
        data_sources = io.query_data_sources()
        self.assertIsNotNone(data_sources)
        self.assertEqual(len(data_sources), 2)
        self.assertEqual(data_sources[0].name, "aerosol")
        self.assertEqual(data_sources[1].name, "ozone")

    def test_query_data_sources_with_name(self):
        data_sources = io.query_data_sources(name_filter="aero")
        self.assertIsNotNone(data_sources)
        self.assertEqual(len(data_sources), 1)
        self.assertEqual(data_sources[0].name, "aerosol")

        data_sources = io.query_data_sources(name_filter="o")
        self.assertIsNotNone(data_sources)
        self.assertEqual(len(data_sources), 2)
        self.assertEqual(data_sources[0].name, "aerosol")
        self.assertEqual(data_sources[1].name, "ozone")

    def test_query_data_sources_with_own_catalogue(self):
        my_catalogue = io.Catalogue(io.DataSource("test"))
        data_sources = io.query_data_sources(catalogues=my_catalogue)
        self.assertIsNotNone(data_sources)
        self.assertEqual(len(data_sources), 1)
        self.assertEqual(data_sources[0].name, "test")

    # def test_open_dataset(self):
    #     ds1 = io.open_dataset(io.DataSource("sst"))
    #     self.assertIsNotNone(ds1)
    #     ds2 = io.open_dataset("sst")
    #     self.assertIsNotNone(ds2)
