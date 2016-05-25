from unittest import TestCase

import ect.core.io as io
from ect.core.cdm_xarray import XArrayDatasetAdapter


class IOTest(TestCase):
    def setUp(self):
        self.TEST_CATALOGUE = io.Catalogue(io.DataSource("aerosol", "*AEROSOL*"), io.DataSource("ozone", "*OZONE*"))

    def test_query_data_sources(self):
        # without a catalogue
        # for the moment we have a default catalogue with only a default entry
        # this will change
        data_sources = io.query_data_sources()
        self.assertIsNotNone(data_sources)
        self.assertEqual(len(data_sources), 1)
        self.assertEqual(data_sources[0].name, "default")

        data_sources = io.query_data_sources(name="au")
        self.assertIsNotNone(data_sources)
        self.assertEqual(len(data_sources), 1)
        self.assertEqual(data_sources[0].name, "default")

        data_sources = io.query_data_sources(name="ZZ")
        self.assertIsNotNone(data_sources)
        self.assertEqual(len(data_sources), 0)

    def test_query_data_sources_with_catalogue(self):
        data_sources = io.query_data_sources(catalogues=self.TEST_CATALOGUE)
        self.assertIsNotNone(data_sources)
        self.assertEqual(len(data_sources), 2)
        self.assertEqual(data_sources[0].name, "aerosol")
        self.assertEqual(data_sources[1].name, "ozone")

    def test_query_data_sources_with_constrains(self):
        data_sources = io.query_data_sources(catalogues=self.TEST_CATALOGUE, name="aero")
        self.assertIsNotNone(data_sources)
        self.assertEqual(len(data_sources), 1)
        self.assertEqual(data_sources[0].name, "aerosol")

        data_sources = io.query_data_sources(catalogues=self.TEST_CATALOGUE, name="o")
        self.assertIsNotNone(data_sources)
        self.assertEqual(len(data_sources), 2)
        self.assertEqual(data_sources[0].name, "aerosol")
        self.assertEqual(data_sources[1].name, "ozone")

        data_sources = io.query_data_sources(catalogues=self.TEST_CATALOGUE, name="Z")
        self.assertIsNotNone(data_sources)
        self.assertEqual(len(data_sources), 0)

    def test_open_dataset(self):
        with self.assertRaises(ValueError):
            dataset0 = io.open_dataset(None)

        class InMemoryDataSource(io.DataSource):
            def __init__(self, data):
                super(InMemoryDataSource, self).__init__("im_mem", "mem")
                self._data = data

            def open_dataset(self, **constraints) -> io.Dataset:
                return XArrayDatasetAdapter(self._data)

        data_source = InMemoryDataSource('42')
        dataset1 = io.open_dataset(data_source)
        self.assertIsNotNone(dataset1)
        self.assertEqual('42', dataset1.wrapped_dataset)

        dataset2 = data_source.open_dataset()
        self.assertIsNotNone(dataset2)
        self.assertEqual('42', dataset2.wrapped_dataset)
