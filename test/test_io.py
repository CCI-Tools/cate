from datetime import datetime
from typing import Sequence
from unittest import TestCase

import xarray as xr
import ect.core.io as io
from ect import Monitor


class SimpleDataStore(io.DataStore):
    def __init__(self, data_sources: Sequence[io.DataSource]):
        self._data_sources = list(data_sources)

    def query(self, name=None) -> Sequence[io.DataSource]:
        return [ds for ds in self._data_sources if ds.matches_filter(name)]

    def _repr_html_(self):
        return ''


class SimpleDataSource(io.DataSource):
    def __init__(self, name: str):
        self._name = name
        self._data_store = None

    @property
    def data_store(self) -> io.DataStore:
        return self.data_store

    @property
    def schema(self) -> io.Schema:
        return None

    @property
    def name(self) -> str:
        return self._name

    def open_dataset(self, time_range=None):
        return None

    def __repr__(self):
        return "SimpleDataSource(%s)" % repr(self._name)

    def _repr_html_(self):
        return self._name


class InMemoryDataSource(SimpleDataSource):
    def __init__(self, data):
        super(InMemoryDataSource, self).__init__("in_memory")
        self._data = data

    def open_dataset(self, time_range=None) -> xr.Dataset:
        return xr.Dataset({'a':self._data})

    def __repr__(self):
        return "InMemoryDataSource(%s)" % repr(self._data)

    def _repr_html_(self):
        import html
        return html.escape(repr(self._data))


class IOTest(TestCase):
    def setUp(self):
        self.DS_AEROSOL = SimpleDataSource('aerosol')
        self.DS_OZONE = SimpleDataSource('ozone')
        self.TEST_DATA_STORE = SimpleDataStore([self.DS_AEROSOL, self.DS_OZONE])
        self.DS_AEROSOL._data_store = self.TEST_DATA_STORE
        self.DS_OZONE._data_store = self.TEST_DATA_STORE
        self.DS_SST = SimpleDataSource('sst')
        self.TEST_DATA_STORE_SST = SimpleDataStore([self.DS_SST])

    def test_query_data_sources_default_data_store(self):
        self.assertEqual(0, len(io.DATA_STORE_REGISTRY))
        try:
            from ect.ds.esa_cci_ftp import set_default_data_store
            set_default_data_store()
            self.assertEqual(1, len(io.DATA_STORE_REGISTRY))

            data_sources = io.query_data_sources()
            self.assertIsNotNone(data_sources)
            self.assertEqual(len(data_sources), 98)
            self.assertEqual(data_sources[0].name, "AEROSOL_ATSR2_SU_L3_V4.2_DAILY")

            data_sources = io.query_data_sources(name="AEROSOL_ATSR2_SU_L3_V4.2_DAILY")
            self.assertIsNotNone(data_sources)
            self.assertEqual(len(data_sources), 1)

            data_sources = io.query_data_sources(name="ZZ")
            self.assertIsNotNone(data_sources)
            self.assertEqual(len(data_sources), 0)
        finally:
            io.DATA_STORE_REGISTRY._data_stores.clear()
        self.assertEqual(0, len(io.DATA_STORE_REGISTRY))

    def test_query_data_sources_with_data_store_value(self):
        data_sources = io.query_data_sources(data_stores=self.TEST_DATA_STORE)
        self.assertIsNotNone(data_sources)
        self.assertEqual(len(data_sources), 2)
        self.assertEqual(data_sources[0].name, "aerosol")
        self.assertEqual(data_sources[1].name, "ozone")

    def test_query_data_sources_with_data_store_list(self):
        data_stores = [self.TEST_DATA_STORE, self.TEST_DATA_STORE_SST]
        data_sources = io.query_data_sources(data_stores=data_stores)
        self.assertIsNotNone(data_sources)
        self.assertEqual(len(data_sources), 3)
        self.assertEqual(data_sources[0].name, "aerosol")
        self.assertEqual(data_sources[1].name, "ozone")
        self.assertEqual(data_sources[2].name, "sst")

    def test_query_data_sources_with_constrains(self):
        data_sources = io.query_data_sources(data_stores=self.TEST_DATA_STORE, name="aerosol")
        self.assertIsNotNone(data_sources)
        self.assertEqual(len(data_sources), 1)
        self.assertEqual(data_sources[0].name, "aerosol")

        data_sources = io.query_data_sources(data_stores=self.TEST_DATA_STORE, name="ozone")
        self.assertIsNotNone(data_sources)
        self.assertEqual(len(data_sources), 1)
        self.assertEqual(data_sources[0].name, "ozone")

        data_sources = io.query_data_sources(data_stores=self.TEST_DATA_STORE, name="Z")
        self.assertIsNotNone(data_sources)
        self.assertEqual(len(data_sources), 0)

    def test_open_dataset(self):
        with self.assertRaises(ValueError) as cm:
            io.open_dataset(None)
        self.assertEqual('No data_source given', str(cm.exception))

        with self.assertRaises(ValueError) as cm:
            io.open_dataset('foo')
        self.assertEqual("No data_source found for the given query term 'foo'", str(cm.exception))

        inmem_data_source = InMemoryDataSource(42)
        dataset1 = io.open_dataset(inmem_data_source)
        self.assertIsNotNone(dataset1)
        self.assertIsInstance(dataset1, xr.Dataset)
        self.assertEqual(42, dataset1.a.values)

        dataset2 = inmem_data_source.open_dataset()
        self.assertIsInstance(dataset2, xr.Dataset)
        self.assertEqual(42, dataset2.a.values)

    def test_open_dataset_duplicated_names(self):
        try:
            ds_a1 = SimpleDataSource('aerosol')
            ds_a2 = SimpleDataSource('aerosol')
            duplicated_cat = SimpleDataStore([ds_a1, ds_a2])
            io.DATA_STORE_REGISTRY.add_data_store('duplicated_cat', duplicated_cat)
            with self.assertRaises(ValueError) as cm:
                io.open_dataset('aerosol')
            self.assertEqual("2 data_sources found for the given query term 'aerosol'", str(cm.exception))
        finally:
            io.DATA_STORE_REGISTRY._data_stores.clear()

