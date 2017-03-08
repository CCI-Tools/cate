from typing import Sequence
from unittest import TestCase, skipIf
import os.path as op
import os

import xarray as xr

import cate.core.ds as ds
from cate.util import Monitor

_TEST_DATA_PATH = op.join(op.dirname(op.realpath(__file__)), 'test_data')


class SimpleDataStore(ds.DataStore):
    def __init__(self, name: str, data_sources: Sequence[ds.DataSource]):
        super().__init__(name)
        self._data_sources = list(data_sources)

    def query(self, name=None, monitor: Monitor = Monitor.NONE) -> Sequence[ds.DataSource]:
        return [ds for ds in self._data_sources if ds.matches_filter(name)]

    def _repr_html_(self):
        return ''


class SimpleDataSource(ds.DataSource):
    def __init__(self, name: str):
        self._name = name
        self._data_store = None

    @property
    def data_store(self) -> ds.DataStore:
        return self.data_store

    @property
    def schema(self) -> ds.Schema:
        return None

    @property
    def name(self) -> str:
        return self._name

    def open_dataset(self, time_range=None, protocol: str = None):
        return None

    def __repr__(self):
        return "SimpleDataSource(%s)" % repr(self._name)

    def _repr_html_(self):
        return self._name


class InMemoryDataSource(SimpleDataSource):
    def __init__(self, data):
        super(InMemoryDataSource, self).__init__("in_memory")
        self._data = data

    def open_dataset(self, time_range=None, protocol: str = None) -> xr.Dataset:
        return xr.Dataset({'a': self._data})

    def __repr__(self):
        return "InMemoryDataSource(%s)" % repr(self._data)

    def _repr_html_(self):
        import html
        return html.escape(repr(self._data))


class IOTest(TestCase):
    def setUp(self):
        self.DS_AEROSOL = SimpleDataSource('aerosol')
        self.DS_OZONE = SimpleDataSource('ozone')
        self.TEST_DATA_STORE = SimpleDataStore('test_aero_ozone', [self.DS_AEROSOL, self.DS_OZONE])
        self.DS_AEROSOL._data_store = self.TEST_DATA_STORE
        self.DS_OZONE._data_store = self.TEST_DATA_STORE
        self.DS_SST = SimpleDataSource('sst')
        self.TEST_DATA_STORE_SST = SimpleDataStore('test_sst', [self.DS_SST])

    def test_query_data_sources_default_data_store(self):
        size_before = len(ds.DATA_STORE_REGISTRY)
        orig_stores = list(ds.DATA_STORE_REGISTRY.get_data_stores())
        try:
            ds.DATA_STORE_REGISTRY._data_stores.clear()
            self.assertEqual(0, len(ds.DATA_STORE_REGISTRY))

            from cate.ds.esa_cci_ftp import set_default_data_store as set_default_data_store_ftp
            set_default_data_store_ftp()
            self.assertEqual(1, len(ds.DATA_STORE_REGISTRY))

            data_sources = ds.query_data_sources()
            self.assertIsNotNone(data_sources)
            self.assertEqual(len(data_sources), 98)
            self.assertEqual(data_sources[0].name, "AEROSOL_ATSR2_SU_L3_V4.2_DAILY")

            data_sources = ds.query_data_sources(name="AEROSOL_ATSR2_SU_L3_V4.2_DAILY")
            self.assertIsNotNone(data_sources)
            self.assertEqual(len(data_sources), 1)

            data_sources = ds.query_data_sources(name="ZZ")
            self.assertIsNotNone(data_sources)
            self.assertEqual(len(data_sources), 0)
        finally:
            ds.DATA_STORE_REGISTRY._data_stores.clear()
            for data_store in orig_stores:
                ds.DATA_STORE_REGISTRY.add_data_store(data_store)
        self.assertEqual(size_before, len(ds.DATA_STORE_REGISTRY))

    def test_query_data_sources_with_data_store_value(self):
        data_sources = ds.query_data_sources(data_stores=self.TEST_DATA_STORE)
        self.assertIsNotNone(data_sources)
        self.assertEqual(len(data_sources), 2)
        self.assertEqual(data_sources[0].name, "aerosol")
        self.assertEqual(data_sources[1].name, "ozone")

    def test_query_data_sources_with_data_store_list(self):
        data_stores = [self.TEST_DATA_STORE, self.TEST_DATA_STORE_SST]
        data_sources = ds.query_data_sources(data_stores=data_stores)
        self.assertIsNotNone(data_sources)
        self.assertEqual(len(data_sources), 3)
        self.assertEqual(data_sources[0].name, "aerosol")
        self.assertEqual(data_sources[1].name, "ozone")
        self.assertEqual(data_sources[2].name, "sst")

    def test_query_data_sources_with_constrains(self):
        data_sources = ds.query_data_sources(data_stores=self.TEST_DATA_STORE, name="aerosol")
        self.assertIsNotNone(data_sources)
        self.assertEqual(len(data_sources), 1)
        self.assertEqual(data_sources[0].name, "aerosol")

        data_sources = ds.query_data_sources(data_stores=self.TEST_DATA_STORE, name="ozone")
        self.assertIsNotNone(data_sources)
        self.assertEqual(len(data_sources), 1)
        self.assertEqual(data_sources[0].name, "ozone")

        data_sources = ds.query_data_sources(data_stores=self.TEST_DATA_STORE, name="Z")
        self.assertIsNotNone(data_sources)
        self.assertEqual(len(data_sources), 0)

    @skipIf(os.environ.get('CATE_DISABLE_WEB_TESTS', None) == '1', 'CATE_DISABLE_WEB_TESTS = 1')
    def test_open_dataset(self):
        with self.assertRaises(ValueError) as cm:
            ds.open_dataset(None)
        self.assertEqual('No data_source given', str(cm.exception))

        with self.assertRaises(ValueError) as cm:
            ds.open_dataset('foo')
        self.assertEqual("No data_source found for the given query term 'foo'", str(cm.exception))

        inmem_data_source = InMemoryDataSource(42)
        dataset1 = ds.open_dataset(inmem_data_source)
        self.assertIsNotNone(dataset1)
        self.assertIsInstance(dataset1, xr.Dataset)
        self.assertEqual(42, dataset1.a.values)

        dataset2 = inmem_data_source.open_dataset()
        self.assertIsInstance(dataset2, xr.Dataset)
        self.assertEqual(42, dataset2.a.values)

    @skipIf(os.environ.get('CATE_DISABLE_WEB_TESTS', None) == '1', 'CATE_DISABLE_WEB_TESTS = 1')
    def test_open_dataset_duplicated_names(self):
        try:
            ds_a1 = SimpleDataSource('duplicate')
            ds_a2 = SimpleDataSource('duplicate')
            duplicated_cat = SimpleDataStore('duplicated_cat', [ds_a1, ds_a2])
            ds.DATA_STORE_REGISTRY.add_data_store(duplicated_cat)
            with self.assertRaises(ValueError) as cm:
                ds.open_dataset('duplicate')
            self.assertEqual("2 data_sources found for the given query term 'duplicate'", str(cm.exception))
        finally:
            ds.DATA_STORE_REGISTRY.remove_data_store('duplicated_cat')

    def test_autochunking(self):
        path_large = op.join(_TEST_DATA_PATH, 'large', '*.nc')
        path_small = op.join(_TEST_DATA_PATH, 'small', '*.nc')
        print(path_large)
        print(path_small)
        ds_large = ds.open_xarray_dataset(path_large)
        ds_small = ds.open_xarray_dataset(path_small)
        large_expected = {'lat': (1800, 1800), 'time': (1,), 'bnds': (2,),
                          'lon': (3600, 3600)}
        small_expected = {'lat': (720,), 'time': (1,), 'lon': (1440,)}
        self.assertEqual(ds_small.chunks, small_expected)
        self.assertEqual(ds_large.chunks, large_expected)
