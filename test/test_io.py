from datetime import datetime
from typing import Sequence
from unittest import TestCase

import ect.core.io as io
from ect.core.cdm_xarray import XArrayDatasetAdapter


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
    def name(self) -> str:
        return self._name

    def open_dataset(self, time_range=None) -> io.Dataset:
        return None

    def __repr__(self):
        return "SimpleDataSource(%s)" % repr(self._name)

    def _repr_html_(self):
        return self._name


class InMemoryDataSource(SimpleDataSource):
    def __init__(self, data):
        super(InMemoryDataSource, self).__init__("in_memory")
        self._data = data

    def open_dataset(self, time_range=None) -> io.Dataset:
        return XArrayDatasetAdapter(self._data)

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
        self.assertEqual('No data_source found', str(cm.exception))

        inmem_data_source = InMemoryDataSource('42')
        dataset1 = io.open_dataset(inmem_data_source)
        self.assertIsNotNone(dataset1)
        self.assertIsInstance(dataset1, XArrayDatasetAdapter)
        self.assertEqual('42', dataset1.wrapped_dataset)

        dataset2 = inmem_data_source.open_dataset()
        self.assertIsInstance(dataset2, XArrayDatasetAdapter)
        self.assertEqual('42', dataset2.wrapped_dataset)

    def test_open_dataset_duplicated_names(self):
        try:
            ds_a1 = SimpleDataSource('aerosol')
            ds_a2 = SimpleDataSource('aerosol')
            duplicated_cat = SimpleDataStore([ds_a1, ds_a2])
            io.DATA_STORE_REGISTRY.add_data_store('duplicated_cat', duplicated_cat)
            with self.assertRaises(ValueError) as cm:
                io.open_dataset('aerosol')
            self.assertEqual('2 data_sources found for the given query term', str(cm.exception))
        finally:
            io.DATA_STORE_REGISTRY._data_stores.clear()


class FileSetDataSourceTest(TestCase):
    JSON = '''{
     "remore_url": null,
     "data_sources": [
     {
        "name":"aerosol/ATSR2_SU/L3/v4.2/DAILY",
        "base_dir":"aerosol/data/ATSR2_SU/L3/v4.2/DAILY",
        "start_date":"1995-06-01",
        "end_date":"2003-06-30",
        "num_files":2631,
        "size_mb":42338,
        "file_pattern":"{YYYY}/{MM}/{YYYY}{MM}{DD}-ESACCI-L3C_AEROSOL-AOD-ATSR2_ERS2-SU_DAILY-fv4.1.nc"
      },
      {
        "name":"aerosol/ATSR2_SU/L3/v4.21/MONTHLY",
        "base_dir":"aerosol/data/ATSR2_SU/L3/v4.21/MONTHLY",
        "file_pattern":"{YYYY}/{YYYY}{MM}-ESACCI-L3C_AEROSOL-AER_PRODUCTS-ATSR2_ERS2-SU_MONTHLY-v4.21.nc"
      }
     ]}'''

    def setUp(self):
        data_store = io.FileSetDataStore.from_json('TEST_ROOT_DIR', FileSetDataSourceTest.JSON)
        self.assertIsNotNone(data_store)
        self.assertEqual(2, len(data_store._data_sources))
        self.ds0 = data_store._data_sources[0]
        self.ds1 = data_store._data_sources[1]

    def test_from_json(self):
        self.assertEqual('AEROSOL_ATSR2_SU_L3_V4.2_DAILY', self.ds0.name)
        json_dict = self.ds0.to_json_dict()

        self.assertEqual('aerosol/data/ATSR2_SU/L3/v4.2/DAILY', json_dict['base_dir'])
        self.assertEqual('{YYYY}/{MM}/{YYYY}{MM}{DD}-ESACCI-L3C_AEROSOL-AOD-ATSR2_ERS2-SU_DAILY-fv4.1.nc',
                         json_dict['file_pattern'])
        fileset_info = json_dict['fileset_info']

        self.assertEqual(datetime(1995, 6, 1), fileset_info['start_time'])
        self.assertEqual(datetime(2003, 6, 30), fileset_info['end_time'])
        self.assertEqual(2631, fileset_info['num_files'])
        self.assertEqual(42338, fileset_info['size_in_mb'])

        self.assertEqual('AEROSOL_ATSR2_SU_L3_V4.21_MONTHLY', self.ds1.name)
        json_dict = self.ds1.to_json_dict()

        self.assertEqual('aerosol/data/ATSR2_SU/L3/v4.21/MONTHLY', json_dict['base_dir'])
        self.assertEqual('{YYYY}/{YYYY}{MM}-ESACCI-L3C_AEROSOL-AER_PRODUCTS-ATSR2_ERS2-SU_MONTHLY-v4.21.nc',
                         json_dict['file_pattern'])
        self.assertNotIn('fileset_info', json_dict)

    def test_resolve_paths(self):
        paths1 = self.ds0.resolve_paths(time_range=('2001-01-01', '2001-01-03'))
        self.assertIsNotNone(paths1)
        self.assertEqual(3, len(paths1))
        self.assertEqual(
            'TEST_ROOT_DIR/aerosol/data/ATSR2_SU/L3/v4.2/DAILY/2001/01/20010101-ESACCI-L3C_AEROSOL-AOD-ATSR2_ERS2-SU_DAILY-fv4.1.nc',
            paths1[0])

        paths2 = self.ds0.resolve_paths(time_range=(datetime(2001, 1, 1), datetime(2001, 1, 3)))
        self.assertEqual(paths1, paths2)

    def test_resolve_paths_open_interval(self):
        paths = self.ds0.resolve_paths(time_range=('2003-06-20', None))
        self.assertIsNotNone(paths)
        self.assertEqual(11, len(paths))
        self.assertEqual(
            'TEST_ROOT_DIR/aerosol/data/ATSR2_SU/L3/v4.2/DAILY/2003/06/20030620-ESACCI-L3C_AEROSOL-AOD-ATSR2_ERS2-SU_DAILY-fv4.1.nc',
            paths[0])
        self.assertEqual(
            'TEST_ROOT_DIR/aerosol/data/ATSR2_SU/L3/v4.2/DAILY/2003/06/20030630-ESACCI-L3C_AEROSOL-AOD-ATSR2_ERS2-SU_DAILY-fv4.1.nc',
            paths[-1])

        paths = self.ds0.resolve_paths(time_range=(None, '1995-06-01'))
        self.assertIsNotNone(paths)
        self.assertEqual(1, len(paths))
        self.assertEqual(
            'TEST_ROOT_DIR/aerosol/data/ATSR2_SU/L3/v4.2/DAILY/1995/06/19950601-ESACCI-L3C_AEROSOL-AOD-ATSR2_ERS2-SU_DAILY-fv4.1.nc',
            paths[0])

        self.ds0._fileset_info._start_time = datetime(2001, 1, 1)
        self.ds0._fileset_info._end_time = datetime(2001, 1, 3)
        paths = self.ds0.resolve_paths()
        self.assertIsNotNone(paths)
        self.assertEqual(3, len(paths))
        self.assertEqual(
            'TEST_ROOT_DIR/aerosol/data/ATSR2_SU/L3/v4.2/DAILY/2001/01/20010101-ESACCI-L3C_AEROSOL-AOD-ATSR2_ERS2-SU_DAILY-fv4.1.nc',
            paths[0])

    def test_resolve_paths_validaton(self):
        with self.assertRaises(ValueError):
            self.ds0.resolve_paths(time_range=('2001-01-03', '2001-01-01'))

        self.ds0._fileset_info._start_time = None
        with self.assertRaises(ValueError):
            self.ds0.resolve_paths(time_range=(None, '2001-01-01'))

        self.ds0._fileset_info._end_time = None
        with self.assertRaises(ValueError):
            self.ds0.resolve_paths(time_range=('2001-01-03', None))

    def test_as_datetime(self):
        d1 = io._as_datetime('2001-01-01', None)
        self.assertIsInstance(d1, datetime)
        self.assertEqual(datetime(2001, 1, 1), d1)

        d1 = io._as_datetime('2001-01-01 2:3:5', None)
        self.assertIsInstance(d1, datetime)
        self.assertEqual(datetime(2001, 1, 1, 2, 3, 5), d1)

        d1 = io._as_datetime(datetime(2001, 1, 1), None)
        self.assertIsInstance(d1, datetime)
        self.assertEqual(datetime(2001, 1, 1), d1)

        d1 = io._as_datetime(None, datetime(2001, 1, 1))
        self.assertIsInstance(d1, datetime)
        self.assertEqual(datetime(2001, 1, 1), d1)

        with self.assertRaises(TypeError):
            io._as_datetime(1, None)
        with self.assertRaises(ValueError):
            io._as_datetime("42", None)


class DataStoreRegistryTest(TestCase):
    def setUp(self):
        self.c1 = io.FileSetDataStore('root')
        self.c2 = io.FileSetDataStore('root')
        self.c3 = io.FileSetDataStore('root')

    def test_init(self):
        data_store_registry = io.DataStoreRegistry()
        self.assertEqual(0, len(data_store_registry))

    def test_add(self):
        data_store_registry = io.DataStoreRegistry()
        data_store_registry.add_data_store('c1', self.c1)
        self.assertEqual(1, len(data_store_registry))
        data_store_registry.add_data_store('c2', self.c2)
        self.assertEqual(2, len(data_store_registry))
        data_store_registry.add_data_store('c2', self.c3)
        self.assertEqual(2, len(data_store_registry))

    def test_remove(self):
        data_store_registry = io.DataStoreRegistry()
        data_store_registry.add_data_store('c1', self.c1)
        data_store_registry.add_data_store('c2', self.c2)
        self.assertEqual(2, len(data_store_registry))
        data_store_registry.remove_data_store('c1')
        self.assertEqual(1, len(data_store_registry))
        with self.assertRaises(KeyError):
            data_store_registry.remove_data_store('c0')
        self.assertEqual(1, len(data_store_registry))

    def test_get_data_store(self):
        data_store_registry = io.DataStoreRegistry()
        data_store_registry.add_data_store('c1', self.c1)
        data_store_registry.add_data_store('c2', self.c2)
        self.assertEqual(2, len(data_store_registry))

        rc2 = data_store_registry.get_data_store('c2')
        self.assertIsNotNone(rc2)
        self.assertIs(self.c2, rc2)
        self.assertEqual(2, len(data_store_registry))

        rc0 = data_store_registry.get_data_store('c0')
        self.assertIsNone(rc0)

    def test_get_data_stores(self):
        data_store_registry = io.DataStoreRegistry()
        data_store_registry.add_data_store('c1', self.c1)
        data_store_registry.add_data_store('c2', self.c2)
        self.assertEqual(2, len(data_store_registry))

        data_stores = data_store_registry.get_data_stores()
        self.assertIsNotNone(data_stores)
        self.assertEqual(2, len(data_stores))


class FileSetDataStoreTest(TestCase):
    def test_it(self):
        root_dir = 'ROOT'
        data_store = io.FileSetDataStore.from_json(root_dir, FileSetDataSourceTest.JSON)
        self.assertIsNotNone(data_store)
        self.assertEqual('ROOT', data_store.root_dir)
        query_results = data_store.query()
        self.assertIsNotNone(query_results)
        self.assertEqual(2, len(query_results))

        # dataset = result[0].open_dataset()
        # self.assertIsNotNone(dataset)
