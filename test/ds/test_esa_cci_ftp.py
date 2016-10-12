import os
import os.path
import os.path
from datetime import datetime
from unittest import TestCase

import ect.core.ds as io
from ect.core.ds import DATA_STORE_REGISTRY
from ect.ds.esa_cci_ftp import FileSetDataStore, set_default_data_store


class EsaCciFtpTest(TestCase):
    def test_set_default_data_store(self):
        set_default_data_store()
        data_store = DATA_STORE_REGISTRY.get_data_store('esa_cci_ftp')
        self.assertIsInstance(data_store, FileSetDataStore)
        self.assertEqual(data_store.root_dir,
                         os.path.expanduser(os.path.join('~', '.ect', 'data_stores', 'esa_cci_ftp')))


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
        data_store = FileSetDataStore.from_json('test', 'TEST_ROOT_DIR', FileSetDataSourceTest.JSON)
        self.assertIsNotNone(data_store)
        self.assertEqual(2, len(data_store._data_sources))
        self.assertEqual('test', data_store.name)
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


class DataStoreRegistryTest(TestCase):
    def setUp(self):
        self.c1 = FileSetDataStore('c1', 'root')
        self.c2 = FileSetDataStore('c2', 'root')
        self.c3 = FileSetDataStore('c3', 'root')

    def test_init(self):
        data_store_registry = io.DataStoreRegistry()
        self.assertEqual(0, len(data_store_registry))

    def test_add(self):
        data_store_registry = io.DataStoreRegistry()
        data_store_registry.add_data_store(self.c1)
        self.assertEqual(1, len(data_store_registry))
        data_store_registry.add_data_store(self.c2)
        self.assertEqual(2, len(data_store_registry))
        data_store_registry.add_data_store(self.c2)
        self.assertEqual(2, len(data_store_registry))

    def test_remove(self):
        data_store_registry = io.DataStoreRegistry()
        data_store_registry.add_data_store(self.c1)
        data_store_registry.add_data_store(self.c2)
        self.assertEqual(2, len(data_store_registry))
        data_store_registry.remove_data_store('c1')
        self.assertEqual(1, len(data_store_registry))
        with self.assertRaises(KeyError):
            data_store_registry.remove_data_store('c0')
        self.assertEqual(1, len(data_store_registry))

    def test_get_data_store(self):
        data_store_registry = io.DataStoreRegistry()
        data_store_registry.add_data_store(self.c1)
        data_store_registry.add_data_store(self.c2)
        self.assertEqual(2, len(data_store_registry))

        rc2 = data_store_registry.get_data_store('c2')
        self.assertIsNotNone(rc2)
        self.assertIs(self.c2, rc2)
        self.assertEqual(2, len(data_store_registry))

        rc0 = data_store_registry.get_data_store('c0')
        self.assertIsNone(rc0)

    def test_get_data_stores(self):
        data_store_registry = io.DataStoreRegistry()
        data_store_registry.add_data_store(self.c1)
        data_store_registry.add_data_store(self.c2)
        self.assertEqual(2, len(data_store_registry))

        data_stores = data_store_registry.get_data_stores()
        self.assertIsNotNone(data_stores)
        self.assertEqual(2, len(data_stores))


class FileSetDataStoreTest(TestCase):
    def test_it(self):
        root_dir = 'ROOT'
        data_store = FileSetDataStore.from_json('test', root_dir, FileSetDataSourceTest.JSON)
        self.assertIsNotNone(data_store)
        self.assertEqual('ROOT', data_store.root_dir)
        query_results = data_store.query()
        self.assertIsNotNone(query_results)
        self.assertEqual(2, len(query_results))

        # dataset = result[0].open_dataset()
        # self.assertIsNotNone(dataset)
