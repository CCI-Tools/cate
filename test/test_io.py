from datetime import datetime
from unittest import TestCase

import ect.core.io as io
from ect.core.cdm_xarray import XArrayDatasetAdapter


class IOTest(TestCase):
    def setUp(self):
        self.TEST_CATALOGUE = io.Catalogue([io.DataSource('aerosol'), io.DataSource('ozone')])

    def test_query_data_sources(self):
        data_sources = io.query_data_sources()
        self.assertIsNotNone(data_sources)
        self.assertEqual(len(data_sources), 98)
        self.assertEqual(data_sources[0].name, "aerosol/ATSR2_SU/L3/v4.2/DAILY")

        data_sources = io.query_data_sources(name="DAILY")
        self.assertIsNotNone(data_sources)
        self.assertEqual(len(data_sources), 13)

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
                super(InMemoryDataSource, self).__init__("im_mem")
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


class FileSetDataSourceTest(TestCase):
    JSON = '''[
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
     ]'''

    def setUp(self):
        self.filesets = io.FileSetDataSource.from_json(FileSetDataSourceTest.JSON)
        self.assertIsNotNone(self.filesets)
        self.assertEqual(2, len(self.filesets))
        self.fs0 = self.filesets[0]
        self.fs1 = self.filesets[1]

    def test_from_json(self):
        self.assertEqual('aerosol/ATSR2_SU/L3/v4.2/DAILY', self.fs0.name)
        json_dict = self.fs0.to_json_dict()

        self.assertEqual('aerosol/data/ATSR2_SU/L3/v4.2/DAILY', json_dict['base_dir'])
        self.assertEqual('{YYYY}/{MM}/{YYYY}{MM}{DD}-ESACCI-L3C_AEROSOL-AOD-ATSR2_ERS2-SU_DAILY-fv4.1.nc',
                         json_dict['file_pattern'])
        fileset_info = json_dict['fileset_info']

        self.assertEqual(datetime(1995, 6, 1), fileset_info['start_time'])
        self.assertEqual(datetime(2003, 6, 30), fileset_info['end_time'])
        self.assertEqual(2631, fileset_info['num_files'])
        self.assertEqual(42338, fileset_info['size_in_mb'])

        self.assertEqual('aerosol/ATSR2_SU/L3/v4.21/MONTHLY', self.fs1.name)
        json_dict = self.fs1.to_json_dict()

        self.assertEqual('aerosol/data/ATSR2_SU/L3/v4.21/MONTHLY', json_dict['base_dir'])
        self.assertEqual('{YYYY}/{YYYY}{MM}-ESACCI-L3C_AEROSOL-AER_PRODUCTS-ATSR2_ERS2-SU_MONTHLY-v4.21.nc',
                         json_dict['file_pattern'])
        self.assertNotIn('fileset_info', json_dict)

    def test_resolve_paths(self):
        paths1 = self.fs0.resolve_paths('2001-01-01', '2001-01-03')
        self.assertIsNotNone(paths1)
        self.assertEqual(3, len(paths1))
        self.assertEqual(
            'aerosol/data/ATSR2_SU/L3/v4.2/DAILY/2001/01/20010101-ESACCI-L3C_AEROSOL-AOD-ATSR2_ERS2-SU_DAILY-fv4.1.nc',
            paths1[0])

        paths2 = self.fs0.resolve_paths(datetime(2001, 1, 1), datetime(2001, 1, 3))
        self.assertEqual(paths1, paths2)

    def test_resolve_paths_with_rootdir(self):
        filesets = io.FileSetDataSource.from_json(FileSetDataSourceTest.JSON)
        cat =  io.FileSetCatalogue('TEST_ROOT_DIR', filesets)

        paths1 = filesets[0].resolve_paths('2001-01-01', '2001-01-03')
        self.assertIsNotNone(paths1)
        self.assertEqual(3, len(paths1))
        self.assertEqual(
            'TEST_ROOT_DIR/aerosol/data/ATSR2_SU/L3/v4.2/DAILY/2001/01/20010101-ESACCI-L3C_AEROSOL-AOD-ATSR2_ERS2-SU_DAILY-fv4.1.nc',
            paths1[0])

    def test_resolve_paths_open_interval(self):
        paths = self.fs0.resolve_paths('2003-06-20')
        self.assertIsNotNone(paths)
        self.assertEqual(11, len(paths))
        self.assertEqual(
            'aerosol/data/ATSR2_SU/L3/v4.2/DAILY/2003/06/20030620-ESACCI-L3C_AEROSOL-AOD-ATSR2_ERS2-SU_DAILY-fv4.1.nc',
            paths[0])
        self.assertEqual(
            'aerosol/data/ATSR2_SU/L3/v4.2/DAILY/2003/06/20030630-ESACCI-L3C_AEROSOL-AOD-ATSR2_ERS2-SU_DAILY-fv4.1.nc',
            paths[-1])

        paths = self.fs0.resolve_paths(None, '1995-06-01')
        self.assertIsNotNone(paths)
        self.assertEqual(1, len(paths))
        self.assertEqual(
            'aerosol/data/ATSR2_SU/L3/v4.2/DAILY/1995/06/19950601-ESACCI-L3C_AEROSOL-AOD-ATSR2_ERS2-SU_DAILY-fv4.1.nc',
            paths[0])

        self.fs0._fileset_info._start_time = datetime(2001, 1, 1)
        self.fs0._fileset_info._end_time = datetime(2001, 1, 3)
        paths = self.fs0.resolve_paths()
        self.assertIsNotNone(paths)
        self.assertEqual(3, len(paths))
        self.assertEqual(
            'aerosol/data/ATSR2_SU/L3/v4.2/DAILY/2001/01/20010101-ESACCI-L3C_AEROSOL-AOD-ATSR2_ERS2-SU_DAILY-fv4.1.nc',
            paths[0])

    def test_resolve_paths_validaton(self):
        with self.assertRaises(ValueError):
            self.fs0.resolve_paths('2001-01-03', '2001-01-01')

        self.fs0._fileset_info._start_time = None
        with self.assertRaises(ValueError):
            self.fs0.resolve_paths(None, '2001-01-01')

        self.fs0._fileset_info._end_time = None
        with self.assertRaises(ValueError):
            self.fs0.resolve_paths('2001-01-03', None)

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

        with self.assertRaises(ValueError):
            io._as_datetime(1, None)
        with self.assertRaises(ValueError):
            io._as_datetime("42", None)


class CatalogueRegistryTest(TestCase):
    def test_init(self):
        catalogue_registry = io.CatalogueRegistry()
        self.assertEqual(0, len(catalogue_registry))

    def test_add(self):
        catalogue_registry = io.CatalogueRegistry()
        c1 = io.Catalogue(None)
        c2 = io.Catalogue(None)
        c3 = io.Catalogue(None)
        catalogue_registry.add_catalogue('c1', c1)
        self.assertEqual(1, len(catalogue_registry))
        catalogue_registry.add_catalogue('c2', c2)
        self.assertEqual(2, len(catalogue_registry))
        catalogue_registry.add_catalogue('c2', c3)
        self.assertEqual(2, len(catalogue_registry))

    def test_remove(self):
        catalogue_registry = io.CatalogueRegistry()
        c1 = io.Catalogue(None)
        c2 = io.Catalogue(None)
        catalogue_registry.add_catalogue('c1', c1)
        catalogue_registry.add_catalogue('c2', c2)
        self.assertEqual(2, len(catalogue_registry))
        catalogue_registry.remove_catalogue('c1')
        self.assertEqual(1, len(catalogue_registry))
        with self.assertRaises(KeyError):
            catalogue_registry.remove_catalogue('c0')
        self.assertEqual(1, len(catalogue_registry))

    def test_get_catalogue(self):
        catalogue_registry = io.CatalogueRegistry()
        c1 = io.Catalogue(None)
        c2 = io.Catalogue(None)
        c3 = io.Catalogue(None)
        catalogue_registry.add_catalogue('c1', c1)
        catalogue_registry.add_catalogue('c2', c2)
        self.assertEqual(2, len(catalogue_registry))

        rc2 = catalogue_registry.get_catalogue('c2')
        self.assertIsNotNone(rc2)
        self.assertIs(c2, rc2)
        self.assertEqual(2, len(catalogue_registry))

        rc0 = catalogue_registry.get_catalogue('c0')
        self.assertIsNone(rc0)

    def test_get_catalogues(self):
        catalogue_registry = io.CatalogueRegistry()
        c1 = io.Catalogue(None)
        c2 = io.Catalogue(None)
        c3 = io.Catalogue(None)
        catalogue_registry.add_catalogue('c1', c1)
        catalogue_registry.add_catalogue('c2', c2)
        self.assertEqual(2, len(catalogue_registry))

        rcALL = catalogue_registry.get_catalogues()
        self.assertIsNotNone(rcALL)
        self.assertEqual(2, len(rcALL))


class FileSetCatalogueTest(TestCase):
    def test_it(self):
        datasources = io.FileSetDataSource.from_json(FileSetDataSourceTest.JSON)
        root_dir = 'ROOT'
        catalogue = io.FileSetCatalogue(root_dir, datasources)
        self.assertIsNotNone(catalogue)
        self.assertEqual('ROOT', catalogue.root_dir)
        result = catalogue.query()
        self.assertIsNotNone(result)
        self.assertEqual(2, len(result))

        # dataset = result[0].open_dataset()
        # self.assertIsNotNone(dataset)
