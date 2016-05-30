from unittest import TestCase

from datetime import date, datetime

import pytest

import ect.core.io as io
from ect.core.cdm_xarray import XArrayDatasetAdapter


class IOTest(TestCase):
    def setUp(self):
        self.TEST_CATALOGUE = io.Catalogue(io.DataSource("aerosol"), io.DataSource("ozone"))

    @pytest.mark.skip(reason="to be fixed, once we have a default Catalogue")
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
        "start_date":"1995-01-01",
        "end_date":"2003-12-31",
        "num_files":89,
        "size_mb":429,
        "file_pattern":"{YYYY}/{YYYY}{MM}-ESACCI-L3C_AEROSOL-AER_PRODUCTS-ATSR2_ERS2-SU_MONTHLY-v4.21.nc"
      }
     ]'''

    def setUp(self):
        self.filesets = io.fileset_datasources_from_json(FileSetDataSourceTest.JSON)
        self.assertIsNotNone(self.filesets)
        self.assertEqual(2, len(self.filesets))

    def test_from_json(self):
        fs0 = self.filesets[0]
        self.assertEqual('aerosol/ATSR2_SU/L3/v4.2/DAILY', fs0.name)

        self.assertEqual('aerosol/data/ATSR2_SU/L3/v4.2/DAILY', fs0._base_dir)
        self.assertEqual(datetime(1995, 6, 1), fs0._fileset_info._start_time)
        self.assertEqual(datetime(2003, 6, 30), fs0._fileset_info._end_time)
        self.assertEqual(2631, fs0._fileset_info._num_files)
        self.assertEqual(42338, fs0._fileset_info._size_in_mb)
        self.assertEqual('{YYYY}/{MM}/{YYYY}{MM}{DD}-ESACCI-L3C_AEROSOL-AOD-ATSR2_ERS2-SU_DAILY-fv4.1.nc',
                         fs0._file_pattern)

        p = 'aerosol/data/ATSR2_SU/L3/v4.2/DAILY/{YYYY}/{MM}/{YYYY}{MM}{DD}-ESACCI-L3C_AEROSOL-AOD-ATSR2_ERS2-SU_DAILY-fv4.1.nc'
        self.assertEqual(p, fs0._full_pattern)

    def test_resolve_paths(self):
        fs0 = self.filesets[0]
        paths1 = fs0._resolve_paths('2001-01-01', '2001-01-03')
        self.assertIsNotNone(paths1)
        self.assertEqual(3, len(paths1))
        self.assertEqual(
            'aerosol/data/ATSR2_SU/L3/v4.2/DAILY/2001/01/20010101-ESACCI-L3C_AEROSOL-AOD-ATSR2_ERS2-SU_DAILY-fv4.1.nc',
            paths1[0])

        paths2 = fs0._resolve_paths(datetime(2001, 1, 1), datetime(2001, 1, 3))
        self.assertEqual(paths1, paths2)

    def test_resolve_paths_open_interval(self):
        paths = self.filesets[0]._resolve_paths('2003-06-20')
        self.assertIsNotNone(paths)
        self.assertEqual(11, len(paths))
        self.assertEqual(
            'aerosol/data/ATSR2_SU/L3/v4.2/DAILY/2003/06/20030620-ESACCI-L3C_AEROSOL-AOD-ATSR2_ERS2-SU_DAILY-fv4.1.nc',
            paths[0])
        self.assertEqual(
            'aerosol/data/ATSR2_SU/L3/v4.2/DAILY/2003/06/20030630-ESACCI-L3C_AEROSOL-AOD-ATSR2_ERS2-SU_DAILY-fv4.1.nc',
            paths[-1])

        paths = self.filesets[0]._resolve_paths(None, '1995-06-01')
        self.assertIsNotNone(paths)
        self.assertEqual(1, len(paths))
        self.assertEqual(
            'aerosol/data/ATSR2_SU/L3/v4.2/DAILY/1995/06/19950601-ESACCI-L3C_AEROSOL-AOD-ATSR2_ERS2-SU_DAILY-fv4.1.nc',
            paths[0])

        self.filesets[0]._fileset_info._start_time = datetime(2001, 1, 1)
        self.filesets[0]._fileset_info._end_time = datetime(2001, 1, 3)
        paths = self.filesets[0]._resolve_paths()
        self.assertIsNotNone(paths)
        self.assertEqual(3, len(paths))
        self.assertEqual(
            'aerosol/data/ATSR2_SU/L3/v4.2/DAILY/2001/01/20010101-ESACCI-L3C_AEROSOL-AOD-ATSR2_ERS2-SU_DAILY-fv4.1.nc',
            paths[0])

    def test_resolve_paths_validaton(self):
        with self.assertRaises(ValueError):
            self.filesets[0]._resolve_paths('2001-01-03', '2001-01-01')

        self.filesets[0]._fileset_info._start_time = None
        with self.assertRaises(ValueError):
            self.filesets[0]._resolve_paths(None, '2001-01-01')

        self.filesets[0]._fileset_info._end_time = None
        with self.assertRaises(ValueError):
            self.filesets[0]._resolve_paths('2001-01-03', None)

    def test_as_date(self):
        d1 = io._as_datetime('2001-01-01', None)
        self.assertIsInstance(d1, datetime)
        self.assertEqual(datetime(2001, 1, 1), d1)

        d1 = io._as_datetime(datetime(2001, 1, 1), None)
        self.assertIsInstance(d1, datetime)
        self.assertEqual(datetime(2001, 1, 1), d1)

        d1 = io._as_datetime(None, datetime(2001, 1, 1))
        self.assertIsInstance(d1, datetime)
        self.assertEqual(datetime(2001, 1, 1), d1)

        with self.assertRaises(ValueError):
            io._as_datetime(1, None)


class FileSetCatalogueTest(TestCase):
    def test_foo(self):
        datasources = io.fileset_datasources_from_json(FileSetDataSourceTest.JSON)
        root_dir = 'ROOT'
        catalogue = io.FileSetCatalogue(root_dir, datasources)
        self.assertIsNotNone(catalogue)
        result = catalogue.query()
        self.assertIsNotNone(result)
        self.assertEqual(2, len(result))
        # dataset = result[0].open_dataset()
        # self.assertIsNotNone(dataset)
