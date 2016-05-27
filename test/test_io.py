from unittest import TestCase

from datetime import date

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


class FileSetTypeTest(TestCase):
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
        self.filesets = io.fileset_types_from_json(FileSetTypeTest.JSON)
        self.assertIsNotNone(self.filesets)
        self.assertEqual(2, len(self.filesets))

    def test_from_json(self):
        fs0 = self.filesets[0]
        self.assertEqual('aerosol/ATSR2_SU/L3/v4.2/DAILY', fs0.name)
        self.assertEqual('aerosol/data/ATSR2_SU/L3/v4.2/DAILY', fs0.base_dir)
        self.assertEqual(date(1995, 6, 1), fs0.start_date)
        self.assertEqual(date(2003, 6, 30), fs0.end_date)
        self.assertEqual(2631, fs0.num_files)
        self.assertEqual(42338, fs0.size_in_mb)
        self.assertEqual('{YYYY}/{MM}/{YYYY}{MM}{DD}-ESACCI-L3C_AEROSOL-AOD-ATSR2_ERS2-SU_DAILY-fv4.1.nc', fs0.file_pattern)
        p = 'aerosol/data/ATSR2_SU/L3/v4.2/DAILY/{YYYY}/{MM}/{YYYY}{MM}{DD}-ESACCI-L3C_AEROSOL-AOD-ATSR2_ERS2-SU_DAILY-fv4.1.nc'
        self.assertEqual(p, fs0.full_pattern)

    def test_resolve_paths(self):
        fs0 = self.filesets[0]
        paths1 = fs0.resolve_paths('2001-01-01', '2001-01-03')
        self.assertIsNotNone(paths1)
        self.assertEqual(3, len(paths1))
        self.assertEqual('aerosol/data/ATSR2_SU/L3/v4.2/DAILY/2001/01/20010101-ESACCI-L3C_AEROSOL-AOD-ATSR2_ERS2-SU_DAILY-fv4.1.nc', paths1[0])

        paths2 = fs0.resolve_paths(date(2001, 1, 1), date(2001, 1, 3))
        self.assertEqual(paths1, paths2)

    def test_resolve_paths_open_interval(self):
        paths = self.filesets[0].resolve_paths('2003-06-20')
        self.assertIsNotNone(paths)
        self.assertEqual(11, len(paths))
        self.assertEqual('aerosol/data/ATSR2_SU/L3/v4.2/DAILY/2003/06/20030620-ESACCI-L3C_AEROSOL-AOD-ATSR2_ERS2-SU_DAILY-fv4.1.nc', paths[0])
        self.assertEqual('aerosol/data/ATSR2_SU/L3/v4.2/DAILY/2003/06/20030630-ESACCI-L3C_AEROSOL-AOD-ATSR2_ERS2-SU_DAILY-fv4.1.nc', paths[-1])

        paths = self.filesets[0].resolve_paths(None, '1995-06-01')
        self.assertIsNotNone(paths)
        self.assertEqual(1, len(paths))
        self.assertEqual('aerosol/data/ATSR2_SU/L3/v4.2/DAILY/1995/06/19950601-ESACCI-L3C_AEROSOL-AOD-ATSR2_ERS2-SU_DAILY-fv4.1.nc', paths[0])

        self.filesets[0]._start_date = date(2001, 1, 1)
        self.filesets[0]._end_date = date(2001, 1, 3)
        paths = self.filesets[0].resolve_paths()
        self.assertIsNotNone(paths)
        self.assertEqual(3, len(paths))
        self.assertEqual('aerosol/data/ATSR2_SU/L3/v4.2/DAILY/2001/01/20010101-ESACCI-L3C_AEROSOL-AOD-ATSR2_ERS2-SU_DAILY-fv4.1.nc', paths[0])

    def test_resolve_paths_validaton(self):
        with self.assertRaises(ValueError) as cm:
            self.filesets[0].resolve_paths('2001-01-03', '2001-01-01')

        self.filesets[0]._start_date = None
        with self.assertRaises(ValueError) as cm:
            self.filesets[0].resolve_paths(None, '2001-01-01')

        self.filesets[0]._end_date = None
        with self.assertRaises(ValueError) as cm:
            self.filesets[0].resolve_paths('2001-01-03', None)

    def test_as_date(self):
        d1 = io.FileSetType._as_date('2001-01-01', None)
        self.assertIsInstance(d1, date)
        self.assertEqual(date(2001, 1, 1), d1)

        d1 = io.FileSetType._as_date(date(2001, 1, 1), None)
        self.assertIsInstance(d1, date)
        self.assertEqual(date(2001, 1, 1), d1)

        d1 = io.FileSetType._as_date(None, date(2001, 1, 1))
        self.assertIsInstance(d1, date)
        self.assertEqual(date(2001, 1, 1), d1)

        with self.assertRaises(ValueError):
            io.FileSetType._as_date(1, None)

