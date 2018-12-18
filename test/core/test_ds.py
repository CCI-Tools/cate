import os
import unittest
from typing import Sequence, Any, Optional
from unittest import TestCase, skipIf

import numpy as np
import xarray as xr

from cate.core.ds import DataStore, DataSource, Schema, DataAccessError, open_xarray_dataset, find_data_sources, \
    open_dataset, DATA_STORE_REGISTRY, get_spatial_ext_chunk_sizes, get_ext_chunk_sizes, NetworkError
from cate.core.types import PolygonLike, TimeRangeLike, VarNamesLike
from cate.util.monitor import Monitor
from test.util.test_monitor import RecordingMonitor

_TEST_DATA_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'test_data')


class SimpleDataStore(DataStore):
    def __init__(self, ds_id: str, data_sources: Sequence[DataSource]):
        super().__init__(ds_id, title='Simple Test Store', is_local=True)
        self._data_sources = list(data_sources)

    def query(self, ds_id: str = None, query_expr: str = None, monitor: Monitor = Monitor.NONE) \
            -> Sequence[DataSource]:
        if ds_id or query_expr:
            return [ds for ds in self._data_sources if ds.matches(ds_id=ds_id, query_expr=query_expr)]
        return self._data_sources

    def _repr_html_(self):
        return ''


class SimpleDataSource(DataSource):
    def __init__(self, ds_id: str, meta_info: dict = None):
        self._id = ds_id
        self._data_store = None
        self._meta_info = meta_info

    @property
    def data_store(self) -> DataStore:
        return self.data_store

    @property
    def schema(self) -> Optional[Schema]:
        return None

    @property
    def id(self) -> str:
        return self._id

    @property
    def meta_info(self) -> Optional[dict]:
        return self._meta_info

    def open_dataset(self,
                     time_range: TimeRangeLike.TYPE = None,
                     region: PolygonLike.TYPE = None,
                     var_names: VarNamesLike.TYPE = None,
                     protocol: str = None,
                     monitor: Monitor = Monitor.NONE) -> Any:
        return None

    def make_local(self,
                   local_name: str,
                   local_id: str = None,
                   time_range: TimeRangeLike.TYPE = None,
                   region: PolygonLike.TYPE = None,
                   var_names: VarNamesLike.TYPE = None,
                   monitor: Monitor = Monitor.NONE) -> Optional[DataSource]:
        return None

    def __repr__(self):
        return "SimpleDataSource(%s)" % repr(self._id)

    def _repr_html_(self):
        return self._id


class InMemoryDataSource(SimpleDataSource):
    def __init__(self, data):
        super(InMemoryDataSource, self).__init__("in_memory")
        self._data = data

    def open_dataset(self,
                     time_range: TimeRangeLike.TYPE = None,
                     region: PolygonLike.TYPE = None,
                     var_names: VarNamesLike.TYPE = None,
                     protocol: str = None,
                     monitor: Monitor = Monitor.NONE) -> Any:
        return xr.Dataset({'a': self._data})

    def __repr__(self):
        return "InMemoryDataSource(%s)" % repr(self._data)

    def _repr_html_(self):
        import html
        return html.escape(repr(self._data))


class IOTest(TestCase):
    def setUp(self):
        self.DS_AEROSOL = SimpleDataSource('aerosol')
        self.DS_OZONE = SimpleDataSource('ozone', meta_info=dict(title='This is pure Ozone'))
        self.TEST_DATA_STORE = SimpleDataStore('test_aero_ozone', [self.DS_AEROSOL, self.DS_OZONE])
        self.DS_AEROSOL._data_store = self.TEST_DATA_STORE
        self.DS_OZONE._data_store = self.TEST_DATA_STORE
        self.DS_SST = SimpleDataSource('sst', meta_info=dict())
        self.TEST_DATA_STORE_SST = SimpleDataStore('test_sst', [self.DS_SST])

    def test_title(self):
        self.assertEqual(self.DS_AEROSOL.title, None)
        self.assertEqual(self.DS_OZONE.title, 'This is pure Ozone')
        self.assertEqual(self.DS_SST.title, None)

    def test_meta_info(self):
        self.assertEqual(self.DS_AEROSOL.meta_info, None)
        self.assertEqual(self.DS_OZONE.meta_info, dict(title='This is pure Ozone'))
        self.assertEqual(self.DS_SST.meta_info, dict())

    def test_find_data_sources_default_data_store(self):
        size_before = len(DATA_STORE_REGISTRY)
        orig_stores = list(DATA_STORE_REGISTRY.get_data_stores())
        try:
            DATA_STORE_REGISTRY._data_stores.clear()
            self.assertEqual(0, len(DATA_STORE_REGISTRY))

            from cate.ds.esa_cci_ftp import set_default_data_store as set_default_data_store_ftp
            set_default_data_store_ftp()
            self.assertEqual(1, len(DATA_STORE_REGISTRY))

            data_sources = find_data_sources()
            self.assertIsNotNone(data_sources)
            self.assertEqual(len(data_sources), 98)
            self.assertEqual(data_sources[0].id, "AEROSOL_ATSR2_SU_L3_V4.2_DAILY")

            data_sources = find_data_sources(ds_id="AEROSOL_ATSR2_SU_L3_V4.2_DAILY")
            self.assertIsNotNone(data_sources)
            self.assertEqual(len(data_sources), 1)

            data_sources = find_data_sources(ds_id="ZZ")
            self.assertIsNotNone(data_sources)
            self.assertEqual(len(data_sources), 0)
        finally:
            DATA_STORE_REGISTRY._data_stores.clear()
            for data_store in orig_stores:
                DATA_STORE_REGISTRY.add_data_store(data_store)
        self.assertEqual(size_before, len(DATA_STORE_REGISTRY))

    def test_find_data_sources_with_data_store_value(self):
        data_sources = find_data_sources(data_stores=self.TEST_DATA_STORE)
        self.assertIsNotNone(data_sources)
        self.assertEqual(len(data_sources), 2)
        self.assertEqual(data_sources[0].id, "aerosol")
        self.assertEqual(data_sources[1].id, "ozone")

    def test_find_data_sources_with_data_store_list(self):
        data_stores = [self.TEST_DATA_STORE, self.TEST_DATA_STORE_SST]
        data_sources = find_data_sources(data_stores=data_stores)
        self.assertIsNotNone(data_sources)
        self.assertEqual(len(data_sources), 3)
        self.assertEqual(data_sources[0].id, "aerosol")
        self.assertEqual(data_sources[1].id, "ozone")
        self.assertEqual(data_sources[2].id, "sst")

    def test_find_data_sources_with_id_constrains(self):
        data_sources = find_data_sources(data_stores=self.TEST_DATA_STORE, ds_id="aerosol")
        self.assertIsNotNone(data_sources)
        self.assertEqual(len(data_sources), 1)
        self.assertEqual(data_sources[0].id, "aerosol")

        data_sources = find_data_sources(data_stores=self.TEST_DATA_STORE, ds_id="ozone")
        self.assertIsNotNone(data_sources)
        self.assertEqual(len(data_sources), 1)
        self.assertEqual(data_sources[0].id, "ozone")

        data_sources = find_data_sources(data_stores=self.TEST_DATA_STORE, ds_id="Z")
        self.assertIsNotNone(data_sources)
        self.assertEqual(len(data_sources), 0)

        data_sources = find_data_sources(data_stores=self.TEST_DATA_STORE, ds_id="x")
        self.assertIsNotNone(data_sources)
        self.assertEqual(len(data_sources), 0)

    def test_find_data_sources_with_query_expr_constrains(self):
        data_sources = find_data_sources(data_stores=self.TEST_DATA_STORE, query_expr="aerosol")
        self.assertIsNotNone(data_sources)
        self.assertEqual(len(data_sources), 1)
        self.assertEqual(data_sources[0].id, "aerosol")

        data_sources = find_data_sources(data_stores=self.TEST_DATA_STORE, query_expr="Z")
        self.assertIsNotNone(data_sources)
        self.assertEqual(len(data_sources), 1)
        self.assertEqual(data_sources[0].id, "ozone")

        data_sources = find_data_sources(data_stores=self.TEST_DATA_STORE, query_expr="x")
        self.assertIsNotNone(data_sources)
        self.assertEqual(len(data_sources), 0)

    def test_find_data_sources_with_id_and_query_expr_constrains(self):
        data_sources = find_data_sources(data_stores=self.TEST_DATA_STORE, ds_id="foo", query_expr="aerosol")
        self.assertIsNotNone(data_sources)
        self.assertEqual(len(data_sources), 1)
        self.assertEqual(data_sources[0].id, "aerosol")

        data_sources = find_data_sources(data_stores=self.TEST_DATA_STORE, ds_id="aerosol", query_expr="foo")
        self.assertIsNotNone(data_sources)
        self.assertEqual(len(data_sources), 1)
        self.assertEqual(data_sources[0].id, "aerosol")

        data_sources = find_data_sources(data_stores=self.TEST_DATA_STORE, ds_id="foo", query_expr="bar")
        self.assertIsNotNone(data_sources)
        self.assertEqual(len(data_sources), 0)

    @skipIf(os.environ.get('CATE_DISABLE_WEB_TESTS', None) == '1', 'CATE_DISABLE_WEB_TESTS = 1')
    def test_open_dataset(self):
        with self.assertRaises(ValueError) as cm:
            # noinspection PyTypeChecker
            open_dataset(None)
        self.assertTupleEqual(('No data source given',), cm.exception.args)

        with self.assertRaises(ValueError) as cm:
            open_dataset('foo')
        self.assertEqual(("No data sources found for the given ID 'foo'",), cm.exception.args)

        inmem_data_source = InMemoryDataSource(42)
        dataset1 = open_dataset(inmem_data_source)
        self.assertIsNotNone(dataset1)
        self.assertIsInstance(dataset1, xr.Dataset)
        self.assertEqual(42, dataset1.a.values)

        dataset2 = inmem_data_source.open_dataset()
        self.assertIsInstance(dataset2, xr.Dataset)
        self.assertEqual(42, dataset2.a.values)

    # @skipIf(os.environ.get('CATE_DISABLE_WEB_TESTS', None) == '1', 'CATE_DISABLE_WEB_TESTS = 1')
    @unittest.skip(reason="Test has to be fixed, user shouldn't be able to add two ds with the same name")
    def test_open_dataset_duplicated_names(self):
        try:
            ds_a1 = SimpleDataSource('duplicate')
            ds_a2 = SimpleDataSource('duplicate')
            duplicated_cat = SimpleDataStore('duplicated_cat', [ds_a1, ds_a2])
            DATA_STORE_REGISTRY.add_data_store(duplicated_cat)
            with self.assertRaises(ValueError) as cm:
                open_dataset('duplicate')
            self.assertEqual("2 data_sources found for the given query term 'duplicate'", str(cm.exception))
        finally:
            DATA_STORE_REGISTRY.remove_data_store('duplicated_cat')

    def test_open_xarray_dataset(self):
        path_large = os.path.join(_TEST_DATA_PATH, 'large', '*.nc')
        path_small = os.path.join(_TEST_DATA_PATH, 'small', '*.nc')

        ds_large_mon = RecordingMonitor()
        ds_small_mon = RecordingMonitor()
        ds_large = open_xarray_dataset(path_large, monitor=ds_large_mon)
        ds_small = open_xarray_dataset(path_small, monitor=ds_small_mon)

        # Test monitors
        self.assertEqual(ds_large_mon.records, [('start', 'Opening dataset', 1), ('progress', 1, None, 100), ('done',)])
        self.assertEqual(ds_small_mon.records, [('start', 'Opening dataset', 1), ('progress', 1, None, 100), ('done',)])

        # Test chunking
        self.assertEqual(ds_small.chunks, {'lon': (1440,), 'lat': (720,), 'time': (1,)})
        self.assertEqual(ds_large.chunks, {'lon': (7200,), 'lat': (3600,), 'time': (1,), 'bnds': (2,)})

    def test_empty_error(self):
        data_source = SimpleDataSource("foo")
        error = data_source._empty_error()
        self.assertIsInstance(error, DataAccessError)
        self.assertEqual('Data source "foo" does not seem to have any datasets',
                         f"{error}")
        error = data_source._empty_error(TimeRangeLike.convert("2010-01-01,2010-01-06"))
        self.assertIsInstance(error, DataAccessError)
        self.assertEqual('Data source "foo" does not seem to have any datasets'
                         ' in given time range 2010-01-01, 2010-01-06T23:59:59',
                         f"{error}")

    def test_cannot_access_error(self):
        data_source = SimpleDataSource("foo")
        error = data_source._cannot_access_error()
        self.assertIsInstance(error, DataAccessError)
        self.assertEqual('Failed to open data source "foo"',
                         f"{error}")
        error = data_source._cannot_access_error("a", "")
        self.assertIsInstance(error, DataAccessError)
        self.assertEqual('Failed to open data source "foo" for given time range',
                         f"{error}")
        error = data_source._cannot_access_error("a", "b", "c", error_cls=NetworkError)
        self.assertIsInstance(error, NetworkError)
        self.assertEqual('Failed to open data source "foo" for given time range, region, variable names',
                         f"{error}")
        error = data_source._cannot_access_error("a", "b", "c", cause=RuntimeError("timeout"), error_cls=NetworkError)
        self.assertIsInstance(error, NetworkError)
        self.assertEqual('Failed to open data source "foo" for given time range, region, variable names: timeout',
                         f"{error}")


class ChunkUtilsTest(unittest.TestCase):
    def test_get_spatial_ext_chunk_sizes(self):
        ds = xr.Dataset({
            'v1': (['lat', 'lon'], np.zeros([45, 90])),
            'v2': (['lat', 'lon'], np.zeros([45, 90])),
            'v3': (['lon'], np.zeros(90)),
            'lon': (['lon'], np.linspace(-178, 178, 90)),
            'lat': (['lat'], np.linspace(-88, 88, 45))})
        ds.v1.encoding['chunksizes'] = (5, 10)
        ds.v2.encoding['chunksizes'] = (15, 30)
        chunk_sizes = get_spatial_ext_chunk_sizes(ds)
        self.assertIsNotNone(chunk_sizes)
        self.assertEqual(chunk_sizes, dict(lat=15, lon=30))

    def test_get_spatial_ext_chunk_sizes_wo_lat_lon(self):
        ds = xr.Dataset({
            'v1': (['lon'], np.zeros([90])),
            'v2': (['lat'], np.zeros([45]))})
        ds.v1.encoding['chunksizes'] = (90,)
        ds.v2.encoding['chunksizes'] = (45,)
        chunk_sizes = get_spatial_ext_chunk_sizes(ds)
        self.assertIsNone(chunk_sizes)

    def test_get_spatial_ext_chunk_sizes_with_time(self):
        ds = xr.Dataset({
            'v1': (['time', 'lat', 'lon'], np.zeros([12, 45, 90])),
            'v2': (['time', 'lat', 'lon'], np.zeros([12, 45, 90])),
            'v3': (['lon'], np.zeros(90)),
            'lon': (['lon'], np.linspace(-178, 178, 90)),
            'lat': (['lat'], np.linspace(-88, 88, 45)),
            'time': (['time'], np.linspace(0, 1, 12))})
        ds.v1.encoding['chunksizes'] = (1, 5, 10)
        ds.v2.encoding['chunksizes'] = (1, 15, 30)
        ds.v3.encoding['chunksizes'] = (90,)
        chunk_sizes = get_spatial_ext_chunk_sizes(ds)
        self.assertIsNotNone(chunk_sizes)
        self.assertEqual(chunk_sizes, dict(lat=15, lon=30))

    def test_get_ext_chunk_sizes(self):
        ds = xr.Dataset({
            'v1': (['time', 'lat', 'lon'], np.zeros([12, 45, 90])),
            'v2': (['time', 'lat', 'lon'], np.zeros([12, 45, 90])),
            'v3': (['time', 'lat', 'lon'], np.zeros([12, 45, 90])),
            'v4': (['lat', 'lon'], np.zeros([45, 90])),
            'v5': (['time'], np.zeros(12)),
            'lon': (['lon'], np.linspace(-178, 178, 90)),
            'lat': (['lat'], np.linspace(-88, 88, 45)),
            'time': (['time'], np.linspace(0, 1, 12))})
        ds.v1.encoding['chunksizes'] = (12, 5, 10)
        ds.v2.encoding['chunksizes'] = (12, 15, 30)
        ds.v3.encoding['chunksizes'] = (12, 5, 10)
        ds.v4.encoding['chunksizes'] = (5, 10)
        ds.v5.encoding['chunksizes'] = (1,)

        def map_fn(size, prev_value):
            """Collect all sizes."""
            return [size] + prev_value

        def reduce_fn(values):
            """Median."""
            values = sorted(values)
            return values[len(values) // 2]

        chunk_sizes = get_ext_chunk_sizes(ds, init_value=[], map_fn=map_fn, reduce_fn=reduce_fn)
        self.assertIsNotNone(chunk_sizes)
        self.assertEqual(chunk_sizes, dict(time=12, lat=5, lon=10))

    def test_open_xarray(self):
        wrong_path = os.path.join(_TEST_DATA_PATH, 'small', '*.nck')
        wrong_url = 'httpz://www.acme.com'
        path = [wrong_path, wrong_url]
        try:
            open_xarray_dataset(path)
        except IOError as e:
            self.assertEqual(str(e), 'File {} not found'.format(path))

        right_path = os.path.join(_TEST_DATA_PATH, 'small', '*.nc')
        wrong_url = 'httpz://www.acme.com'
        path = [right_path, wrong_url]
        dsa = open_xarray_dataset(path)
        self.assertIsNotNone(dsa)


class DataAccessErrorTest(unittest.TestCase):
    def test_plain(self):
        try:
            raise DataAccessError("haha")
        except DataAccessError as e:
            self.assertEqual(str(e), "haha")
            self.assertIsInstance(e, Exception)


class NetworkErrorTest(unittest.TestCase):
    def test_plain(self):
        try:
            raise NetworkError("hoho")
        except NetworkError as e:
            self.assertEqual(str(e), "hoho")
            self.assertIsInstance(e, ConnectionError)
