import datetime
import json
import os
import os.path
import shutil
import tempfile
import unittest
import unittest.mock
import urllib.request

from cate.core.ds import DATA_STORE_REGISTRY, DataAccessError, format_variables_info_string
from cate.core.types import PolygonLike, TimeRangeLike, VarNamesLike
from cate.ds.esa_cci_odp import EsaCciOdpDataStore, find_datetime_format, _DownloadStatistics
from cate.ds.local import LocalDataStore


@unittest.skip(reason='Because it writes a lot of files')
# @unittest.skipUnless(condition=os.environ.get('CATE_ODP_TEST', None), reason="skipped unless CATE_ODP_TEST=1")
class EsaCciOdpDataStoreIndexCacheTest(unittest.TestCase):
    def test_index_cache(self):
        self.data_store = EsaCciOdpDataStore(index_cache_used=True, index_cache_expiration_days=1.0e-6)
        data_sources = self.data_store.query()
        self.assertIsNotNone(data_sources)
        for data_source in data_sources:
            data_source.update_file_list()
            # data_source.sync()


def _create_test_data_store():
    with open(os.path.join(os.path.dirname(__file__), 'esgf-index-cache.json')) as fp:
        json_text = fp.read()
    json_dict = json.loads(json_text)
    for d in DATA_STORE_REGISTRY.get_data_stores():
        d.get_updates(reset=True)
    # The EsaCciOdpDataStore created with an initial json_dict avoids fetching it from remote
    data_store = EsaCciOdpDataStore('test-odp', index_cache_json_dict=json_dict, index_cache_update_tag='test1')
    DATA_STORE_REGISTRY.add_data_store(data_store)
    return data_store


class EsaCciOdpDataStoreTest(unittest.TestCase):
    def setUp(self):
        self.data_store = _create_test_data_store()

    def tearDown(self):
        self.data_store.get_updates(reset=True)

    def test_id_title_and_is_local(self):
        self.assertEqual(self.data_store.id, 'test-odp')
        self.assertEqual(self.data_store.title, 'ESA CCI Open Data Portal')
        self.assertEqual(self.data_store.is_local, False)

    def test_description_and_usage_notes(self):
        self.assertIsNotNone(self.data_store.description)
        self.assertTrue(len(self.data_store.description) > 40)
        self.assertIsNotNone(self.data_store.usage_notes)
        self.assertEqual(2, len(self.data_store.usage_notes))
        self.assertTrue(len(self.data_store.usage_notes[0]) > 20)
        self.assertTrue(len(self.data_store.usage_notes[1]) > 20)

    def test_query(self):
        data_sources = self.data_store.query()
        self.assertIsNotNone(data_sources)
        self.assertEqual(len(data_sources), 61)

    def test_query_with_string(self):
        data_sources = self.data_store.query(query_expr='OC')
        self.assertIsNotNone(data_sources)
        self.assertEqual(len(data_sources), 20)


class EsaCciOdpDataSourceTest(unittest.TestCase):
    def setUp(self):
        self.data_store = _create_test_data_store()
        oc_data_sources = self.data_store.query(query_expr='OC')
        self.assertIsNotNone(oc_data_sources)
        self.assertIsNotNone(oc_data_sources[0])
        self.first_oc_data_source = oc_data_sources[0]
        self.tmp_dir = tempfile.mkdtemp()

        self._existing_local_data_store = DATA_STORE_REGISTRY.get_data_store('local')
        DATA_STORE_REGISTRY.add_data_store(LocalDataStore('local', self.tmp_dir))

    def tearDown(self):
        if self._existing_local_data_store:
            DATA_STORE_REGISTRY.add_data_store(self._existing_local_data_store)
        shutil.rmtree(self.tmp_dir, ignore_errors=True)
        self.data_store.get_updates(reset=True)

    def test_make_local_and_update(self):

        soilmoisture_data_sources = self.data_store.query(
            query_expr='esacci.SOILMOISTURE.day.L3S.SSMV.multi-sensor.multi-platform.COMBINED.02-1.r1')
        soilmoisture_data_source = soilmoisture_data_sources[0]

        reference_path = os.path.join(os.path.dirname(__file__),
                                      os.path.normpath('resources/datasources/local/files/'))

        def find_files_mock(_, time_range):

            def build_file_item(item_name: str, date_from: datetime, date_to: datetime, size: int):

                return [item_name, date_from, date_to, size,
                        {'OPENDAP': os.path.join(reference_path, item_name),
                         'HTTPServer': 'file:' + urllib.request.pathname2url(os.path.join(reference_path, item_name))}]

            reference_files = {
                'ESACCI-SOILMOISTURE-L3S-SSMV-COMBINED-19781114000000-fv02.2.nc': {
                    'date_from': datetime.datetime(1978, 11, 14, 0, 0),
                    'date_to': datetime.datetime(1978, 11, 14, 23, 59),
                    'size': 21511378
                },
                'ESACCI-SOILMOISTURE-L3S-SSMV-COMBINED-19781115000000-fv02.2.nc': {
                    'date_from': datetime.datetime(1978, 11, 15, 0, 0),
                    'date_to': datetime.datetime(1978, 11, 15, 23, 59),
                    'size': 21511378
                },
                'ESACCI-SOILMOISTURE-L3S-SSMV-COMBINED-19781116000000-fv02.2.nc': {
                    'date_from': datetime.datetime(1978, 11, 16, 0, 0),
                    'date_to': datetime.datetime(1978, 11, 16, 23, 59),
                    'size': 21511378
                }
            }

            reference_files_list = []

            for reference_file in reference_files.items():
                file_name = reference_file[0]
                file_date_from = reference_file[1].get('date_from')
                file_date_to = reference_file[1].get('date_to')
                file_size = reference_file[1].get('size')
                if time_range:
                    if file_date_from >= time_range[0] and file_date_to <= time_range[1]:
                        reference_files_list.append(build_file_item(file_name,
                                                                    file_date_from,
                                                                    file_date_to,
                                                                    file_size))
                else:
                    reference_files_list.append(build_file_item(file_name,
                                                                file_date_from,
                                                                file_date_to,
                                                                file_size))
            return reference_files_list

        with unittest.mock.patch('cate.ds.esa_cci_odp.EsaCciOdpDataSource._find_files', find_files_mock):
            with unittest.mock.patch.object(EsaCciOdpDataStore, 'query', return_value=[]):

                new_ds_title = 'local_ds_test'
                new_ds_time_range = TimeRangeLike.convert((datetime.datetime(1978, 11, 14, 0, 0),
                                                           datetime.datetime(1978, 11, 16, 23, 59)))
                try:
                    new_ds = soilmoisture_data_source.make_local(new_ds_title, time_range=new_ds_time_range)
                except Exception:
                    raise ValueError(reference_path, os.listdir(reference_path))
                self.assertIsNotNone(new_ds)

                self.assertEqual(new_ds.id, "local.%s" % new_ds_title)
                self.assertEqual(new_ds.temporal_coverage(), new_ds_time_range)

                new_ds_w_one_variable_title = 'local_ds_test_var'
                new_ds_w_one_variable_time_range = TimeRangeLike.convert((datetime.datetime(1978, 11, 14, 0, 0),
                                                                          datetime.datetime(1978, 11, 16, 23, 59)))
                new_ds_w_one_variable_var_names = VarNamesLike.convert(['sm'])

                new_ds_w_one_variable = soilmoisture_data_source.make_local(
                    new_ds_w_one_variable_title,
                    time_range=new_ds_w_one_variable_time_range,
                    var_names=new_ds_w_one_variable_var_names
                )
                self.assertIsNotNone(new_ds_w_one_variable)

                self.assertEqual(new_ds_w_one_variable.id, "local.%s" % new_ds_w_one_variable_title)
                ds = new_ds_w_one_variable.open_dataset()

                new_ds_w_one_variable_var_names.extend(['lat', 'lon', 'time'])

                self.assertSetEqual(set(ds.variables),
                                    set(new_ds_w_one_variable_var_names))

                new_ds_w_region_title = 'from_local_to_local_region'
                new_ds_w_region_time_range = TimeRangeLike.convert((datetime.datetime(1978, 11, 14, 0, 0),
                                                                    datetime.datetime(1978, 11, 16, 23, 59)))
                new_ds_w_region_spatial_coverage = PolygonLike.convert("10,20,30,40")

                new_ds_w_region = soilmoisture_data_source.make_local(
                    new_ds_w_region_title,
                    time_range=new_ds_w_region_time_range,
                    region=new_ds_w_region_spatial_coverage)  # type: LocalDataSource

                self.assertIsNotNone(new_ds_w_region)

                self.assertEqual(new_ds_w_region.id, "local.%s" % new_ds_w_region_title)

                self.assertEqual(new_ds_w_region.spatial_coverage(), new_ds_w_region_spatial_coverage)

                new_ds_w_region_title = 'from_local_to_local_region_one_var'
                new_ds_w_region_time_range = TimeRangeLike.convert((datetime.datetime(1978, 11, 14, 0, 0),
                                                                    datetime.datetime(1978, 11, 16, 23, 59)))
                new_ds_w_region_var_names = VarNamesLike.convert(['sm'])
                new_ds_w_region_spatial_coverage = PolygonLike.convert("10,20,30,40")

                new_ds_w_region = soilmoisture_data_source.make_local(
                    new_ds_w_region_title,
                    time_range=new_ds_w_region_time_range,
                    var_names=new_ds_w_region_var_names,
                    region=new_ds_w_region_spatial_coverage)  # type: LocalDataSource

                self.assertIsNotNone(new_ds_w_region)

                self.assertEqual(new_ds_w_region.id, "local.%s" % new_ds_w_region_title)

                self.assertEqual(new_ds_w_region.spatial_coverage(), new_ds_w_region_spatial_coverage)
                data_set = new_ds_w_region.open_dataset()
                new_ds_w_region_var_names.extend(['lat', 'lon', 'time'])

                self.assertSetEqual(set(data_set.variables), set(new_ds_w_region_var_names))

                new_ds_w_region_title = 'from_local_to_local_region_two_var_sm_uncertainty'
                new_ds_w_region_time_range = TimeRangeLike.convert((datetime.datetime(1978, 11, 14, 0, 0),
                                                                    datetime.datetime(1978, 11, 16, 23, 59)))
                new_ds_w_region_var_names = VarNamesLike.convert(['sm', 'sm_uncertainty'])
                new_ds_w_region_spatial_coverage = PolygonLike.convert("10,20,30,40")

                new_ds_w_region = soilmoisture_data_source.make_local(
                    new_ds_w_region_title,
                    time_range=new_ds_w_region_time_range,
                    var_names=new_ds_w_region_var_names,
                    region=new_ds_w_region_spatial_coverage)  # type: LocalDataSource

                self.assertIsNotNone(new_ds_w_region)

                self.assertEqual(new_ds_w_region.id, "local.%s" % new_ds_w_region_title)

                self.assertEqual(new_ds_w_region.spatial_coverage(), new_ds_w_region_spatial_coverage)
                data_set = new_ds_w_region.open_dataset()
                new_ds_w_region_var_names.extend(['lat', 'lon', 'time'])

                self.assertSetEqual(set(data_set.variables), set(new_ds_w_region_var_names))

                empty_ds_timerange = (datetime.datetime(2017, 12, 1, 0, 0), datetime.datetime(2017, 12, 31, 23, 59))
                with self.assertRaises(DataAccessError) as cm:
                    soilmoisture_data_source.make_local('empty_ds', time_range=empty_ds_timerange)
                self.assertEqual(f'Data source "{soilmoisture_data_source.id}" does not'
                                 f' seem to have any datasets in given'
                                 f' time range {TimeRangeLike.format(empty_ds_timerange)}',
                                 str(cm.exception))

                new_ds_time_range = TimeRangeLike.convert((datetime.datetime(1978, 11, 14, 0, 0),
                                                           datetime.datetime(1978, 11, 14, 23, 59)))

                new_ds = soilmoisture_data_source.make_local("title_test_copy", time_range=new_ds_time_range)
                self.assertIsNotNone(new_ds)
                self.assertEqual(new_ds.meta_info['title'], soilmoisture_data_source.meta_info['title'])

                title = "Title Test!"
                new_ds = soilmoisture_data_source.make_local("title_test_set", title, time_range=new_ds_time_range)
                self.assertIsNotNone(new_ds)
                self.assertEqual(new_ds.meta_info['title'], title)

    def test_data_store(self):
        self.assertIs(self.first_oc_data_source.data_store,
                      self.data_store)

    def test_id(self):
        self.assertEqual(self.first_oc_data_source.id,
                         'esacci.OC.day.L3S.K_490.multi-sensor.multi-platform.MERGED.1-0.r2')

    def test_schema(self):
        self.assertEqual(self.first_oc_data_source.schema,
                         None)

    @unittest.skip(reason='outdated info string')
    def test_info_string(self):
        self.assertIn('product_string:          MERGED\n',
                      self.first_oc_data_source.info_string)

    def test_variables_info_string(self):
        self.assertIn('kd_490 (m-1):\n',
                      format_variables_info_string(self.first_oc_data_source.variables_info),
                      self.first_oc_data_source.variables_info)
        self.assertIn('Long name:        Downwelling attenuation coefficient at 490nm',
                      format_variables_info_string(self.first_oc_data_source.variables_info))

    @unittest.skip(reason='ssl error on windows')
    def test_temporal_coverage(self):
        self.assertEqual(self.first_oc_data_source.temporal_coverage(),
                         (datetime.datetime(1997, 9, 4, 0, 0), datetime.datetime(2000, 6, 24, 0, 0)))

    def assert_tf(self, filename: str, expected_time_format: str):
        time_format, p1, p2 = find_datetime_format(filename)
        self.assertEqual(time_format, expected_time_format)

    def test_time_filename_patterns(self):
        self.assert_tf('20020730174408-ESACCI-L3U_GHRSST-SSTskin-AATSR-LT-v02.0-fv01.1.nc', '%Y%m%d%H%M%S')
        self.assert_tf('19911107054700-ESACCI-L2P_GHRSST-SSTskin-AVHRR12_G-LT-v02.0-fv01.0.nc', '%Y%m%d%H%M%S')
        self.assert_tf('ESACCI-SEAICE-L4-SICONC-SSMI-NH25kmEASE2-19920610-fv01.11.nc', '%Y%m%d')
        self.assert_tf('ESACCI-SEAICE-L4-SICONC-SSMI-SH25kmEASE2-20000101-20001231-fv01.11.nc', '%Y%m%d')
        self.assert_tf('ESACCI-SEAICE-L4-SICONC-AMSR-NH25kmEASE2-20070204-fv01.11.nc', '%Y%m%d')
        self.assert_tf('ESACCI-SEAICE-L4-SICONC-AMSR-SH25kmEASE2-20040427-fv01.11.nc', '%Y%m%d')
        self.assert_tf('19921018120000-ESACCI-L4_GHRSST-SSTdepth-OSTIA-GLOB_LT-v02.0-fv01.0.nc', '%Y%m%d%H%M%S')
        self.assert_tf('19940104120000-ESACCI-L4_GHRSST-SSTdepth-OSTIA-GLOB_LT-v02.0-fv01.1.nc', '%Y%m%d%H%M%S')
        self.assert_tf('ESACCI-OZONE-L3S-TC-MERGED-DLR_1M-20090301-fv0100.nc', '%Y%m%d')
        self.assert_tf('20070328-ESACCI-L3U_CLOUD-CLD_PRODUCTS-AVHRR_NOAA-15-fv1.0.nc', '%Y%m%d')
        self.assert_tf('20091002-ESACCI-L3U_CLOUD-CLD_PRODUCTS-AVHRR_NOAA-16-fv1.0.nc', '%Y%m%d')
        self.assert_tf('20090729-ESACCI-L3U_CLOUD-CLD_PRODUCTS-AVHRR_NOAA-18-fv1.0.nc', '%Y%m%d')
        self.assert_tf('20070410-ESACCI-L3U_CLOUD-CLD_PRODUCTS-AVHRR_NOAA-17-fv1.0.nc', '%Y%m%d')
        self.assert_tf('ESACCI-OC-L3S-K_490-MERGED-1D_DAILY_4km_SIN_PML_KD490_Lee-20000129-fv1.0.nc', '%Y%m%d')
        self.assert_tf('ESACCI-OC-L3S-K_490-MERGED-1D_DAILY_4km_GEO_PML_KD490_Lee-19980721-fv1.0.nc', '%Y%m%d')
        self.assert_tf('ESACCI-OZONE-L3-NP-MERGED-KNMI-200812-fv0002.nc', '%Y%m')
        self.assert_tf('ESACCI-OC-L3S-CHLOR_A-MERGED-1D_DAILY_4km_GEO_PML_OC4v6-19971130-fv1.0.nc', '%Y%m%d')
        self.assert_tf('ESACCI-OC-L3S-CHLOR_A-MERGED-1D_DAILY_4km_SIN_PML_OC4v6-19980625-fv1.0.nc', '%Y%m%d')
        self.assert_tf('200903-ESACCI-L3C_CLOUD-CLD_PRODUCTS-AVHRR_NOAA-15-fv1.0.nc', '%Y%m')
        self.assert_tf('ESACCI-GHG-L2-CH4-GOSAT-SRPR-20100501-fv1.nc', '%Y%m%d')
        self.assert_tf('ESACCI-GHG-L2-CH4-GOSAT-SRPR-20091201-fv1.nc', '%Y%m%d')
        self.assert_tf('ESACCI-GHG-L2-CO2-GOSAT-SRFP-20101220-fv1.nc', '%Y%m%d')
        self.assert_tf('ESACCI-GHG-L2-CH4-GOSAT-SRFP-20100109-fv1.nc', '%Y%m%d')
        self.assert_tf('ESACCI-GHG-L2-CO2-GOSAT-SRFP-20090527-fv1.nc', '%Y%m%d')
        self.assert_tf('ESACCI-GHG-L2-CH4-GOSAT-SRFP-20100714-fv1.nc', '%Y%m%d')
        self.assert_tf('20090616-ESACCI-L3U_CLOUD-CLD_PRODUCTS-MODIS_TERRA-fv1.0.nc', '%Y%m%d')
        self.assert_tf('20070717-ESACCI-L3U_CLOUD-CLD_PRODUCTS-MODIS_AQUA-fv1.0.nc', '%Y%m%d')
        self.assert_tf('ESACCI-OC-L3S-OC_PRODUCTS-MERGED-8D_DAILY_4km_GEO_PML_OC4v6_QAA-19971211-fv1.0.nc', '%Y%m%d')
        self.assert_tf('ESACCI-OC-L3S-OC_PRODUCTS-MERGED-8D_DAILY_4km_SIN_PML_OC4v6_QAA-20080921-fv1.0.nc', '%Y%m%d')
        self.assert_tf('ESACCI-OC-L3S-OC_PRODUCTS-MERGED-1M_MONTHLY_4km_SIN_PML_OC4v6_QAA-200906-fv1.0.nc', '%Y%m')
        self.assert_tf('ESACCI-OC-L3S-OC_PRODUCTS-MERGED-1M_MONTHLY_4km_GEO_PML_OC4v6_QAA-200707-fv1.0.nc', '%Y%m')
        self.assert_tf('ESACCI-OC-L3S-OC_PRODUCTS-MERGED-1Y_YEARLY_4km_GEO_PML_OC4v6_QAA-2005-fv1.0.nc', '%Y')
        self.assert_tf('ESACCI-OC-L3S-OC_PRODUCTS-MERGED-1Y_YEARLY_4km_GEO_PML_OC4v6_QAA-2003-fv1.0.nc', '%Y')
        self.assert_tf('ESACCI-OC-L3S-OC_PRODUCTS-MERGED-8D_DAILY_4km_GEO_PML_OC4v6_QAA-19970914-fv1.0.nc', '%Y%m%d')
        self.assert_tf('ESACCI-OC-L3S-IOP-MERGED-1D_DAILY_4km_GEO_PML_QAA-19970915-fv1.0.nc', '%Y%m%d')
        self.assert_tf('ESACCI-OC-L3S-IOP-MERGED-1D_DAILY_4km_GEO_PML_QAA-19980724-fv1.0.nc', '%Y%m%d')
        self.assert_tf('20020822103843-ESACCI-L3U_GHRSST-SSTskin-AATSR-LT-v02.0-fv01.0.nc', '%Y%m%d%H%M%S')
        self.assert_tf('ESACCI-OZONE-L3S-TC-MERGED-DLR_1M-19980301-fv0100.nc', '%Y%m%d')
        self.assert_tf('ESACCI-SOILMOISTURE-L3S-SSMV-COMBINED-19781120000000-fv02.1.nc', '%Y%m%d%H%M%S')
        self.assert_tf('ESACCI-SOILMOISTURE-L3S-SSMV-PASSIVE-19791011000000-fv02.1.nc', '%Y%m%d%H%M%S')
        self.assert_tf('ESACCI-SOILMOISTURE-L3S-SSMV-PASSIVE-19790519000000-fv02.2.nc', '%Y%m%d%H%M%S')
        self.assert_tf('ESACCI-SOILMOISTURE-L3S-SSMS-ACTIVE-19911026000000-fv02.1.nc', '%Y%m%d%H%M%S')
        self.assert_tf('ESACCI-SOILMOISTURE-L3S-SSMS-ACTIVE-19911010000000-fv02.2.nc', '%Y%m%d%H%M%S')
        self.assert_tf('ESACCI-SEALEVEL-IND-MSL-MERGED-20151104000000-fv01.nc', '%Y%m%d%H%M%S')
        self.assert_tf('ESACCI-SEALEVEL-IND-MSLAMPH-MERGED-20151104000000-fv01.nc', '%Y%m%d%H%M%S')
        self.assert_tf('ESACCI-SEALEVEL-IND-MSLTR-MERGED-20151104000000-fv01.nc', '%Y%m%d%H%M%S')
        self.assert_tf('ESACCI-OC-L3S-RRS-MERGED-1D_DAILY_4km_GEO_PML_RRS-19980418-fv1.0.nc', '%Y%m%d')
        self.assert_tf('ESACCI-OC-L3S-RRS-MERGED-1D_DAILY_4km_SIN_PML_RRS-19980925-fv1.0.nc', '%Y%m%d')
        self.assert_tf('200811-ESACCI-L3C_CLOUD-CLD_PRODUCTS-AVHRR_NOAA-18-fv1.0.nc', '%Y%m')
        self.assert_tf('200704-ESACCI-L3C_CLOUD-CLD_PRODUCTS-AVHRR_NOAA-16-fv1.0.nc', '%Y%m')
        self.assert_tf('200811-ESACCI-L3C_CLOUD-CLD_PRODUCTS-AVHRR_NOAA-17-fv1.0.nc', '%Y%m')
        self.assert_tf('200712-ESACCI-L3C_CLOUD-CLD_PRODUCTS-MODIS_TERRA-fv1.0.nc', '%Y%m')
        self.assert_tf('200902-ESACCI-L3C_CLOUD-CLD_PRODUCTS-MODIS_AQUA-fv1.0.nc', '%Y%m')
        self.assert_tf('200706-ESACCI-L3S_CLOUD-CLD_PRODUCTS-MODIS_MERGED-fv1.0.nc', '%Y%m')
        self.assert_tf('200901-ESACCI-L3S_CLOUD-CLD_PRODUCTS-AVHRR_MERGED-fv1.0.nc', '%Y%m')
        self.assert_tf('ESACCI-OC-L3S-OC_PRODUCTS-MERGED-1M_MONTHLY_4km_GEO_PML_OC4v6_QAA-200505-fv1.0.nc', '%Y%m')
        self.assert_tf('ESACCI-OC-L3S-OC_PRODUCTS-MERGED-1D_DAILY_4km_SIN_PML_OC4v6_QAA-19980720-fv1.0.nc', '%Y%m%d')
        self.assert_tf('ESACCI-OC-L3S-OC_PRODUCTS-MERGED-1D_DAILY_4km_GEO_PML_OC4v6_QAA-19990225-fv1.0.nc', '%Y%m%d')
        self.assert_tf('ESACCI-OC-L3S-OC_PRODUCTS-MERGED-8D_DAILY_4km_GEO_PML_OC4v6_QAA-19990407-fv1.0.nc', '%Y%m%d')
        self.assert_tf('ESACCI-OC-L3S-OC_PRODUCTS-MERGED-1D_DAILY_4km_GEO_PML_OC4v6_QAA-19970915-fv1.0.nc', '%Y%m%d')
        self.assert_tf('20060107-ESACCI-L4_FIRE-BA-MERIS-fv4.1.nc', '%Y%m%d')


class DownloadStatisticsTest(unittest.TestCase):

    def test_make_local_and_update(self):
        download_stats = _DownloadStatistics(64000000)
        self.assertEqual(str(download_stats), '0 of 64 MB @ 0.000 MB/s, 0.0% complete')
        download_stats.handle_chunk(16000000)
        self.assertEqual(str(download_stats), '16 of 64 MB @ 0.000 MB/s, 25.0% complete')
        download_stats.handle_chunk(32000000)
        self.assertEqual(str(download_stats), '48 of 64 MB @ 0.000 MB/s, 75.0% complete')
        download_stats.handle_chunk(16000000)
        self.assertEqual(str(download_stats), '64 of 64 MB @ 0.000 MB/s, 100.0% complete')


@unittest.skip(reason='Used for debugging to fix Cate issues #823, #822, #818, #816, #783')
class SpatialSubsetTest(unittest.TestCase):

    def test_make_local_spatial(self):
        data_store = EsaCciOdpDataStore()
        data_source = data_store.query(ds_id='esacci.SST.day.L4.SSTdepth.multi-sensor.multi-platform.OSTIA.1-1.r1')[0]
        # The following always worked fine:
        ds = data_source.open_dataset(time_range=['2010-01-01', '2010-01-04'], region='-10,40,20,70')
        self.assertIsNotNone(ds)
        # The following reproduced Cate issues #823, #822, #818, #816, #783:
        ds = data_source.make_local('SST_DAY_L4', time_range=['2010-01-01', '2010-01-04'], region='-10,40,20,70')
        self.assertIsNotNone(ds)
