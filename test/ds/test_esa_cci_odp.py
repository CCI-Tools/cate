import json
import os
import os.path
import unittest

from cate.ds.esa_cci_odp import EsaCciOdpDataStore, find_datetime_format


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
    # The EsaCciOdpDataStore created with an initial json_dict avoids fetching it from remote
    return EsaCciOdpDataStore('test-odp', index_cache_json_dict=json_dict)


class EsaCciOdpDataStoreTest(unittest.TestCase):
    def setUp(self):
        self.data_store = _create_test_data_store()

    def test_query(self):
        data_sources = self.data_store.query()
        self.assertIsNotNone(data_sources)
        self.assertEqual(len(data_sources), 61)

    def test_query_with_string(self):
        data_sources = self.data_store.query('OC')
        self.assertIsNotNone(data_sources)
        self.assertEqual(len(data_sources), 20)


@unittest.skip(reason='Hardcoded values from remote service, contains outdated assumptions')
class EsaCciOdpDataSourceTest(unittest.TestCase):
    def setUp(self):
        self.data_store = _create_test_data_store()
        data_sources = self.data_store.query('OC')
        self.assertIsNotNone(data_sources)
        self.assertIsNotNone(data_sources[0])
        self.data_source = data_sources[0]

    def test_data_store(self):
        self.assertIs(self.data_source.data_store,
                      self.data_store)

    def test_id(self):
        self.assertEqual(self.data_source.name,
                         'esacci.OC.day.L3S.K_490.multi-sensor.multi-platform.MERGED.1-0.r2')

    def test_schema(self):
        self.assertEqual(self.data_source.schema,
                         None)

    def test_info_string(self):
        # print(self.data_source.info_string)
        self.assertIn('product_string:          MERGED\n',
                      self.data_source.info_string)

    def test_variables_info_string(self):
        # print(self.data_source.variables_info_string)
        self.assertIn('kd_490 (m-1):\n',
                      self.data_source.variables_info_string)
        self.assertIn('Long name:        Downwelling attenuation coefficient at 490nm',
                      self.data_source.variables_info_string)

    def test_temporal_coverage(self):
        self.assertEqual(self.data_source.temporal_coverage(), None)

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
