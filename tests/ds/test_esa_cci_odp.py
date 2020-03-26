from datetime import datetime
from lxml.etree import XML
import asyncio
import json
import os
import shutil
import tempfile
import unittest
import unittest.mock
import urllib.request
from cate.core.ds import DATA_STORE_REGISTRY, DataAccessError, DataStoreNotice
from cate.ds.esa_cci_odp import _fetch_file_list_json, _extract_metadata_from_odd, _extract_metadata_from_odd_url, \
    _extract_metadata_from_descxml, _extract_metadata_from_descxml_url, _harmonize_info_field_names, \
    _DownloadStatistics, EsaCciOdpDataStore, find_datetime_format, _retrieve_infos_from_dds
from cate.core.types import PolygonLike, TimeRangeLike, VarNamesLike
from cate.ds.local import LocalDataStore


class EsaCciOdpOsTest(unittest.TestCase):

    @unittest.skip(reason='Requires web access')
    def test_fetch_opensearch_json_1(self):
        file_list = asyncio.run(_fetch_file_list_json('49bcb6f29c824ae49e41d2d3656f11be'))
        self.assertEqual("ESACCI-OC-L3S-K_490-MERGED-8D_DAILY_4km_GEO_PML_KD490_Lee-19970829-fv2.0.nc", file_list[0][0])
        self.assertEqual("1997-09-04T00:00:00", file_list[0][1])
        self.assertEqual("1997-09-04T00:00:00", file_list[0][2])
        self.assertEqual(5924361, file_list[0][3])
        self.assertEqual(2, len(file_list[0][4]))
        self.assertTrue("Download" in file_list[0][4])
        self.assertEqual("http://dap.ceda.ac.uk/thredds/fileServer/neodc/esacci/ocean_colour/data/v2-release/"
                         "geographic/netcdf/kd/8day/v2.0/1997/"
                         "ESACCI-OC-L3S-K_490-MERGED-8D_DAILY_4km_GEO_PML_KD490_Lee-19970829-fv2.0.nc",
                         file_list[0][4].get("Download"))
        self.assertEqual("http://dap.ceda.ac.uk/thredds/dodsC/dap//neodc/esacci/ocean_colour/data/v2-release/"
                         "geographic/netcdf/kd/8day/v2.0/1997/"
                         "ESACCI-OC-L3S-K_490-MERGED-8D_DAILY_4km_GEO_PML_KD490_Lee-19970829-fv2.0.nc",
                         file_list[0][4].get("Opendap"))

    @unittest.skip(reason='Requires web access. Also, we are getting an error 500 currently')
    def test_fetch_opensearch_json_2(self):
        file_list = asyncio.run(_fetch_file_list_json('b382ebe6679d44b8b0e68ea4ef4b701c'))
        self.assertEqual(54, len(file_list))
        self.assertEqual("ESACCI-LC-L4-LCCS-Map-300m-P1Y-1992-v2.0.7b.nc", file_list[1][0])
        self.assertEqual("1992-01-01T00:00:00", file_list[1][1])
        self.assertEqual("1992-01-01T00:00:00", file_list[1][2])
        self.assertEqual(2543742078, file_list[1][3])
        self.assertEqual(2, len(file_list[1][4]))
        self.assertTrue("Download" in file_list[1][4])
        self.assertEqual("http://cci-odp-data2.ceda.ac.uk/thredds/fileServer/esacci/land_cover/data/land_cover_maps/"
                         "v2.0.7/ESACCI-LC-L4-LCCS-Map-300m-P1Y-1992-v2.0.7b.nc",
                         file_list[1][4].get("Download"))
        self.assertEqual('http://cci-odp-data2.ceda.ac.uk/thredds/dodsC/esacci/land_cover/data/land_cover_maps/'
                         'v2.0.7/ESACCI-LC-L4-LCCS-Map-300m-P1Y-1992-v2.0.7b.nc',
                         file_list[1][4].get("Opendap"))

    def test_extract_metadata_from_odd_file(self):
        odd_file = os.path.join(os.path.dirname(__file__), 'resources/odd.xml')
        with open(odd_file) as odd:
            json_obj = _extract_metadata_from_odd(XML(odd.read()))
            self.assertFalse('query' in json_obj)
            self.assertTrue('ecv' in json_obj)
            self.assertEqual('LC', json_obj['ecv'])
            self.assertTrue('time_frequencies' in json_obj)
            self.assertEqual(['day', 'year'], json_obj['time_frequencies'])
            self.assertTrue('institute' in json_obj)
            self.assertEqual('Universite Catholique de Louvain', json_obj['institute'])
            self.assertTrue('processing_level' in json_obj)
            self.assertEqual('L4', json_obj['processing_level'])
            self.assertTrue('product_string' in json_obj)
            self.assertEqual('Map', json_obj['product_string'])
            self.assertTrue('product_version' in json_obj)
            self.assertEqual('2.0.7', json_obj['product_version'])
            self.assertTrue('data_type' in json_obj)
            self.assertEqual('LCCS', json_obj['data_type'])
            self.assertFalse('sensor_id' in json_obj)
            self.assertFalse('platform_id' in json_obj)
            self.assertTrue('file_format' in json_obj)
            self.assertEqual('.nc', json_obj['file_format'])

    @unittest.skip(reason='Requires web access')
    def test_extract_metadata_from_odd_url(self):
        odd_url = 'http://opensearch-test.ceda.ac.uk/opensearch/description.xml?' \
                  'parentIdentifier=7c7a38b2d2ce448b99194bff85a85248'
        json_obj = _extract_metadata_from_odd_url(odd_url)
        self.assertFalse('query' in json_obj)
        self.assertTrue('ecv' in json_obj)
        self.assertEqual('SOILMOISTURE', json_obj['ecv'])
        self.assertTrue('time_frequency' in json_obj)
        self.assertEqual('day', json_obj['time_frequency'])
        self.assertFalse('institute' in json_obj)
        self.assertTrue('processing_level' in json_obj)
        self.assertEqual('L3S', json_obj['processing_level'])
        self.assertTrue('product_string' in json_obj)
        self.assertEqual('COMBINED', json_obj['product_string'])
        self.assertTrue('product_version' in json_obj)
        self.assertEqual('03.2', json_obj['product_version'])
        self.assertTrue('data_type' in json_obj)
        self.assertEqual('SSMV', json_obj['data_type'])
        self.assertTrue('sensor_id' in json_obj)
        self.assertEqual('multi-sensor', json_obj['sensor_id'])
        self.assertTrue('platform_id' in json_obj)
        self.assertEqual('multi-platform', json_obj['platform_id'])
        self.assertTrue('file_format' in json_obj)
        self.assertEqual('.nc', json_obj['file_format'])

    @unittest.skip(reason='Requires web access')
    def test_extract_metadata_from_descxml_url(self):
        desc_url = 'https://catalogue.ceda.ac.uk/export/xml/49bcb6f29c824ae49e41d2d3656f11be.xml'
        json_obj = asyncio.run(_extract_metadata_from_descxml_url(None, desc_url))
        self.assert_json_obj_from_desc_xml(json_obj)

    def test_extract_metadata_from_descxml(self):
        desc_file = os.path.join(os.path.dirname(__file__), 'resources/49bcb6f29c824ae49e41d2d3656f11be.xml')
        with open(desc_file) as desc:
            json_obj = _extract_metadata_from_descxml(XML(desc.read()))
            self.assert_json_obj_from_desc_xml(json_obj)

    @unittest.skip(reason='Requires web access')
    def test_retrieve_dimensions_from_dds_url(self):
        dds_url = "http://dap.ceda.ac.uk/thredds/dodsC/dap//neodc/esacci/soil_moisture/data/daily_files/" \
                  "COMBINED/v04.4/1986/ESACCI-SOILMOISTURE-L3S-SSMV-COMBINED-19861125000000-fv04.4.nc.dds"
        response = urllib.request.urlopen(dds_url)
        dimensions, variable_infos = _retrieve_infos_from_dds(response.readlines())
        self.assertEqual(3, len(dimensions))
        self.assertTrue('lat' in dimensions)
        self.assertEqual(720, dimensions['lat'])
        self.assertTrue('lon' in dimensions)
        self.assertEqual(1440, dimensions['lon'])
        self.assertTrue('time' in dimensions)
        self.assertEqual(1, dimensions['time'])
        self.assertEqual(11, len(variable_infos))
        self.assertTrue('freqbandID' in variable_infos)
        self.assertEqual('Int16', variable_infos['freqbandID']['data_type'])
        self.assertEqual(3, len(variable_infos['freqbandID']['dimensions']))
        self.assertTrue('time' in variable_infos['freqbandID']['dimensions'])
        self.assertTrue('lat' in variable_infos['freqbandID']['dimensions'])
        self.assertTrue('lon' in variable_infos['freqbandID']['dimensions'])

    def test_retrieve_dimensions_from_dds(self):
        dds_file = os.path.join(os.path.dirname(__file__),
                                "resources/ESACCI-SOILMOISTURE-L3S-SSMV-COMBINED-19861125000000-fv04.4.nc.dds")
        dds = open(dds_file)
        dimensions, variable_infos = _retrieve_infos_from_dds(dds.readlines())
        self.assertEqual(4, len(dimensions))
        self.assertTrue('xc' in dimensions)
        self.assertEqual(216, dimensions['xc'])
        self.assertTrue('yc' in dimensions)
        self.assertEqual(216, dimensions['yc'])
        self.assertTrue('time' in dimensions)
        self.assertEqual(1, dimensions['time'])
        self.assertTrue('nv' in dimensions)
        self.assertEqual(2, dimensions['nv'])
        self.assertEqual(13, len(variable_infos))
        self.assertTrue('Lambert_Azimuthal_Grid' in variable_infos)
        self.assertEqual('Int32', variable_infos['Lambert_Azimuthal_Grid']['data_type'])
        self.assertEqual(0, len(variable_infos['Lambert_Azimuthal_Grid']['dimensions']))
        self.assertTrue('time_bnds' in variable_infos)
        self.assertEqual('Float64', variable_infos['time_bnds']['data_type'])
        self.assertEqual(2, len(variable_infos['time_bnds']['dimensions']))
        self.assertTrue('time' in variable_infos['time_bnds']['dimensions'])
        self.assertTrue('nv' in variable_infos['time_bnds']['dimensions'])
        self.assertTrue('ice_conc' in variable_infos)
        self.assertEqual('Int32', variable_infos['ice_conc']['data_type'])
        self.assertEqual(3, len(variable_infos['ice_conc']['dimensions']))
        self.assertTrue('time' in variable_infos['ice_conc']['dimensions'])
        self.assertTrue('yc' in variable_infos['ice_conc']['dimensions'])
        self.assertTrue('xc' in variable_infos['ice_conc']['dimensions'])

    def assert_json_obj_from_desc_xml(self, json_obj: dict):
        self.assertTrue('abstract' in json_obj)
        self.maxDiff = None
        self.assertTrue('title' in json_obj)
        self.assertTrue('licences' in json_obj)
        self.assertTrue('bbox_minx' in json_obj)
        self.assertEqual('-180.0', json_obj['bbox_minx'])
        self.assertTrue('bbox_miny' in json_obj)
        self.assertEqual('-90.0', json_obj['bbox_miny'])
        self.assertTrue('bbox_maxx' in json_obj)
        self.assertEqual('180.0', json_obj['bbox_maxx'])
        self.assertTrue('bbox_maxy' in json_obj)
        self.assertEqual('90.0', json_obj['bbox_maxy'])
        self.assertTrue('temporal_coverage_start' in json_obj)
        self.assertEqual('1997-09-03T23:00:00', json_obj['temporal_coverage_start'])
        self.assertTrue('temporal_coverage_end' in json_obj)
        self.assertEqual('2013-12-31T23:59:59', json_obj['temporal_coverage_end'])
        self.assertTrue('file_formats' in json_obj)
        self.assertEqual('.nc', json_obj['file_formats'])
        self.assertTrue('publication_date' in json_obj)
        self.assertTrue('creation_date' in json_obj)
        self.assertEqual('2016-12-12T17:08:42', json_obj['creation_date'])

    def test_harmonize_info_field_names(self):
        test_catalogue = {'file_format': '.tiff', 'platform_ids': ['dfjh', 'ftrzg6'], 'sensor_id': 'hxfb75z',
                          'sensor_ids': ['hxfb75z'], 'processing_level': 'L2', 'processing_levels': ['L1'],
                          'time_frequency': 'gsyhdx', 'time_frequencies': ['gsyhdx', 'zsgsteczh', 'fzgu'],
                          'field6': 'njgil', 'field6s': ['<dshjbre', 'hsr6u'], 'field7': 'multiple_field7s',
                          'field7s': ['saedf', 'kihji']}

        _harmonize_info_field_names(test_catalogue, 'file_format', 'file_formats')
        _harmonize_info_field_names(test_catalogue, 'platform_id', 'platform_ids')
        _harmonize_info_field_names(test_catalogue, 'sensor_id', 'sensor_ids')
        _harmonize_info_field_names(test_catalogue, 'processing_level', 'processing_levels')
        _harmonize_info_field_names(test_catalogue, 'time_frequency', 'time_frequencies')
        _harmonize_info_field_names(test_catalogue, 'field6', 'field6s', 'multiple_field6s'),
        _harmonize_info_field_names(test_catalogue, 'field7', 'field7s', 'multiple_field7s')

        self.assertTrue('file_format' in test_catalogue)
        self.assertEqual('.tiff', test_catalogue['file_format'])
        self.assertFalse('file_formats' in test_catalogue)
        self.assertFalse('platform_id' in test_catalogue)
        self.assertTrue('platform_ids' in test_catalogue)
        self.assertEqual(['dfjh', 'ftrzg6'], test_catalogue['platform_ids'])
        self.assertTrue('sensor_id' in test_catalogue)
        self.assertFalse('sensor_ids' in test_catalogue)
        self.assertEqual('hxfb75z', test_catalogue['sensor_id'])
        self.assertFalse('processing_level' in test_catalogue)
        self.assertTrue('processing_levels' in test_catalogue)
        self.assertEqual(['L1', 'L2'], test_catalogue['processing_levels'])
        self.assertFalse('time_frequency' in test_catalogue)
        self.assertTrue('time_frequencies' in test_catalogue)
        self.assertEqual(['gsyhdx', 'zsgsteczh', 'fzgu'], test_catalogue['time_frequencies'])
        self.assertFalse('field6' in test_catalogue)
        self.assertTrue('field6s' in test_catalogue)
        self.assertEqual(['<dshjbre', 'hsr6u', 'njgil'], test_catalogue['field6s'])
        self.assertFalse('field7' in test_catalogue)
        self.assertTrue('field7s' in test_catalogue)
        self.assertEqual(['saedf', 'kihji'], test_catalogue['field7s'])


@unittest.skip(reason='Because it writes a lot of files')
# @unittest.skipUnless(condition=os.environ.get('CATE_ODP_TEST', None), reason="skipped unless CATE_ODP_TEST=1")
class EsaCciOdpDataStoreIndexCacheTest(unittest.TestCase):
    def test_index_cache(self):
        self.data_store = EsaCciOdpDataStore(index_cache_used=True, index_cache_expiration_days=1.0e-6)
        data_sources = self.data_store.query()
        self.assertIsNotNone(data_sources)
        for data_source in data_sources:
            data_source.update_file_list()


def _create_test_data_store():
    with open(os.path.join(os.path.dirname(__file__), 'resources/os-data-list.json')) as fp:
        json_text = fp.read()
    json_dict = json.loads(json_text)
    for d in DATA_STORE_REGISTRY.get_data_stores():
        d.get_updates(reset=True)
    metadata_path = os.path.join(os.path.dirname(__file__), 'resources/datasources/metadata')
    # The EsaCciOdpDataStore created with an initial json_dict and a metadata dir avoids fetching from remote
    data_store = EsaCciOdpDataStore('test-odp', index_cache_json_dict=json_dict, index_cache_update_tag='test1',
                                    meta_data_store_path=metadata_path)
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

    def test_description(self):
        self.assertIsNotNone(self.data_store.description)
        self.assertTrue(len(self.data_store.description) > 40)

    def test_notices(self):
        self.assertIsInstance(self.data_store.notices, list)
        self.assertEqual(2, len(self.data_store.notices))

        notice0 = self.data_store.notices[0]
        self.assertIsInstance(notice0, DataStoreNotice)
        self.assertEqual(notice0.id, "terminologyClarification")
        self.assertEqual(notice0.title, "Terminology Clarification")
        self.assertEqual(notice0.icon, "info-sign")
        self.assertEqual(notice0.intent, "primary")
        self.assertTrue(len(notice0.content) > 20)

        notice1 = self.data_store.notices[1]
        self.assertIsInstance(notice0, DataStoreNotice)
        self.assertEqual(notice1.id, "dataCompleteness")
        self.assertEqual(notice1.title, "Data Completeness")
        self.assertEqual(notice1.icon, "warning-sign")
        self.assertEqual(notice1.intent, "warning")
        self.assertTrue(len(notice1.content) > 20)

    def test_query(self):
        data_sources = self.data_store.query()
        self.assertIsNotNone(data_sources)
        self.assertEqual(len(data_sources), 5)

    def test_query_with_string(self):
        data_sources = self.data_store.query(query_expr='OC')
        self.assertIsNotNone(data_sources)
        self.assertEqual(len(data_sources), 1)


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
        soil_moisture_data_sources = self.data_store.query(
            query_expr='esacci2.SOILMOISTURE.day.L3S.SSMS.multi-sensor.multi-platform.ACTIVE.04-5.r1')
        soilmoisture_data_source = soil_moisture_data_sources[0]

        reference_path = os.path.join(os.path.dirname(__file__),
                                      os.path.normpath('resources/datasources/local/files/'))

        def find_files_mock(_, time_range):

            def build_file_item(item_name: str, date_from: datetime, date_to: datetime, size: int):

                return [item_name, date_from, date_to, size,
                        {'Opendap': os.path.join(reference_path, item_name),
                         'Download': 'file:' + urllib.request.pathname2url(os.path.join(reference_path, item_name))}]

            reference_files = {
                'ESACCI-SOILMOISTURE-L3S-SSMV-COMBINED-19781114000000-fv02.2.nc': {
                    'date_from': datetime(1995, 11, 14, 0, 0),
                    'date_to': datetime(1995, 11, 14, 23, 59),
                    'size': 21511378
                },
                'ESACCI-SOILMOISTURE-L3S-SSMV-COMBINED-19781115000000-fv02.2.nc': {
                    'date_from': datetime(1995, 11, 15, 0, 0),
                    'date_to': datetime(1995, 11, 15, 23, 59),
                    'size': 21511378
                },
                'ESACCI-SOILMOISTURE-L3S-SSMV-COMBINED-19781116000000-fv02.2.nc': {
                    'date_from': datetime(1995, 11, 16, 0, 0),
                    'date_to': datetime(1995, 11, 16, 23, 59),
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
                new_ds_time_range = TimeRangeLike.convert((datetime(1995, 11, 14, 0, 0),
                                                           datetime(1995, 11, 16, 23, 59)))
                # new_ds_time_range = TimeRangeLike.convert((datetime(1997, 5, 10, 0, 0),
                #                                            datetime(1997, 5, 12, 23, 59)))
                try:
                    new_ds = soilmoisture_data_source.make_local(new_ds_title, time_range=new_ds_time_range)
                except Exception:
                    raise ValueError(reference_path, os.listdir(reference_path))
                self.assertIsNotNone(new_ds)

                self.assertEqual(new_ds.id, "local.%s" % new_ds_title)
                self.assertEqual(new_ds.temporal_coverage(), new_ds_time_range)

                new_ds_w_one_variable_title = 'local_ds_test_var'
                new_ds_w_one_variable_time_range = TimeRangeLike.convert((datetime(1995, 11, 14, 0, 0),
                                                                          datetime(1995, 11, 16, 23, 59)))
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
                new_ds_w_region_time_range = TimeRangeLike.convert((datetime(1995, 11, 14, 0, 0),
                                                                    datetime(1995, 11, 16, 23, 59)))
                new_ds_w_region_spatial_coverage = PolygonLike.convert("10,20,30,40")

                new_ds_w_region = soilmoisture_data_source.make_local(
                    new_ds_w_region_title,
                    time_range=new_ds_w_region_time_range,
                    region=new_ds_w_region_spatial_coverage)

                self.assertIsNotNone(new_ds_w_region)

                self.assertEqual(new_ds_w_region.id, "local.%s" % new_ds_w_region_title)

                self.assertEqual(new_ds_w_region.spatial_coverage(), new_ds_w_region_spatial_coverage)

                new_ds_w_region_title = 'from_local_to_local_region_one_var'
                new_ds_w_region_time_range = TimeRangeLike.convert((datetime(1995, 11, 14, 0, 0),
                                                                    datetime(1995, 11, 16, 23, 59)))
                new_ds_w_region_var_names = VarNamesLike.convert(['sm'])
                new_ds_w_region_spatial_coverage = PolygonLike.convert("10,20,30,40")

                new_ds_w_region = soilmoisture_data_source.make_local(
                    new_ds_w_region_title,
                    time_range=new_ds_w_region_time_range,
                    var_names=new_ds_w_region_var_names,
                    region=new_ds_w_region_spatial_coverage)

                self.assertIsNotNone(new_ds_w_region)

                self.assertEqual(new_ds_w_region.id, "local.%s" % new_ds_w_region_title)

                self.assertEqual(new_ds_w_region.spatial_coverage(), new_ds_w_region_spatial_coverage)
                data_set = new_ds_w_region.open_dataset()
                new_ds_w_region_var_names.extend(['lat', 'lon', 'time'])

                self.assertSetEqual(set(data_set.variables), set(new_ds_w_region_var_names))

                new_ds_w_region_title = 'from_local_to_local_region_two_var_sm_uncertainty'
                new_ds_w_region_time_range = TimeRangeLike.convert((datetime(1995, 11, 14, 0, 0),
                                                                    datetime(1995, 11, 16, 23, 59)))
                new_ds_w_region_var_names = VarNamesLike.convert(['sm', 'sm_uncertainty'])
                new_ds_w_region_spatial_coverage = PolygonLike.convert("10,20,30,40")

                new_ds_w_region = soilmoisture_data_source.make_local(
                    new_ds_w_region_title,
                    time_range=new_ds_w_region_time_range,
                    var_names=new_ds_w_region_var_names,
                    region=new_ds_w_region_spatial_coverage)

                self.assertIsNotNone(new_ds_w_region)

                self.assertEqual(new_ds_w_region.id, "local.%s" % new_ds_w_region_title)

                self.assertEqual(new_ds_w_region.spatial_coverage(), new_ds_w_region_spatial_coverage)
                data_set = new_ds_w_region.open_dataset()
                new_ds_w_region_var_names.extend(['lat', 'lon', 'time'])

                self.assertSetEqual(set(data_set.variables), set(new_ds_w_region_var_names))

                empty_ds_timerange = (datetime(1917, 12, 1, 0, 0), datetime(1917, 12, 31, 23, 59))
                with self.assertRaises(DataAccessError) as cm:
                    soilmoisture_data_source.make_local('empty_ds', time_range=empty_ds_timerange)
                self.assertEqual(f'Data source "{soilmoisture_data_source.id}" does not'
                                 f' seem to have any datasets in given'
                                 f' time range {TimeRangeLike.format(empty_ds_timerange)}',
                                 str(cm.exception))

                new_ds_time_range = TimeRangeLike.convert((datetime(1995, 11, 14, 0, 0),
                                                           datetime(1995, 11, 14, 23, 59)))

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
        self.assertEqual('esacci2.OC.day.L3S.CHLOR_A.multi-sensor.multi-platform.MERGED.3-1.sinusoidal',
                         self.first_oc_data_source.id)

    def test_schema(self):
        self.assertEqual(self.first_oc_data_source.schema,
                         None)

    def test_temporal_coverage(self):
        self.assertEqual(self.first_oc_data_source.temporal_coverage(),
                         (datetime(1997, 9, 3, 23, 0, 0), datetime(2016, 12, 31, 23, 59, 59)))

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

    @unittest.skip(reason='Requires variable access which is not integrated yet.')
    def test_make_local_spatial(self):
        data_store = EsaCciOdpDataStore()
        data_source = data_store.query(ds_id='esacci.SOILMOISTURE.day.L3S.SSMV.multi-sensor.multi-platform.COMBINED.04.5.r1')[0]
        # The following always worked fine:
        ds = data_source.open_dataset(time_range=['2004-01-01', '2004-01-14'], region='-10,40,20,70')
        self.assertIsNotNone(ds)
        # The following reproduced Cate issues #823, #822, #818, #816, #783:
        ds = data_source.make_local('local_name', time_range=['2004-01-01', '2004-01-14'], region='-10,40,20,70')
        self.assertIsNotNone(ds)
