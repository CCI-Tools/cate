# The MIT License (MIT)
# Copyright (c) 2016, 2017 by the ESA CCI Toolbox development team and contributors
#
# Permission is hereby granted, free of charge, to any person obtaining a copy of
# this software and associated documentation files (the "Software"), to deal in
# the Software without restriction, including without limitation the rights to
# use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies
# of the Software, and to permit persons to whom the Software is furnished to do
# so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

"""
Description
===========

This plugin module adds the ESA CCI Open Data Portal's (ODP) OpenSearch service to
the data store registry.

Verification
============

The module's unit-tests are located in
`test/ds/test_esa_cci_odp_os.py <https://github.com/CCI-Tools/cate/blob/master/test/ds/test_esa_cci_odp_os.py>`_
and may be executed using
``$ py.test test/ds/test_esa_cci_odp.py --cov=cate/ds/esa_cci_odp_os.py`` for extra code coverage information.

Components
==========
"""
import json
import logging
import os
import re
import socket
import ssl
import urllib.parse
import urllib.request
from collections import OrderedDict
from datetime import datetime, timedelta
import lxml.etree as etree
from typing import Sequence, Tuple, Optional, Any, Dict, List, Union
from urllib.error import URLError, HTTPError

import pandas as pd
import xarray as xr

from cate.conf import get_config_value, get_data_stores_path
from cate.conf.defaults import NETCDF_COMPRESSION_LEVEL
from cate.core.ds import DATA_STORE_REGISTRY, DataAccessError, NetworkError, DataStore, DataSource, Schema, \
    open_xarray_dataset, DataStoreNotice
from cate.core.opimpl import subset_spatial_impl, normalize_impl, adjust_spatial_attrs_impl
from cate.core.types import PolygonLike, TimeLike, TimeRange, TimeRangeLike, VarNamesLike
from cate.ds.local import add_to_data_store_registry, LocalDataSource, LocalDataStore
from cate.util.monitor import Cancellation, Monitor

ESA_CCI_ODP_DATA_STORE_ID = 'esa_cci_odp_os'

__author__ = "Norman Fomferra (Brockmann Consult GmbH), " \
             "Tonio Fincke (Brockmann Consult GmbH), " \
             "Marco Zühlke (Brockmann Consult GmbH), " \
             "Chris Bernat (Telespazio VEGA UK Ltd), " \
             "Paolo Pesciullesi (Telespazio VEGA UK Ltd)"

_OPENSEARCH_CEDA_URL = "http://opensearch-test.ceda.ac.uk/opensearch/request"

ODD_NS = {'os': 'http://a9.com/-/spec/opensearch/1.1/',
          'param': 'http://a9.com/-/spec/opensearch/extensions/parameters/1.0/'}
DESC_NS = {'gmd': 'http://www.isotc211.org/2005/gmd',
           'gml': 'http://www.opengis.net/gml/3.2',
           'gco': 'http://www.isotc211.org/2005/gco',
           'gmx': 'http://www.isotc211.org/2005/gmx',
           'xlink': 'http://www.w3.org/1999/xlink'
           }

_TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S"

_RE_TO_DATETIME_FORMATS = patterns = [(re.compile(14 * '\\d'), '%Y%m%d%H%M%S'),
                                      (re.compile(12 * '\\d'), '%Y%m%d%H%M'),
                                      (re.compile(8 * '\\d'), '%Y%m%d'),
                                      (re.compile(6 * '\\d'), '%Y%m'),
                                      (re.compile(4 * '\\d'), '%Y')]

# days = 0, seconds = 0, microseconds = 0, milliseconds = 0, minutes = 0, hours = 0, weeks = 0
_TIME_FREQUENCY_TO_TIME_DELTA = dict([
    ('second', timedelta(seconds=1)),
    ('day', timedelta(days=1)),
    ('8-days', timedelta(days=8)),
    ('mon', timedelta(weeks=4)),
    ('yr', timedelta(days=365)),
])

_ODP_PROTOCOL_HTTP = 'Download'
_ODP_PROTOCOL_OPENDAP = 'Opendap'

_TIMEOUT = 10
_MAX_RESULTS = 1000

_LOG = logging.getLogger('cate')

# by default there is no timeout
socket.setdefaulttimeout(90)


def get_data_store_path():
    return os.environ.get('CATE_LOCAL_DATA_STORE_PATH',
                          os.path.join(get_data_stores_path(), 'local'))


def get_metadata_store_path():
    return os.environ.get('CATE_ESA_CCI_ODP_DATA_STORE_PATH',
                          os.path.join(get_data_stores_path(), ESA_CCI_ODP_DATA_STORE_ID))


def add_data_store():
    """
    Defines the ESA CCI ODP data store and makes it the default data store.

    All data sources of the FTP data store are read from a JSON file ``esa_cci_ftp.json`` contained in this package.
    This JSON file has been generated from a scan of the entire FTP tree.
    """
    DATA_STORE_REGISTRY.add_data_store(EsaCciOdpOsDataStore())


def find_datetime_format(filename: str) -> Tuple[Optional[str], int, int]:
    for regex, time_format in _RE_TO_DATETIME_FORMATS:
        searcher = regex.search(filename)
        if searcher:
            p1, p2 = searcher.span()
            return time_format, p1, p2
    return None, -1, -1


def get_exclude_variables_fix_known_issues(ds_id: str) -> [str]:
    """
    This method applies fixes to the parameters of a 'make_local' invocation.
    """
    if ds_id:
        # the 't0' variable in these SOILMOISTURE data sources
        # can not be decoded by xarray and lead to an unusable dataset
        # see: https://github.com/CCI-Tools/cate/issues/326
        soil_moisture_datasets = [
            'esacci.SOILMOISTURE.day.L3S.SSMS.multi-sensor.multi-platform.ACTIVE.03-2.r1',
            'esacci.SOILMOISTURE.day.L3S.SSMV.multi-sensor.multi-platform.COMBINED.03-2.r1',
            'esacci.SOILMOISTURE.day.L3S.SSMV.multi-sensor.multi-platform.PASSIVE.03-2.r1'
        ]
        if ds_id in soil_moisture_datasets:
            return [{'name': 't0', 'comment': "can not be decoded by xarray and lead to an unusable dataset\n"
                                              "see: https://github.com/CCI-Tools/cate/issues/326"}]
        return []


def _extract_metadata_from_odd_url(odd_url: str = None) -> dict:
    if not odd_url:
        return {}
    with urllib.request.urlopen(odd_url) as response:
        return _extract_metadata_from_odd(etree.XML(response.read()))


def _extract_metadata_from_odd(odd_xml: etree.XML) -> dict:
    metadata = {}
    metadata_names = {'ecv': ['ecv', 'ecvs'], 'frequency': ['time_frequency', 'time_frequencies'],
                      'institute': ['institute', 'institutes'],
                      'processingLevel': ['processing_level', 'processing_levels'],
                      'productString': ['product_string', 'product_strings'],
                      'productVersion': ['product_version', 'product_versions'],
                      'dataType': ['data_type', 'data_types'], 'sensor': ['sensor_id', 'sensor_ids'],
                      'platform': ['platform_id', 'platform_ids'], 'fileFormat': ['file_format', 'file_formats']}
    for param_elem in odd_xml.findall('os:Url/param:Parameter', namespaces=ODD_NS):
        if param_elem.attrib['name'] in metadata_names:
            param_content = _get_from_param_elem(param_elem)
            if param_content:
                if type(param_content) == str:
                    metadata[metadata_names[param_elem.attrib['name']][0]] = param_content
                else:
                    metadata[metadata_names[param_elem.attrib['name']][1]] = param_content
    return metadata


def _get_from_param_elem(param_elem: etree.Element) -> Optional[Union[str, List[str]]]:
    options = param_elem.findall('param:Option', namespaces=ODD_NS)
    if not options:
        return None
    if len(options) == 1:
        return options[0].get('value')
    return [option.get('value') for option in options]


def _extract_metadata_from_descxml_url(descxml_url: str = None) -> dict:
    if not descxml_url:
        return {}
    with urllib.request.urlopen(descxml_url) as response:
        try:
            return _extract_metadata_from_descxml(etree.XML(response.read()))
        except etree.ParseError:
            _LOG.info(f'Cannot read metadata from {descxml_url} due to parsing error.')
            return {}


def _extract_metadata_from_descxml(descxml: etree.XML) -> dict:
    metadata = {}
    metadata_elems = {
        'abstract': 'gmd:identificationInfo/gmd:MD_DataIdentification/gmd:abstract/gco:CharacterString',
        'title': 'gmd:identificationInfo/gmd:MD_DataIdentification/gmd:citation/gmd:CI_Citation/gmd:title/'
                 'gco:CharacterString',
        'licences': 'gmd:identificationInfo/gmd:MD_DataIdentification/gmd:resourceConstraints/gmd:MD_Constraints/'
                    'gmd:useLimitation/gco:CharacterString',
        'bbox_minx': 'gmd:identificationInfo/gmd:MD_DataIdentification/gmd:extent/gmd:EX_Extent/'
                     'gmd:geographicElement/gmd:EX_GeographicBoundingBox/gmd:westBoundLongitude/gco:Decimal',
        'bbox_miny': 'gmd:identificationInfo/gmd:MD_DataIdentification/gmd:extent/gmd:EX_Extent/'
                     'gmd:geographicElement/gmd:EX_GeographicBoundingBox/gmd:southBoundLatitude/gco:Decimal',
        'bbox_maxx': 'gmd:identificationInfo/gmd:MD_DataIdentification/gmd:extent/gmd:EX_Extent/'
                     'gmd:geographicElement/gmd:EX_GeographicBoundingBox/gmd:eastBoundLongitude/gco:Decimal',
        'bbox_maxy': 'gmd:identificationInfo/gmd:MD_DataIdentification/gmd:extent/gmd:EX_Extent/'
                     'gmd:geographicElement/gmd:EX_GeographicBoundingBox/gmd:northBoundLatitude/gco:Decimal',
        'temporal_coverage_start': 'gmd:identificationInfo/gmd:MD_DataIdentification/gmd:extent/gmd:EX_Extent/'
                                   'gmd:temporalElement/gmd:EX_TemporalExtent/gmd:extent/gml:TimePeriod/'
                                   'gml:beginPosition',
        'temporal_coverage_end': 'gmd:identificationInfo/gmd:MD_DataIdentification/gmd:extent/gmd:EX_Extent/'
                                 'gmd:temporalElement/gmd:EX_TemporalExtent/gmd:extent/gml:TimePeriod/gml:endPosition'
    }
    for identifier in metadata_elems:
        content = _get_element_content(descxml, metadata_elems[identifier])
        if content:
            metadata[identifier] = content
    metadata_elems_with_replacement = {'file_formats': [
        'gmd:identificationInfo/gmd:MD_DataIdentification/gmd:resourceFormat/gmd:MD_Format/gmd:name/'
        'gco:CharacterString', 'Data are in NetCDF format', '.nc']
    }
    for metadata_elem in metadata_elems_with_replacement:
        content = _get_replaced_content_from_descxml_elem(descxml, metadata_elems_with_replacement[metadata_elem])
        if content:
            metadata[metadata_elem] = content
    metadata_linked_elems = {
        'publication_date': ['gmd:identificationInfo/gmd:MD_DataIdentification/gmd:citation/'
                             'gmd:CI_Citation/gmd:date/gmd:CI_Date/gmd:dateType/gmd:CI_DateTypeCode', 'publication',
                             '../../gmd:date/gco:DateTime'],
        'creation_date': ['gmd:identificationInfo/gmd:MD_DataIdentification/gmd:citation/'
                          'gmd:CI_Citation/gmd:date/gmd:CI_Date/gmd:dateType/gmd:CI_DateTypeCode', 'creation',
                          '../../gmd:date/gco:DateTime']
    }
    for identifier in metadata_linked_elems:
        content = _get_linked_content_from_descxml_elem(descxml, metadata_linked_elems[identifier])
        if content:
            metadata[identifier] = content

    anchor_elem_path = 'gmd:identificationInfo/gmd:MD_DataIdentification/' \
                       'gmd:descriptiveKeywords/gmd:MD_Keywords/gmd:keyword/gmx:Anchor'
    anchor_collect_elems = {
        'http://vocab-test.ceda.ac.uk/collection/cci/platform/': ['platform_id', 'platform_ids'],
        'http://vocab-test.ceda.ac.uk/collection/cci/sensor/': ['sensor_id', 'sensor_ids'],
        'http://vocab-test.ceda.ac.uk/collection/cci/procLev/': ['processing_level', 'processing_levels'],
        'http://vocab-test.ceda.ac.uk/collection/cci/freq/': ['time_frequency', 'time_frequencies']
    }
    metadata_collect = {}
    for anchor_collect_elem in anchor_collect_elems:
        metadata_collect[anchor_collect_elem] = []
    anchor_elems = descxml.findall(anchor_elem_path, namespaces=DESC_NS)
    for anchor_elem in anchor_elems:
        href_attrib = anchor_elem.xpath('@xlink:href', namespaces=DESC_NS)
        if href_attrib:
            for identifier in anchor_collect_elems:
                if str(href_attrib[0]).startswith(identifier):
                    metadata_collect[identifier].append(anchor_elem.text)
    for identifier in metadata_collect:
        if len(metadata_collect[identifier]) == 1:
            metadata[anchor_collect_elems[identifier][0]] = metadata_collect[identifier][0]
        elif len(metadata_collect[identifier]) > 0:
            metadata[anchor_collect_elems[identifier][1]] = metadata_collect[identifier]
    return metadata


def _get_element_content(descxml: etree.XML, path: str) -> Optional[Union[str, List[str]]]:
    elems = descxml.findall(path, namespaces=DESC_NS)
    if not elems:
        return None
    if len(elems) == 1:
        return elems[0].text
    return [elem.text for elem in elems]


def _get_replaced_content_from_descxml_elem(descxml: etree.XML, paths: List[str]) -> Optional[str]:
    descxml_elem = descxml.find(paths[0], namespaces=DESC_NS)
    if descxml_elem is None:
        return None
    if descxml_elem.text == paths[1]:
        return paths[2]


def _get_linked_content_from_descxml_elem(descxml: etree.XML, paths: List[str]) -> Optional[str]:
    descxml_elems = descxml.findall(paths[0], namespaces=DESC_NS)
    if descxml is None:
        return None
    for descxml_elem in descxml_elems:
        if descxml_elem.text == paths[1]:
            return _get_element_content(descxml_elem, paths[2])


def _fetch_feature_at(base_url, query_args, index, timeout=10) -> Optional[Dict]:
    paging_query_args = dict(query_args or {})
    paging_query_args.update(startPage=index, maximumRecords=1, httpAccept='application/geo+json')
    url = base_url + '?' + urllib.parse.urlencode(paging_query_args)
    error_message = f"Failed accessing CCI OpenSearch service {base_url}"
    try:
        context = ssl._create_unverified_context()
        with urllib.request.urlopen(url, timeout=timeout, context=context) as response:
            json_text = response.read()
            json_dict = json.loads(json_text.decode('utf-8'))
            feature_list = json_dict.get("features", [])
            if len(feature_list) > 0:
                return feature_list[0]
    except urllib.error.HTTPError as e:
        _LOG.warning(f"{error_message}: {e}")
    except (urllib.error.URLError, socket.timeout) as e:
        _LOG.warning(f"{error_message}: {e}")
    except OSError as e:
        _LOG.warning(f"{error_message}: {e}")
    return None


def _fetch_opensearch_feature_list(base_url, query_args, start_page=1, maximum_records=1000, timeout=10,
                                   monitor: Monitor = Monitor.NONE) -> List:
    """
    Return JSON value read from Opensearch web service.
    :return:
    """
    full_feature_list = []
    with monitor.starting("Loading", 10):
        while True:
            monitor.progress(work=1)
            paging_query_args = dict(query_args or {})
            paging_query_args.update(startPage=start_page, maximumRecords=maximum_records,
                                     httpAccept='application/geo+json')
            url = base_url + '?' + urllib.parse.urlencode(paging_query_args)
            try:
                # noinspection PyProtectedMember
                context = ssl._create_unverified_context()
                with urllib.request.urlopen(url, timeout=timeout, context=context) as response:
                    json_text = response.read()
                    json_dict = json.loads(json_text.decode('utf-8'))
                    feature_list = json_dict.get("features", [])
                    full_feature_list.extend(feature_list)
                    if len(feature_list) < maximum_records:
                        break
                    start_page += 1
            except urllib.error.HTTPError as e:
                _LOG.warning(f'Could not extract features from page {start_page} of {query_args["parentIdentifier"]}')
            except (urllib.error.URLError, socket.timeout) as e:
                _LOG.warning(f'Could not extract features from page {start_page} of {query_args["parentIdentifier"]}')
            except OSError as e:
                _LOG.warning(f'Could not extract features from page {start_page} of {query_args["parentIdentifier"]}')
    return full_feature_list


def _harmonize_info_field_names(catalogue: dict, single_field_name: str, multiple_fields_name: str,
                                multiple_items_name: Optional[str] = None):
    if single_field_name in catalogue and multiple_fields_name in catalogue:
        if len(multiple_fields_name) == 0:
            catalogue.pop(multiple_fields_name)
        elif len(catalogue[multiple_fields_name]) == 1:
            if catalogue[multiple_fields_name][0] is catalogue[single_field_name]:
                catalogue.pop(multiple_fields_name)
            else:
                catalogue[multiple_fields_name].append(catalogue[single_field_name])
                catalogue.pop(single_field_name)
        else:
            if catalogue[single_field_name] not in catalogue[multiple_fields_name] \
                    and (multiple_items_name is None or catalogue[single_field_name] != multiple_items_name):
                catalogue[multiple_fields_name].append(catalogue[single_field_name])
            catalogue.pop(single_field_name)


def _load_or_fetch_json(fetch_json_function,
                        fetch_json_args: list = None,
                        fetch_json_kwargs: dict = None,
                        cache_used: bool = False,
                        cache_dir: str = None,
                        cache_json_filename: str = None,
                        cache_timestamp_filename: str = None,
                        cache_expiration_days: float = 1.0) -> Sequence:
    """
    Return (JSON) value of fetch_json_function or return value of a cached JSON file.
    """
    json_obj = None
    cache_json_file = None

    if cache_used:
        if cache_dir is None:
            raise ValueError('if cache_used argument is True, cache_dir argument must not be None')
        if cache_json_filename is None:
            raise ValueError('if cache_used argument is True, cache_json_filename argument must not be None')
        if cache_timestamp_filename is None:
            raise ValueError('if cache_used argument is True, cache_timestamp_filename argument must not be None')
        if cache_expiration_days is None:
            raise ValueError('if cache_used argument is True, cache_expiration_days argument must not be None')

        cache_json_file = os.path.join(cache_dir, cache_json_filename)
        cache_timestamp_file = os.path.join(cache_dir, cache_timestamp_filename)

        timestamp = datetime(year=2000, month=1, day=1)
        if os.path.exists(cache_timestamp_file):
            with open(cache_timestamp_file) as fp:
                timestamp_text = fp.read()
                timestamp = datetime.strptime(timestamp_text, _TIMESTAMP_FORMAT)

        time_diff = datetime.now() - timestamp
        time_diff_days = time_diff.days + time_diff.seconds / 3600. / 24.
        if time_diff_days < cache_expiration_days:
            if os.path.exists(cache_json_file):
                with open(cache_json_file) as fp:
                    json_text = fp.read()
                    json_obj = json.loads(json_text)

    if json_obj is None:
        # noinspection PyArgumentList
        try:
            # noinspection PyArgumentList
            json_obj = fetch_json_function(*(fetch_json_args or []), **(fetch_json_kwargs or {}))
            if cache_used:
                os.makedirs(cache_dir, exist_ok=True)
                # noinspection PyUnboundLocalVariable
                with open(cache_json_file, 'w') as fp:
                    fp.write(json.dumps(json_obj, indent='  '))
                # noinspection PyUnboundLocalVariable
                with open(cache_timestamp_file, 'w') as fp:
                    fp.write(datetime.utcnow().strftime(_TIMESTAMP_FORMAT))
        except Exception as e:
            if cache_json_file and os.path.exists(cache_json_file):
                with open(cache_json_file) as fp:
                    json_text = fp.read()
                    json_obj = json.loads(json_text)
            else:
                raise e

    return json_obj


def _fetch_data_source_list_json(base_url, query_args, monitor: Monitor = Monitor.NONE) -> Sequence:
    feature_collection_list = _fetch_opensearch_feature_list(base_url, query_args, monitor=monitor)
    catalogue = {}
    for fc in feature_collection_list:
        fc_props = fc.get("properties", {})
        fc_id = f'esacci.{fc_props.get("identifier", None)}'
        if not fc_id:
            continue
        catalogue[fc_id] = {}
        catalogue[fc_id]['title'] = fc_props.get("title", "")
        fc_props_links = fc_props.get("links", None)
        if fc_props_links:
            search = fc_props_links.get("search", None)
            if search:
                odd_url = search[0].get('href', None)
                if odd_url:
                    catalogue[fc_id]['odd_url'] = odd_url
                    odd_metadata = _extract_metadata_from_odd_url(odd_url)
                    catalogue[fc_id].update(odd_metadata)
            described_by = fc_props_links.get("describedby", None)
            if described_by:
                metadata_url = described_by[0].get("href", None)
                if metadata_url:
                    catalogue[fc_id]['metadata_url'] = metadata_url
                    desc_metadata = _extract_metadata_from_descxml_url(metadata_url)
                    for item in desc_metadata:
                        if item in catalogue[fc_id] and type(catalogue[fc_id]) == list:
                            catalogue[fc_id][item].extend(desc_metadata[item])
                            catalogue[fc_id][item] = list(dict.fromkeys(catalogue[fc_id][item]))
                        else:
                            catalogue[fc_id][item] = desc_metadata[item]
            index = 1
            catalogue[fc_id]['variables'] = []
            catalogue[fc_id]['dimensions'] = {}
            catalogue[fc_id]['variable_infos'] = {}
            feature = None
            while len(catalogue[fc_id]['variables']) == 0 and feature is None:
                feature = _fetch_feature_at(base_url, dict(parentIdentifier=fc_id), index)
                if feature is None:
                    break
                index += 1
                feature_variables = _get_variables_from_feature(feature)
                feature_dimensions, feature_variable_infos = _get_infos_from_feature(feature)
                if len(feature_variables) > 0 and len(feature_variable_infos) > 0:
                    catalogue[fc_id]['variables'] = feature_variables
                    catalogue[fc_id]['dimensions'] = feature_dimensions
                    catalogue[fc_id]['variable_infos'] = feature_variable_infos
            _harmonize_info_field_names(catalogue[fc_id], 'file_format', 'file_formats')
            _harmonize_info_field_names(catalogue[fc_id], 'platform_id', 'platform_ids', 'multi-platform')
            _harmonize_info_field_names(catalogue[fc_id], 'sensor_id', 'sensor_ids', 'multi-sensor')
            _harmonize_info_field_names(catalogue[fc_id], 'processing_level', 'processing_levels')
            _harmonize_info_field_names(catalogue[fc_id], 'time_frequency', 'time_frequencies')
        _LOG.info(f'Added data source {fc_id}')
    return catalogue


def _get_variables_from_feature(feature: dict) -> List:
    feature_props = feature.get("properties", {})
    variables = feature_props.get("variables", [])
    variable_dicts = []
    for variable in variables:
        variable_dict = {
            'name': variable.get("var_id", None),
            'units': variable.get("units", ""),
            'long_name': variable.get("long_name", None)}
        variable_dicts.append(variable_dict)
    return variable_dicts


def _get_infos_from_feature(feature: dict) -> tuple:
    feature_info = _extract_feature_info(feature)
    opendap_dds_url = f"{feature_info[4]['Opendap']}.dds"
    try:
        with urllib.request.urlopen(opendap_dds_url) as response:
            return _retrieve_infos_from_dds(response.readlines())
    except urllib.error.HTTPError as e:
        _LOG.warning(f'Could not access {opendap_dds_url}: {e}.')
    except (urllib.error.URLError, socket.timeout) as e:
        _LOG.warning(f'Could not access {opendap_dds_url}: {e}.')
    except OSError as e:
        _LOG.warning(f'Could not access {opendap_dds_url}: {e}.')
    return {}, {}


def _retrieve_infos_from_dds(dds_lines: List) -> tuple:
    dimensions = {}
    variable_infos = {}
    dim_info_pattern = '[a-zA-Z0-9_]+ [a-zA-Z0-9_]+[\[\w* = \d{1,7}\]]*;'
    type_and_name_pattern = '[a-zA-Z0-9_]+'
    dimension_pattern = '\[[a-zA-Z]* = \d{1,7}\]'
    for dds_line in dds_lines:
        if type(dds_line) is bytes:
            dds_line = str(dds_line, 'utf-8')
        dim_info_search_res = re.search(dim_info_pattern, dds_line)
        if dim_info_search_res is None:
            continue
        type_and_name = re.findall(type_and_name_pattern, dim_info_search_res.string)
        if type_and_name[1] not in variable_infos:
            dimension_names = []
            variable_dimensions = re.findall(dimension_pattern, dim_info_search_res.string)
            for variable_dimension in variable_dimensions:
                dimension_name, dimension_size = variable_dimension[1:-1].split(' = ')
                dimension_names.append(dimension_name)
                if dimension_name not in dimensions:
                    dimensions[dimension_name] = int(dimension_size)
            variable_infos[type_and_name[1]] = {'data_type': type_and_name[0], 'dimensions': dimension_names}
    return dimensions, variable_infos


def _fetch_file_list_json(dataset_id: str, monitor: Monitor = Monitor.NONE) -> Sequence:
    feature_list = _fetch_opensearch_feature_list(_OPENSEARCH_CEDA_URL,
                                                  dict(parentIdentifier=dataset_id),
                                                  monitor=monitor)
    file_list = []
    for feature in feature_list:
        feature_info = _extract_feature_info(feature)
        if feature_info[0] in file_list:
            raise ValueError('filename {} already seen in dataset {}'.format(feature_info[0], dataset_id))
        file_list.append(feature_info)

    def pick_start_time(file_info_rec):
        return file_info_rec[1] if file_info_rec[1] else datetime.max

    return sorted(file_list, key=pick_start_time)


def _extract_feature_info(feature: dict) -> List:
    feature_props = feature.get("properties", {})
    filename = feature_props.get("title", "")
    date = feature_props.get("date", None)
    start_time = ""
    end_time = ""
    if date and "/" in date:
        start_time, end_time = date.split("/")
    elif filename:
        time_format, p1, p2 = find_datetime_format(filename)
        if time_format:
            start_time = datetime.strptime(filename[p1:p2], time_format)
            # Convert back to text, so we can JSON-encode it
            start_time = datetime.strftime(start_time, _TIMESTAMP_FORMAT)
            end_time = start_time
    file_size = feature_props.get("filesize", 0)
    related_links = feature_props.get("links", {}).get("related", [])
    urls = {}
    for related_link in related_links:
        urls[related_link.get("title")] = related_link.get("href")
    return [filename, start_time, end_time, file_size, urls]


class EsaCciOdpOsDataStore(DataStore):
    # noinspection PyShadowingBuiltins
    def __init__(self,
                 id: str = 'esa_cci_odp_os',
                 title: str = 'ESA CCI Open Data Portal OpenSearch',
                 index_cache_used: bool = True,
                 index_cache_expiration_days: float = 1.0,
                 index_cache_json_dict: dict = None,
                 index_cache_update_tag: str = None):
        super().__init__(id, title=title, is_local=False)
        self._index_cache_used = index_cache_used
        self._index_cache_expiration_days = index_cache_expiration_days
        self._esgf_data = index_cache_json_dict
        self._index_cache_update_tag = index_cache_update_tag
        self._data_sources = []
        self._catalogue = None

        self._csw_data = None

    @property
    def description(self) -> Optional[str]:
        """
        Return a human-readable description for this data store as plain text.

        The text may use Markdown formatting.
        """
        return ("This data store represents the [ESA CCI Open Data Portal](http://cci.esa.int/data)"
                " in the CCI Toolbox.\n"
                "It currently provides all CCI data that are published through an Opensearch Interface. "
                "The store will be extended shortly to also provide TIFF and Shapefile Data, see usage "
                "notes.\n"
                "Remote data downloaded to your computer is made available through the *Local Data Store*.")

    @property
    def notices(self) -> Optional[List[DataStoreNotice]]:
        """
        Return an optional list of notices for this data store that can be used to inform users about the
        conventions, standards, and data extent used in this data store or upcoming service outages.
        """
        return [
            DataStoreNotice("terminologyClarification",
                            "Terminology Clarification",
                            "The ESA CCI Open Data Portal (ODP) utilises an "
                            "[ontology](http://vocab-test.ceda.ac.uk/ontology/cci/cci-content/index.html) whose terms "
                            "might slightly differ from the ones used in this software."
                            "\n"
                            "For example, a *Dataset* in the CCI terminology may refer to all data products "
                            "generated by a certain CCI project using a specific configuration of algorithms "
                            "and auxiliary data."
                            "\n"
                            "In this software, a *Data Source* refers to a subset (a file set) "
                            "of a given ODP dataset whose data share a common spatio-temporal grid and/or share "
                            "other common properties, e.g. the instrument used for the original measurements."
                            "\n"
                            "In addition, Cate uses the term *Dataset* to represent in-memory "
                            "instances of gridded data sources or subsets of them.",
                            intent="primary",
                            icon="info-sign"),
            DataStoreNotice("dataCompleteness",
                            "Data Completeness",
                            "This data store currently provides **only a subset of all datasets** provided by the "
                            "ESA CCI Open Data Portal (ODP), namely gridded datasets originally stored in NetCDF "
                            "format."
                            "\n"
                            "In upcoming versions of Cate, the ODP data store will also allow for browsing "
                            "and accessing the remaining ODP datasets. This includes gridded data in TIFF format and "
                            "also vector data using ESRI Shapefile format."
                            "\n"
                            "For time being users can download the missing vector data from the "
                            "[ODP FTP server](http://cci.esa.int/data#ftp) `ftp://anon-ftp.ceda.ac.uk/neodc/esacci/` "
                            "and then use operation `read_geo_data_frame()` in Cate to read the "
                            "downloaded Shapefiles:"
                            "\n"
                            "* CCI Glaciers in FTP directory `glaciers`\n"
                            "* CCI Ice Sheets in FTP directories `ice_sheets_antarctica` and `ice_sheets_greenland`\n",
                            intent="warning",
                            icon="warning-sign"),
        ]

    @property
    def index_cache_used(self):
        return self._index_cache_used

    @property
    def index_cache_expiration_days(self):
        return self._index_cache_expiration_days

    @property
    def data_store_path(self) -> str:
        return get_metadata_store_path()

    def query(self, ds_id: str = None, query_expr: str = None, monitor: Monitor = Monitor.NONE) \
            -> Sequence['DataSource']:
        self._init_data_sources()
        if ds_id or query_expr:
            return [ds for ds in self._data_sources if ds.matches(ds_id=ds_id, query_expr=query_expr)]
        return self._data_sources

    def get_updates(self, reset=False) -> Dict:
        """
        Ask to retrieve the differences found between a previous
        dataStore status and the current one,
        The implementation return a dictionary with the new ['new'] and removed ['del'] dataset.
        it also return the reference time to the datastore status taken as previous snapshot,
        Reset flag is used to clean up the support files, freeze and diff.
        :param: reset=False. Set this flag to true to clean up all the support files forcing a
                synchronization with the remote catalog
        :return: A dictionary with keys { 'generated', 'source_ref_time', 'new', 'del' }.
                 genetated: generation time, when the check has been executed
                 source_ref_time: when the local copy of the remoted dataset hes been made.
                                  It is also used by the system to refresh the current images when
                                  is older then 1 day.
                 new: a list of new dataset entry
                 del: a list of removed dataset
        """
        diff_file = os.path.join(get_metadata_store_path(), self._get_update_tag() + '-diff.json')

        if os.path.isfile(diff_file):
            with open(diff_file, 'r') as json_in:
                report = json.load(json_in)
        else:
            generated = datetime.now()
            report = {"generated": str(generated),
                      "source_ref_time": str(generated),
                      "new": list(),
                      "del": list()}

            # clean up when requested
        if reset:
            if os.path.isfile(diff_file):
                os.remove(diff_file)
            frozen_file = os.path.join(get_metadata_store_path(), self._get_update_tag() + '-freeze.json')
            if os.path.isfile(frozen_file):
                os.remove(frozen_file)
        return report

    def _repr_html_(self) -> str:
        self._init_data_sources()
        rows = []
        row_count = 0
        for data_source in self._data_sources:
            row_count += 1
            # noinspection PyProtectedMember
            rows.append('<tr><td><strong>%s</strong></td><td>%s</td></tr>' % (row_count, data_source._repr_html_()))
        return '<p>Contents of FileSetFileStore</p><table>%s</table>' % ('\n'.join(rows))

    def __repr__(self) -> str:
        return "EsaCciOdpDataStore (%s)" % self.id

    def _init_data_sources(self):
        os.makedirs(get_metadata_store_path(), exist_ok=True)
        if self._data_sources:
            return
        if self._catalogue is None:
            self._load_index()
        data_sources = []
        if self._catalogue:
            for catalogue_item in self._catalogue:
                data_sources.append(EsaCciOdpOsDataSource(self, self._catalogue[catalogue_item], catalogue_item))
        self._data_sources = data_sources
        self._check_source_diff()
        self._freeze_source()

    def _get_update_tag(self):
        """
        return the name to be used and TAG to check and freee the dataset list
        A datastore could be created with a json file so to avoid collision with a
        previous frozen DS the user can set a TAG
        :return:
        """
        if self._index_cache_update_tag:
            return self._index_cache_update_tag
        return 'dataset-list'

    def _check_source_diff(self):
        """
        This routine is responsible to find differences (new dataset or removed dataset)
        between the most updated list and the provious frozen one.
        It generate a file dataset-list-diff.json with the results.
        generated time: time of report generation
        source_ref_time : time of the frozen (previous) dataset list
        new: list of item founded as new
        del: list of items found as removed

        It use a persistent output to keep trace of the previous changes in case the
        frozen dataset is updated too.
        :return:
        """
        frozen_file = os.path.join(get_metadata_store_path(), self._get_update_tag() + '-freeze.json')
        diff_file = os.path.join(get_metadata_store_path(), self._get_update_tag() + '-diff.json')
        deleted = []
        added = []

        if os.path.isfile(frozen_file):
            with open(frozen_file, 'r') as json_in:
                frozen_source = json.load(json_in)

            ds_new = set([ds.id for ds in self._data_sources])
            ds_old = set([ds for ds in frozen_source['data']])
            for ds in (ds_old - ds_new):
                deleted.append(ds)

            for ds in (ds_new - ds_old):
                added.append(ds)

            if deleted or added:
                generated = datetime.now()
                diff_source = {'generated': str(generated),
                               'source_ref_time': frozen_source['source_ref_time'],
                               'new': added,
                               'del': deleted}
                with open(diff_file, 'w+') as json_out:
                    json.dump(diff_source, json_out)

    def _freeze_source(self):
        """
        Freeze a dataset list when needed.
        The file generated by this method is a snapshop of the dataset list with an expiration time of one day.
        It is updated to the current when is expired, the file is used to compare the
        previous dataset status with the current in order to find differences.

        the frozen file is saved in the data store meta directory with the nane
        'dataset-list-freeze.json'
        :return:
        """
        frozen_file = os.path.join(get_metadata_store_path(), self._get_update_tag() + '-freeze.json')
        save_it = True
        now = datetime.now()
        if os.path.isfile(frozen_file):
            with open(frozen_file, 'r') as json_in:
                freezed_source = json.load(json_in)
            source_ref_time = pd.to_datetime(freezed_source['source_ref_time'])
            save_it = (now > source_ref_time + timedelta(days=1))

        if save_it:
            data = [ds.id for ds in self._data_sources]
            freezed_source = {'source_ref_time': str(now),
                              'data': data}
            with open(frozen_file, 'w+') as json_out:
                json.dump(freezed_source, json_out)

    def _load_index(self):
        self._catalogue = _load_or_fetch_json(_fetch_data_source_list_json,
                                              fetch_json_args=[
                                                  _OPENSEARCH_CEDA_URL,
                                                  dict(parentIdentifier='cci')
                                              ],
                                              cache_used=self._index_cache_used,
                                              cache_dir=get_metadata_store_path(),
                                              cache_json_filename='dataset-list.json',
                                              cache_timestamp_filename='dataset-list-timestamp.json',
                                              cache_expiration_days=self._index_cache_expiration_days
                                              )


INFO_FIELD_NAMES = sorted(["title",
                           "abstract",
                           "licences",
                           "bbox_minx",
                           "bbox_miny",
                           "bbox_maxx",
                           "bbox_maxy",
                           "temporal_coverage_start",
                           "temporal_coverage_end",
                           "file_format",
                           "file_formats",
                           "publication_date",
                           "creation_date",
                           "platform_ids",
                           "platform_id",
                           "sensor_ids",
                           "sensor_id",
                           "processing_levels",
                           "processing_level",
                           "time_frequencies",
                           "time_frequency",
                           "ecv",
                           "institute",
                           "institutes",
                           "product_string",
                           "product_strings",
                           "product_version",
                           "product_versions",
                           "data_type",
                           "data_types"
                           ])


class EsaCciOdpOsDataSource(DataSource):
    def __init__(self,
                 data_store: EsaCciOdpOsDataStore,
                 json_dict: dict,
                 datasource_id: str,
                 schema: Schema = None):
        super(EsaCciOdpOsDataSource, self).__init__()
        self._datasource_id = datasource_id
        self._raw_id = datasource_id.split('.')[-1]
        self._data_store = data_store
        self._json_dict = json_dict
        self._schema = schema
        self._file_list = None
        self._meta_info = None
        self._temporal_coverage = None

    @property
    def id(self) -> str:
        return self._datasource_id

    @property
    def data_store(self) -> EsaCciOdpOsDataStore:
        return self._data_store

    @property
    def spatial_coverage(self) -> Optional[PolygonLike]:
        if self._json_dict \
                and self._json_dict.get('bbox_minx', None) and self._json_dict.get('bbox_miny', None) \
                and self._json_dict.get('bbox_maxx', None) and self._json_dict.get('bbox_maxy', None):
            return PolygonLike.convert([
                self._json_dict.get('bbox_minx'),
                self._json_dict.get('bbox_miny'),
                self._json_dict.get('bbox_maxx'),
                self._json_dict.get('bbox_maxy')
            ])
        return None

    def temporal_coverage(self, monitor: Monitor = Monitor.NONE) -> Optional[TimeRange]:
        if not self._temporal_coverage:
            temp_coverage_start = self._json_dict.get('temporal_coverage_start', None)
            temp_coverage_end = self._json_dict.get('temporal_coverage_end', None)
            if temp_coverage_start and temp_coverage_end:
                self._temporal_coverage = TimeRangeLike.convert("{},{}".format(temp_coverage_start, temp_coverage_end))
            else:
                self.update_file_list(monitor)
        if self._temporal_coverage:
            return self._temporal_coverage
        return None

    @property
    def variables_info(self):
        variables = []
        coordinate_variable_names = ['lat', 'lon', 'time', 'lat_bnds', 'lon_bnds', 'time_bnds', 'crs']
        for variable in self._json_dict['variables']:
            if variable['name'] not in coordinate_variable_names:
                variables.append(variable)
        return variables

    @property
    def schema(self) -> Schema:
        return self._schema

    @property
    def meta_info(self) -> OrderedDict:
        # noinspection PyBroadException
        if not self._meta_info:
            self._meta_info = OrderedDict()
            for name in INFO_FIELD_NAMES:
                value = self._json_dict.get(name, None)
                # Many values in the index JSON are one-element lists: turn them into scalars
                if isinstance(value, list) and len(value) == 1:
                    value = value[0]
                if value is not None:
                    self._meta_info[name] = value
            self._meta_info['variables'] = self.variables_info
        return self._meta_info

    @property
    def cache_info(self) -> OrderedDict:
        coverage = OrderedDict()
        selected_file_list = self._find_files(None)
        if selected_file_list:
            dataset_dir = self.local_dataset_dir()
            for filename, date_from, date_to, none, none \
                    in selected_file_list:
                if os.path.exists(os.path.join(dataset_dir, filename)):
                    if date_from in coverage.values():
                        for temp_date_from, temp_date_to in coverage.items():
                            if temp_date_to == date_from:
                                coverage[temp_date_from] = date_to
                    elif date_to in coverage.keys():
                        temp_date_to = coverage[date_to]
                        coverage.pop(date_to)
                        coverage[date_from] = temp_date_to
                    else:
                        coverage[date_from] = date_to
        return coverage

    def update_file_list(self, monitor: Monitor = Monitor.NONE) -> None:
        self._file_list = None
        self._init_file_list(monitor)

    def local_dataset_dir(self):
        return os.path.join(get_data_store_path(), self._datasource_id)

    def local_metadata_dataset_dir(self):
        return os.path.join(get_metadata_store_path(), self._datasource_id)

    def _find_files(self, time_range):
        requested_start_date, requested_end_date = time_range if time_range else (None, None)
        self._init_file_list()
        if requested_start_date or requested_end_date:
            selected_file_list = []
            for file_rec in self._file_list:
                start_time = file_rec[1]
                ok = False
                if start_time:
                    if requested_start_date and requested_end_date:
                        ok = requested_start_date <= start_time <= requested_end_date
                    elif requested_start_date:
                        ok = requested_start_date <= start_time
                    elif requested_end_date:
                        ok = start_time <= requested_end_date
                if ok:
                    selected_file_list.append(file_rec)
        else:
            selected_file_list = self._file_list
        return selected_file_list

    def open_dataset(self,
                     time_range: TimeRangeLike.TYPE = None,
                     region: PolygonLike.TYPE = None,
                     var_names: VarNamesLike.TYPE = None,
                     protocol: str = None,
                     monitor: Monitor = Monitor.NONE) -> Any:
        time_range = TimeRangeLike.convert(time_range) if time_range else None
        var_names = VarNamesLike.convert(var_names) if var_names else None

        selected_file_list = self._find_files(time_range)
        if not selected_file_list:
            raise self._empty_error(time_range)

        files = self._get_urls_list(selected_file_list, _ODP_PROTOCOL_OPENDAP)
        try:
            return open_xarray_dataset(files, region=region, var_names=var_names, monitor=monitor)
        except HTTPError as e:
            raise self._cannot_access_error(time_range, region, var_names,
                                            cause=e) from e
        except (URLError, socket.timeout) as e:
            raise self._cannot_access_error(time_range, region, var_names,
                                            cause=e, error_cls=NetworkError) from e
        except OSError as e:
            raise self._cannot_access_error(time_range, region, var_names,
                                            cause=e) from e

    @staticmethod
    def _get_urls_list(files_description_list, protocol) -> Sequence[str]:
        """
        Returns urls list extracted from reference esgf specific files description json list
        :param files_description_list:
        :param protocol:
        :return:
        """
        return [file_rec[4][protocol].replace('.html', '') for file_rec in files_description_list]

    def _make_local(self,
                    local_ds: LocalDataSource,
                    time_range: TimeRangeLike.TYPE = None,
                    region: PolygonLike.TYPE = None,
                    var_names: VarNamesLike.TYPE = None,
                    monitor: Monitor = Monitor.NONE):

        local_id = local_ds.id
        time_range = TimeRangeLike.convert(time_range)
        var_names = VarNamesLike.convert(var_names)

        excluded_variables = get_exclude_variables_fix_known_issues(self.id)

        compression_level = get_config_value('NETCDF_COMPRESSION_LEVEL', NETCDF_COMPRESSION_LEVEL)
        compression_enabled = True if compression_level > 0 else False

        do_update_of_verified_time_coverage_start_once = True
        verified_time_coverage_start = None
        verified_time_coverage_end = None

        encoding_update = dict()
        if compression_enabled:
            encoding_update.update({'zlib': True, 'complevel': compression_level})

        if region or var_names:
            protocol = _ODP_PROTOCOL_OPENDAP
        else:
            protocol = _ODP_PROTOCOL_HTTP

        local_path = os.path.join(local_ds.data_store.data_store_path, local_id)
        if not os.path.exists(local_path):
            os.makedirs(local_path)

        selected_file_list = self._find_files(time_range)
        if not selected_file_list:
            raise self._empty_error(time_range)

        try:
            if protocol == _ODP_PROTOCOL_OPENDAP:
                do_update_of_variables_meta_info_once = True
                do_update_of_region_meta_info_once = True

                files = self._get_urls_list(selected_file_list, protocol)
                with monitor.starting('Sync ' + self.id, total_work=len(files)):
                    for idx, dataset_uri in enumerate(files):
                        child_monitor = monitor.child(work=1)

                        file_name = os.path.basename(dataset_uri)
                        local_filepath = os.path.join(local_path, file_name)

                        time_coverage_start = selected_file_list[idx][1]
                        time_coverage_end = selected_file_list[idx][2]

                        with child_monitor.starting(label=file_name, total_work=100):
                            remote_dataset = xr.open_dataset(dataset_uri,
                                                             drop_variables=[variable.get('name') for variable in
                                                                             excluded_variables])
                            remote_dataset_root = remote_dataset
                            child_monitor.progress(work=20)

                            if var_names:
                                remote_dataset = remote_dataset.drop_vars(
                                    [var_name for var_name in remote_dataset.data_vars.keys()
                                     if var_name not in var_names]
                                )
                            if region:
                                remote_dataset = normalize_impl(remote_dataset)
                                remote_dataset = subset_spatial_impl(remote_dataset, region)
                                remote_dataset = adjust_spatial_attrs_impl(remote_dataset, allow_point=False)
                                if do_update_of_region_meta_info_once:
                                    local_ds.meta_info['bbox_minx'] = remote_dataset.attrs['geospatial_lon_min']
                                    local_ds.meta_info['bbox_maxx'] = remote_dataset.attrs['geospatial_lon_max']
                                    local_ds.meta_info['bbox_maxy'] = remote_dataset.attrs['geospatial_lat_max']
                                    local_ds.meta_info['bbox_miny'] = remote_dataset.attrs['geospatial_lat_min']
                                    do_update_of_region_meta_info_once = False
                            if compression_enabled:
                                for sel_var_name in remote_dataset.variables.keys():
                                    remote_dataset.variables.get(sel_var_name).encoding.update(encoding_update)
                            # Note: we are using engine='h5netcdf' here because the default engine='netcdf4'
                            # causes crashes in file "netCDF4/_netCDF4.pyx" with currently used netcdf4-1.4.2 conda
                            # package from conda-forge. This occurs whenever remote_dataset.to_netcdf() is called a
                            # second time in this loop.
                            # Probably related to https://github.com/pydata/xarray/issues/2560.
                            # And probably fixes Cate issues #823, #822, #818, #816, #783.
                            remote_dataset.to_netcdf(local_filepath, format='NETCDF4', engine='h5netcdf')
                            child_monitor.progress(work=75)

                            if do_update_of_variables_meta_info_once:
                                variables_info = local_ds.meta_info.get('variables', [])
                                local_ds.meta_info['variables'] = [var_info for var_info in variables_info
                                                                   if var_info.get('name')
                                                                   in remote_dataset.variables.keys()
                                                                   and var_info.get('name')
                                                                   not in remote_dataset.dims.keys()]
                                do_update_of_variables_meta_info_once = False
                            local_ds.add_dataset(os.path.join(local_id, file_name),
                                                 (time_coverage_start, time_coverage_end))
                            if do_update_of_verified_time_coverage_start_once:
                                verified_time_coverage_start = time_coverage_start
                                do_update_of_verified_time_coverage_start_once = False
                            verified_time_coverage_end = time_coverage_end
                            child_monitor.progress(work=5)

                            remote_dataset_root.close()
            else:
                outdated_file_list = []
                for file_rec in selected_file_list:
                    filename, _, _, file_size, url = file_rec
                    dataset_file = os.path.join(local_path, filename)
                    # todo (forman, 20160915): must perform better checks on dataset_file if it is...
                    # ... outdated or incomplete or corrupted.
                    # JSON also includes "checksum" and "checksum_type" fields.
                    if not os.path.isfile(dataset_file) or (file_size and os.path.getsize(dataset_file) != file_size):
                        outdated_file_list.append(file_rec)

                if outdated_file_list:
                    with monitor.starting('Sync ' + self.id, len(outdated_file_list)):
                        bytes_to_download = sum([file_rec[3] for file_rec in outdated_file_list])
                        dl_stat = _DownloadStatistics(bytes_to_download)

                        file_number = 1

                        for filename, coverage_from, coverage_to, file_size, url in outdated_file_list:
                            dataset_file = os.path.join(local_path, filename)
                            child_monitor = monitor.child(work=1.0)

                            # noinspection PyUnusedLocal
                            def reporthook(block_number, read_size, total_file_size):
                                dl_stat.handle_chunk(read_size)
                                child_monitor.progress(work=read_size, msg=str(dl_stat))

                            sub_monitor_msg = "file %d of %d" % (file_number, len(outdated_file_list))
                            with child_monitor.starting(sub_monitor_msg, file_size):
                                actual_url = url[protocol]
                                _LOG.info(f"Downloading {actual_url} to {dataset_file}")
                                urllib.request.urlretrieve(actual_url, filename=dataset_file, reporthook=reporthook)
                            file_number += 1
                            local_ds.add_dataset(os.path.join(local_id, filename), (coverage_from, coverage_to))

                            if do_update_of_verified_time_coverage_start_once:
                                verified_time_coverage_start = coverage_from
                                do_update_of_verified_time_coverage_start_once = False
                            verified_time_coverage_end = coverage_to
        except HTTPError as e:
            raise self._cannot_access_error(time_range, region, var_names,
                                            verb="synchronize", cause=e) from e
        except (URLError, socket.timeout) as e:
            raise self._cannot_access_error(time_range, region, var_names,
                                            verb="synchronize", cause=e,
                                            error_cls=NetworkError) from e
        except OSError as e:
            raise self._cannot_access_error(time_range, region, var_names,
                                            verb="synchronize", cause=e) from e

        local_ds.meta_info['temporal_coverage_start'] = TimeLike.format(verified_time_coverage_start)
        local_ds.meta_info['temporal_coverage_end'] = TimeLike.format(verified_time_coverage_end)
        local_ds.meta_info['exclude_variables'] = excluded_variables
        local_ds.save(True)

    def make_local(self,
                   local_name: str,
                   local_id: str = None,
                   time_range: TimeRangeLike.TYPE = None,
                   region: PolygonLike.TYPE = None,
                   var_names: VarNamesLike.TYPE = None,
                   monitor: Monitor = Monitor.NONE) -> Optional[DataSource]:

        time_range = TimeRangeLike.convert(time_range) if time_range else None
        region = PolygonLike.convert(region) if region else None
        var_names = VarNamesLike.convert(var_names) if var_names else None

        ds_id = local_name
        title = local_id

        local_store = DATA_STORE_REGISTRY.get_data_store('local')
        if not local_store:
            add_to_data_store_registry()
            local_store = DATA_STORE_REGISTRY.get_data_store('local')
        if not local_store:
            raise ValueError('Cannot initialize `local` DataStore')

        uuid = LocalDataStore.generate_uuid(ref_id=self.id, time_range=time_range, region=region, var_names=var_names)

        if not ds_id or len(ds_id) == 0:
            ds_id = "local.{}.{}".format(self.id, uuid)
            existing_ds_list = local_store.query(ds_id=ds_id)
            if len(existing_ds_list) == 1:
                return existing_ds_list[0]
        else:
            existing_ds_list = local_store.query(ds_id='local.%s' % ds_id)
            if len(existing_ds_list) == 1:
                if existing_ds_list[0].meta_info.get('uuid', None) == uuid:
                    return existing_ds_list[0]
                else:
                    raise ValueError('Datastore {} already contains dataset {}'.format(local_store.id, ds_id))

        local_meta_info = self.meta_info.copy()
        local_meta_info['uuid'] = uuid

        local_ds = local_store.create_data_source(ds_id, title=title,
                                                  time_range=time_range, region=region, var_names=var_names,
                                                  meta_info=local_meta_info, lock_file=True)
        if local_ds:
            if not local_ds.is_complete:
                try:
                    self._make_local(local_ds, time_range, region, var_names, monitor=monitor)
                except Cancellation as c:
                    local_store.remove_data_source(local_ds)
                    raise c
                except Exception as e:
                    if local_ds.is_empty:
                        local_store.remove_data_source(local_ds)
                    raise e

            if local_ds.is_empty:
                local_store.remove_data_source(local_ds)
                return None

            local_store.register_ds(local_ds)
            return local_ds
        else:
            return None

    def _init_file_list(self, monitor: Monitor = Monitor.NONE):
        if self._file_list:
            return
        file_list = _load_or_fetch_json(_fetch_file_list_json,
                                        fetch_json_args=[self._raw_id],
                                        fetch_json_kwargs=dict(monitor=monitor),
                                        cache_used=self._data_store.index_cache_used,
                                        cache_dir=self.local_metadata_dataset_dir(),
                                        cache_json_filename='file-list.json',
                                        cache_timestamp_filename='file-list-timestamp.txt',
                                        cache_expiration_days=self._data_store.index_cache_expiration_days)

        time_frequency = self._json_dict.get('time_frequency', None)
        if time_frequency is None and 'time_frequencies' in self._json_dict:
            time_frequency = self._json_dict.get('time_frequencies')[0]
        if time_frequency:
            time_delta = _TIME_FREQUENCY_TO_TIME_DELTA.get(time_frequency, timedelta(days=0))
        else:
            time_delta = timedelta(days=0)

        data_source_start_date = datetime(3000, 1, 1)
        data_source_end_date = datetime(1000, 1, 1)
        # Convert file_start_date from string to datetime object
        # Compute file_end_date from 'time_frequency' field
        # Compute the data source's temporal coverage
        for file_rec in file_list:
            if file_rec[1]:
                file_start_date = datetime.strptime(file_rec[1], _TIMESTAMP_FORMAT)
                file_end_date = file_start_date + time_delta
                data_source_start_date = min(data_source_start_date, file_start_date)
                data_source_end_date = max(data_source_end_date, file_end_date)
                file_rec[1] = file_start_date
                file_rec[2] = file_end_date
        self._temporal_coverage = data_source_start_date, data_source_end_date
        self._file_list = file_list

    def __str__(self):
        return self.info_string

    def _repr_html_(self):
        return self.id

    def __repr__(self):
        return self.id


class _DownloadStatistics:
    def __init__(self, bytes_total):
        self.bytes_total = bytes_total
        self.bytes_done = 0
        self.start_time = datetime.now()

    def handle_chunk(self, chunk_size: int):
        self.bytes_done += chunk_size

    def __str__(self):
        seconds = (datetime.now() - self.start_time).seconds
        if seconds > 0:
            mb_per_sec = self._to_megas(self.bytes_done) / seconds
        else:
            mb_per_sec = 0.
        percent = 100. * self.bytes_done / self.bytes_total
        return "%d of %d MB @ %.3f MB/s, %.1f%% complete" % \
               (self._to_megas(self.bytes_done), self._to_megas(self.bytes_total), mb_per_sec, percent)

    @staticmethod
    def _to_megas(bytes_count: int) -> float:
        return bytes_count / (1000. * 1000.)
