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

This plugin module adds the ESA CCI Open Data Portal's (ODP) service to the data store registry.
As of April 2020, the ODP service provides a OpenSearch-compatible catalogue, which is utilised in this implementation.

Verification
============

The module's unit-tests are located in
`test/ds/test_esa_cci_odp.py <https://github.com/CCI-Tools/cate/blob/master/test/ds/test_esa_cci_odp.py>`_
and may be executed using
``$ py.test test/ds/test_esa_cci_odp_legacy.py --cov=cate/ds/esa_cci_odp.py`` for extra code coverage information.

Components
==========
"""
import contextlib

import aiofiles
import aiohttp
import asyncio
import copy
import json
import logging
import os
import random
import re
import socket
import time
import urllib.parse
import urllib.request
import warnings
from collections import OrderedDict
from datetime import datetime, timedelta
import lxml.etree as etree
from typing import Sequence, Tuple, Optional, Any, Dict, List, Union, AbstractSet
from urllib.error import URLError, HTTPError

import pandas as pd
import xarray as xr

from cate.conf import get_config_value, get_data_stores_path
from cate.conf.defaults import NETCDF_COMPRESSION_LEVEL
from cate.core.ds import DATA_STORE_REGISTRY, NetworkError, DataStore, DataSource, Schema, open_xarray_dataset, \
    DataStoreNotice, DataAccessError, DataAccessWarning
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

_OPENSEARCH_CEDA_URL = "https://archive.opensearch.ceda.ac.uk/opensearch/request"
_OPENSEARCH_CEDA_ODD_URL = 'https://archive.opensearch.ceda.ac.uk/opensearch/description.xml?parentIdentifier=cci'
# switch to these to use test server
# _OPENSEARCH_CEDA_URL = "http://opensearch-test.ceda.ac.uk/opensearch/request"
# _OPENSEARCH_CEDA_ODD_URL = 'http://opensearch-test.ceda.ac.uk/opensearch/description.xml?parentIdentifier=cci'

ODD_NS = {'os': 'http://a9.com/-/spec/opensearch/1.1/',
          'param': 'http://a9.com/-/spec/opensearch/extensions/parameters/1.0/'}
DESC_NS = {'gmd': 'http://www.isotc211.org/2005/gmd',
           'gml': 'http://www.opengis.net/gml/3.2',
           'gco': 'http://www.isotc211.org/2005/gco',
           'gmx': 'http://www.isotc211.org/2005/gmx',
           'xlink': 'http://www.w3.org/1999/xlink'
           }

_NUM_RETRIES = 200
_RETRY_BACKOFF_MAX = 40
_RETRY_BACKOFF_BASE = 1.001

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


def set_default_data_store():
    """
    Defines the ESA CCI ODP data store and makes it the default data store.

    All data sources of the FTP data store are read from a JSON file ``esa_cci_ftp.json`` contained in this package.
    This JSON file has been generated from a scan of the entire FTP tree.
    """
    DATA_STORE_REGISTRY.add_data_store(EsaCciOdpDataStore())


def find_datetime_format(filename: str) -> Tuple[Optional[str], int, int]:
    for regex, time_format in _RE_TO_DATETIME_FORMATS:
        searcher = regex.search(filename)
        if searcher:
            p1, p2 = searcher.span()
            return time_format, p1, p2
    return None, -1, -1


async def _extract_metadata_from_odd_url(session=None, odd_url: str = None) -> dict:
    if session is None:
        session = aiohttp.ClientSession()
    if not odd_url:
        return {}
    resp = await get_response(session, odd_url)
    if not resp:
        return {}
    xml_text = await resp.read()
    return _extract_metadata_from_odd(etree.XML(xml_text))


def _extract_metadata_from_odd(odd_xml: etree.XML) -> dict:
    metadata = {}
    metadata_names = {'ecv': ['ecv', 'ecvs'], 'frequency': ['time_frequency', 'time_frequencies'],
                      'institute': ['institute', 'institutes'],
                      'processingLevel': ['processing_level', 'processing_levels'],
                      'productString': ['product_string', 'product_strings'],
                      'productVersion': ['product_version', 'product_versions'],
                      'dataType': ['data_type', 'data_types'], 'sensor': ['sensor_id', 'sensor_ids'],
                      'platform': ['platform_id', 'platform_ids'], 'fileFormat': ['file_format', 'file_formats'],
                      'drsId': ['drs_id', 'drs_ids']}
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


async def _extract_metadata_from_descxml_url(session=None, descxml_url: str = None) -> dict:
    if session is None:
        session = aiohttp.ClientSession()
    if not descxml_url:
        return {}
    resp = await get_response(session, descxml_url)
    if not resp:
        return {}
    content = await resp.read()
    try:
        descxml = etree.XML(content)
        return _extract_metadata_from_descxml(descxml)
    except etree.XMLSyntaxError:
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


async def get_response(session: aiohttp.ClientSession, url: str) -> Optional[aiohttp.ClientResponse]:
    response = None
    retry_backoff_max = _RETRY_BACKOFF_MAX
    for i in range(_NUM_RETRIES):
        resp = await session.request(method='GET', url=url)
        if resp.status == 200:
            return resp
        elif 500 <= resp.status < 600:
            error_message = f'Error {resp.status}: Cannot access url.'
            warnings.warn(error_message)
            return None
        elif resp.status == 429:
            # Retry after 'Retry-After' with exponential backoff
            retry_min = int(response.headers.get('Retry-After', '100'))
            retry_backoff = random.random() * retry_backoff_max
            retry_total = retry_min + retry_backoff
            retry_message = f'Error 429: Too Many Requests. ' \
                            f'Attempt {i + 1} of {_NUM_RETRIES} to retry after ' \
                            f'{"%.2f" % retry_min} + {"%.2f" % retry_backoff} = {"%.2f" % retry_total} ms...'
            warnings.warn(retry_message, DataAccessWarning)
            time.sleep(retry_total / 1000.0)
            retry_backoff_max *= _RETRY_BACKOFF_BASE
        else:
            break
    warnings.warn(f'Request for {url} failed with status {resp.status}: {resp.reason}', DataAccessWarning)
    return None


async def _fetch_feature_at(session, base_url, query_args, index) -> Optional[Dict]:
    paging_query_args = dict(query_args or {})
    maximum_records = 1
    paging_query_args.update(startPage=index,
                             maximumRecords=maximum_records,
                             httpAccept='application/geo+json',
                             fileFormat='.nc')
    url = base_url + '?' + urllib.parse.urlencode(paging_query_args)
    resp = await get_response(session, url)
    if resp:
        json_text = await resp.read()
        json_dict = json.loads(json_text.decode('utf-8'))
        feature_list = json_dict.get("features", [])
        if len(feature_list) > 0:
            return feature_list[0]
    return None


async def _fetch_opensearch_feature_list(base_url, query_args, max_wanted_results=100000,
                                         monitor: Monitor = Monitor.NONE) -> List:
    """
    Return JSON value read from Opensearch web service.
    :return:
    """
    start_page = 1
    records_per_page = 1000
    full_feature_list = []
    with monitor.starting("Loading", 10):
        async with aiohttp.ClientSession() as session:
            while len(full_feature_list) < max_wanted_results:
                monitor.progress(work=1)
                paging_query_args = dict(query_args or {})
                maximum_records = min(records_per_page, max_wanted_results-len(full_feature_list))
                paging_query_args.update(startPage=start_page, maximumRecords=maximum_records,
                                         httpAccept='application/geo+json')
                url = base_url + '?' + urllib.parse.urlencode(paging_query_args)
                resp = await get_response(session, url)
                if resp:
                    json_text = await resp.read()
                    try:
                        json_dict = json.loads(json_text.decode('utf-8'))
                        feature_list = json_dict.get("features", [])
                        full_feature_list.extend(feature_list)
                        if len(feature_list) < maximum_records:
                            break
                    except json.decoder.JSONDecodeError:
                        print(json_text)
                else:
                    break
                start_page += 1
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
                if not isinstance(catalogue[multiple_fields_name], list):
                    catalogue[multiple_fields_name] = [catalogue[multiple_fields_name]]
                catalogue[multiple_fields_name].append(catalogue[single_field_name])
            catalogue.pop(single_field_name)


async def _load_or_fetch_json(fetch_json_function,
                              fetch_json_args: list = None,
                              fetch_json_kwargs: dict = None,
                              cache_used: bool = False,
                              cache_dir: str = None,
                              cache_json_filename: str = None,
                              cache_timestamp_filename: str = None,
                              cache_expiration_days: float = 1.0) -> Union[Sequence, Dict]:
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
            json_obj = await fetch_json_function(*(fetch_json_args or []), **(fetch_json_kwargs or {}))
            if cache_used and len(json_obj) > 0:
                os.makedirs(cache_dir, exist_ok=True)
                # noinspection PyUnboundLocalVariable
                async with aiofiles.open(cache_json_file, "w") as fp:
                    await fp.write(json.dumps(json_obj, indent='  '))
                # noinspection PyUnboundLocalVariable
                async with aiofiles.open(cache_timestamp_file, "w") as fp:
                    await fp.write(datetime.utcnow().strftime(_TIMESTAMP_FORMAT))
        except Exception as e:
            if cache_json_file and os.path.exists(cache_json_file):
                with open(cache_json_file) as fp:
                    json_text = fp.read()
                    json_obj = json.loads(json_text)
            else:
                raise e

    return json_obj


async def _fetch_data_source_list_json(base_url, query_args, max_wanted_results=100000,
                                       monitor: Monitor = Monitor.NONE) -> Sequence:
    feature_collection_list = await _fetch_opensearch_feature_list(base_url,
                                                                   query_args,
                                                                   max_wanted_results,
                                                                   monitor=monitor)
    catalogue = {}
    for fc in feature_collection_list:
        fc_props = fc.get("properties", {})
        fc_id = fc_props.get("identifier", None)
        if not fc_id:
            continue
        catalogue[fc_id] = {}
        catalogue[fc_id]['uuid'] = fc.get("id", "").split("=")[-1]
        catalogue[fc_id]['title'] = fc_props.get("title", "")
        variables = _get_variables_from_feature(fc)
        catalogue[fc_id]['variables'] = variables
        fc_props_links = fc_props.get("links", None)
        if fc_props_links:
            search = fc_props_links.get("search", None)
            if search:
                odd_url = search[0].get('href', None)
                if odd_url:
                    catalogue[fc_id]['odd_url'] = odd_url
            described_by = fc_props_links.get("describedby", None)
            if described_by:
                for entry in described_by:
                    if entry.get('title', '') == 'ISO19115':
                        metadata_url = entry.get("href", None)
                        if metadata_url:
                            catalogue[fc_id]['metadata_url'] = metadata_url
                    elif entry.get('title', '') == 'Dataset Information':
                        catalogue_url = entry.get("href", None)
                        if catalogue_url:
                            catalogue[fc_id]['catalogue_url'] = catalogue_url
        _LOG.info(f'Read meta info from {fc_id}')
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


async def _get_infos_from_feature(session, feature: dict, dataset_id: str) -> tuple:
    feature_info = _extract_feature_info(feature)
    opendap_dds_url = f"{feature_info[4]['Opendap']}.dds"
    resp = await get_response(session, opendap_dds_url)
    if not resp:
        warnings.warn(f'Could not derive info about dimensions for {dataset_id}', DataAccessWarning)
        return {}, {}
    content = await resp.read()
    return _retrieve_infos_from_dds(str(content, 'utf-8').split('\n'))


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


async def _fetch_meta_info(dataset_id: str, odd_url: str, metadata_url: str, variables: List, read_dimensions: bool) \
        -> Dict:
    async with aiohttp.ClientSession() as session:
        meta_info_dict = {}
        if odd_url:
            meta_info_dict = await _extract_metadata_from_odd_url(session, odd_url)
        if metadata_url:
            desc_metadata = await _extract_metadata_from_descxml_url(session, metadata_url)
            for item in desc_metadata:
                if item not in meta_info_dict:
                    meta_info_dict[item] = desc_metadata[item]
        meta_info_dict['dimensions'] = {}
        meta_info_dict['variable_infos'] = {}
        if read_dimensions and len(variables) > 0:
            feature = await _fetch_feature_at(session,
                                              _OPENSEARCH_CEDA_URL,
                                              dict(parentIdentifier=dataset_id),
                                              1)
            if feature is None:
                _LOG.info(f'Could not derive info about dimensions for {dataset_id}')
            else:
                feature_dimensions, feature_variable_infos = \
                    await _get_infos_from_feature(session, feature, dataset_id)
                meta_info_dict['dimensions'] = feature_dimensions
                meta_info_dict['variable_infos'] = feature_variable_infos
    await _set_drs_metadata(dataset_id, meta_info_dict)
    _harmonize_info_field_names(meta_info_dict, 'file_format', 'file_formats')
    _harmonize_info_field_names(meta_info_dict, 'platform_id', 'platform_ids')
    _harmonize_info_field_names(meta_info_dict, 'sensor_id', 'sensor_ids')
    _harmonize_info_field_names(meta_info_dict, 'processing_level', 'processing_levels')
    _harmonize_info_field_names(meta_info_dict, 'time_frequency', 'time_frequencies')
    return meta_info_dict


async def _set_drs_metadata(datasource_id, metainfo_dict):
    data_source_list = await _fetch_data_source_list_json(_OPENSEARCH_CEDA_URL,
                                                          {'parentIdentifier': datasource_id},
                                                          max_wanted_results=20)
    for data_source_key, data_source_value in data_source_list.items():
        drs_id = data_source_value.get('title', 'All Files')
        variables = data_source_value.get('variables', None)
        uuid = data_source_value.get('uuid', None)
        if drs_id != 'All Files':
            if variables:
                if 'variables' not in metainfo_dict:
                    metainfo_dict['variables'] = {}
                metainfo_dict['variables'][drs_id] = variables
                if uuid:
                    if 'uuids' not in metainfo_dict:
                        metainfo_dict['uuids'] = {}
                    metainfo_dict['uuids'][drs_id] = uuid


async def _fetch_file_list_json(dataset_id: str, drs_id: str, monitor: Monitor = Monitor.NONE) \
        -> Sequence:
    feature_list = await _fetch_opensearch_feature_list(_OPENSEARCH_CEDA_URL,
                                                        dict(parentIdentifier=dataset_id,
                                                             drsId=drs_id,
                                                             fileFormat='.nc'),
                                                        monitor=monitor)
    file_list = []
    for feature in feature_list:
        feature_info = _extract_feature_info(feature)
        if feature_info[0] in file_list:
            raise ValueError('filename {} already seen in dataset {}'.
                             format(feature_info[0], dataset_id))
        file_list.append(feature_info)
    max_time = datetime.strftime(datetime.max, _TIMESTAMP_FORMAT)

    def pick_start_time(file_info_rec):
        return file_info_rec[1] if file_info_rec[1] else max_time

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


class EsaCciOdpDataStore(DataStore):
    # noinspection PyShadowingBuiltins
    def __init__(self,
                 id: str = 'esa_cci_odp_os',
                 title: str = 'ESA CCI Open Data Portal',
                 index_cache_used: bool = True,
                 index_cache_expiration_days: float = 1.0,
                 index_cache_json_dict: dict = None,
                 index_cache_update_tag: str = None,
                 meta_data_store_path: str = get_metadata_store_path(),
                 drs_ids: List[str] = None
                 ):
        super().__init__(id, title=title, is_local=False)
        if drs_ids is None:
            drs_ids = []
        self._index_cache_used = index_cache_used
        self._index_cache_expiration_days = index_cache_expiration_days
        self._catalogue = index_cache_json_dict
        self._index_cache_update_tag = index_cache_update_tag
        self._metadata_store_path = meta_data_store_path
        self._drs_ids = drs_ids
        self._data_sources = []
        self._dataset_states = {}
        loc = os.path.dirname(os.path.abspath(__file__))
        with open(f'{loc}/data/dataset_states.json', 'r') as fp:
            self._dataset_states = json.load(fp)

    @property
    def description(self) -> Optional[str]:
        """
        Return a human-readable description for this data store as plain text.

        The text may use Markdown formatting.
        """
        return ("This data store represents the [ESA CCI Open Data Portal](http://cci.esa.int/data)"
                " in the CCI Toolbox.\n"
                "It currently provides all CCI data that are published through an OpenSearch interface. "
                "The store will be extended shortly to also provide TIFF and Shapefile Data, see usage "
                "notes.\n"
                "Remote data downloaded to your computer is made available through the *File Data Store*.")

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
        return self._metadata_store_path

    def query(self, ds_id: str = None, query_expr: str = None, monitor: Monitor = Monitor.NONE) \
            -> Sequence['DataSource']:
        asyncio.run(self._init_data_sources())
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
        diff_file = os.path.join(self._metadata_store_path, self._get_update_tag() + '-diff.json')

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
            frozen_file = os.path.join(self._metadata_store_path, self._get_update_tag() + '-freeze.json')
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

    async def _fetch_dataset_names(self, session):
        meta_info_dict = await _extract_metadata_from_odd_url(session, _OPENSEARCH_CEDA_ODD_URL)
        drs_ids = meta_info_dict.get('drs_ids', None)
        if not drs_ids:
            raise DataAccessError('Could not read names of datasets of ODP Store')
        if 'drs_ids' in meta_info_dict:
            return meta_info_dict['drs_ids']

    async def _init_data_sources(self):
        os.makedirs(self._metadata_store_path, exist_ok=True)
        if self._data_sources:
            return
        if self._catalogue is None:
            await self._load_index()
        if not self._drs_ids:
            async with aiohttp.ClientSession() as session:
                self._drs_ids = await self._fetch_dataset_names(session)
        if self._catalogue:
            self._data_sources = []
            tasks = []
            for catalogue_item in self._catalogue:
                tasks.append(self._create_data_source(self._catalogue[catalogue_item], catalogue_item))
            await asyncio.gather(*tasks)
        self._check_source_diff()
        self._freeze_source()

    async def _create_data_source(self, json_dict: dict, datasource_id: str):
        local_metadata_dataset_dir = os.path.join(self._metadata_store_path, datasource_id)
        # todo set True when dimensions shall be read during meta data fetching
        meta_info = await _load_or_fetch_json(_fetch_meta_info,
                                              fetch_json_args=[datasource_id,
                                                               json_dict.get('odd_url', None),
                                                               json_dict.get('metadata_url', None),
                                                               json_dict.get('variables', []),
                                                               False],
                                              fetch_json_kwargs=dict(),
                                              cache_used=self.index_cache_used,
                                              cache_dir=local_metadata_dataset_dir,
                                              cache_json_filename='meta-info.json',
                                              cache_timestamp_filename='meta-info-timestamp.txt',
                                              cache_expiration_days=self.index_cache_expiration_days)
        drs_ids = self._get_as_list(meta_info, 'drs_id', 'drs_ids')
        for drs_id in drs_ids:
            if drs_id not in self._drs_ids:
                continue
            if self.is_dataset_dropped(drs_id, datasource_id):
                continue
            drs_meta_info = copy.deepcopy(meta_info)
            drs_variables = drs_meta_info.get('variables', {}).get(drs_id, None)
            drs_meta_info.update(json_dict)
            if drs_variables:
                drs_meta_info['variables'] = drs_variables
            drs_meta_info['uuid'] = drs_meta_info.get('uuids', {}).get(drs_id, datasource_id)
            self._adjust_json_dict(drs_meta_info, drs_id)
            drs_meta_info['cci_project'] = drs_meta_info['ecv']
            drs_meta_info['fid'] = datasource_id
            verification_flags = self._dataset_states.get(drs_id, {}).get('verification_flags', [])
            type_specifier = self._dataset_states.get(drs_id, {}).get('type_specifier', None)
            data_source = EsaCciOdpDataSource(self, drs_meta_info, drs_id,
                                              verification_flags, type_specifier)
            self._data_sources.append(data_source)

    def _adjust_json_dict(self, json_dict: dict, drs_id: str):
        values = drs_id.split('.')
        self._adjust_json_dict_for_param(json_dict, 'time_frequency', 'time_frequencies',
                                         self._convert_time_from_drs_id(values[2]))
        self._adjust_json_dict_for_param(json_dict, 'processing_level', 'processing_levels', values[3])
        self._adjust_json_dict_for_param(json_dict, 'data_type', 'data_types', values[4])
        self._adjust_json_dict_for_param(json_dict, 'sensor_id', 'sensor_ids', values[5])
        self._adjust_json_dict_for_param(json_dict, 'platform_id', 'platform_ids', values[6])
        self._adjust_json_dict_for_param(json_dict, 'product_string', 'product_strings', values[7])
        self._adjust_json_dict_for_param(json_dict, 'product_version', 'product_versions', values[8])

    @staticmethod
    def _convert_time_from_drs_id(time_value: str) -> str:
        time_value_lookup = {'mon': 'month', 'month': 'month', 'yr': 'year', 'year': 'year', 'day': 'day',
                             'satellite-orbit-frequency': 'satellite-orbit-frequency', 'climatology': 'climatology',
                             'unspecified': 'unspecified'}
        if time_value in time_value_lookup:
            return time_value_lookup[time_value]
        if re.match('[0-9]+-[days|yrs]', time_value):
            split_time_value = time_value.split('-')
            return f'{split_time_value[0]} {split_time_value[1].replace("yrs", "years")}'
        raise ValueError(f'Unknown time frequency format: {time_value}')

    def _adjust_json_dict_for_param(self, json_dict: dict, single_name: str, list_name: str, param_value: str):
        json_dict[single_name] = param_value
        if list_name in json_dict:
            json_dict.pop(list_name)

    def _get_pretty_id(self, json_dict: dict, value_tuple: Tuple, drs_id: str) -> str:
        pretty_values = []
        for value in value_tuple:
            pretty_values.append(self._make_string_pretty(value))
        return f'esacci2.{json_dict["ecv"]}.{".".join(pretty_values)}.{drs_id}'

    def _make_string_pretty(self, string: str):
        string = string.replace(" ", "-")
        if string.startswith("."):
            string = string[1:]
        if string.endswith("."):
            string = string[:-1]
        if "." in string:
            string = string.replace(".", "-")
        return string

    def _get_as_list(self, meta_info: dict, single_name: str, list_name: str) -> List:
        if single_name in meta_info:
            return [meta_info[single_name]]
        if list_name in meta_info:
            return meta_info[list_name]
        return []

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
        frozen_file = os.path.join(self._metadata_store_path, self._get_update_tag() + '-freeze.json')
        diff_file = os.path.join(self._metadata_store_path, self._get_update_tag() + '-diff.json')
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
        frozen_file = os.path.join(self._metadata_store_path, self._get_update_tag() + '-freeze.json')
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

    async def _load_index(self):
        self._catalogue = await _load_or_fetch_json(_fetch_data_source_list_json,
                                                    fetch_json_args=[
                                                        _OPENSEARCH_CEDA_URL,
                                                        dict(parentIdentifier='cci')
                                                    ],
                                                    cache_used=self._index_cache_used,
                                                    cache_dir=self._metadata_store_path,
                                                    cache_json_filename='dataset-list.json',
                                                    cache_timestamp_filename='dataset-list-timestamp.json',
                                                    cache_expiration_days=self._index_cache_expiration_days
                                                    )

    def is_dataset_dropped(self, drs_id, datasource_id):
        # todo remove as soon as the double occurrence of the drs id is removed from the odp
        """
        Sometimes a drs_id contains more than one feature and therefore is associated with more than one datasource_id.
        This is an error on ODP and is not handled by cate correctly. To prohibit problems, it is manually checked,
        which feature contains less datasets and is dropped in favor of the feature with more datasets.
        :return:
        """
        to_be_dropped = [('esacci.AEROSOL.satellite-orbit-frequency.L2P.AER_PRODUCTS.ATSR-2.ERS-2.SU.4-21.r1',
                          '59f3a38819e140b49ffe46f32176709e'),
                         ('esacci.AEROSOL.day.L3C.AER_PRODUCTS.ATSR-2.Envisat.ATSR2.v2-6.r1',
                          'c183044b88734442b6d37f5c4f6b0092'),
                         (
                             'esacci.AEROSOL.satellite-orbit-frequency.L2P.AER_PRODUCTS.AATSR.Envisat.AATSR-ENVISAT-ENS.v2-6.r1',
                             '4afb736dc395442aa9b327c11f0d704b')
                         ]
        if (drs_id, datasource_id) in to_be_dropped:
            return True
        else:
            return False


INFO_FIELD_NAMES = sorted(["title",
                           "uuid",
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
                           "data_types",
                           "cci_project",
                           "catalogue_url"
                           ])


class EsaCciOdpDataSource(DataSource):
    def __init__(self,
                 data_store: EsaCciOdpDataStore,
                 json_dict: dict,
                 datasource_id: str,
                 verification_flags: List[str] = None,
                 type_specifier = None,
                 schema: Schema = None):
        super(EsaCciOdpDataSource, self).__init__()
        self._datasource_id = datasource_id
        self._data_store = data_store
        self._json_dict = json_dict
        self._schema = schema
        self._file_list = None
        self._meta_info = None
        self._temporal_coverage = None
        if verification_flags:
            self._verification_flags = set(verification_flags)
        else:
            self._verification_flags = set()
        self._type_specifier = type_specifier

    @property
    def id(self) -> str:
        return self._datasource_id

    @property
    def data_store(self) -> EsaCciOdpDataStore:
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

    @property
    def verification_flags(self) -> AbstractSet[str]:
        return self._verification_flags

    @property
    def type_specifier(self) -> str:
        return self._type_specifier

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
        non_data_variable_names =  ['period', 'hist1d_cla_vis006_bin_centre', 'lon_bnds', 'air_pressure',
                                    'field_name_length', 'lon', 'view', 'hist2d_cot_bin_centre',
                                    'hist1d_cer_bin_border', 'altitude', 'vegetation_class',
                                    'hist1d_cla_vis006_bin_border', 'time_bnds', 'hist1d_ctp_bin_border',
                                    'hist1d_cot_bin_centre', 'hist1d_cot_bin_border', 'hist1d_cla_vis008_bin_centre',
                                    'lat_bnds', 'hist1d_cwp_bin_border', 'layers', 'hist1d_cer_bin_centre',
                                    'aerosol_type', 'hist1d_ctt_bin_border', 'hist1d_ctp_bin_centre', 'fieldsp1',
                                    'time', 'hist_phase', 'hist1d_cwp_bin_centre', 'hist2d_ctp_bin_border', 'lat',
                                    'fields', 'hist2d_cot_bin_border', 'hist2d_ctp_bin_centre',
                                    'hist1d_ctt_bin_centre', 'hist1d_cla_vis008_bin_border', 'crs', 'field_name',
                                    'vegetation_class_name']
        for variable in self._json_dict['variables']:
            if 'variable_infos' in self._meta_info and variable['name'] in self._meta_info['variable_infos'] and \
                    len(self._meta_info['variable_infos'][variable['name']]['dimensions']) == 0:
                continue
            if 'dimensions' in self._meta_info and variable['name'] in self._meta_info['dimensions']:
                continue
            if variable['name'] not in non_data_variable_names:
                variables.append(variable)
        return variables

    @property
    def schema(self) -> Schema:
        return self._schema

    @property
    def title(self) -> Optional[str]:
        return self._json_dict['title']

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
        asyncio.run(self._init_file_list(monitor))

    def local_dataset_dir(self):
        return os.path.join(get_data_store_path(), self._json_dict['fid'], self._datasource_id)

    def local_metadata_dataset_dir(self):
        return os.path.join(get_metadata_store_path(), self._json_dict['fid'], self._datasource_id)

    def _find_files(self, time_range):
        requested_start_date, requested_end_date = time_range if time_range else (None, None)
        asyncio.run(self._init_file_list())
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
        # we need to add this very ugly part so that netcdf works with all files
        files = [f + '#fillmismatch' for f in files]
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

    def _update_local_ds(self, local_ds: LocalDataSource, time_range: TimeRangeLike.TYPE = None,
                         region: PolygonLike.TYPE = None, var_names: VarNamesLike.TYPE = None,
                         monitor: Monitor = Monitor.NONE):
        time_range = TimeRangeLike.convert(time_range)
        var_names = VarNamesLike.convert(var_names)
        local_path = os.path.join(local_ds.data_store.data_store_path, local_ds.id)
        if not os.path.exists(local_path):
            os.makedirs(local_path)
        selected_file_list = self._find_files(time_range)
        if not selected_file_list:
            raise self._empty_error(time_range)
        if var_names:
            self._update_ds_using_opendap(local_path, local_ds, selected_file_list, time_range,
                                          region, var_names, monitor)
        else:
            self._update_ds_using_http(local_path, local_ds, selected_file_list, time_range,
                                       region, var_names, monitor)
        local_ds.save(True)

    def _update_ds_using_opendap(self, local_path, local_ds, selected_file_list, time_range,
                                 region, var_names, monitor):
        do_update_of_verified_time_coverage_start_once = True
        do_update_of_variables_meta_info_once = True
        do_update_of_region_meta_info_once = True
        verified_time_coverage_start = None
        verified_time_coverage_end = None
        compression_level = get_config_value('NETCDF_COMPRESSION_LEVEL', NETCDF_COMPRESSION_LEVEL)
        compression_enabled = True if compression_level > 0 else False
        encoding_update = dict()
        if compression_enabled:
            encoding_update.update({'zlib': True, 'complevel': compression_level})
        files = self._get_urls_list(selected_file_list, _ODP_PROTOCOL_OPENDAP)
        with monitor.starting('Sync ' + self.id, total_work=len(files)):
            for idx, dataset_uri in enumerate(files):
                child_monitor = monitor.child(work=1)
                file_name = os.path.basename(dataset_uri)
                local_filepath = os.path.join(local_path, file_name)
                time_coverage_start = selected_file_list[idx][1]
                time_coverage_end = selected_file_list[idx][2]
                with child_monitor.starting(label=file_name, total_work=100):
                    attempts = 0
                    to_append = ''
                    while attempts < 2:
                        try:
                            attempts += 1
                            remote_dataset = xr.open_dataset(dataset_uri + to_append)
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
                            to_netcdf_attempts = 0
                            format = 'NETCDF4'
                            # format = 'NETCDF3_64BIT'
                            engine = 'h5netcdf'
                            # engine = None
                            while to_netcdf_attempts < 2:
                                try:
                                    to_netcdf_attempts += 1
                                    # Note: we are using engine='h5netcdf' here because the default engine='netcdf4'
                                    # causes crashes in file "netCDF4/_netCDF4.pyx" with currently used netcdf4-1.4.2 conda
                                    # package from conda-forge. This occurs whenever remote_dataset.to_netcdf() is called a
                                    # second time in this loop.
                                    # Probably related to https://github.com/pydata/xarray/issues/2560.
                                    # And probably fixes Cate issues #823, #822, #818, #816, #783.
                                    remote_dataset.to_netcdf(local_filepath, format=format, engine=engine)
                                    break
                                except AttributeError as e:
                                    if to_netcdf_attempts == 1:
                                        format = 'NETCDF3_64BIT'
                                        engine = None
                                        continue
                                    raise self._cannot_access_error(time_range, region, var_names,
                                                                    verb="synchronize", cause=e) from e
                            child_monitor.progress(work=75)

                            if do_update_of_variables_meta_info_once:
                                variables_info = local_ds.meta_info.get('variables', [])
                                local_ds.meta_info['variables'] = [var_info for var_info in variables_info
                                                                   if var_info.get('name')
                                                                   in remote_dataset.variables.keys()
                                                                   and var_info.get('name')
                                                                   not in remote_dataset.dims.keys()]
                                do_update_of_variables_meta_info_once = False
                            local_ds.add_dataset(os.path.join(local_ds.id, file_name),
                                                 (time_coverage_start, time_coverage_end))
                            if do_update_of_verified_time_coverage_start_once:
                                verified_time_coverage_start = time_coverage_start
                                do_update_of_verified_time_coverage_start_once = False
                            verified_time_coverage_end = time_coverage_end
                            child_monitor.progress(work=5)
                            remote_dataset_root.close()
                        except HTTPError as e:
                            if attempts == 1:
                                to_append = '#fillmismatch'
                                continue
                            raise self._cannot_access_error(time_range, region, var_names,
                                                            verb="synchronize", cause=e) from e
                        except (URLError, socket.timeout) as e:
                            if attempts == 1:
                                to_append = '#fillmismatch'
                                continue
                            raise self._cannot_access_error(time_range, region, var_names,
                                                            verb="synchronize", cause=e,
                                                            error_cls=NetworkError) from e
                        except OSError as e:
                            if attempts == 1:
                                to_append = '#fillmismatch'
                                continue
                            raise self._cannot_access_error(time_range, region, var_names,
                                                            verb="synchronize", cause=e) from e
        local_ds.meta_info['temporal_coverage_start'] = TimeLike.format(verified_time_coverage_start)
        local_ds.meta_info['temporal_coverage_end'] = TimeLike.format(verified_time_coverage_end)

    def _update_ds_using_http(self, local_path, local_ds, selected_file_list, time_range, region,
                              var_names, monitor):
        do_update_of_verified_time_coverage_start_once = True
        verified_time_coverage_start = None
        verified_time_coverage_end = None
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
                url_scheme = None
                for filename, coverage_from, coverage_to, file_size, url in outdated_file_list:
                    dataset_file = os.path.join(local_path, filename)
                    child_monitor = monitor.child(work=1.0)

                    # noinspection PyUnusedLocal
                    def reporthook(block_number, read_size, total_file_size):
                        dl_stat.handle_chunk(read_size)
                        child_monitor.progress(work=read_size, msg=str(dl_stat))

                    sub_monitor_msg = "file %d of %d" % (file_number, len(outdated_file_list))
                    with child_monitor.starting(sub_monitor_msg, file_size):
                        actual_url = url[_ODP_PROTOCOL_HTTP]

                        if url_scheme:
                            url_object = urllib.parse.urlparse(actual_url)
                            url_object = url_object._replace(scheme=url_scheme)
                            actual_url = urllib.parse.urlunparse(url_object)

                    _LOG.info(f"Downloading {actual_url} to {dataset_file}")
                    try:
                        urllib.request.urlretrieve(actual_url, filename=dataset_file, reporthook=reporthook)
                    except HTTPError as e:
                        try:
                            actual_url = e.headers['Location']
                            urllib.request.urlretrieve(actual_url, filename=dataset_file, reporthook=reporthook)
                            # the following presumption has been made in order to save runtime: if a HTTP Error 308
                            # occurs, it's assumed to be due to protocol change and that for all downloads within this
                            # download que the redirect will be true.
                            url_scheme = urllib.parse.urlparse(actual_url).scheme
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
                    file_number += 1
                    local_ds.add_dataset(os.path.join(local_ds.id, filename), (coverage_from, coverage_to))
                    if do_update_of_verified_time_coverage_start_once:
                        verified_time_coverage_start = coverage_from
                        do_update_of_verified_time_coverage_start_once = False
                    verified_time_coverage_end = coverage_to
        local_ds.meta_info['temporal_coverage_start'] = TimeLike.format(verified_time_coverage_start)
        local_ds.meta_info['temporal_coverage_end'] = TimeLike.format(verified_time_coverage_end)

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
                    self._update_local_ds(local_ds, time_range, region, var_names, monitor=monitor)
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

    async def _init_file_list(self, monitor: Monitor = Monitor.NONE):
        if self._file_list:
            return
        file_list = await _load_or_fetch_json(_fetch_file_list_json,
                                              fetch_json_args=[self._json_dict['uuid'], self._datasource_id],
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
                # check if time_format matches _TIMESTAMP_FORMAT e.g '1997-09-03T00:00:00'
                match_pattern = re.match('(\d{4})[-](\d{2})[-](\d{2})T(\d{2})[:](\d{2})[:](\d{2})$',
                                         file_rec[1].split('.')[0])
                if match_pattern is not None:
                    file_start_date = datetime.strptime(file_rec[1].split('.')[0], _TIMESTAMP_FORMAT)
                else:
                    # check if time_format matches _TIMESTAMP_FORMAT with zonal information e.g '1997-09-03T00:00:00+00:00'
                    match_pattern = re.match(
                        '(\d{4})[-](\d{2})[-](\d{2})T(\d{2})[:](\d{2})[:](\d{2})[+](\d{2})[:](\d{2})$',
                        file_rec[1].split('.')[0])
                    if match_pattern is not None:
                        file_start_date = datetime.fromisoformat(file_rec[1].split('.')[0])
                        file_start_date = file_start_date.replace(tzinfo=None)
                    else:
                        raise ValueError(f"cannot extract date/time information from {file_rec[1]}.")

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
