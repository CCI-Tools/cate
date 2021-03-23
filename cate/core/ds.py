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

This module provides Cate's data access API.

Technical Requirements
======================

**Query data store**

:Description: Allow querying registered ECV data stores using a simple function that takes a
set of query parameters and returns data source identifiers that can be used to open
datasets in Cate.

:URD-Source:
    * CCIT-UR-DM0006: Data access to ESA CCI
    * CCIT-UR-DM0010: The data module shall have the means to attain meta-level status information
    per ECV type
    * CCIT-UR-DM0013: The CCI Toolbox shall allow filtering

----

**Add data store**

:Description: Allow adding of user defined data stores specifying the access protocol and the
layout of the data.
    These data stores can be used to access datasets.

:URD-Source:
    * CCIT-UR-DM0011: Data access to non-CCI data

----

**Open dataset**

:Description: Allow opening an ECV dataset given an identifier returned by the *data store query*.
   The dataset returned complies to the Cate common data model.
   The dataset to be returned can optionally be constrained in time and space.

:URD-Source:
    * CCIT-UR-DM0001: Data access and input
    * CCIT-UR-DM0004: Open multiple inputs
    * CCIT-UR-DM0005: Data access using different protocols>
    * CCIT-UR-DM0007: Open single ECV
    * CCIT-UR-DM0008: Open multiple ECV
    * CCIT-UR-DM0009: Open any ECV
    * CCIT-UR-DM0012: Open different formats


Verification
============

The module's unit-tests are located in
`test/test_ds.py <https://github.com/CCI-Tools/cate/blob/master/test/test_ds.py>`_
and may be executed using ``$ py.test test/test_ds.py --cov=cate/core/ds.py`` for extra code
coverage information.


Components
==========
"""

import datetime
import glob
import logging
import re
from typing import Sequence, Optional, Union, Any, Dict, Set, List, Tuple

import xarray as xr
import xcube.core.store as xcube_store
from xcube.core.select import select_subset
from xcube.util.progress import ProgressObserver
from xcube.util.progress import ProgressState

from .cdm import get_lon_dim_name, get_lat_dim_name
from .types import PolygonLike, TimeRangeLike, VarNamesLike, ValidationError
from ..util.monitor import ChildMonitor
from ..util.monitor import Monitor

_TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S"

__author__ = "Chris Bernat (Telespazio VEGA UK Ltd), ", \
             "Tonio Fincke (Brockmann Consult GmbH), " \
             "Norman Fomferra (Brockmann Consult GmbH), " \
             "Marco Zühlke (Brockmann Consult GmbH)"

URL_REGEX = re.compile(
    r'^(?:http|ftp)s?://'  # http:// or https://
    r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+(?:[A-Z]{2,6}\.?|[A-Z0-9-]{2,}\.?)|'  # domain
    r'localhost|'  # localhost...
    r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})'  # ...or ip
    r'(?::\d+)?'  # optional port
    r'(?:/?|[/?]\S+)$', re.IGNORECASE)

_LOG = logging.getLogger('cate')

DATA_STORE_POOL = xcube_store.DataStorePool()


class DataAccessWarning(UserWarning):
    """
    Warnings produced by Cate's data stores and data sources instances,
    used to report any problems handling data.
    """
    pass


class DataAccessError(Exception):
    """
    Exceptions produced by Cate's data stores and data sources instances,
    used to report any problems handling data.
    """


class NetworkError(ConnectionError):
    """
    Exceptions produced by Cate's data stores and data sources instances,
    used to report any problems with the network or in case an endpoint
    couldn't be found nor reached.
    """
    pass


class DataStoreNotice:
    """
    A short notice that can be exposed to users by data stores.
    """

    def __init__(self, id: str, title: str, content: str, intent: str = None, icon: str = None):
        """
        A short notice that can be exposed to users by data stores.

        :param id: Notice ID.
        :param title: A human-readable, plain text title.
        :param content: A human-readable, plain text title that may be formatted using Markdown.
        :param intent: Notice intent,
        may be one of "default", "primary", "success", "warning", "danger"
        :param icon: An option icon name. See https://blueprintjs.com/docs/versions/1/#core/icons
        """
        if id is None or id == "":
            raise ValueError("invalid id")
        if title is None or title == "":
            raise ValueError("invalid title")
        if content is None or content == "":
            raise ValueError("invalid content")
        if intent not in {None, "default", "primary", "success", "warning", "danger"}:
            raise ValueError("invalid intent")

        self._dict = dict(id=id, title=title, content=content, icon=icon, intent=intent)

    @property
    def id(self):
        return self._dict["id"]

    @property
    def title(self):
        return self._dict["title"]

    @property
    def content(self):
        return self._dict["content"]

    @property
    def intent(self):
        return self._dict["intent"]

    @property
    def icon(self):
        return self._dict["icon"]

    def to_dict(self):
        return dict(self._dict)


class XcubeProgressObserver(ProgressObserver):

    def __init__(self, monitor: Monitor):
        self._monitor = monitor
        self._latest_completed_work = 0.0

    def on_begin(self, state_stack: Sequence[ProgressState]):
        if len(state_stack) == 1:
            self._monitor.start(state_stack[0].label, state_stack[0].total_work)

    def on_update(self, state_stack: Sequence[ProgressState]):
        if state_stack[0].completed_work > self._latest_completed_work:
            self._monitor.progress(state_stack[0].completed_work - self._latest_completed_work,
                                   state_stack[-1].label)
            self._latest_completed_work = state_stack[0].completed_work

    def on_end(self, state_stack: Sequence[ProgressState]):
        if len(state_stack) == 1:
            self._monitor.done()


INFO_FIELD_NAMES = sorted(["abstract",
                           "bbox_minx",
                           "bbox_miny",
                           "bbox_maxx",
                           "bbox_maxy",
                           "catalog_url",
                           "catalogue_url",
                           "cci_project",
                           "creation_date",
                           "data_type",
                           "data_types",
                           "ecv",
                           "file_format",
                           "file_formats",
                           "info_url",
                           "institute",
                           "institutes",
                           "licences",
                           "platform_id",
                           "platform_ids",
                           "processing_level",
                           "processing_levels",
                           "product_string",
                           "product_strings",
                           "product_version",
                           "product_versions",
                           "publication_date",
                           "sensor_id",
                           "sensor_ids",
                           "temporal_coverage_end",
                           "temporal_coverage_start",
                           "time_frequencies",
                           "time_frequency",
                           "title",
                           "uuid"])


def get_metadata_from_descriptor(descriptor: xcube_store.DataDescriptor) -> Dict:
    metadata = dict(data_id=descriptor.data_id,
                    type_specifier=str(descriptor.type_specifier))
    if descriptor.crs:
        metadata['crs'] = descriptor.crs
    if descriptor.bbox:
        metadata['bbox'] = descriptor.bbox
    if hasattr(descriptor, 'spatial_res'):
        metadata['spatial_res'] = descriptor.spatial_res
    if descriptor.time_range:
        metadata['time_range'] = descriptor.time_range
    if descriptor.time_period:
        metadata['time_period'] = descriptor.time_period
    if hasattr(descriptor, 'attrs'):
        for name in INFO_FIELD_NAMES:
            value = descriptor.attrs.get(name, None)
            # Many values are one-element lists: turn them into scalars
            if isinstance(value, list) and len(value) == 1:
                value = value[0]
            if value is not None:
                metadata[name] = value
    if hasattr(descriptor, 'data_vars'):
        metadata['variables'] = []
        var_attrs = ['units', 'long_name', 'standard_name']
        for var_name, var_descriptor in descriptor.data_vars.items():
            var_dict = dict(name=var_name)
            if var_descriptor.attrs:
                for var_attr in var_attrs:
                    if var_attr in var_descriptor.attrs:
                        var_dict[var_attr] = var_descriptor.attrs.get(var_attr)
            metadata['variables'].append(var_dict)
    return metadata


def get_info_string_from_data_descriptor(descriptor: xcube_store.DataDescriptor) -> str:
    meta_info = get_metadata_from_descriptor(descriptor)

    max_len = 0
    for name in meta_info.keys():
        max_len = max(max_len, len(name))

    info_lines = []
    for name, value in meta_info.items():
        if name != 'variables':
            info_lines.append('%s:%s %s' % (name, (1 + max_len - len(name)) * ' ', value))

    return '\n'.join(info_lines)


def find_data_store(ds_id: str) -> Tuple[Optional[str], Optional[xcube_store.DataStore]]:
    """
    Find the data store that includes the given *ds_id*.
    This will raise an exception if the *ds_id* is given in more than one data store.

    :param ds_id:  A data source identifier.
    :return: All data sources matching the given constrains.
    """
    results = []
    for store_instance_id in DATA_STORE_POOL.store_instance_ids:
        data_store = DATA_STORE_POOL.get_store(store_instance_id)
        if data_store.has_data(ds_id):
            results.append((store_instance_id, data_store))
    if len(results) > 1:
        raise ValidationError(f'{len(results)} data sources found for the given ID {ds_id!r}')
    if len(results) == 1:
        return results[0]
    return None, None


def get_data_store_notices(datastore_id: str) -> Sequence[dict]:
    store_id = DATA_STORE_POOL.get_store_config(datastore_id).store_id

    def name_is(extension):
        return store_id == extension.name

    extensions = xcube_store.find_data_store_extensions(predicate=name_is)
    if len(extensions) == 0:
        _LOG.warning(f'Found no extension for data store {datastore_id}')
        return []
    return extensions[0].metadata.get('data_store_notices', [])


def get_data_descriptor(ds_id: str) -> Optional[xcube_store.DataDescriptor]:
    data_store_id, data_store = find_data_store(ds_id)
    if data_store:
        return data_store.describe_data(ds_id)


def open_dataset(dataset_id: str,
                 time_range: TimeRangeLike.TYPE = None,
                 region: PolygonLike.TYPE = None,
                 var_names: VarNamesLike.TYPE = None,
                 force_local: bool = False,
                 local_ds_id: str = None,
                 monitor: Monitor = Monitor.NONE) -> Tuple[Any, str]:
    """
    Open a dataset from a data source.

    :param dataset_id: The identifier of an ECV dataset. Must not be empty.
    :param time_range: An optional time constraint comprising start and end date.
           If given, it must be a :py:class:`TimeRangeLike`.
    :param region: An optional region constraint.
           If given, it must be a :py:class:`PolygonLike`.
    :param var_names: Optional names of variables to be included.
           If given, it must be a :py:class:`VarNamesLike`.
    :param force_local: Optional flag for remote data sources only
           Whether to make a local copy of data source if it's not present
    :param local_ds_id: Optional, fpr remote data sources only
           Local data source ID for newly created copy of remote data source
    :param monitor: A progress monitor
    :return: A tuple consisting of a new dataset instance and its id
    """
    if not dataset_id:
        raise ValidationError('No data source given')

    data_store_id, data_store = find_data_store(ds_id=dataset_id)
    if not data_store:
        raise ValidationError(f"No data store found that contains the ID '{dataset_id}'")

    type_spec = None
    potential_type_specs = data_store.get_type_specifiers_for_data(dataset_id)
    for potential_type_spec in potential_type_specs:
        if xcube_store.TYPE_SPECIFIER_CUBE.is_satisfied_by(potential_type_spec):
            type_spec = potential_type_spec
            break
    if type_spec is None:
        for potential_type_spec in potential_type_specs:
            if xcube_store.TYPE_SPECIFIER_DATASET.is_satisfied_by(potential_type_spec):
                type_spec = potential_type_spec
                break
    if type_spec is None:
        raise ValidationError(f"Could not open '{dataset_id}' as dataset.")
    openers = data_store.get_data_opener_ids(dataset_id, type_spec)
    if len(openers) == 0:
        raise DataAccessError(f'Could not find an opener for "{dataset_id}".')
    opener_id = openers[0]

    total_amount_of_work = 20 if force_local else 10

    open_schema = data_store.get_open_data_params_schema(dataset_id, opener_id)
    open_args = {}
    subset_args = {}
    if var_names:
        var_names_list = VarNamesLike.convert(var_names)
        if _in_schema('variable_names', open_schema):
            open_args['variable_names'] = var_names_list
        elif _in_schema('drop_variables', open_schema):
            data_desc = data_store.describe_data(dataset_id, type_spec)
            open_args['drop_variables'] = [var_name for var_name in data_desc.data_vars.keys()
                                           if var_name not in var_names_list]
        else:
            subset_args['var_names'] = var_names_list
            total_amount_of_work += 1
    if time_range:
        time_range = TimeRangeLike.convert(time_range)
        time_range = [datetime.datetime.strftime(time_range[0], '%Y-%m-%d'),
                      datetime.datetime.strftime(time_range[1], '%Y-%m-%d')]
        if _in_schema('time_range', open_schema):
            open_args['time_range'] = time_range
        else:
            subset_args['time_range'] = time_range
            total_amount_of_work += 1
    if region:
        bbox = list(PolygonLike.convert(region).bounds)
        if _in_schema('bbox', open_schema):
            open_args['bbox'] = bbox
        else:
            subset_args['bbox'] = bbox
            total_amount_of_work += 1

    monitor.start('Open dataset', total_amount_of_work)
    observer = XcubeProgressObserver(ChildMonitor(monitor, 10))
    observer.activate()
    dataset = data_store.open_data(data_id=dataset_id, opener_id=opener_id, **open_args)
    observer.deactivate()
    dataset = select_subset(dataset, **subset_args)
    monitor.progress(len(subset_args))
    if force_local:
        observer2 = XcubeProgressObserver(ChildMonitor(monitor, 10))
        observer2.activate()
        dataset, dataset_id = make_local(data=dataset,
                                         local_name=local_ds_id)
        observer2.deactivate()
    monitor.done()
    return dataset, dataset_id


def _in_schema(param: str, schema):
    return param in schema.properties or param in schema.additional_properties


def make_local(data: Any, local_name: Optional[str] = None) -> Tuple[Any, str]:
    local_store = DATA_STORE_POOL.get_store('local')
    if not local_store:
        raise ValueError('Cannot initialize `local` DataStore')

    local_data_id = local_store.write_data(data=data, data_id=local_name)
    return local_store.open_data(data_id=local_data_id), local_data_id


def add_as_local(data_source_id: str, paths: Union[str, Sequence[str]] = None) -> Tuple[Any, str]:
    paths = _resolve_input_paths(paths)
    if not paths:
        raise ValueError("No paths found")
    # todo also support geodataframes
    if len(paths) == 1:
        ds = xr.open_dataset(paths[0])
    else:
        ds = xr.open_mfdataset(paths=paths)
    return make_local(ds, data_source_id)


def _resolve_input_paths(paths: Union[str, Sequence[str]]) -> Sequence[str]:
    # very similar code is used in nc2zarr
    resolved_input_files = []
    if isinstance(paths, str):
        resolved_input_files.extend(glob.glob(paths, recursive=True))
    elif paths is not None and len(paths):
        for file in paths:
            resolved_input_files.extend(glob.glob(file, recursive=True))
    # Get rid of doubles, but preserve order
    seen_input_files = set()
    unique_input_files = []
    for input_file in resolved_input_files:
        if input_file not in seen_input_files:
            unique_input_files.append(input_file)
            seen_input_files.add(input_file)
    return unique_input_files


def get_spatial_ext_chunk_sizes(ds_or_path: Union[xr.Dataset, str]) -> Dict[str, int]:
    """
    Get the spatial, external chunk sizes for the latitude and longitude dimensions
    of a dataset as provided in a variable's encoding object.

    :param ds_or_path: An xarray dataset or a path to file that can be opened by xarray.
    :return: A mapping from dimension name to external chunk sizes.
    """
    if isinstance(ds_or_path, str):
        ds = xr.open_dataset(ds_or_path, decode_times=False)
    else:
        ds = ds_or_path
    lon_name = get_lon_dim_name(ds)
    lat_name = get_lat_dim_name(ds)
    if lon_name and lat_name:
        chunk_sizes = get_ext_chunk_sizes(ds, {lat_name, lon_name})
    else:
        chunk_sizes = None
    if isinstance(ds_or_path, str):
        ds.close()
    return chunk_sizes


def get_ext_chunk_sizes(ds: xr.Dataset, dim_names: Set[str] = None,
                        init_value=0, map_fn=max, reduce_fn=None) -> Dict[str, int]:
    """
    Get the external chunk sizes for each dimension of a dataset as provided in a variable's encoding object.

    :param ds: The dataset.
    :param dim_names: The names of dimensions of data variables whose external chunking should be collected.
    :param init_value: The initial value (not necessarily a chunk size) for mapping multiple different chunk sizes.
    :param map_fn: The mapper function that maps a chunk size from a previous (initial) value.
    :param reduce_fn: The reducer function the reduces multiple mapped chunk sizes to a single one.
    :return: A mapping from dimension name to external chunk sizes.
    """
    agg_chunk_sizes = None
    for var_name in ds.variables:
        var = ds[var_name]
        if var.encoding:
            chunk_sizes = var.encoding.get('chunksizes')
            if chunk_sizes \
                    and len(chunk_sizes) == len(var.dims) \
                    and (not dim_names or dim_names.issubset(set(var.dims))):
                for dim_name, size in zip(var.dims, chunk_sizes):
                    if not dim_names or dim_name in dim_names:
                        if agg_chunk_sizes is None:
                            agg_chunk_sizes = dict()
                        old_value = agg_chunk_sizes.get(dim_name)
                        agg_chunk_sizes[dim_name] = map_fn(size, init_value if old_value is None else old_value)
    if agg_chunk_sizes and reduce_fn:
        agg_chunk_sizes = {k: reduce_fn(v) for k, v in agg_chunk_sizes.items()}
    return agg_chunk_sizes


def format_variables_info_string(descriptor: xcube_store.DataDescriptor):
    """
    Return some textual information about the variables described by this DataDescriptor.
    Useful for CLI / REPL applications.
    :param variables:
    :return:
    """
    meta_info = get_metadata_from_descriptor(descriptor)
    variables = meta_info.get('variables', [])
    if len(variables) == 0:
        return 'No variables information available.'

    info_lines = []
    for variable in variables:
        info_lines.append('%s (%s):' % (variable.get('name', '?'), variable.get('units', '-')))
        info_lines.append('  Long name:        %s' % variable.get('long_name', '?'))
        info_lines.append('  CF standard name: %s' % variable.get('standard_name', '?'))
        info_lines.append('')

    return '\n'.join(info_lines)


def format_cached_datasets_coverage_string(cache_coverage: dict) -> str:
    """
    Return a textual representation of information about cached, locally available data sets.
    Useful for CLI / REPL applications.
    :param cache_coverage:
    :return:
    """
    if not cache_coverage:
        return 'No information about cached datasets available.'

    info_lines = []
    for date_from, date_to in sorted(cache_coverage.items()):
        info_lines.append('{date_from} to {date_to}'
                          .format(date_from=date_from.strftime('%Y-%m-%d'),
                                  date_to=date_to.strftime('%Y-%m-%d')))

    return '\n'.join(info_lines)
