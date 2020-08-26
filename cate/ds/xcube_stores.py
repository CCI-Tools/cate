# The MIT License (MIT)
# Copyright (c) 2020 by the ESA CCI Toolbox development team and contributors
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

import xcube.core.store as x_store
import xcube.util.extension as x_extension

from collections import OrderedDict
from typing import Any
from typing import Optional

from cate.core.ds import DataSource
from cate.core.ds import DataStore
from cate.core.ds import DataStoreNotice
from cate.core.ds import DATA_STORE_REGISTRY
from cate.core.types import PolygonLike
from cate.core.types import TimeRangeLike
from cate.core.types import VarNamesLike
from cate.util.monitor import Monitor

def add_xcube_data_stores():
    data_store_extensions = x_store.find_data_store_extensions()
    for extension in data_store_extensions:
        DATA_STORE_REGISTRY.add_data_store(XcubeDataStore(extension))
        # if extension.is_lazy:
        #     DATA_STORE_REGISTRY.add_data_store(XcubeDataStore(extension.name,
        #                                                       metadata=extension.metadata,
        #                                                       loader=extension.loader))
        # else:
        #     DATA_STORE_REGISTRY.add_data_store(XcubeDataStore(extension.name,
        #                                                       metadata=extension.metadata,
        #                                                       component = extension.component))

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
                           "data_types",
                           "cci_project"
                           ])


class XcubeDataStore(DataStore):

    def __init__(self, extension: x_extension.Extension):
        super().__init__(ds_id=extension.name,
                       title=extension.metadata.get('description', extension.name),
                       is_local=False
                       )
        self._id = extension.name
        self._title = extension.metadata.get('description', '')
        self._notices = [DataStoreNotice('xcube',
                                         'XCube Store',
                                         "This Data Store is accessed via the XCube Store Framework",
                                         intent="primary",
                                         icon="info-sign")
                                         ]
        self._xcube_extension = extension
        self._xcube_store = None
        self._data_sources = []

    @property
    def notices(self):
        return self._notices

    def query(self, ds_id: str = None, query_expr: str = None, monitor: Monitor = Monitor.NONE):
        pass

    def invalidate(self):
        if not self.is_local:
            pass
        #todo implement

    def get_updates(self, reset=False):
        if not self.is_local:
            pass
        # todo implement

    def _ensure_xcube_store_loaded(self):
        if not self._xcube_store:
            self._xcube_store = self._xcube_extension.component

    def _init_data_sources(self):
        self._ensure_xcube_store_loaded()
        if not self._data_sources:
            self._data_sources = []
            data_ids = self._xcube_store.get_data_ids()
            for data_id in data_ids:
                # it would be significantly better if we could access more than one piece of data at once
                data_descriptor = self._xcube_store.describe_data(data_id)
                self._data_sources.append(XCubeDataSource(data_descriptor))


    def _repr_html_(self) -> str:
        self._init_data_sources()
        rows = []
        row_count = 0
        for data_source in self._data_sources:
            row_count += 1
            # noinspection PyProtectedMember
            rows.append('<tr><td><strong>%s</strong></td><td>%s</td></tr>' % (row_count, data_source._repr_html_()))
        return '<p>Contents of FileSetFileStore</p><table>%s</table>' % ('\n'.join(rows))


class XCubeDataSource(DataSource):
    def __init__(self,
                 data_store: DataStore,
                 data_descriptor: x_store.DataDescriptor):
        self._data_store = data_store
        self._descriptor = data_descriptor
        # todo add function to harmonize meta info
        # self._meta_info = self._data_descriptor.attrs if self._data_descriptor.attrs else {}
        # self._harmonize_meta_info()

    def _harmonize_meta_info(self):
        if 'title' not in self._meta_info:
            self._meta_info['title'] = self._descriptor.data_id

    @property
    def id(self):
        return self._descriptor.data_id

    @property
    def temporal_coverage(self, monitor: Monitor = Monitor.NONE):
        # todo implement
        return None

    @property
    def data_store(self):
        return self._data_store

    def open_dataset(self,
                     time_range: TimeRangeLike.TYPE = None,
                     region: PolygonLike.TYPE = None,
                     var_names: VarNamesLike.TYPE = None,
                     protocol: str = None,
                     monitor: Monitor = Monitor.NONE) -> Any:
        # todo implement
        pass

    def make_local(self,
                   local_name: str,
                   local_id: str = None,
                   time_range: TimeRangeLike.TYPE = None,
                   region: PolygonLike.TYPE = None,
                   var_names: VarNamesLike.TYPE = None,
                   monitor: Monitor = Monitor.NONE) -> Optional['DataSource']:
        # todo implement
        pass

    @property
    def title(self) -> Optional[str]:
        if self._descriptor.attrs:
            return self._descriptor.attrs.get('title', '')
        return ''

    @property
    def meta_info(self) -> Optional[dict]:
        if not self._meta_info:
            self._meta_info = OrderedDict()
            for name in INFO_FIELD_NAMES:
                value = self._descriptor.attrs.get(name, None)
                # Many values in the index JSON are one-element lists: turn them into scalars
                if isinstance(value, list) and len(value) == 1:
                    value = value[0]
                if value is not None:
                    self._meta_info[name] = value
            self._meta_info['variables'] = self.variables_info
        return self._meta_info

    @property
    def variables_info(self):
        variables = []
        if self._descriptor.data_vars:
            for variable_descriptor in self._descriptor.data_vars:
                variables.append(dict(name=variable_descriptor.name,
                                      units=variable_descriptor.attrs.get('units', '') if variable_descriptor.attrs else None,
                                      long_name=variable_descriptor.attrs.get('long_name', '')  if variable_descriptor.attrs else None))
        return variables

    def _repr_html_(self):
        return self.id
