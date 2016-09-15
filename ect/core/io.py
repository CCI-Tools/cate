# The MIT License (MIT)
# Copyright (c) 2016 by the ECT Development Team and contributors
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

This module provides ECT's data access API.

Technical Requirements
======================

**Query data store**

:Description: Allow querying registered ECV data stores using a simple function that takes a set of query parameters
    and returns data source identifiers that can be used to open respective ECV dataset in the ECT.

:URD-Source:
    * CCIT-UR-DM0006: Data access to ESA CCI
    * CCIT-UR-DM0010: The data module shall have the means to attain meta-level status information per ECV type
    * CCIT-UR-DM0013: The CCI Toolbox shall allow filtering

----

**Add data store**

:Description: Allow adding of user defined data stores specifying the access protocol and the layout of the data.
    These data stores can be used to access datasets.

:URD-Source:
    * CCIT-UR-DM0011: Data access to non-CCI data

----

**Open dataset**

:Description: Allow opening an ECV dataset given an identifier returned by the *data store query*.
   The dataset returned complies to the ECT common data model.
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

The module's unit-tests are located in `test/test_io.py <https://github.com/CCI-Tools/ect-core/blob/master/test/test_io.py>`_
and may be executed using ``$ py.test test/test_io.py --cov=ect/core/io.py`` for extra code coverage information.


Components
==========
"""
import ftplib
import json
import os
import urllib.parse
from abc import ABCMeta, abstractmethod
from collections import OrderedDict
from datetime import datetime, date, timedelta
from io import StringIO, IOBase
from typing import Sequence, Union, List, Tuple, Mapping, Any

import xarray as xr
from ect.core.cdm import Schema
from ect.core.monitor import Monitor, ConsoleMonitor
from ect.core.util import to_datetime

Time = Union[str, datetime]
TimeRange = Tuple[Time, Time]


class DataSource(metaclass=ABCMeta):
    """
    An abstract data source from which datasets can be retrieved.
    """

    @property
    @abstractmethod
    def name(self) -> str:
        """Human-readable data source name."""

    @property
    @abstractmethod
    def schema(self) -> Schema:
        """The data :py:class:`Schema` for any dataset provided by this data source."""

    @property
    @abstractmethod
    def data_store(self) -> 'DataStore':
        """The data store to which this data source belongs."""

    @abstractmethod
    def open_dataset(self, time_range=None) -> xr.Dataset:
        """
        Open a dataset with the given constraints.

        :param time_range: a tuple of datetime or str, optional. To limits the dataset in time.
        :return: A xarray dataset or a Shapfile TBD.
        """

    def matches_filter(self, name=None) -> bool:
        """Test if this data source matches the given *constraints*."""
        if name and name != self.name:
            return False
        return True

    # noinspection PyMethodMayBeStatic
    def sync(self, time_range: TimeRange = (None, None), monitor: Monitor = Monitor.NULL):
        """
        Synchronize remote data with locally stored data.
        The default implementation does nothing.

        :param time_range: a tuple of two datetime objects or datetime strings of the form ``YYYY-MM-DD``
        :param monitor: a progress monitor.
        """
        pass

    @property
    def info_string(self):
        """Return some textual information about this data source. Useful for CLI / REPL applications."""
        return self.name

    def __str__(self):
        return self.info_string

    @abstractmethod
    def _repr_html_(self):
        """Provide an HTML representation of this object for IPython."""


class DataStore(metaclass=ABCMeta):
    """Represents a data store of data sources."""

    # TODO (mzuehlke, forman, 20160603): define constraints --> have a look at Iris Constraint class
    @abstractmethod
    def query(self, name=None) -> Sequence[DataSource]:
        """
        Retrieve data sources in this data store using the given constraints.

        :param name: Name of the data source.
        :return: Sequence of data sources.
        """

    @abstractmethod
    def _repr_html_(self):
        """Provide an HTML representation of this object for IPython."""


class DataStoreRegistry:
    """
    Registry of :py:class:`DataStore` objects.
    """

    def __init__(self):
        self._data_stores = dict()

    def get_data_store(self, name: str) -> DataStore:
        return self._data_stores.get(name, None)

    def get_data_stores(self) -> Sequence[DataStore]:
        return self._data_stores.values()

    def add_data_store(self, name: str, data_store: DataStore):
        self._data_stores[name] = data_store

    def remove_data_store(self, name: str):
        del self._data_stores[name]

    def __len__(self):
        return len(self._data_stores)

    def __str__(self):
        return self.__repr__()

    def __repr__(self):
        import pprint
        return pprint.pformat(self._data_stores)

    def _repr_html_(self):
        rows = []
        for name, data_store in self._data_stores.items():
            rows.append('<tr><td>%s</td><td>%s</td></tr>' % (name, repr(data_store)))
        return '<table>%s</table>' % '\n'.join(rows)


#: The data data store registry of type :py:class:`DataStoreRegistry`.
#: Use it add new data stores to ECT.
DATA_STORE_REGISTRY = DataStoreRegistry()


def query_data_sources(data_stores: Union[DataStore, Sequence[DataStore]] = None, name=None) -> Sequence[DataSource]:
    """Query the data store(s) for data sources matching the given constrains.

    Parameters
    ----------
    data_stores : DataStore or Sequence[DataStore]
       If given these data stores will be queried. Otherwise all registered data stores will be used.
    name : str, optional
       The name of a data source.

    Returns
    -------
    data_source : List[DataSource]
       All data sources matching the given constrains.

    See Also
    --------
    open_dataset
    """
    if data_stores is None:
        data_store_list = DATA_STORE_REGISTRY.get_data_stores()
    elif isinstance(data_stores, DataStore):
        data_store_list = [data_stores]
    else:
        data_store_list = data_stores
    results = []
    # noinspection PyTypeChecker
    for data_store in data_store_list:
        results.extend(data_store.query(name))
    return results


def open_dataset(data_source: Union[DataSource, str], time_range=None) -> xr.Dataset:
    """Load and decode a dataset.

    Parameters
    ----------
    data_source : str or DataSource
       Strings are interpreted as the identifier of an ECV dataset.
    time_range : a tuple of datetime or str, optional
       The *time_range*, if given, limits the dataset in time.

    Returns
    -------
    dataset : xarry.Dataset
       The newly created dataset  or a Shapfile TBD..

    See Also
    --------
    query_data_sources
    """
    if data_source is None:
        raise ValueError('No data_source given')

    if isinstance(data_source, str):
        data_store_list = DATA_STORE_REGISTRY.get_data_stores()
        data_sources = query_data_sources(data_store_list, name=data_source)
        if len(data_sources) == 0:
            raise ValueError("No data_source found for the given query term '%s'" % data_source)
        elif len(data_sources) > 1:
            raise ValueError("%s data_sources found for the given query term '%s'" % (len(data_sources), data_source))
        data_source = data_sources[0]
    return data_source.open_dataset(time_range)


class FileSetDataSource(DataSource):
    """A class representing the a specific file set with the meta information belonging to it.

    Parameters
    ----------
    name : str
        The name of the file set
    base_dir : str
        The base directory
    file_pattern : str
        The file pattern with wildcards for year, month, and day
    fileset_info : FileSetInfo
        The file set info generated by a scanning, can be None

    Returns
    -------
    new  : FileSetDataSource
    """

    def __init__(self,
                 file_set_data_store: 'FileSetDataStore',
                 name: str,
                 base_dir: str,
                 file_pattern: str,
                 fileset_info: 'FileSetInfo' = None):
        self._file_set_data_store = file_set_data_store
        self._name = name
        self._base_dir = base_dir
        self._file_pattern = file_pattern
        self._fileset_info = fileset_info

    @property
    def name(self):
        return self._name

    @property
    def schema(self) -> Schema:
        # TODO (forman, 20160623): let FileSetDataSource return a valid schema
        return None

    @property
    def data_store(self) -> 'FileSetDataStore':
        return self._file_set_data_store

    def open_dataset(self, time_range=None) -> xr.Dataset:
        paths = self.resolve_paths(time_range)
        unique_paths = list(set(paths))
        existing_paths = [p for p in unique_paths if os.path.exists(p)]
        if len(existing_paths) == 0:
            raise ValueError('No local file available. Consider syncing the dataset.')
        # TODO (mzuehlke, 20160603): differentiate between xarray and shapefile
        # return open_xarray_dataset(existing_paths)
        # TODO (Gailis, 20160729): The normal way still does not work for me.
        return xr.open_mfdataset(existing_paths, concat_dim='time')

    def to_json_dict(self):
        """
        Return a JSON-serializable dictionary representation of this object.

        :return: A JSON-serializable dictionary
        """
        fsds_dict = OrderedDict()
        fsds_dict['name'] = self.name
        fsds_dict['base_dir'] = self._base_dir
        fsds_dict['file_pattern'] = self._file_pattern
        if self._fileset_info:
            fsds_dict['fileset_info'] = self._fileset_info.to_json_dict()
        return fsds_dict

    @property
    def _full_pattern(self) -> str:
        return self._base_dir + '/' + self._file_pattern

    def resolve_paths(self, time_range: TimeRange = (None, None)) -> List[str]:
        """Return a list of all paths between the given times.

        For all dates, including the first and the last time, the wildcard in the pattern is resolved for the date.

        Parameters
        ----------
        time_range : a tuple of datetime or str, optional
               The *time_range*, if given, limits the dataset in time.
               The first date of the time range, can be None if the file set has a *start_time*.
               In this case the *start_time* is used.
               The last date of the time range, can be None if the file set has a *end_time*.
               In this case the *end_time* is used.
        """
        return [self.data_store.root_dir + '/' + p for p in self.resolve_base_paths(time_range)]

    def resolve_base_paths(self, time_range: TimeRange = (None, None)) -> List[str]:
        """Return a list of all paths between the given times.

        For all dates, including the first and the last time, the wildcard in the pattern is resolved for the date.

        Parameters
        ----------
        time_range : a tuple of datetime or str, optional
               The *time_range*, if given, limits the dataset in time.
               The first date of the time range, can be None if the file set has a *start_time*.
               In this case the *start_time* is used.
               The last date of the time range, can be None if the file set has a *end_time*.
               In this case the *end_time* is used.
        """

        date1 = to_datetime(time_range[0], self._fileset_info.start_time if self._fileset_info else None)
        date2 = to_datetime(time_range[1], self._fileset_info.end_time if self._fileset_info else None)

        if date1 is None:
            raise ValueError("illegal time_range: can't determine start of interval")

        if date2 is None:
            raise ValueError("illegal time_range: can't determine end of interval")

        if date1 > date2:
            raise ValueError("start time '%s' is after end time '%s'" % (date1, date2))

        return [self._resolve_base_path(date1 + timedelta(days=i)) for i in range((date2 - date1).days + 1)]

    def _resolve_base_path(self, date: datetime):
        resolved_path = self._file_pattern
        resolved_path = resolved_path.replace('{YYYY}', '%04d' % date.year)
        resolved_path = resolved_path.replace('{MM}', '%02d' % date.month)
        resolved_path = resolved_path.replace('{DD}', '%02d' % date.day)
        return self._base_dir + '/' + resolved_path

    def sync(self, time_range: TimeRange = (None, None), monitor: Monitor = Monitor.NULL):
        """
        Synchronize remote data with locally stored data.

        :param time_range: a tuple of two datetime objects or datetime strings of the form ``YYYY-MM-DD``
        :param monitor: a progress monitor.
        """
        assert self._file_set_data_store.remote_url

        url = urllib.parse.urlparse(self._file_set_data_store.remote_url)
        if url.scheme != 'ftp':
            raise ValueError("invalid remote URL: cannot deal with scheme %s" % repr(url.scheme))
        ftp_host_name = url.hostname
        ftp_base_dir = url.path

        expected_remote_files = self._get_expected_remote_files(time_range)
        with monitor.starting('Synchronising %s' % self._name, len(expected_remote_files)):
            try:
                with ftplib.FTP(ftp_host_name) as ftp:
                    ftp.login()
                    self._sync_files(ftp, ftp_base_dir, expected_remote_files, monitor)
            except ftplib.Error as ftp_err:
                if not monitor.is_cancelled():
                    print('FTP error: %s' % ftp_err)

    def _sync_files(self, ftp, ftp_base_dir, expected_remote_files, monitor: Monitor):
        for expected_dir_path, expected_filename_dict in expected_remote_files.items():
            if monitor.is_cancelled():
                return

            ftp_dir = ftp_base_dir + '/' + expected_dir_path
            try:
                ftp.cwd(ftp_dir)
            except ftplib.Error:
                # Note: If we can't CWD to ftp_dir, this usually means,
                # expected_dir_path may refer to a time range that is not covered remotely.
                monitor.progress(work=1)
                continue

            try:
                remote_dir_content = ftp.mlsd(facts=['type', 'size', 'modify'])
            except ftplib.Error:
                # Note: If we can't MLSD the CWD ftp_dir, we have a problem.
                monitor.progress(work=1)
                continue

            files_to_download = OrderedDict()

            file_set_size = 0
            for existing_filename, facts in remote_dir_content:
                if monitor.is_cancelled():
                    return
                if facts.get('type', None) == 'file' and existing_filename in expected_filename_dict:
                    # update expected_filename_dict with facts of existing_filename
                    expected_filename_dict[existing_filename] = facts
                    file_size = int(facts.get('size', '-1'))
                    if file_size > 0:
                        file_set_size += file_size
                    # TODO (forman, 20160619): put also 'modify' in file_info, to update outdated local files
                    existing_file_info = dict(size=file_size)
                    files_to_download[existing_filename] = existing_file_info

            if files_to_download:
                size_in_mibs = file_set_size / (1024 * 1024)
                print('Synchronising %s, contains %d file(s), total size %.1f MiB' % (expected_dir_path,
                                                                                      len(files_to_download),
                                                                                      size_in_mibs))
                local_dir = os.path.join(self._file_set_data_store.root_dir, expected_dir_path)
                os.makedirs(local_dir, exist_ok=True)
                child_monitor = monitor.child(work=1)
                with child_monitor.starting(expected_dir_path, len(files_to_download)):
                    for existing_filename, existing_file_info in files_to_download.items():
                        if monitor.is_cancelled():
                            return
                        # TODO (forman, 20160619): design problem here, download_monitor should be a child of monitor
                        # but I want the child monitor to be a individual ConsoleMonitor as well.
                        download_monitor = ConsoleMonitor(stay_in_line=True, progress_bar_size=34)
                        downloader = FtpDownloader(ftp,
                                                   existing_filename, existing_file_info, local_dir,
                                                   download_monitor)
                        downloader.start()
                        if download_monitor.is_cancelled():
                            monitor.cancel()

            monitor.progress(work=1, msg=expected_dir_path)

    def _get_expected_remote_files(self, time_range: TimeRange = (None, None)) -> Mapping[str, Mapping[str, Any]]:
        expected_remote_files = OrderedDict()
        expected_remote_files_list = self.resolve_base_paths(time_range)
        for expected_remote_file in expected_remote_files_list:
            # we don't use os.path here, because FTP always uses '/' separators
            components = expected_remote_file.split('/')
            dir_path = '/'.join(components[0:-1])
            filename = components[-1]
            filename_dict = expected_remote_files.get(dir_path, None)
            if filename_dict is None:
                filename_dict = OrderedDict()
                expected_remote_files[dir_path] = filename_dict
            filename_dict[filename] = None
        return expected_remote_files

    def __repr__(self):
        return "FileSetDataSource(%s, %s, %s)" % (repr(self._name), repr(self._base_dir), repr(self._file_pattern))

    @property
    def info_string(self):
        table_data = self.get_table_data()
        if self._fileset_info:
            table_data.update(self._fileset_info.get_table_data())
        return '\n'.join(['%s: %s' % (name, value)
                          for name, value in table_data.items()])

    def _repr_html_(self):
        import html
        table_data = self.get_table_data()
        if self._fileset_info:
            table_data.update(self._fileset_info.get_table_data())
        rows = '\n'.join(['<tr><td>%s</td><td><strong>%s</strong></td></tr>' % (name, html.escape(str(value)))
                          for name, value in table_data.items()])
        return '<table style="border:0;">%s</table>' % rows

    def get_table_data(self):
        return OrderedDict([('Name', self._name),
                            ('Base directory', self._base_dir),
                            ('File pattern', self._file_pattern)])


class FtpDownloader:
    def __init__(self,
                 ftp: ftplib.FTP,
                 filename: str,
                 file_info: dict,
                 local_dir: str,
                 monitor: Monitor,
                 block_size: int = 10 * 1024):
        self._ftp = ftp
        self._filename = filename
        self._local_dir = local_dir
        self._monitor = monitor
        self._block_size = block_size
        self._file_size = file_info.get('size', 0)
        self._bytes_written = 0
        self._fp = None
        self._message = None

    def start(self) -> bool:
        with self._monitor.starting(self._filename, total_work=self._file_size):
            return self._start()

    def _start(self) -> bool:
        local_file = os.path.join(self._local_dir, self._filename)
        if os.path.exists(local_file):
            local_size = os.path.getsize(local_file)
            # TODO (forman, 20160619): use 'modify' from file_info, to update outdated local files
            if local_size > 0 and local_size == self._file_size:
                self._monitor.progress(msg='local file is up-to-date')
                return True
            else:
                # remove the old outdated file
                os.remove(local_file)
        rest = None
        filename_incomplete = self._filename + '.incomplete'
        local_file_incomplete = os.path.join(self._local_dir, filename_incomplete)
        if os.path.exists(local_file_incomplete):
            # TODO (forman, 20160619): reuse what has already been downloaded, then set variable 'rest' accordingly.
            # Brute force approach here: delete what has already been downloaded
            os.remove(local_file_incomplete)
        error_msg = None
        with open(local_file_incomplete, 'wb') as fp:
            self._fp = fp
            try:
                self._ftp.retrbinary('RETR ' + self._filename, self.on_new_block, blocksize=self._block_size, rest=rest)
            except KeyboardInterrupt:
                error_msg = 'download cancelled'
            except ftplib.Error as ftp_err:
                error_msg = 'download error: ' + str(ftp_err)
        # sys.stdout.write('\n')
        if error_msg is None:
            os.rename(local_file_incomplete, local_file)
        else:
            self._monitor.progress(msg=error_msg)
            os.remove(local_file_incomplete)
        return error_msg is None

    def on_new_block(self, bytes_block):
        if self._monitor.is_cancelled():
            raise KeyboardInterrupt()
        self._fp.write(bytes_block)
        block_size = len(bytes_block)
        self._bytes_written += block_size
        self._monitor.progress(block_size)


class FileSetInfo:
    def __init__(self,
                 info_update_time: Union[str, datetime],
                 start_time: Union[str, datetime],
                 end_time: Union[str, datetime],
                 num_files: int,
                 size_in_mb: int):
        self._info_update_time = to_datetime(info_update_time, None)
        self._start_time = to_datetime(start_time, None)
        self._end_time = to_datetime(end_time, None)
        self._num_files = num_files
        self._size_in_mb = size_in_mb

    def to_json_dict(self):
        """
        Return a JSON-serializable dictionary representation of this object.

        :return: A JSON-serializable dictionary
        """
        return dict(info_update_time=self._info_update_time,
                    start_time=self._start_time,
                    end_time=self._end_time,
                    num_files=self._num_files,
                    size_in_mb=self._size_in_mb)

    @property
    def info_update_time(self):
        return self._info_update_time

    @property
    def start_time(self):
        return self._start_time

    @property
    def end_time(self):
        return self._end_time

    @property
    def num_files(self):
        return self._num_files

    @property
    def size_in_mb(self):
        return self._size_in_mb

    def info_string(self):
        table_data = self.get_table_data()
        return '\n'.join(['%s:\t\t%s' % (name, str(value)) for name, value in table_data.items()])

    def _repr_html_(self):
        import html
        table_data = self.get_table_data()
        return '<table style="border:0;">%s</table>' % '\n'.join(
            ['<tr><td>%s</td><td><strong>%s</strong></td></tr>' % (name, html.escape(str(value)))
             for name, value in table_data.items()])

    def get_table_data(self):
        table_data = OrderedDict([('Last update time', self._info_update_time),
                                  ('Data start time', self._start_time),
                                  ('Data stop time', self._end_time),
                                  ('#Files', self._num_files),
                                  ('Size (MB)', self._size_in_mb),
                                  ])
        return table_data


class FileSetDataStore(DataStore):
    """
    A data store for a fileset in the the operating system's file system.
    It is composed of data sources of type :py:class:`FileSetDataSource` that are represented by
    the operating system's file system.

    :param root_dir: The path to the fileset's root directory.
    :param remote_url: Optional URL of the data store's remote service.
    """

    def __init__(self, root_dir: str, remote_url: str = None):
        self._root_dir = root_dir
        self._remote_url = remote_url
        self._data_sources = []

    @property
    def root_dir(self) -> str:
        """The path to the fileset's root directory."""
        return self._root_dir

    @property
    def remote_url(self) -> str:
        """Optional URL of the data store's remote service."""
        return self._remote_url

    def query(self, name=None) -> Sequence[DataSource]:
        return [ds for ds in self._data_sources if ds.matches_filter(name)]

    def load_from_json(self, json_fp_or_str: Union[str, IOBase]):
        if isinstance(json_fp_or_str, str):
            fp = StringIO(json_fp_or_str)
        else:
            fp = json_fp_or_str
        data_store_dict = json.load(fp)
        remote_url = data_store_dict.get('remote_url', self._remote_url)
        data_sources_json = data_store_dict.get('data_sources', [])
        data_sources = []
        for data in data_sources_json:
            file_set_info = None
            if 'start_date' in data and 'end_date' in data and 'num_files' in data and 'size_mb' in data:
                # TODO (mzuehlke, 20160603): used named parameters
                file_set_info = FileSetInfo('2016-05-26 15:32:52',
                                            # TODO (mzuehlke, 20160603): include scan time in JSON
                                            data['start_date'],
                                            data['end_date'],
                                            data['num_files'],
                                            data['size_mb'])

            # TODO (mzuehlke, 20160603): used named parameters
            file_set_data_source = FileSetDataSource(self,
                                                     # TODO (mzuehlke, 20160603): change this in the JSON file
                                                     data['name'].replace('/', '_').upper(),
                                                     data['base_dir'],
                                                     data['file_pattern'],
                                                     fileset_info=file_set_info)
            data_sources.append(file_set_data_source)

        self._remote_url = remote_url
        self._data_sources.extend(data_sources)

    @classmethod
    def from_json(cls, root_dir: str, json_fp_or_str: Union[str, IOBase]) -> 'FileSetDataStore':
        data_store = FileSetDataStore(root_dir)
        data_store.load_from_json(json_fp_or_str)
        return data_store

    def __repr__(self):
        return "FileSetFileStore(%s)" % repr(self._root_dir)

    def _repr_html_(self):
        rows = []
        row_count = 0
        for ds in self._data_sources:
            row_count += 1
            rows.append('<tr><td><strong>%s</strong></td><td>%s</td></tr>' % (row_count, ds._repr_html_()))
        return '<p>Contents of FileSetFileStore for root <code>%s<code></p><table>%s</table>' % (
            self._root_dir, '\n'.join(rows))


