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

__author__ = "Norman Fomferra (Brockmann Consult GmbH), " \
             "Marco Zühlke (Brockmann Consult GmbH)" \
             "Helge Dzierzon (Brockmann Consult GmbH)"

import concurrent.futures
import datetime
import importlib.resources
import json
import logging
import os
import sys
import tempfile
import time
import zipfile
from typing import Sequence, Any, Optional

import fiona
import geopandas as gpd
import numpy as np
import tornado.gen
import tornado.web
import xarray as xr
from tornado import escape

from .geojson import write_feature_collection, write_feature
from ..conf import get_config
from ..conf.defaults import \
    WORKSPACE_CACHE_DIR_NAME, \
    WEBAPI_WORKSPACE_FILE_TILE_CACHE_CAPACITY, \
    WEBAPI_WORKSPACE_MEM_TILE_CACHE_CAPACITY, \
    WEBAPI_USE_WORKSPACE_IMAGERY_CACHE
from ..core.cdm import get_tiling_scheme
from ..core.types import GeoDataFrame
from ..core.wsmanag import WorkspaceManager
from ..util.cache import Cache, MemoryCacheStore, FileCacheStore
from ..util.im import ImagePyramid, TransformArrayImage, ColorMappedRgbaImage
from ..util.im.ds import NaturalEarth2Image
from ..util.misc import cwd
from ..util.misc import is_debug_mode
from ..util.monitor import Monitor, ConsoleMonitor
from ..util.web.webapi import WebAPIRequestHandler
from ..version import __version__

# TODO (forman): We must keep a MemoryCacheStore Cache for each workspace.
#                We can use the Workspace.user_data dict for this purpose.
#                However, a global cache is fine as long as we have just one workspace open at a time.
#
MEM_TILE_CACHE = Cache(MemoryCacheStore(),
                       capacity=WEBAPI_WORKSPACE_MEM_TILE_CACHE_CAPACITY,
                       threshold=0.75)

# Note, the following "get_config()" call in the code will make sure "~/.cate/<version>" is created
USE_WORKSPACE_IMAGERY_CACHE = get_config().get('use_workspace_imagery_cache',
                                               WEBAPI_USE_WORKSPACE_IMAGERY_CACHE)

TRACE_PERF = is_debug_mode()

THREAD_POOL = concurrent.futures.ThreadPoolExecutor()

_LOG = logging.getLogger('cate')

_NUM_GEOM_SIMP_LEVELS = 8

_MAX_CSV_ROW_COUNT = 10000

# Explicitly load Cate-internal plugins.
__import__('cate.ds')
__import__('cate.ops')


# noinspection PyAbstractClass
class NE2Handler(WebAPIRequestHandler):
    PYRAMID = NaturalEarth2Image.get_pyramid()

    def get(self, z, y, x):
        # print('NE2Handler.get(%s, %s, %s)' % (z, y, x))
        self.set_header('Content-Type', 'image/jpg')
        self.write(NE2Handler.PYRAMID.get_tile(int(x), int(y), int(z)))


# noinspection PyAbstractClass
class WorkspaceResourceHandler(WebAPIRequestHandler):

    def get_workspace_resource(self, base_dir, res_id: str):
        res_id = self.to_int("res_id", res_id)
        # noinspection PyUnresolvedReferences
        workspace_manager: WorkspaceManager = self.application.workspace_manager
        base_dir = workspace_manager.resolve_path(base_dir)
        workspace = workspace_manager.get_workspace(base_dir)
        res_name = workspace.resource_cache.get_key(res_id)
        resource = workspace.resource_cache[res_name]
        return workspace, res_id, res_name, resource


# noinspection PyAbstractClass,PyBroadException
class ResVarTileHandler(WorkspaceResourceHandler):
    PYRAMIDS = None

    def get(self, base_dir, res_id, z, y, x):
        try:
            workspace, res_id, res_name, dataset = self.get_workspace_resource(base_dir, res_id)

            # GLOBAL_LOCK.acquire()

            if not isinstance(dataset, xr.Dataset):
                self.write_status_error(message='Resource "%s" must be a Dataset' % res_name)
                self.finish()
                return

            var_name = self.get_query_argument('var')
            var_index = self.get_query_argument_int_tuple('index', ())
            cmap_name = self.get_query_argument('cmap', default='jet')
            cmap_min = self.get_query_argument_float('min', default=float('nan'))
            cmap_max = self.get_query_argument_float('max', default=float('nan'))

            if ResVarTileHandler.PYRAMIDS is None:
                ResVarTileHandler.PYRAMIDS = dict()

            array_id = '%s-%s-%s' % (res_name,
                                     var_name,
                                     ','.join(map(str, var_index)))
            image_id = '%s-%s-%s-%s' % (array_id,
                                        cmap_name,
                                        cmap_min,
                                        cmap_max)

            pyramid_id = '%s-%s' % (base_dir, image_id)

            if pyramid_id in ResVarTileHandler.PYRAMIDS:
                pyramid = ResVarTileHandler.PYRAMIDS[pyramid_id]
            else:
                variable = dataset[var_name]
                no_data_value = variable.attrs.get('_FillValue')
                valid_range = variable.attrs.get('valid_range')
                if valid_range is None:
                    valid_min = variable.attrs.get('valid_min')
                    valid_max = variable.attrs.get('valid_max')
                    if valid_min is not None and valid_max is not None:
                        valid_range = [valid_min, valid_max]

                # Make sure we work with 2D image arrays only
                if variable.ndim == 2:
                    array = variable
                elif variable.ndim > 2:
                    if not var_index or len(var_index) != variable.ndim - 2:
                        var_index = (0,) * (variable.ndim - 2)

                    # noinspection PyTypeChecker
                    var_index += (slice(None), slice(None),)

                    # print('var_index =', var_index)
                    array = variable[var_index]
                else:
                    self.write_status_error(message='Variable must be an N-D Dataset with N >= 2, '
                                                    'but "%s" is only %d-D' % (var_name, variable.ndim))
                    self.finish()
                    return

                cmap_min = np.nanmin(array.values) if np.isnan(cmap_min) else cmap_min
                cmap_max = np.nanmax(array.values) if np.isnan(cmap_max) else cmap_max
                # print('cmap_min =', cmap_min)
                # print('cmap_max =', cmap_max)

                if USE_WORKSPACE_IMAGERY_CACHE:
                    mem_tile_cache = MEM_TILE_CACHE
                    rgb_tile_cache_dir = os.path.join(base_dir, WORKSPACE_CACHE_DIR_NAME, 'v%s' % __version__, 'tiles')
                    rgb_tile_cache = Cache(FileCacheStore(rgb_tile_cache_dir, ".png"),
                                           capacity=WEBAPI_WORKSPACE_FILE_TILE_CACHE_CAPACITY,
                                           threshold=0.75)
                else:
                    mem_tile_cache = MEM_TILE_CACHE
                    rgb_tile_cache = None

                def array_image_id_factory(level):
                    return 'arr-%s/%s' % (array_id, level)

                tiling_scheme = get_tiling_scheme(variable)
                if tiling_scheme is None:
                    self.write_status_error(
                        message='Internal error: failed to compute tiling scheme for array_id="%s"' % array_id)
                    self.finish()
                    return

                # print('tiling_scheme =', repr(tiling_scheme))
                pyramid = ImagePyramid.create_from_array(array, tiling_scheme,
                                                         level_image_id_factory=array_image_id_factory)
                pyramid = pyramid.apply(lambda image, level:
                                        TransformArrayImage(image,
                                                            image_id='tra-%s/%d' % (array_id, level),
                                                            flip_y=tiling_scheme.geo_extent.inv_y,
                                                            force_masked=True,
                                                            no_data_value=no_data_value,
                                                            valid_range=valid_range,
                                                            tile_cache=mem_tile_cache))
                pyramid = pyramid.apply(lambda image, level:
                                        ColorMappedRgbaImage(image,
                                                             image_id='rgb-%s/%d' % (image_id, level),
                                                             value_range=(cmap_min, cmap_max),
                                                             cmap_name=cmap_name,
                                                             encode=True,
                                                             format='PNG',
                                                             tile_cache=rgb_tile_cache))
                ResVarTileHandler.PYRAMIDS[pyramid_id] = pyramid
                if TRACE_PERF:
                    print('Created pyramid "%s":' % pyramid_id)
                    print('  tile_size:', pyramid.tile_size)
                    print('  num_level_zero_tiles:', pyramid.num_level_zero_tiles)
                    print('  num_levels:', pyramid.num_levels)

            if TRACE_PERF:
                print('PERF: >>> Tile:', image_id, z, y, x)

            t1 = time.perf_counter()
            tile = pyramid.get_tile(int(x), int(y), int(z))
            t2 = time.perf_counter()

            self.set_header('Content-Type', 'image/png')
            self.write(tile)

            if TRACE_PERF:
                print('PERF: <<< Tile:', image_id, z, y, x, 'took', t2 - t1, 'seconds')

            # GLOBAL_LOCK.release()

        except Exception:
            self.write_status_error(exc_info=sys.exc_info())
            self.finish()


# noinspection PyAbstractClass,PyBroadException
class ResourcePlotHandler(WorkspaceResourceHandler):
    def get(self, base_dir, res_name):
        try:
            # noinspection PyUnresolvedReferences
            workspace_manager: WorkspaceManager = self.application.workspace_manager
            var_name = self.get_query_argument('var_name', default=None)
            file_path = self.get_query_argument('file_path', default=None)
            with cwd(base_dir):
                workspace_manager.plot_workspace_resource(base_dir, res_name,
                                                          var_name=var_name,
                                                          file_path=file_path)
            self.write_status_ok()
            self.finish()
        except Exception:
            self.write_status_error(exc_info=sys.exc_info())
            self.finish()


# noinspection PyAbstractClass,PyBroadException
class GeoJSONHandler(WebAPIRequestHandler):
    def __init__(self, application, request, shapefile_path, **kwargs):
        super().__init__(application, request, **kwargs)
        self._shapefile_path = shapefile_path

    # see http://stackoverflow.com/questions/20018684/tornado-streaming-http-response-as-asynchttpclient-receives-chunks
    @tornado.gen.coroutine
    def get(self):
        try:
            level = int(self.get_query_argument('level', default=str(_NUM_GEOM_SIMP_LEVELS)))
            collection = fiona.open(self._shapefile_path)
            self.set_header('Content-Type', 'application/json')

            def job():
                conservation_ratio = _level_to_conservation_ratio(level, _NUM_GEOM_SIMP_LEVELS)
                write_feature_collection(collection, self,
                                         num_features=len(collection),
                                         conservation_ratio=conservation_ratio)
                self.finish()

            yield [THREAD_POOL.submit(job)]
        except Exception:
            self.write_status_error(exc_info=sys.exc_info())
            self.finish()


DEFAULT_COUNTRIES_RESOLUTION = '50m'


# noinspection PyAbstractClass,PyBroadException
class CountriesGeoJSONHandler(WebAPIRequestHandler):
    def get(self, resolution: str = DEFAULT_COUNTRIES_RESOLUTION):
        """
        :param resolution: '10m', '50m', or '110m' (default), refer to https://geojson-maps.ash.ms/
        """
        filename = f'countries-{resolution or DEFAULT_COUNTRIES_RESOLUTION}.geojson'
        try:
            path = os.path.join(os.path.dirname(__file__),
                                '..', 'ds', 'data', 'countries', filename)
            with open(path) as fp:
                self.write(fp.read())
        except Exception:
            self.write_status_error(exc_info=sys.exc_info())
        self.finish()


# noinspection PyAbstractClass,PyBroadException
class ResFeatureCollectionHandler(WorkspaceResourceHandler):
    # see http://stackoverflow.com/questions/20018684/tornado-streaming-http-response-as-asynchttpclient-receives-chunks
    @tornado.gen.coroutine
    def get(self, base_dir, res_id):
        try:
            _, res_id, res_name, resource = self.get_workspace_resource(base_dir, res_id)
            level = self.get_query_argument_int('level', default=_NUM_GEOM_SIMP_LEVELS)

            if isinstance(resource, fiona.Collection):
                features = resource
                crs = features.crs
                num_features = len(features)
            elif isinstance(resource, GeoDataFrame):
                features = resource.features
                crs = features.crs
                num_features = len(resource)
            elif isinstance(resource, gpd.GeoDataFrame):
                features = resource.iterfeatures()
                crs = resource.crs
                num_features = len(resource)
            else:
                features = None
                crs = None
                num_features = 0
                self.write_status_error(message='Resource "%s" is not a GeoDataFrame' % res_name)

            if features is not None:
                if TRACE_PERF:
                    print('ResFeatureCollectionHandler: features CRS:', crs)
                    print('ResFeatureCollectionHandler: streaming started at ', datetime.datetime.now())
                self.set_header('Content-Type', 'application/json')

                def job():
                    conservation_ratio = _level_to_conservation_ratio(level, _NUM_GEOM_SIMP_LEVELS)
                    write_feature_collection(features, self,
                                             crs=crs,
                                             res_id=res_id,
                                             num_features=num_features,
                                             max_num_display_geometries=1000,
                                             max_num_display_geometry_points=100,
                                             conservation_ratio=conservation_ratio)
                    self.finish()
                    if TRACE_PERF:
                        print('ResFeatureCollectionHandler: streaming done at ', datetime.datetime.now())

                yield [THREAD_POOL.submit(job)]
        except Exception:
            self.write_status_error(exc_info=sys.exc_info())
            self.finish()


# noinspection PyAbstractClass,PyBroadException
class ResFeatureHandler(WorkspaceResourceHandler):
    # see http://stackoverflow.com/questions/20018684/tornado-streaming-http-response-as-asynchttpclient-receives-chunks
    @tornado.gen.coroutine
    def get(self, base_dir, res_id, feature_index):
        try:
            _, res_id, res_name, resource = self.get_workspace_resource(base_dir, res_id)
            feature_index = self.to_int('feature_index', feature_index)
            level = self.get_query_argument_int('level', default=_NUM_GEOM_SIMP_LEVELS)

            if isinstance(resource, fiona.Collection):
                if not self._check_feature_index(feature_index, len(resource)):
                    return
                feature = resource[feature_index]
                crs = resource.crs
            elif isinstance(resource, GeoDataFrame):
                if not self._check_feature_index(feature_index, len(resource)):
                    return
                feature = resource.features[feature_index]
                crs = resource.features.crs
            elif isinstance(resource, gpd.GeoDataFrame):
                if not self._check_feature_index(feature_index, len(resource)):
                    return
                row = resource.iloc[feature_index]
                geometry = None
                properties = row.to_dict()
                if 'geometry' in properties:
                    geometry = row['geometry'].__geo_interface__
                    del properties['geometry']
                feature = {
                    'type': 'Feature',
                    'properties': properties,
                    'geometry': geometry
                }
                crs = resource.crs
            else:
                feature = None
                crs = None
                self.write_status_error(message='Resource "%s" is not a GeoDataFrame' % res_name)
                self.finish()

            if feature is not None:
                if TRACE_PERF:
                    print('ResFeatureHandler: feature CRS:', crs)
                    print('ResFeatureHandler: streaming started at ', datetime.datetime.now())
                self.set_header('Content-Type', 'application/json')
                conservation_ratio = _level_to_conservation_ratio(level, _NUM_GEOM_SIMP_LEVELS)

                def job():
                    try:
                        write_feature(feature,
                                      self,
                                      crs=crs,
                                      res_id=res_id,
                                      feature_index=feature_index,
                                      conservation_ratio=conservation_ratio)
                    except BaseException:
                        self.write_status_error(exc_info=sys.exc_info())
                    self.finish()
                    if TRACE_PERF:
                        print('ResFeatureHandler: streaming done at ', datetime.datetime.now())

                yield [THREAD_POOL.submit(job)]
        except Exception:
            self.write_status_error(exc_info=sys.exc_info())
            self.finish()

    def _check_feature_index(self, feature_index, num_features):
        ok = feature_index < num_features
        if not ok:
            self.write_status_error(message='feature_index {} out of bounds, num_features={}'
                                    .format(feature_index, num_features))
            self.finish()
        return ok


# noinspection PyAbstractClass,PyBroadException
class ResVarCsvHandler(WorkspaceResourceHandler):
    def get(self, base_dir, res_id):
        try:
            _, _, _, resource = self.get_workspace_resource(base_dir, res_id)
            var_name = self.get_query_argument('var', default=None)

            var_data = resource
            if var_name:
                try:
                    var_data = resource[var_name]
                except Exception:
                    self.write_status_error(exc_info=sys.exc_info())
                    self.finish()
                    return

            # noinspection PyBroadException
            try:
                # assume var_data is a pandas.dataframe
                dataframe = var_data
                num_rows, _ = dataframe.shape
                if num_rows > _MAX_CSV_ROW_COUNT:
                    dataframe = dataframe[:_MAX_CSV_ROW_COUNT]
                csv = dataframe.to_csv()
            except Exception:
                # noinspection PyBroadException
                try:
                    # assume var_data is a xarray.dataset or xarray.dataarray
                    dataframe = var_data.to_dataframe()
                    num_rows, _ = dataframe.shape
                    if num_rows > _MAX_CSV_ROW_COUNT:
                        dataframe = dataframe[:_MAX_CSV_ROW_COUNT]
                    csv = dataframe.to_csv()
                except Exception:
                    csv = var_data.to_series().to_csv()

            self.set_header('Content-Type', 'text/csv')
            self.write(csv)
        except Exception:
            self.write_status_error(exc_info=sys.exc_info())
            self.finish()

        self.finish()


# noinspection PyAbstractClass,PyBroadException
class ResVarHtmlHandler(WorkspaceResourceHandler):
    def get(self, base_dir, res_id):
        try:
            _, _, _, resource = self.get_workspace_resource(base_dir, res_id)
            self.set_header('Content-Type', 'text/html')
            self.write(resource)
            self.finish()
        except Exception:
            self.write_status_error(exc_info=sys.exc_info())
            self.finish()


def save_files(files: Sequence, target_dir: str):
    for file in files:
        file_path = os.path.join(target_dir, file.filename)
        if not os.path.exists(target_dir):
            os.mkdir(target_dir)

        with open(file_path, 'wb') as fp:
            fp.write(file.body)

    return 'Files: ' + ', '.join([file.filename for file in files])


def _ensure_str(value: Any) -> str:
    if isinstance(value, list):
        value = value[0]

    if isinstance(value, bytes):
        value = value.decode()

    if isinstance(value, str):
        return value
    else:
        return str(value)


MAX_STREAMED_SIZE = 1024 * 1024 * 1024 * 1024


@tornado.web.stream_request_body
class FilesUploadHandler(WebAPIRequestHandler):
    # noinspection PyAttributeOutsideInit
    def initialize(self):
        # Member to collect meta info of the streaming process
        self.bytes_read = 0
        self.meta = dict()
        self.receiver = self.get_receiver()
        self.error = False
        self.fp = None

    def data_received(self, chunk):
        # Call get_receiver on received chunk
        self.receiver(chunk)

    def get_receiver(self):
        index = 0
        separate = b'\r\n'

        def receiver(chunk):
            nonlocal index
            # noinspection PyUnresolvedReferences
            workspace_manager: WorkspaceManager = self.application.workspace_manager
            # Unfortunately we have to parse the header from the first chunk ourselves as we are streaming.
            if index == 0:
                index += 1
                split_chunk = chunk.split(separate)
                self.meta['boundary'] = separate + split_chunk[0] + b'--' + separate
                self.meta['header'] = separate.join(split_chunk[0:7])
                self.meta['header'] += separate * 2
                self.meta['dir'] = split_chunk[3].decode()
                self.meta['filename'] = split_chunk[5].split(b'=')[-1].replace(b'"', b'').decode()

                chunk = chunk[len(self.meta['header']):]  # Stream
                fn = workspace_manager.resolve_path(os.path.join(self.meta['dir']))
                if not os.path.isdir(fn):
                    os.mkdir(fn)

                fn = os.path.join(fn, self.meta['filename'])
                self.fp = open(fn, "wb")
                self.fp.write(chunk)
            else:
                self.fp.write(chunk)

        return receiver

    def truncate_fp(self):
        if self.fp:
            self.fp.seek(self.meta['content_length'], 0)
            self.fp.truncate()
            self.fp.close()

    def post(self):
        # Stream
        self.meta['content_length'] = int(self.request.headers.get('Content-Length')) - \
                                      len(self.meta['header']) - \
                                      len(self.meta['boundary'])

        self.truncate_fp()
        megabytes = int(self.meta['content_length'] / 2 ** 20)
        self.finish(json.dumps({'status': 'success', 'message': str(megabytes) + 'MBs uploaded.'}))


# noinspection PyAbstractClass
class FilesDownloadHandler(WebAPIRequestHandler):
    def _zip_files(self, file_paths: Sequence[str]):
        zip_file_path = tempfile.mktemp('.zip')

        with zipfile.ZipFile(zip_file_path, "w") as zip_file:
            for file_path in file_paths:
                # noinspection PyUnresolvedReferences
                manager: WorkspaceManager = self.application.workspace_manager
                file_name = manager.resolve_path(file_path)
                if os.path.isfile(file_name):
                    zip_file.write(file_name, file_path)

        return zip_file

    def _return_zip_file(self, result, process_id):
        if result is None:
            return

        self.set_header('Content-Type', 'application/zip')
        # self.set_header("Content-Disposition", "attachment; filename=%s" % target_dir + '.zip')

        self._stream_file_content(result, process_id)
        os.remove(result.filename)

    def _stream_file_content(self, result, process_id):
        with open(result.filename, 'rb') as f:
            while True:
                chunk_size = int(512 * 64)
                data = f.read(chunk_size)
                if not data:
                    break
                self.write(data)

    def post(self):
        body_dict = escape.json_decode(self.request.body)

        target_files = body_dict.get('target_files')
        process_id = body_dict.get('process_id')

        zip_file = self._zip_files(target_files)
        self._return_zip_file(zip_file, process_id)

        self.finish(
            json.dumps({'status': 'success', 'error': '', 'message': 'Done'}))


def get_app_resources_path() -> Optional[str]:
    app_path = os.environ.get("CATE_APP_PATH")
    if app_path:
        _LOG.warning(f"Endpoint '/app' will be served from {app_path}")
        return app_path
    try:
        with importlib.resources.files("cate.webapi") as path:
            app_path = path / "app"
            if (app_path / "index.html").is_file():
                return str(app_path)
    except ImportError:
        pass
    _LOG.warning(f"Cannot find 'cate/webapi/app,"
                 f" consider setting environment variable"
                 f" CATE_APP_PATH",
                 exc_info=True)
    return None


def _new_monitor() -> Monitor:
    return ConsoleMonitor(stay_in_line=True, progress_bar_size=30)


def _level_to_conservation_ratio(level: int, num_levels: int):
    if level <= 0:
        return 0.0
    if level >= num_levels - 1:
        return 1.0
    return 2 ** -(num_levels - (level + 1))
