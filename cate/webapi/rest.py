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
             "Marco Zühlke (Brockmann Consult GmbH)"

import concurrent.futures
import datetime
import os
import sys
import time

import fiona
import geopandas as gpd
import numpy as np
import tornado.gen
import tornado.web
import xarray as xr

from .geojson import write_feature_collection, write_feature
from ..conf import get_config
from ..conf.defaults import \
    WORKSPACE_CACHE_DIR_NAME, \
    WEBAPI_WORKSPACE_FILE_TILE_CACHE_CAPACITY, \
    WEBAPI_WORKSPACE_MEM_TILE_CACHE_CAPACITY, \
    WEBAPI_ON_ALL_CLOSED_AUTO_STOP_AFTER, \
    WEBAPI_USE_WORKSPACE_IMAGERY_CACHE
from ..core.cdm import get_tiling_scheme
from ..core.types import GeoDataFrame
from ..util.cache import Cache, MemoryCacheStore, FileCacheStore
from ..util.im import ImagePyramid, TransformArrayImage, ColorMappedRgbaImage
from ..util.im.ds import NaturalEarth2Image
from ..util.misc import cwd
from ..util.monitor import Monitor, ConsoleMonitor
from ..util.web.webapi import WebAPIRequestHandler, check_for_auto_stop
from ..version import __version__

# TODO (forman): We must keep a MemoryCacheStore Cache for each workspace.
#                We can use the Workspace.user_data dict for this purpose.
#                However, a global cache is fine as long as we have just one workspace open at a time.
#
MEM_TILE_CACHE = Cache(MemoryCacheStore(),
                       capacity=WEBAPI_WORKSPACE_MEM_TILE_CACHE_CAPACITY,
                       threshold=0.75)

# Note, the following "get_config()" call in the code will make sure "~/.cate/<version>" is created
USE_WORKSPACE_IMAGERY_CACHE = get_config().get('use_workspace_imagery_cache', WEBAPI_USE_WORKSPACE_IMAGERY_CACHE)

TRACE_PERF = True

THREAD_POOL = concurrent.futures.ThreadPoolExecutor()

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
        workspace_manager = self.application.workspace_manager
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
            var_name = self.get_query_argument('var_name', default=None)
            file_path = self.get_query_argument('file_path', default=None)
            with cwd(base_dir):
                workspace_manager = self.application.workspace_manager
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
    @tornado.web.asynchronous
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


# noinspection PyAbstractClass,PyBroadException
class CountriesGeoJSONHandler(GeoJSONHandler):
    def __init__(self, application, request, **kwargs):
        try:
            shapefile_path = os.path.join(os.path.dirname(__file__),
                                          '..', 'ds', 'data', 'countries', 'countries.geojson')
            super().__init__(application, request, shapefile_path=shapefile_path, **kwargs)
        except Exception:
            self.write_status_error(exc_info=sys.exc_info())
            self.finish()


# noinspection PyAbstractClass,PyBroadException
class ResFeatureCollectionHandler(WorkspaceResourceHandler):
    # see http://stackoverflow.com/questions/20018684/tornado-streaming-http-response-as-asynchttpclient-receives-chunks
    @tornado.web.asynchronous
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
    @tornado.web.asynchronous
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
                    except BaseException as e:
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
                except Exception as e:
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
        except Exception as e:
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


def _new_monitor() -> Monitor:
    return ConsoleMonitor(stay_in_line=True, progress_bar_size=30)


def _on_workspace_closed(application: tornado.web.Application):
    # noinspection PyUnresolvedReferences
    workspace_manager = application.workspace_manager
    num_open_workspaces = workspace_manager.num_open_workspaces()
    check_for_auto_stop(application, num_open_workspaces == 0, interval=WEBAPI_ON_ALL_CLOSED_AUTO_STOP_AFTER)


def _level_to_conservation_ratio(level: int, num_levels: int):
    if level <= 0:
        return 0.0
    if level >= num_levels - 1:
        return 1.0
    return 2 ** -(num_levels - (level + 1))
