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
import os.path
import time
import traceback

import fiona
import numpy as np
import tornado.gen
import tornado.web
import xarray as xr
import geopandas as gpd

from .geojson import write_feature_collection
from ..conf import get_config
from ..conf.defaults import \
    WORKSPACE_CACHE_DIR_NAME, \
    WEBAPI_WORKSPACE_FILE_TILE_CACHE_CAPACITY, \
    WEBAPI_WORKSPACE_MEM_TILE_CACHE_CAPACITY, \
    WEBAPI_ON_ALL_CLOSED_AUTO_STOP_AFTER, \
    WEBAPI_USE_WORKSPACE_IMAGERY_CACHE
from ..core.cdm import get_tiling_scheme
from ..core.types import GeoDataFrame
from ..util.monitor import Monitor, ConsoleMonitor
from ..util.cache import Cache, MemoryCacheStore, FileCacheStore
from ..util.im import ImagePyramid, TransformArrayImage, ColorMappedRgbaImage
from ..util.im.ds import NaturalEarth2Image
from ..util.misc import cwd
from ..util.web.webapi import WebAPIRequestHandler, check_for_auto_stop
from ..version import __version__

# TODO (forman): We must keep a MemoryCacheStore Cache for each workspace.
#                We can use the Workspace.user_data dict for this purpose.
#                However, a global cache is fine as long as we have just one workspace open at a time.
#
MEM_TILE_CACHE = Cache(MemoryCacheStore(),
                       capacity=WEBAPI_WORKSPACE_MEM_TILE_CACHE_CAPACITY,
                       threshold=0.75)

USE_WORKSPACE_IMAGERY_CACHE = get_config().get('use_workspace_imagery_cache', WEBAPI_USE_WORKSPACE_IMAGERY_CACHE)

TRACE_TILE_PERF = False

THREAD_POOL = concurrent.futures.ThreadPoolExecutor()

_NUM_GEOM_SIMP_LEVELS = 8

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
class ResVarTileHandler(WebAPIRequestHandler):
    PYRAMIDS = None

    def get(self, base_dir, res_name, z, y, x):

        if not ResVarTileHandler.PYRAMIDS:
            ResVarTileHandler.PYRAMIDS = dict()

        # GLOBAL_LOCK.acquire()
        workspace_manager = self.application.workspace_manager
        workspace = workspace_manager.get_workspace(base_dir)

        if res_name not in workspace.resource_cache:
            self.write_status_error(message='Unknown resource "%s"' % res_name)
            return

        dataset = workspace.resource_cache[res_name]
        if not isinstance(dataset, xr.Dataset):
            self.write_status_error(message='Resource "%s" must be a Dataset' % res_name)
            return

        var_name = self.get_query_argument('var')
        var_index = self.get_query_argument('index', default=None)
        var_index = tuple(map(int, var_index.split(','))) if var_index else []
        cmap_name = self.get_query_argument('cmap', default='jet')
        cmap_min = float(self.get_query_argument('min', default='nan'))
        cmap_max = float(self.get_query_argument('max', default='nan'))

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

            # Make sure we work with 2D image arrays only
            if variable.ndim == 2:
                array = variable
            elif variable.ndim > 2:
                if not var_index or len(var_index) != variable.ndim - 2:
                    var_index = (0,) * (variable.ndim - 2)

                # noinspection PyTypeChecker
                var_index += (slice(None), slice(None),)

                print('var_index =', var_index)
                array = variable[var_index]
            else:
                self.write_status_error(message='Variable must be an N-D Dataset with N >= 2, '
                                                'but "%s" is only %d-D' % (var_name, variable.ndim))
                return

            cmap_min = np.nanmin(array.values) if np.isnan(cmap_min) else cmap_min
            cmap_max = np.nanmax(array.values) if np.isnan(cmap_max) else cmap_max
            print('cmap_min =', cmap_min)
            print('cmap_max =', cmap_max)

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
                return

            print('tiling_scheme =', repr(tiling_scheme))
            pyramid = ImagePyramid.create_from_array(array, tiling_scheme,
                                                     level_image_id_factory=array_image_id_factory)
            pyramid = pyramid.apply(lambda image, level:
                                    TransformArrayImage(image,
                                                        image_id='tra-%s/%d' % (array_id, level),
                                                        no_data_value=no_data_value,
                                                        force_masked=True,
                                                        flip_y=tiling_scheme.geo_extent.inv_y,
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
            if TRACE_TILE_PERF:
                print('Created pyramid "%s":' % pyramid_id)
                print('  tile_size:', pyramid.tile_size)
                print('  num_level_zero_tiles:', pyramid.num_level_zero_tiles)
                print('  num_levels:', pyramid.num_levels)

        try:
            if TRACE_TILE_PERF:
                print('PERF: >>> Tile:', image_id, z, y, x)
            t1 = time.clock()
            tile = pyramid.get_tile(int(x), int(y), int(z))
            t2 = time.clock()

            self.set_header('Content-Type', 'image/png')
            self.write(tile)

            if TRACE_TILE_PERF:
                print('PERF: <<< Tile:', image_id, z, y, x, 'took', t2 - t1, 'seconds')
                # GLOBAL_LOCK.release()
        except Exception as e:
            traceback.print_exc()
            self.write_status_error(message='Internal error: %s' % e)


# noinspection PyAbstractClass
class ResourcePlotHandler(WebAPIRequestHandler):
    def get(self, base_dir, res_name):
        var_name = self.get_query_argument('var_name', default=None)
        file_path = self.get_query_argument('file_path', default=None)
        workspace_manager = self.application.workspace_manager
        try:
            with cwd(base_dir):
                workspace_manager.plot_workspace_resource(base_dir, res_name, var_name=var_name,
                                                          file_path=file_path)
            self.write_status_ok()
        except Exception as e:
            self.write_status_error(exception=e)


# noinspection PyAbstractClass
class GeoJSONHandler(WebAPIRequestHandler):
    def __init__(self, application, request, shapefile_path, **kwargs):
        print('GeoJSONHandler', shapefile_path)
        super(GeoJSONHandler, self).__init__(application, request, **kwargs)
        self._shapefile_path = shapefile_path

    # see http://stackoverflow.com/questions/20018684/tornado-streaming-http-response-as-asynchttpclient-receives-chunks
    @tornado.web.asynchronous
    @tornado.gen.coroutine
    def get(self):
        level = int(self.get_query_argument('level', default=str(_NUM_GEOM_SIMP_LEVELS)))
        print('GeoJSONHandler: shapefile_path:', self._shapefile_path, ', level: ', level)
        collection = fiona.open(self._shapefile_path)
        print('GeoJSONHandler: collection:', collection)
        print('GeoJSONHandler: collection CRS:', collection.crs)
        try:
            self.set_header('Content-Type', 'application/json')
            yield [THREAD_POOL.submit(write_feature_collection, collection, self,
                                      simp_ratio=_level_to_simp_ratio(level, _NUM_GEOM_SIMP_LEVELS))]
        except Exception as e:
            traceback.print_exc()
            self.write_status_error(message='Internal error: %s' % e)
        self.finish()


# noinspection PyAbstractClass
class CountriesGeoJSONHandler(GeoJSONHandler):
    def __init__(self, application, request, **kwargs):
        print('CountriesGeoJSONHandler', request)
        shapefile_path = os.path.join(os.path.dirname(__file__), '..', 'ds', 'data', 'countries', 'countries.geojson')
        super(CountriesGeoJSONHandler, self).__init__(application, request, shapefile_path=shapefile_path, **kwargs)


# noinspection PyAbstractClass
class ResVarGeoJSONHandler(WebAPIRequestHandler):
    # see http://stackoverflow.com/questions/20018684/tornado-streaming-http-response-as-asynchttpclient-receives-chunks
    @tornado.web.asynchronous
    @tornado.gen.coroutine
    def get(self, base_dir, res_name):

        print('ResVarGeoJSONHandler:', base_dir, res_name)

        level = int(self.get_query_argument('level', default=str(_NUM_GEOM_SIMP_LEVELS)))
        var_name = self.get_query_argument('var')
        var_index = self.get_query_argument('index', default=None)
        var_index = tuple(map(int, var_index.split(','))) if var_index else []
        cmap_name = self.get_query_argument('cmap', default='jet')
        cmap_min = float(self.get_query_argument('min', default='nan'))
        cmap_max = float(self.get_query_argument('max', default='nan'))

        print('ResVarGeoJSONHandler:', level, var_name, var_index, cmap_name, cmap_min, cmap_max)

        workspace_manager = self.application.workspace_manager
        workspace = workspace_manager.get_workspace(base_dir)

        if res_name not in workspace.resource_cache:
            self.write_status_error(message='Unknown resource "%s"' % res_name)
            return

        resource = workspace.resource_cache[res_name]
        if isinstance(resource, fiona.Collection):
            features = resource
            crs = features.crs
        elif isinstance(resource, GeoDataFrame):
            features = resource.features
            crs = features.crs
        elif isinstance(resource, gpd.GeoDataFrame):
            features = resource.iterfeatures()
            crs = resource.crs
        else:
            self.write_status_error(message='Resource "%s" must provide a fiona.Collection' % res_name)
            return

        print('ResVarGeoJSONHandler: features:', features)
        print('ResVarGeoJSONHandler: features CRS:', crs)
        print('ResVarGeoJSONHandler: streaming started at ', datetime.datetime.now())
        try:
            self.set_header('Content-Type', 'application/json')
            yield [THREAD_POOL.submit(write_feature_collection, features, self,
                                      crs=crs,
                                      simp_ratio=_level_to_simp_ratio(level, _NUM_GEOM_SIMP_LEVELS))]
        except Exception as e:
            traceback.print_exc()
            self.write_status_error(message='Internal error: %s' % e)

        print('ResVarGeoJSONHandler: streaming done at ', datetime.datetime.now())
        self.finish()


# noinspection PyAbstractClass
class ResVarCsvHandler(WebAPIRequestHandler):
    def get(self, base_dir, res_name):

        var_name = self.get_query_argument('var', default=None)

        print('ResVarCsvHandler:', base_dir, res_name, var_name)

        workspace_manager = self.application.workspace_manager
        workspace = workspace_manager.get_workspace(base_dir)

        if res_name not in workspace.resource_cache:
            self.write_status_error(message='Unknown resource named "%s"' % res_name)
            return

        resource = workspace.resource_cache[res_name]
        if resource is None:
            self.write_status_error(message='Resource "%s" is None' % res_name)
            return

        var_data = resource
        if var_name:
            try:
                var_data = resource[var_name]
            except Exception as e:
                traceback.print_exc()
                self.write_status_error(message='Internal error: %s' % e)
                return
        print("type:", type(var_data))
        print(var_data.__dict__)
        try:
            # TODO: remove this crappy threshold 1000
            # assume var_data is a pandas.dataframe
            dataframe = var_data
            nRows, nCols = dataframe.shape
            if nRows > 1000:
                dataframe = dataframe[:1000]
            csv = dataframe.to_csv()
        except Exception as e:
            try:
                # TODO: remove this crappy threshold 1000
                # assume var_data is a xarray.dataset or xarray.dataarray
                dataframe = var_data.to_dataframe()
                nRows, nCols = dataframe.shape
                if nRows > 1000:
                    dataframe = dataframe[:1000]
                csv = dataframe.to_csv()
            except Exception as e:
                try:
                    csv = var_data.to_series().to_csv()
                except Exception as e:
                    traceback.print_exc()
                    self.write_status_error(message='Internal error: %s' % e)
                    return

        self.set_header('Content-Type', 'text/csv')
        self.write(csv)
        self.finish()


def _new_monitor() -> Monitor:
    return ConsoleMonitor(stay_in_line=True, progress_bar_size=30)


def _on_workspace_closed(application: tornado.web.Application):
    # noinspection PyUnresolvedReferences
    workspace_manager = application.workspace_manager
    num_open_workspaces = workspace_manager.num_open_workspaces()
    check_for_auto_stop(application, num_open_workspaces == 0, interval=WEBAPI_ON_ALL_CLOSED_AUTO_STOP_AFTER)


def _level_to_simp_ratio(level: int, num_levels: int):
    if level <= 0:
        return 0.0
    if level >= num_levels - 1:
        return 1.0
    return 2 ** -(num_levels - (level + 1))
