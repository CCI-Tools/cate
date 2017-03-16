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

from cate.util import ConsoleMonitor
from cate.util import Monitor

__author__ = "Norman Fomferra (Brockmann Consult GmbH), " \
             "Marco Zühlke (Brockmann Consult GmbH)"

import json
import os.path
import time
import traceback
from typing import List

import numpy as np
import xarray as xr
import fiona
import pyproj
import tornado.web
import tornado.gen
import concurrent.futures

from cate.conf import get_config
from cate.conf.defaults import \
    WORKSPACE_CACHE_DIR_NAME, \
    WEBAPI_WORKSPACE_FILE_TILE_CACHE_CAPACITY, \
    WEBAPI_WORKSPACE_MEM_TILE_CACHE_CAPACITY, \
    WEBAPI_ON_ALL_CLOSED_AUTO_STOP_AFTER, \
    WEBAPI_USE_WORKSPACE_IMAGERY_CACHE
from cate.util.cache import Cache, MemoryCacheStore, FileCacheStore
from cate.util.im import ImagePyramid, TransformArrayImage, ColorMappedRgbaImage
from cate.util.im.ds import NaturalEarth2Image
from cate.util.misc import cwd
from cate.util.web.webapi import WebAPIRequestHandler, check_for_auto_stop
from cate.version import __version__
from .geojson import write_feature_collection

# TODO (forman): We must keep a MemoryCacheStore Cache for each workspace.
#                However, a global cache is fine as long as we have just one workspace open at a time.
#
MEM_TILE_CACHE = Cache(MemoryCacheStore(),
                       capacity=WEBAPI_WORKSPACE_MEM_TILE_CACHE_CAPACITY,
                       threshold=0.75)

USE_WORKSPACE_IMAGERY_CACHE = get_config().get('use_workspace_imagery_cache', WEBAPI_USE_WORKSPACE_IMAGERY_CACHE)

THREAD_POOL = concurrent.futures.ThreadPoolExecutor()

# Explicitly load Cate-internal plugins.
__import__('cate.ds')
__import__('cate.ops')


# noinspection PyAbstractClass
class WorkspaceGetHandler(WebAPIRequestHandler):
    def get(self, base_dir):
        workspace_manager = self.application.workspace_manager
        try:
            workspace = workspace_manager.get_workspace(base_dir)
            self.write_status_ok(content=workspace.to_json_dict())
        except Exception as e:
            self.write_status_error(exception=e)


# noinspection PyAbstractClass
class WorkspaceGetOpenHandler(WebAPIRequestHandler):
    def get(self):
        workspace_manager = self.application.workspace_manager
        try:
            workspace_list = workspace_manager.get_open_workspaces()
            self.write_status_ok(content=[workspace.to_json_dict() for workspace in workspace_list])
        except Exception as e:
            self.write_status_error(exception=e)


# noinspection PyAbstractClass
class WorkspaceNewHandler(WebAPIRequestHandler):
    def get(self):
        base_dir = self.get_query_argument('base_dir')
        description = self.get_query_argument('description', default='')
        workspace_manager = self.application.workspace_manager
        try:
            workspace = workspace_manager.new_workspace(base_dir, description=description)
            self.write_status_ok(content=workspace.to_json_dict())
        except Exception as e:
            self.write_status_error(exception=e)


# noinspection PyAbstractClass
class WorkspaceOpenHandler(WebAPIRequestHandler):
    def get(self, base_dir):
        workspace_manager = self.application.workspace_manager
        try:
            workspace = workspace_manager.open_workspace(base_dir)
            self.write_status_ok(content=workspace.to_json_dict())
        except Exception as e:
            self.write_status_error(exception=e)


# noinspection PyAbstractClass
class WorkspaceCloseHandler(WebAPIRequestHandler):
    def get(self, base_dir):
        workspace_manager = self.application.workspace_manager
        try:
            workspace_manager.close_workspace(base_dir)
            _on_workspace_closed(self.application)
            self.write_status_ok()
        except Exception as e:
            self.write_status_error(exception=e)


# noinspection PyAbstractClass
class WorkspaceCloseAllHandler(WebAPIRequestHandler):
    def get(self):
        do_save = self.get_query_argument('do_save', default='False').lower() == 'true'
        workspace_manager = self.application.workspace_manager
        try:
            workspace_manager.close_all_workspaces(do_save)
            _on_workspace_closed(self.application)
            self.write_status_ok()
        except Exception as e:
            self.write_status_error(exception=e)


# noinspection PyAbstractClass
class WorkspaceSaveHandler(WebAPIRequestHandler):
    def get(self, base_dir):
        workspace_manager = self.application.workspace_manager
        try:
            workspace = workspace_manager.save_workspace(base_dir)
            self.write_status_ok(content=workspace.to_json_dict())
        except Exception as e:
            self.write_status_error(exception=e)


# noinspection PyAbstractClass
class WorkspaceSaveAsHandler(WebAPIRequestHandler):
    def get(self, base_dir):
        to_dir = self.get_query_argument('to_dir')
        workspace_manager = self.application.workspace_manager
        try:
            workspace = workspace_manager.save_workspace_as(base_dir, to_dir)
            self.write_status_ok(content=workspace.to_json_dict())
        except Exception as e:
            self.write_status_error(exception=e)


# noinspection PyAbstractClass
class WorkspaceSaveAllHandler(WebAPIRequestHandler):
    def get(self):
        workspace_manager = self.application.workspace_manager
        try:
            workspace_manager.save_all_workspaces()
            self.write_status_ok()
        except Exception as e:
            self.write_status_error(exception=e)


# noinspection PyAbstractClass
class WorkspaceDeleteHandler(WebAPIRequestHandler):
    def get(self, base_dir):
        workspace_manager = self.application.workspace_manager
        try:
            workspace_manager.delete_workspace(base_dir)
            self.write_status_ok()
        except Exception as e:
            self.write_status_error(exception=e)


# noinspection PyAbstractClass
class WorkspaceCleanHandler(WebAPIRequestHandler):
    def get(self, base_dir):
        workspace_manager = self.application.workspace_manager
        try:
            workspace = workspace_manager.clean_workspace(base_dir)
            self.write_status_ok(content=workspace.to_json_dict())
        except Exception as e:
            self.write_status_error(exception=e)


# noinspection PyAbstractClass
class WorkspaceRunOpHandler(WebAPIRequestHandler):
    def post(self, base_dir):
        op_name = self.get_body_argument('op_name')
        op_args = self.get_body_argument('op_args', default=None)
        op_args = json.loads(op_args) if op_args else None
        workspace_manager = self.application.workspace_manager
        try:
            with cwd(base_dir):
                workspace = workspace_manager.run_op_in_workspace(base_dir, op_name, op_args=op_args,
                                                                  monitor=_new_monitor())
            self.write_status_ok(content=workspace.to_json_dict())
        except Exception as e:
            self.write_status_error(exception=e)


# noinspection PyAbstractClass
class ResourceDeleteHandler(WebAPIRequestHandler):
    def get(self, base_dir, res_name):
        workspace_manager = self.application.workspace_manager
        try:
            with cwd(base_dir):
                workspace = workspace_manager.delete_workspace_resource(base_dir, res_name)
            self.write_status_ok(content=workspace.to_json_dict())
        except Exception as e:
            self.write_status_error(exception=e)


# noinspection PyAbstractClass
class ResourceSetHandler(WebAPIRequestHandler):
    def post(self, base_dir, res_name):
        op_name = self.get_body_argument('op_name')
        op_args = self.get_body_argument('op_args', default=None)
        op_args = json.loads(op_args) if op_args else None
        workspace_manager = self.application.workspace_manager
        try:
            with cwd(base_dir):
                workspace = workspace_manager.set_workspace_resource(base_dir, res_name, op_name, op_args=op_args,
                                                                     monitor=_new_monitor())
            self.write_status_ok(content=workspace.to_json_dict())
        except Exception as e:
            self.write_status_error(exception=e)


# noinspection PyAbstractClass
class ResourceRenameHandler(WebAPIRequestHandler):
    def get(self, base_dir, res_name):
        new_res_name = self.get_query_argument('new_res_name')
        workspace_manager = self.application.workspace_manager
        try:
            with cwd(base_dir):
                workspace = workspace_manager.rename_workspace_resource(base_dir, res_name, new_res_name)
            self.write_status_ok(content=workspace.to_json_dict())
        except Exception as e:
            self.write_status_error(exception=e)


# noinspection PyAbstractClass
class ResourceWriteHandler(WebAPIRequestHandler):
    def get(self, base_dir, res_name):
        file_path = self.get_query_argument('file_path')
        format_name = self.get_query_argument('format_name', default=None)
        workspace_manager = self.application.workspace_manager
        try:
            with cwd(base_dir):
                workspace = workspace_manager.write_workspace_resource(base_dir, res_name, file_path,
                                                                       format_name=format_name)
            self.write_status_ok(content=workspace.to_json_dict())
        except Exception as e:
            self.write_status_error(exception=e)


# noinspection PyAbstractClass
class ResourcePlotHandler(WebAPIRequestHandler):
    def get(self, base_dir, res_name):
        var_name = self.get_query_argument('var_name', default=None)
        file_path = self.get_query_argument('file_path', default=None)
        workspace_manager = self.application.workspace_manager
        try:
            with cwd(base_dir):
                workspace_manager.plot_workspace_resource(base_dir, res_name, var_name=var_name, file_path=file_path)
            self.write_status_ok()
        except Exception as e:
            self.write_status_error(exception=e)


# noinspection PyAbstractClass
class ResourcePrintHandler(WebAPIRequestHandler):
    def get(self, base_dir):
        res_name_or_expr = self.get_query_argument('res_name_or_expr', default=None)
        workspace_manager = self.application.workspace_manager
        try:
            with cwd(base_dir):
                workspace = workspace_manager.print_workspace_resource(base_dir, res_name_or_expr)
            self.write_status_ok(content=workspace.to_json_dict())
        except Exception as e:
            self.write_status_error(exception=e)


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
            no_data_value = variable.attrs.get('_FillValue', float('nan'))
            is_y_flipped = self.is_y_flipped(dataset, variable)

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

            pyramid = ImagePyramid.create_from_array(array,
                                                     level_image_id_factory=array_image_id_factory)
            pyramid = pyramid.apply(lambda image, level:
                                    TransformArrayImage(image,
                                                        image_id='tra-%s/%d' % (array_id, level),
                                                        no_data_value=no_data_value,
                                                        force_masked=True,
                                                        flip_y=is_y_flipped,
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
            print('Created pyramid "%s":' % pyramid_id)
            print('  tile_size:', pyramid.tile_size)
            print('  num_level_zero_tiles:', pyramid.num_level_zero_tiles)
            print('  num_levels:', pyramid.num_levels)

        try:
            print('PERF: >>> Tile:', image_id, z, y, x)
            t1 = time.clock()
            tile = pyramid.get_tile(int(x), int(y), int(z))
            t2 = time.clock()

            self.set_header('Content-Type', 'image/png')
            self.write(tile)

            print('PERF: <<< Tile:', image_id, z, y, x, 'took', t2 - t1, 'seconds')
            # GLOBAL_LOCK.release()
        except Exception as e:
            traceback.print_exc()
            self.write_status_error(message='Internal error: %s' % e)

    def is_y_flipped(self, dataset: xr.Dataset, variable: xr.DataArray):
        lat_var = self.get_lat_var(dataset, variable)
        if lat_var is not None:
            return lat_var[0] < lat_var[1]
        return False

    def is_lat_lon_image_variable(self, dataset: xr.Dataset, variable: xr.DataArray):
        lon_var = self.get_lon_var(dataset, variable)
        if lon_var is not None and lon_var.shape[0] >= 2:
            lat_var = self.get_lat_var(dataset, variable)
            return lat_var is not None and lat_var.shape[0] >= 2
        return False

    def get_lon_var(self, dataset: xr.Dataset, variable: xr.DataArray):
        return self.get_dim_var(dataset, variable, ['lon', 'longitude', 'long'], -1)

    def get_lat_var(self, dataset: xr.Dataset, variable: xr.DataArray):
        return self.get_dim_var(dataset, variable, ['lat', 'latitude'], -2)

    # noinspection PyMethodMayBeStatic
    def get_dim_var(self, dataset: xr.Dataset, variable: xr.DataArray, names: List[str], pos: int):
        if len(variable.dims) >= -pos:
            dim_name = variable.dims[len(variable.dims) + pos]
            for name in names:
                if name == dim_name:
                    dim_var = dataset.coords[dim_name]
                    if dim_var is not None and len(dim_var.shape) == 1 and dim_var.shape[0] >= 1:
                        return dim_var
        return None


# noinspection PyAbstractClass
class GeoJSONHandler(WebAPIRequestHandler):
    def __init__(self, application, request, shapefile_path, **kwargs):
        print('GeoJSONHandler', shapefile_path)
        super(GeoJSONHandler, self).__init__(application, request, **kwargs)
        self._shapefile_path = shapefile_path

    # see http://stackoverflow.com/questions/20018684/tornado-streaming-http-response-as-asynchttpclient-receives-chunks
    @tornado.web.asynchronous
    @tornado.gen.coroutine
    def get(self, zoom):
        zoom = int(zoom)
        print('GeoJSONHandler: shapefile_path:', self._shapefile_path, zoom)
        collection = fiona.open(self._shapefile_path)
        print('GeoJSONHandler: collection:', collection)
        print('GeoJSONHandler: collection CRS:', collection.crs)
        try:
            self.set_header('Content-Type', 'application/json')
            yield [THREAD_POOL.submit(write_feature_collection, collection, self, 2 ** -zoom)]
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
    def get(self, base_dir, res_name, zoom):

        print('ResVarGeoJSONHandler:', base_dir, res_name, zoom)

        zoom = int(zoom)

        var_name = self.get_query_argument('var')
        var_index = self.get_query_argument('index', default=None)
        var_index = tuple(map(int, var_index.split(','))) if var_index else []
        cmap_name = self.get_query_argument('cmap', default='jet')
        cmap_min = float(self.get_query_argument('min', default='nan'))
        cmap_max = float(self.get_query_argument('max', default='nan'))

        print('ResVarGeoJSONHandler:', var_name, var_index, cmap_name, cmap_min, cmap_max)

        workspace_manager = self.application.workspace_manager
        workspace = workspace_manager.get_workspace(base_dir)

        if res_name not in workspace.resource_cache:
            self.write_status_error(message='Unknown resource "%s"' % res_name)
            return

        collection = workspace.resource_cache[res_name]
        print('ResVarGeoJSONHandler: collection:', collection)
        print('ResVarGeoJSONHandler: collection CRS:', collection.crs)
        if not isinstance(collection, fiona.Collection):
            self.write_status_error(message='Resource "%s" must be a feature collection' % res_name)
            return

        try:
            self.set_header('Content-Type', 'application/json')
            yield [THREAD_POOL.submit(write_feature_collection, collection, self, 2 ** -zoom)]
        except Exception as e:
            traceback.print_exc()
            self.write_status_error(message='Internal error: %s' % e)
        self.finish()



def _new_monitor() -> Monitor:
    return ConsoleMonitor(stay_in_line=True, progress_bar_size=30)


def _on_workspace_closed(application: tornado.web.Application):
    # noinspection PyUnresolvedReferences
    workspace_manager = application.workspace_manager
    num_open_workspaces = workspace_manager.num_open_workspaces()
    check_for_auto_stop(application, num_open_workspaces == 0, interval=WEBAPI_ON_ALL_CLOSED_AUTO_STOP_AFTER)
