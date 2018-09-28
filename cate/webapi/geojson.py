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

Functions for efficiently dealing with GeoJSON.

The following GeoJSON geometry types are supported:

=====================================================================
Type Name         | Coordinates
==================+==================================================
"Point"	          | A single (x, y) tuple
"LineString"      | A list of (x, y) tuple vertices
"Polygon"         | A list of rings (each a list of (x, y) tuples)
"MultiPoint"      | A list of points (each a single (x, y) tuple)
"MultiLineString" | A list of lines (each a list of (x, y) tuples)
"MultiPolygon"    | A list of polygons (see above)
=====================================================================

"""

import heapq
import json
import logging
from typing import Tuple, List, Callable, Union, Dict, Iterable

import fiona
import numba
import numpy as np
import pyproj

__author__ = "Norman Fomferra (Brockmann Consult GmbH)"

Point = Tuple[float, float]
LineString = List[Point]
Ring = List[Point]
Polygon = List[Ring]
MultiPoint = List[Point]
MultiLineString = List[LineString]
MultiPolygon = List[Polygon]
Geometry = Union[Point, LineString, Ring, Polygon, MultiPoint, MultiLineString, MultiPolygon]
GeometryCollection = List[Geometry]
GeometryTransform = Callable[[pyproj.Proj, pyproj.Proj, float, Geometry], Geometry]
GeometryPointCounter = Callable[[Geometry], int]
Feature = Dict

_LOG = logging.getLogger('cate')


# noinspection PyUnusedLocal conservation_ratio
def _transform_point(source_prj: pyproj.Proj, target_prj: pyproj.Proj,
                     conservation_ratio: float, point: Point) -> Point:
    must_reproject = source_prj is not None
    if must_reproject:
        return pyproj.transform(source_prj, target_prj, point[0], point[1])
    return point


def _transform_line_string(source_prj: pyproj.Proj, target_prj: pyproj.Proj,
                           conservation_ratio: float, line_string: LineString) \
        -> Union[Point, LineString]:
    must_reproject = source_prj is not None
    must_pointify = conservation_ratio == 0.0
    must_simplify = 0.0 <= conservation_ratio < 1.0
    if not must_reproject and not must_simplify:
        return line_string
    x = np.array([coord[0] for coord in line_string])
    y = np.array([coord[1] for coord in line_string])
    if must_pointify:
        px, py = np.zeros(1, dtype=x.dtype), np.zeros(1, dtype=y.dtype)
        pointify_geometry(x, y, px, py)
        if must_reproject:
            px, py = pyproj.transform(source_prj, target_prj, px, py)
        return float(px[0]), float(py[0])
    else:
        x, y = simplify_geometry(x, y, conservation_ratio)
        if must_reproject:
            x, y = pyproj.transform(source_prj, target_prj, x, y)
        return [(float(x), float(y)) for x, y in zip(x, y)]


def _transform_polygon(source_prj: pyproj.Proj, target_prj: pyproj.Proj,
                       conservation_ratio: float, polygon: Union[Polygon, MultiLineString]) \
        -> Union[Point, Polygon, MultiLineString]:
    must_reproject = source_prj is not None
    must_pointify = conservation_ratio == 0.0
    must_simplify = 0.0 <= conservation_ratio < 1.0
    if not must_reproject and not must_simplify:
        return polygon
    if must_pointify:
        ring_coords = []
        for ring in polygon:
            ring_coords.extend(ring)
        x = np.array([coord[0] for coord in ring_coords])
        y = np.array([coord[1] for coord in ring_coords])
        px, py = np.zeros(1, dtype=x.dtype), np.zeros(1, dtype=y.dtype)
        pointify_geometry(x, y, px, py)
        if must_reproject:
            px, py = pyproj.transform(source_prj, target_prj, px, py)
        return float(px[0]), float(py[0])
    else:
        transformed_polygon = []
        for ring in polygon:
            x = np.array([coord[0] for coord in ring])
            y = np.array([coord[1] for coord in ring])
            if must_simplify:
                x, y = simplify_geometry(x, y, conservation_ratio)
            if must_reproject:
                x, y = pyproj.transform(source_prj, target_prj, x, y)
            transformed_polygon.append([(float(x), float(y)) for x, y in zip(x, y)])
        return transformed_polygon


def _transform_multi_point(source_prj: pyproj.Proj, target_prj: pyproj.Proj,
                           conservation_ratio: float, multi_point: MultiPoint) \
        -> Union[Point, MultiPoint]:
    return _transform_line_string(source_prj, target_prj, conservation_ratio, multi_point)


def _transform_multi_line_string(source_prj: pyproj.Proj, target_prj: pyproj.Proj,
                                 conservation_ratio: float, multi_line_string: MultiLineString) \
        -> Union[Point, MultiLineString]:
    return _transform_polygon(source_prj, target_prj, conservation_ratio, multi_line_string)


def _transform_multi_polygon(source_prj: pyproj.Proj, target_prj: pyproj.Proj,
                             conservation_ratio: float, multi_polygon: MultiPolygon) \
        -> Union[Point, MultiPolygon]:
    must_reproject = source_prj is not None
    must_simplify = 0.0 <= conservation_ratio < 1.0
    must_pointify = conservation_ratio == 0.0
    if must_reproject or must_simplify:
        if must_pointify:
            ring_coords = []
            for polygon in multi_polygon:
                for ring in polygon:
                    ring_coords.extend(ring)
            x = np.array([coord[0] for coord in ring_coords])
            y = np.array([coord[1] for coord in ring_coords])
            px, py = np.zeros(1, dtype=x.dtype), np.zeros(1, dtype=y.dtype)
            pointify_geometry(x, y, px, py)
            if must_reproject:
                px, py = pyproj.transform(source_prj, target_prj, px, py)
            return float(px[0]), float(py[0])
        else:
            transformed_multi_polygon = []
            for polygon in multi_polygon:
                transformed_polygon = _transform_polygon(source_prj, target_prj, conservation_ratio, polygon)
                transformed_multi_polygon.append(transformed_polygon)
            return transformed_multi_polygon
    return multi_polygon


_GEOMETRY_TRANSFORMS = dict(Point=_transform_point,
                            LineString=_transform_line_string,
                            Polygon=_transform_polygon,
                            MultiPoint=_transform_multi_point,
                            MultiLineString=_transform_multi_line_string,
                            MultiPolygon=_transform_multi_polygon,
                            Unknown=None)


def get_geometry_transform(type_name: str) -> GeometryTransform:
    """
    Return a transformation or `None` for the given GeoJSON geometry *type_name*.
    """
    return _GEOMETRY_TRANSFORMS.get(type_name)


# noinspection PyUnusedLocal
def _count_points_point(point: Point) -> int:
    return 1


def _count_points_line_string(line_string: LineString) -> int:
    return len(line_string)


def _count_points_polygon(polygon: Polygon) -> int:
    num_points = 0
    for ring in polygon:
        num_points += len(ring)
    return num_points


def _count_points_multi_point(multi_point: MultiPoint) -> int:
    return len(multi_point)


def _count_points_multi_line_string(multi_line_string: MultiLineString) -> int:
    num_points = 0
    for line_string in multi_line_string:
        num_points += len(line_string)
    return num_points


def _count_points_multi_polygon(multi_polygon: MultiPolygon) -> int:
    num_points = 0
    for polygon in multi_polygon:
        for ring in polygon:
            num_points += len(ring)
    return num_points


_GEOMETRY_POINT_COUNTERS = dict(Point=_count_points_point,
                                LineString=_count_points_line_string,
                                Polygon=_count_points_polygon,
                                MultiPoint=_count_points_multi_point,
                                MultiLineString=_count_points_multi_line_string,
                                MultiPolygon=_count_points_multi_polygon,
                                Unknown=None)


def get_geometry_point_counter(type_name: str) -> GeometryPointCounter:
    """
    Return a point counter or `None` for the given geometry *type_name*.
    """
    return _GEOMETRY_POINT_COUNTERS.get(type_name)


def write_feature_collection(feature_collection: Union[fiona.Collection, Iterable[Feature]],
                             io,
                             crs=None,
                             res_id: int = None,
                             num_features: int = None,
                             max_num_display_geometries: int = -1,
                             max_num_display_geometry_points: int = -1,
                             conservation_ratio: float = 1.0):
    if crs is None and hasattr(feature_collection, "crs"):
        crs = feature_collection.crs

    if num_features is None:
        try:
            num_features = len(feature_collection)
        except TypeError:
            pass

    if num_features and 0 <= max_num_display_geometries < num_features:
        conservation_ratio = 0.0

    source_prj = target_prj = None
    if crs:
        source_prj = pyproj.Proj(crs)
        target_prj = pyproj.Proj(init='epsg:4326')

    io.write('{"type": "FeatureCollection", "features": [\n')
    io.flush()

    num_features_written = 0
    feature_index = 0
    for feature in feature_collection:
        feature_ok = _transform_feature(feature,
                                        max_num_display_geometry_points,
                                        conservation_ratio,
                                        source_prj, target_prj)
        if feature_ok:
            if num_features_written > 0:
                io.write(',\n')
                io.flush()
            if res_id is not None:
                feature['_resId'] = res_id
            feature['_idx'] = feature_index
            if 'id' not in feature:
                feature['id'] = feature_index
            # Note: io.write(json.dumps(feature)) is 3x faster than json.dump(feature, fp=io)
            json_text = json.dumps(feature)
            io.write(json_text)
            num_features_written += 1

        feature_index += 1

    io.write('\n]}\n')
    io.flush()

    return num_features_written


def write_feature(feature: Feature,
                  io,
                  crs=None,
                  res_id: int = None,
                  feature_index: int = -1,
                  max_num_display_geometry_points: int = 100,
                  conservation_ratio: float = 1.0):
    source_prj = target_prj = None
    if crs:
        source_prj = pyproj.Proj(crs)
        target_prj = pyproj.Proj(init='epsg:4326')

    feature_ok = _transform_feature(feature,
                                    max_num_display_geometry_points,
                                    conservation_ratio,
                                    source_prj, target_prj)
    if feature_ok:
        if res_id is not None:
            feature['_resId'] = res_id
        feature['_idx'] = feature_index
        if 'id' not in feature:
            feature['id'] = feature_index
        # Note: io.write(json.dumps(feature)) is 3x faster than json.dump(feature, fp=io)
        json_text = json.dumps(feature, cls=SeriesJSONEncoder)
        io.write(json_text)
        io.flush()


def _transform_feature(feature: Feature,
                       max_num_display_geometry_points: int,
                       conservation_ratio: float,
                       source_prj, target_prj):
    feature_ok = True
    if 'geometry' in feature:
        geometry = feature['geometry']
        geometry_transform = get_geometry_transform(geometry['type'])
        if geometry_transform is not None:
            # print('transforming feature: ', feature)
            coordinates = geometry['coordinates']
            # noinspection PyBroadException
            try:
                geometry_conservation_ratio = conservation_ratio
                if conservation_ratio > 0.0:
                    num_geometry_points = get_geometry_point_counter(geometry['type'])(geometry)
                    if 0 <= max_num_display_geometry_points < num_geometry_points:
                        geometry_conservation_ratio = 0.0

                coordinates = geometry_transform(source_prj, target_prj,
                                                 geometry_conservation_ratio,
                                                 coordinates)
                geometry['coordinates'] = coordinates
                if geometry_conservation_ratio == 0.0:
                    geometry['type'] = 'Point'
                if geometry_conservation_ratio < 1.0:
                    # We may mask other simplifications,
                    # for time being (simp & 0x01) != 0 means, geometry is simplified
                    feature['_simp'] = 0x01
            except Exception:
                _LOG.exception('transforming feature geometry failed: %s' % geometry['type'])
                feature_ok = False
                pass
    return feature_ok


@numba.jit(nopython=True)
def pointify_geometry(x_data: np.ndarray, y_data: np.ndarray, px: np.ndarray, py: np.ndarray) -> None:
    """
    Convert a ring or line-string given by its coordinates *x_data* and *y_data* from *x_data.size* points to
    a single point representing the ring or line-string's mass center.
    A ring is detected by same start and end points.

    :param x_data: The x coordinates.
    :param y_data: The y coordinates.
    :param px: The point's resulting x coordinate.
    :param py: The point's resulting y coordinate.
    """
    is_ring = x_data[0] == x_data[-1] and y_data[0] == y_data[-1]
    # TODO - must take care of anti-meridian in x_data if we have WGS84 coordinates
    if is_ring:
        px[0] = x_data[0:-1].mean()
        py[0] = y_data[0:-1].mean()
    else:
        px[0] = x_data.mean()
        py[0] = y_data.mean()


@numba.jit(nopython=True)
def triangle_area(x_data: np.ndarray, y_data: np.ndarray, i0: int, i1: int, i2: int) -> float:
    """
    Compute area of triangle given by 3 points given by their coordinates *x_data* and *y_data*, and their
    indices *i0*, *i1*, *i2*.
    """
    x0 = x_data[i0]
    y0 = y_data[i0]
    dx1 = x_data[i1] - x0
    dy1 = y_data[i1] - y0
    dx2 = x_data[i2] - x0
    dy2 = y_data[i2] - y0
    return 0.5 * abs(dx1 * dy2 - dy1 * dx2)


@numba.jit(forceobj=True)
def simplify_geometry(x_data: np.ndarray, y_data: np.ndarray, conservation_ratio: float) \
        -> Tuple[np.ndarray, np.ndarray]:
    """
    Simplify a ring or line-string given by its coordinates *x_data* and *y_data* from *x_data.size* points to
    int(*conservation_ratio* * *x_data*.size + 0.5) points.
    A ring is detected by same start and end points. The first and last coordinates will always be
    maintained therefore the minimum number of resulting points is 3 for rings and 2 for line-strings.

    :param x_data: The x coordinates.
    :param y_data: The x coordinates.
    :param conservation_ratio: The ratio of coordinates to be conserved, 0 <= *conservation_ratio* <= 1.
    :return: A pair comprising the simplified *x_data* and *y_data*.
    """
    is_ring = x_data[0] == x_data[-1] and y_data[0] == y_data[-1]
    old_point_count = int(x_data.size)
    new_point_count = int(conservation_ratio * old_point_count + 0.5)
    min_point_count = 4 if is_ring else 2
    if new_point_count < min_point_count:
        new_point_count = min_point_count
    if old_point_count <= new_point_count:
        return x_data, y_data

    point_heap = PointHeap(x_data, y_data)
    while point_heap.size > new_point_count:
        point_heap.pop()

    new_x_data = np.zeros(new_point_count, dtype=x_data.dtype)
    new_y_data = np.zeros(new_point_count, dtype=y_data.dtype)
    point = point_heap.first_point
    i = 0
    while point is not None:
        index = point[1]
        new_x_data[i] = x_data[index]
        new_y_data[i] = y_data[index]
        point = point[3]
        i += 1

    return new_x_data, new_y_data


# TODO (forman): Optimize me!
# This is an non-optimised version of PointHeap for testing only.
# It uses the pure Python heapq implementation of a min-heap.
# We should ASAP replace heapq by the jit-compiled cate.webapi.minheap implementation
# so that we can compile the PointHeap class using @numba.jitclass().
# See http://numba.pydata.org/numba-doc/dev/user/jitclass.html

# PointHeapSpec = [
#     ('_x_data', np.float64[:]),
#     ('_y_data', np.float64[:]),
#     ('_point_heap', np.int64[:]),
#     ('_size', np.int64),
# ]
# @numba.jitclass(PointHeapSpec)
class PointHeap:
    def __init__(self, x_data: np.ndarray, y_data: np.ndarray):
        size = x_data.size
        if size < 2:
            raise ValueError('x_data.size must be greater than 2')
        if size != y_data.size:
            raise ValueError('x_data.size must be equal to y_data.size')
        self._x_data = x_data
        self._y_data = y_data
        self._point_heap = []
        self._size = size

        first_point = None
        prev_point = None
        for i in range(size):
            curr_point = [-1.0, i, prev_point, None]
            if prev_point is None:
                first_point = curr_point
            else:
                prev_point[3] = curr_point
            if 0 < i < size - 1:
                curr_point[0] = triangle_area(x_data, y_data, i, i - 1, i + 1)
                self._push(curr_point)
            prev_point = curr_point

        # Must store _first_point, so we can later loop through connected points
        self._first_point = first_point
        # _counter is used to generate unique indexes, which don't occur in valid points
        self._counter = size

    @property
    def size(self):
        return self._size

    @property
    def first_point(self):
        return self._first_point

    def pop(self):
        """Remove and return the smallest-area point. Raise KeyError if empty."""
        while self._point_heap:
            point = heapq.heappop(self._point_heap)
            prev_point = point[2]
            next_point = point[3]
            has_prev_point = prev_point is not None
            has_next_point = next_point is not None
            if has_prev_point or has_next_point:  # Not yet removed?
                if has_prev_point:
                    prev_point = self._update_point(prev_point)
                if has_next_point:
                    next_point = self._update_point(next_point)
                if has_prev_point and has_next_point:
                    prev_point[3] = next_point
                    next_point[2] = prev_point
                self._size -= 1
                return point
        raise KeyError('pop from an empty priority queue')

    def _push(self, point):
        heapq.heappush(self._point_heap, point)

    def _update_point(self, point):
        prev_point = point[2]
        next_point = point[3]
        if prev_point is not None and next_point is not None:
            index = point[1]
            self._counter += 1  # Generate a unique, invalid index
            point[1] = self._counter
            point[2] = point[3] = None  # Mark point as removed / invalid
            area = triangle_area(self._x_data, self._y_data, index, prev_point[1], next_point[1])
            new_point = [area, index, prev_point, next_point]
            if prev_point is not None:
                prev_point[3] = new_point
            if next_point is not None:
                next_point[2] = new_point
            self._push(new_point)
            return new_point
        return point


class SeriesJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if hasattr(obj, 'dtype'):
            if np.issubdtype(obj.dtype, np.integer):
                return int(obj)
            elif np.issubdtype(obj.dtype, np.floating):
                return float(obj)
            else:
                return str(obj)

        return json.JSONEncoder.default(self, obj)
