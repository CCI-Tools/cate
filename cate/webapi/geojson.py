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

import json
from typing import Tuple, List, Callable, Union

import fiona
import numba
import numpy as np
import pyproj

import cate.util.minheap as minheap
import heapq

Point = Tuple[float, float]
LineString = List[Point]
Ring = List[Point]
Polygon = List[Ring]
MultiPoint = List[Point]
MultiLineString = List[LineString]
MultiPolygon = List[Polygon]
Geometry = Union[Point, LineString, Ring, Polygon, MultiPoint, MultiLineString, MultiPolygon]
GeometryCollection = List[Geometry]
GeometryTransform = Callable[[pyproj.Proj, pyproj.Proj, Geometry], Geometry]


def _transform_point(source_prj: pyproj.Proj, target_prj: pyproj.Proj, point: Point) -> Point:
    return pyproj.transform(source_prj, target_prj, point[0], point[1])


def _transform_line_string(source_prj: pyproj.Proj, target_prj: pyproj.Proj, line_string: LineString) -> LineString:
    x = np.array([coord[0] for coord in line_string])
    y = np.array([coord[1] for coord in line_string])
    x, y = pyproj.transform(source_prj, target_prj, x, y)
    return [(float(x), float(y)) for x, y in zip(x, y)]


def _transform_polygon(source_prj: pyproj.Proj, target_prj: pyproj.Proj, polygon: Polygon) -> Polygon:
    transformed_polygon = []
    for ring in polygon:
        x = np.array([coord[0] for coord in ring])
        y = np.array([coord[1] for coord in ring])
        x, y = pyproj.transform(source_prj, target_prj, x, y)
        transformed_polygon.append([(float(x), float(y)) for x, y in zip(x, y)])
    return transformed_polygon


def _transform_multi_point(source_prj: pyproj.Proj, target_prj: pyproj.Proj, multi_point: MultiPoint) -> MultiPoint:
    return _transform_line_string(source_prj, target_prj, multi_point)


def _transform_multi_line_string(source_prj: pyproj.Proj, target_prj: pyproj.Proj,
                                 multi_line_string: MultiLineString) -> MultiLineString:
    return _transform_polygon(source_prj, target_prj, multi_line_string)


def _transform_multi_polygon(source_prj: pyproj.Proj, target_prj: pyproj.Proj,
                             multi_polygon: MultiPolygon) -> MultiPolygon:
    transformed_multi_polygon = []
    for polygon in multi_polygon:
        transformed_polygon = _transform_polygon(source_prj, target_prj, polygon)
        transformed_multi_polygon.append(transformed_polygon)
    return transformed_multi_polygon


_GEOMETRY_TRANSFORMS = dict(Point=_transform_point,
                            LineString=_transform_line_string,
                            Polygon=_transform_polygon,
                            MultiPoint=_transform_multi_point,
                            MultiLineString=_transform_multi_line_string,
                            MultiPolygon=_transform_multi_polygon)


def get_geometry_transform(type_name: str) -> GeometryTransform:
    """
    Return a transformation or `None` for the given geometry *type_name* string which may be one of::

    =====================================================================
    type_name  	      | Coordinates
    ==================+==================================================
    "Point"	          | A single (x, y) tuple
    "LineString"      | A list of (x, y) tuple vertices
    "Polygon"         | A list of rings (each a list of (x, y) tuples)
    "MultiPoint"      | A list of points (each a single (x, y) tuple)
    "MultiLineString" | A list of lines (each a list of (x, y) tuples)
    "MultiPolygon"    | A list of polygons (see above)
    =====================================================================

    """
    return _GEOMETRY_TRANSFORMS.get(type_name)


def write_feature_collection(collection: fiona.Collection, io):
    geometry_transform = None
    source_prj = target_prj = None

    if collection.crs:
        source_prj = pyproj.Proj(collection.crs)
        target_prj = pyproj.Proj(init='epsg:4326')
        geometry_transform = get_geometry_transform(collection.schema['geometry'])

    io.write('{"type": "FeatureCollection", "features": [\n')
    io.flush()

    feature_count = 0
    for feature in collection:
        if 'geometry' in feature:
            geometry = feature['geometry']
            if not geometry_transform and source_prj:
                geometry_transform = get_geometry_transform(geometry['type'])
            if geometry_transform:
                coordinates = geometry_transform(source_prj, target_prj, geometry['coordinates'])
                geometry['coordinates'] = coordinates
        if feature_count > 0:
            io.write(',\n')
        io.write(json.dumps(feature))
        io.flush()
        feature_count += 1

    io.write('\n]}\n')
    io.flush()

    return feature_count


@numba.jit(nopython=True)
def compute_area(x_data: np.ndarray, y_data: np.ndarray, i0: int, i1: int, i2: int) -> float:
    """
    Compute area between 3 points given by their coordinates *x_data* and *y_data* and their
    indices *i0*, *i1*, *i2*.
    """
    x0 = x_data[i0]
    y0 = y_data[i0]
    dx1 = x_data[i1] - x0
    dy1 = y_data[i1] - y0
    dx2 = x_data[i2] - x0
    dy2 = y_data[i2] - y0
    return 0.5 * abs(dx1 * dy2 - dy1 * dx2)


@numba.jit()
def simplify_geometry(x_data: np.ndarray, y_data: np.ndarray, old_n: int, new_n: int) -> int:
    """
    Simplify a ring or line-string given by its coordinates *x_data* and *y_data* from *old_n* points to
    *new_n* points. A ring is detected by same start and end points. The first and last coordinates will always be
    maintained therefore the minimum number of resulting points is 3 for rings and 2 for line-strings.

    :param x_data: The x coordinates. Will be changed in place.
    :param y_data: The x coordinates. Will be changed in place.
    :param old_n: The existing number of points.
    :param new_n: The desired number of points.
    :return: The actual number of points after simplification.
    """
    is_ring = x_data[0] == x_data[-1] and y_data[0] == y_data[-1]
    min_new_n =  3 if is_ring else 2
    if new_n < min_new_n:
        new_n = min_new_n
    if old_n < 0:
        old_n = x_data.size
    if old_n <= min_new_n:
        return old_n

    point_heap = PointHeap(x_data, y_data)
    while point_heap.size > new_n:
        point_heap.pop()

    x_copy = np.zeros(new_n, dtype=x_data.dtype)
    y_copy = np.zeros(new_n, dtype=y_data.dtype)
    point = point_heap.first_point
    i = 0
    while point is not None:
        index = point[1]
        x_copy[i] = x_data[index]
        y_copy[i] = y_data[index]
        point = point[3]
        i += 1

    x_data[:new_n] = x_copy[:]
    y_data[:new_n] = y_copy[:]
    return new_n


# TODO (forman): Optimize me!
# This is an non-optimised version of PointHeap for testing only.
# It uses the pure Python heapq implementation of a min-heap.
# We should ASAP replace heapq by the jit-compiled cate.util.minheap implementation
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
                curr_point[0] = compute_area(x_data, y_data, i, i - 1, i + 1)
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
            self._counter += 1 # Generate a unique, invalid index
            point[1] = self._counter
            point[2] = point[3] = None  # Mark point as removed / invalid
            area = compute_area(self._x_data, self._y_data, index, prev_point[1], next_point[1])
            new_point = [area, index, prev_point, next_point]
            if prev_point is not None:
                prev_point[3] = new_point
            if next_point is not None:
                next_point[2] = new_point
            self._push(new_point)
            return new_point
        return point
