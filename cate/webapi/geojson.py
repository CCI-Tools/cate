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
import numpy as np
import pyproj

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
