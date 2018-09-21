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

Operations for resources of type pandas.DataFrame, geopandas.GeoDataFrame and cate.core.types.GeoDataFrame which all
form (Feature) Attribute Tables (FAT).

Functions
=========
"""

import math

import pyproj
import geopandas as gpd
import numpy as np
import pandas as pd
import shapely.geometry
from shapely.geometry import MultiPolygon
from shapely.ops import transform
from functools import partial

from cate.core.op import op, op_input
from cate.core.types import VarName, DataFrameLike, GeometryLike, ValidationError, VarNamesLike, PolygonLike
from cate.util.monitor import Monitor

_DEG2RAD = math.pi / 180.


@op(tags=['filter'], version='1.0')
@op_input('df', data_type=DataFrameLike)
@op_input('var', value_set_source='df', data_type=VarName)
def data_frame_min(df: DataFrameLike.TYPE, var: VarName.TYPE) -> pd.DataFrame:
    """
    Select the first record of a data frame for which the given variable value is minimal.

    :param df: The data frame or dataset.
    :param var: The variable.
    :return: A new, one-record data frame.
    """
    data_frame = DataFrameLike.convert(df)
    var_name = VarName.convert(var)
    row_index = data_frame[var_name].idxmin()
    row_frame = data_frame.loc[[row_index]]
    return _maybe_convert_to_geo_data_frame(data_frame, row_frame)


@op(tags=['filter'], version='1.0')
@op_input('df', data_type=DataFrameLike)
@op_input('var', value_set_source='df', data_type=VarName)
def data_frame_max(df: DataFrameLike.TYPE, var: VarName.TYPE) -> pd.DataFrame:
    """
    Select the first record of a data frame for which the given variable value is maximal.

    :param df: The data frame or dataset.
    :param var: The variable.
    :return: A new, one-record data frame.
    """
    data_frame = DataFrameLike.convert(df)
    var_name = VarName.convert(var)
    row_index = data_frame[var_name].idxmax()
    row_frame = data_frame.loc[[row_index]]
    return _maybe_convert_to_geo_data_frame(data_frame, row_frame)


@op(tags=['filter'], version='1.0')
@op_input('df', data_type=DataFrameLike)
@op_input('query_expr')
def data_frame_query(df: DataFrameLike.TYPE, query_expr: str) -> pd.DataFrame:
    """
    Select records from the given data frame where the given conditional query expression evaluates to "True".

    If the data frame *df* contains a geometry column (a ``GeoDataFrame`` object),
    then the query expression *query_expr* can also contain geometric relationship tests,
    for example the expression ``"population > 100000 and @within('-10, 34, 20, 60')"``
    could be used on a data frame with the *population* and a *geometry* column
    to query for larger cities in West-Europe.

    The geometric relationship tests are
    * ``@almost_equals(geom)`` - does a feature's geometry almost equal the given ``geom``;
    * ``@contains(geom)`` - does a feature's geometry contain the given ``geom``;
    * ``@crosses(geom)`` - does a feature's geometry cross the given ``geom``;
    * ``@disjoint(geom)`` - does a feature's geometry not at all intersect the given ``geom``;
    * ``@intersects(geom)`` - does a feature's geometry intersect with given ``geom``;
    * ``@touches(geom)`` - does a feature's geometry have a point in common with given ``geom``
        but does not intersect it;
    * ``@within(geom)`` - is a feature's geometry contained within given ``geom``.

    The *geom* argument may be a point ``"<lon>, <lat>"`` text string,
    a bounding box ``"<lon1>, <lat1>, <lon2>, <lat2>"`` text, or any valid geometry WKT.

    :param df: The data frame or dataset.
    :param query_expr: The conditional query expression.
    :return: A new data frame.
    """
    data_frame = DataFrameLike.convert(df)

    local_dict = dict(from_wkt=GeometryLike.convert)
    if hasattr(data_frame, 'geometry') \
            and isinstance(data_frame.geometry, gpd.GeoSeries) \
            and hasattr(data_frame, 'crs'):

        crs = data_frame.crs

        def _almost_equals(geometry: GeometryLike):
            return _data_frame_geometry_op(data_frame.geometry.geom_almost_equals, geometry, crs)

        def _contains(geometry: GeometryLike):
            return _data_frame_geometry_op(data_frame.geometry.contains, geometry, crs)

        def _crosses(geometry: GeometryLike):
            return _data_frame_geometry_op(data_frame.geometry.crosses, geometry, crs)

        def _disjoint(geometry: GeometryLike):
            return _data_frame_geometry_op(data_frame.geometry.disjoint, geometry, crs)

        def _intersects(geometry: GeometryLike):
            return _data_frame_geometry_op(data_frame.geometry.intersects, geometry, crs)

        def _touches(geometry: GeometryLike):
            return _data_frame_geometry_op(data_frame.geometry.touches, geometry, crs)

        def _within(geometry: GeometryLike):
            return _data_frame_geometry_op(data_frame.geometry.within, geometry, crs)

        local_dict['almost_equals'] = _almost_equals
        local_dict['contains'] = _contains
        local_dict['crosses'] = _crosses
        local_dict['disjoint'] = _disjoint
        local_dict['intersects'] = _intersects
        local_dict['touches'] = _touches
        local_dict['within'] = _within

    # noinspection PyTypeChecker
    subset_data_frame = data_frame.query(query_expr,
                                         truediv=True,
                                         local_dict=local_dict,
                                         global_dict={})

    return _maybe_convert_to_geo_data_frame(data_frame, subset_data_frame)



REGION_MODES = [
    'almost_equals',
    'contains',
    'crosses',
    'disjoint',
    'intersects',
    'touches',
    'within',
]


@op(tags=['filter'], version='1.0')
@op_input('gdf', data_type=DataFrameLike)
@op_input('var_names', value_set_source='gdf', data_type=VarNamesLike)
@op_input('region', data_type=PolygonLike)
@op_input('geom_op', data_type=str, value_set=REGION_MODES)
def data_frame_subset(gdf: gpd.GeoDataFrame,
                      var_names: VarNamesLike.TYPE = None,
                      region: PolygonLike.TYPE = None,
                      geom_op: bool = 'intersects') -> gpd.GeoDataFrame:
    """
    Create a GeoDataFrame subset from given variables (data frame columns) and/or region.

    :param gdf: A GeoDataFrame.
    :param var_names: The variables (columns) to select.
    :param region: A region polygon used to filter rows.
    :param geom_op: The geometric operation to be performed if *region* is given.
    :return: A GeoDataFrame subset.
    """

    region = PolygonLike.convert(region)

    var_names = VarNamesLike.convert(var_names)

    if not var_names and not region:
        return gdf

    if var_names:
        if 'geometry' not in var_names:
            var_names = ['geometry'] + var_names
        gdf = gdf[var_names]

    if region and geom_op:
        geom_str = PolygonLike.format(region)
        gdf = data_frame_query(gdf, f'@{geom_op}("{geom_str}")')

    return gdf


@op(tags=['filter'], version='1.0')
@op_input('gdf', data_type=DataFrameLike)
@op_input('location', data_type=GeometryLike)
@op_input('max_results')
@op_input('max_dist')
@op_input('dist_col_name')
def data_frame_find_closest(gdf: gpd.GeoDataFrame,
                            location: GeometryLike.TYPE,
                            max_results: int = 1,
                            max_dist: float = 180,
                            dist_col_name: str = 'distance',
                            monitor: Monitor = Monitor.NONE) -> gpd.GeoDataFrame:
    """
    Find the *max_results* records closest to given *location* in the given GeoDataFrame *gdf*.
    Return a new GeoDataFrame containing the closest records.

    If *dist_col_name* is given, store the actual distances in this column.

    Distances are great-circle distances measured in degrees from a representative center of
    the given *location* geometry to the representative centres of each geometry in the *gdf*.

    :param gdf: The GeoDataFrame.
    :param location: A location given as arbitrary geometry.
    :param max_results: Maximum number of results.
    :param max_dist: Ignore records whose distance is greater than this value.
    :param dist_col_name: Optional name of a new column that will store the actual distances.
    :return: A new GeoDataFrame containing the closest records.
    """
    location = GeometryLike.convert(location)
    location_point = location.representative_point()

    location_point = _transform_coordinates(location_point, gdf.crs)

    try:
        geometries = gdf.geometry
    except AttributeError as e:
        raise ValidationError('Missing default geometry column.') from e

    num_rows = len(geometries)
    indexes = list()

    # PERF: Note, this operation may be optimized by computing the great-circle distances using numpy array math!

    total_work = 100
    num_work_rows = 1 + num_rows // total_work
    with monitor.starting('Finding closest records', total_work):
        for i in range(num_rows):
            geometry = geometries[i]
            if geometry is not None:
                try:
                    geom_point = geometry.representative_point()
                except AttributeError as e:
                    raise ValidationError('Invalid geometry column.') from e
                # Features that span the poles will cause shapely.representative_point() to crash.
                # The quick and dirty solution was to catch the exception and ignore it
                except ValueError:
                    pass
                dist = great_circle_distance(location_point, geom_point)
                if dist <= max_dist:
                    indexes.append((i, dist))
            if i % num_work_rows == 0:
                monitor.progress(work=1)

    indexes = sorted(indexes, key=lambda item: item[1])
    num_results = min(max_results, len(indexes))
    indexes, distances = zip(*indexes[0:num_results])

    new_gdf = gdf.iloc[list(indexes)]
    if not isinstance(new_gdf, gpd.GeoDataFrame):
        new_gdf = gpd.GeoDataFrame(new_gdf)

    if dist_col_name:
        new_gdf[dist_col_name] = np.array(distances)

    return new_gdf


@op(tags=['arithmetic'], version='1.0')
@op_input('df', data_type=DataFrameLike)
@op_input('var_names', value_set_source='df', data_type=VarNamesLike)
@op_input('aggregate_geometry', data_type=bool)
def data_frame_aggregate(df: DataFrameLike.TYPE,
                         var_names: VarNamesLike.TYPE = None,
                         aggregate_geometry: bool = False,
                         monitor: Monitor = Monitor.NONE) -> pd.DataFrame:
    """
    Aggregate columns into count, mean, median, sum, std, min, and max. Return a
    new (Geo)DataFrame with a single row containing all aggregated values. Specify whether the geometries of
    the GeoDataFrame are to be aggregated. All geometries are merged union-like.

    The return data type will always be the same as the input data type.

    :param df: The (Geo)DataFrame to be analysed
    :param var_names: Variables to be aggregated ('None' uses all aggregatable columns)
    :param aggregate_geometry: Aggregate (union like) the geometry and add it to the resulting GeoDataFrame
    :param monitor: Monitor for progress bar
    :return: returns either DataFrame or GeoDataFrame. Keeps input data type
    """
    vns = VarNamesLike.convert(var_names)

    df_is_geo = isinstance(df, gpd.GeoDataFrame)
    aggregations = ["count", "mean", "median", "sum", "std", "min", "max"]

    # Check var names integrity (aggregatable, exists in data frame)
    types_accepted_for_agg = ['float64', 'int64', 'bool']
    agg_columns = list(df.select_dtypes(include=types_accepted_for_agg).columns)

    if df_is_geo:
        agg_columns.append('geometry')

    columns = list(df.columns)

    if vns is None:
        vns = agg_columns

    diff = list(set(vns) - set(columns))
    if len(diff) > 0:
        raise ValidationError('Variable ' + ','.join(diff) + ' not in data frame!')

    diff = list(set(vns) - set(agg_columns))
    if len(diff) > 0:
        raise ValidationError('Variable(s) ' + ','.join(diff) + ' not aggregatable!')

    try:
        df['geometry']
    except KeyError as e:
        raise ValidationError('Variable geometry not in GEO data frame!') from e

    # Aggregate columns
    if vns is None:
        df_buff = df.select_dtypes(include=types_accepted_for_agg).agg(aggregations)
    else:
        df_buff = df[vns].select_dtypes(include=types_accepted_for_agg).agg(aggregations)

    res = {}
    for n in df_buff.columns:
        for a in aggregations:
            val = df_buff[n][a]
            h = n + '_' + a
            res[h] = [val]

    df_agg = pd.DataFrame(res)

    # Aggregate (union) geometry if GeoDataFrame
    if df_is_geo and aggregate_geometry:
        total_work = 100
        num_work_rows = 1 + len(df) // total_work
        with monitor.starting('Aggregating geometry: ', total_work):
            multi_polygon = MultiPolygon()
            i = 0
            for rec in df.geometry:
                if monitor.is_cancelled():
                    break
                # noinspection PyBroadException
                try:
                    multi_polygon = multi_polygon.union(other=rec)
                except Exception:
                    pass

                if i % num_work_rows == 0:
                    monitor.progress(work=1)
                i += 1

        df_agg = gpd.GeoDataFrame(df_agg, geometry=[multi_polygon])
        df_agg.crs = df.crs

    return df_agg


def great_circle_distance(p1: shapely.geometry.Point, p2: shapely.geometry.Point) -> float:
    """
    Compute great-circle distance on a Sphere in degrees.

    See https://en.wikipedia.org/wiki/Great-circle_distance.

    :param p1: First point (lon, lat) in degrees
    :param p2: Second point (lon, lat) in degrees
    :return: Great-circle distance in degree
    """

    lam1, phi1 = p1.x, p1.y
    lam2, phi2 = p2.x, p2.y
    dlam = abs(lam2 - lam1)

    if dlam > 180.:
        dlam = 360. - dlam

    phi1 *= _DEG2RAD
    phi2 *= _DEG2RAD
    dlam *= _DEG2RAD

    sin_phi1 = math.sin(phi1)
    cos_phi1 = math.cos(phi1)
    sin_phi2 = math.sin(phi2)
    cos_phi2 = math.cos(phi2)
    sin_dlam = math.sin(dlam)
    cos_dlam = math.cos(dlam)

    dx = cos_phi2 * sin_dlam
    dy = cos_phi1 * sin_phi2 - sin_phi1 * cos_phi2 * cos_dlam

    y = math.sqrt(dx * dx + dy * dy)
    x = sin_phi1 * sin_phi2 + cos_phi1 * cos_phi2 * cos_dlam

    return math.atan2(y, x) / _DEG2RAD


# Transforms a geometry that is used in an operator for e.g. feature selection purposes. It assures
# that both use an identical 'crs' (projection)
def _transform_coordinates(geom, crs: dict):
    if crs and 'init' not in crs:
        raise ValidationError('No crs in GeoDataFrame' + str(crs))

    if crs and crs['init'] != 'epsg:4326':
        project = partial(
            pyproj.transform,
            pyproj.Proj(init='epsg:4326'),  # source coordinate system
            pyproj.Proj(init=crs['init'])  # destination coordinate system
        )

        return transform(project, geom)
    else:
        return geom


def _data_frame_geometry_op(instance_method, geometry: GeometryLike, crs: dict):
    geometry = GeometryLike.convert(geometry)
    geometry = _transform_coordinates(geometry, crs)

    return instance_method(geometry)


def _maybe_convert_to_geo_data_frame(data_frame: pd.DataFrame, data_frame_2: pd.DataFrame) -> pd.DataFrame:
    if isinstance(data_frame, gpd.GeoDataFrame) and not isinstance(data_frame_2, gpd.GeoDataFrame):
        return gpd.GeoDataFrame(data_frame_2, crs=data_frame.crs)
    else:
        return data_frame_2
