from collections import OrderedDict
from unittest import TestCase

import numpy as np
import pyproj

from cate.webapi.geojson import get_geometry_transform, write_feature_collection, simplify_coordinates

source_prj = pyproj.Proj(init='EPSG:4326')
target_prj = pyproj.Proj(init='EPSG:3395')


class GeometryTransformTest(TestCase):
    def test_transform_point(self):
        transform = get_geometry_transform('Point')
        self.assertIsNotNone(transform)
        coordinates = (12.0, 53.0)
        transformed_coordinates = transform(source_prj, target_prj, coordinates)
        self.assertAlmostEqual(transformed_coordinates[0], 1335833., delta=1e0)
        self.assertAlmostEqual(transformed_coordinates[1], 6948849., delta=1e0)

    def test_transform_line_string(self):
        transform = get_geometry_transform('LineString')
        self.assertIsNotNone(transform)
        coordinates = [(12.0, 53.0), (13.0, 54.0)]
        transformed_coordinates = transform(source_prj, target_prj, coordinates)
        self.assertEqual(len(transformed_coordinates), 2)
        self.assertAlmostEqual(transformed_coordinates[0][0], 1335833., delta=1e0)
        self.assertAlmostEqual(transformed_coordinates[0][1], 6948849., delta=1e0)

    def test_transform_polygon(self):
        transform = get_geometry_transform('Polygon')
        self.assertIsNotNone(transform)
        coordinates = [[(12.0, 53.0), (13.0, 54.0), (13.0, 56.0), (12.0, 53.0)]]
        transformed_coordinates = transform(source_prj, target_prj, coordinates)
        self.assertEqual(len(transformed_coordinates), 1)
        self.assertEqual(len(transformed_coordinates[0]), 4)
        self.assertAlmostEqual(transformed_coordinates[0][0][0], 1335833., delta=1e0)
        self.assertAlmostEqual(transformed_coordinates[0][0][1], 6948849., delta=1e0)

    def test_transform_multi_point(self):
        transform = get_geometry_transform('MultiPoint')
        self.assertIsNotNone(transform)
        coordinates = [(12.0, 53.0), (13.0, 54.0)]
        transformed_coordinates = transform(source_prj, target_prj, coordinates)
        self.assertEqual(len(transformed_coordinates), 2)
        self.assertAlmostEqual(transformed_coordinates[0][0], 1335833., delta=1e0)
        self.assertAlmostEqual(transformed_coordinates[0][1], 6948849., delta=1e0)

    def test_transform_multi_line_string(self):
        transform = get_geometry_transform('MultiLineString')
        self.assertIsNotNone(transform)
        coordinates = [[(12.0, 53.0), (13.0, 54.0), (13.0, 56.0)],
                       [(16.0, 53.0), (17.0, 54.0), (17.0, 56.0)]]
        transformed_coordinates = transform(source_prj, target_prj, coordinates)
        self.assertEqual(len(transformed_coordinates), 2)
        self.assertEqual(len(transformed_coordinates[0]), 3)
        self.assertEqual(len(transformed_coordinates[1]), 3)
        self.assertAlmostEqual(transformed_coordinates[0][0][0], 1335833., delta=1e0)
        self.assertAlmostEqual(transformed_coordinates[0][0][1], 6948849., delta=1e0)
        self.assertAlmostEqual(transformed_coordinates[1][0][0], 1781111., delta=1e0)
        self.assertAlmostEqual(transformed_coordinates[1][0][1], 6948849., delta=1e0)

    def test_transform_multi_polygon(self):
        transform = get_geometry_transform('MultiPolygon')
        self.assertIsNotNone(transform)
        coordinates = [[[(12.0, 53.0), (13.0, 54.0), (13.0, 56.0), (12.0, 53.0)]],
                       [[(16.0, 53.0), (17.0, 54.0), (17.0, 56.0), (16.0, 53.0)]]]
        transformed_coordinates = transform(source_prj, target_prj, coordinates)
        self.assertEqual(len(transformed_coordinates), 2)
        self.assertEqual(len(transformed_coordinates[0]), 1)
        self.assertEqual(len(transformed_coordinates[1]), 1)
        self.assertEqual(len(transformed_coordinates[0][0]), 4)
        self.assertEqual(len(transformed_coordinates[1][0]), 4)
        self.assertAlmostEqual(transformed_coordinates[0][0][0][0], 1335833., delta=1e0)
        self.assertAlmostEqual(transformed_coordinates[0][0][0][1], 6948849., delta=1e0)
        self.assertAlmostEqual(transformed_coordinates[1][0][0][0], 1781111., delta=1e0)
        self.assertAlmostEqual(transformed_coordinates[1][0][0][1], 6948849., delta=1e0)


class WriteFeatureCollectionTest(TestCase):
    def test_polygon(self):
        class Collection(list):
            pass

        collection = Collection()
        # collection.crs = dict(proj="merc", lon_0=0, k=1, x_0=0, y_0=0, ellps="WGS84", datum="WGS84", units="m")
        collection.crs = None
        collection.schema = dict(geometry="Polygon")
        collection.extend([
            OrderedDict([("type", "Feature"),
                         ("geometry", OrderedDict([("type", "Polygon"),
                                                   ("coordinates",
                                                    [[(12.0, 53.0), (13.0, 54.0), (13.0, 56.0), (12.0, 53.0)]])])),
                         ("properties", OrderedDict([("id", "1"), ("a", 3), ("b", True)]))]),
            OrderedDict([("type", "Feature"),
                         ("geometry", OrderedDict([("type", "Polygon"),
                                                   ("coordinates",
                                                    [[(12.0, 73.0), (13.0, 74.0), (13.0, 76.0), (12.0, 73.0)]])])),
                         ("properties", OrderedDict([("id", "2"), ("a", 9), ("b", False)]))]),
        ])

        from io import StringIO
        string_io = StringIO()
        num_written = write_feature_collection(collection, string_io)
        # print(num_written, string_io.getvalue())
        self.assertEqual(num_written, 2)
        self.assertEqual(string_io.getvalue(),
                         '{"type": "FeatureCollection", "features": [\n'
                         '{"type": "Feature", "geometry": {"type": "Polygon", "coordinates": [[[12.0, 53.0], [13.0, 54.0], [13.0, 56.0], [12.0, 53.0]]]}, "properties": {"id": "1", "a": 3, "b": true}},\n'
                         '{"type": "Feature", "geometry": {"type": "Polygon", "coordinates": [[[12.0, 73.0], [13.0, 74.0], [13.0, 76.0], [12.0, 73.0]]]}, "properties": {"id": "2", "a": 9, "b": false}}\n'
                         ']}\n')


class SimplifyCoordinatesTest(TestCase):
    def test_simplify_coordinates(self):
        x = np.array([1, 2, 3, 3, 3, 2, 1, 1, 1])
        y = np.array([1, 1, 1, 2, 3, 3, 3, 2, 1])

        n = simplify_coordinates(x, y, 5)
        self.assertEqual(n, 5)
        self.assertEqual(list(x[:5]), [2, 1, 3, 3, 3])
        self.assertEqual(list(y[:5]), [3, 3, 2, 3, 1])
