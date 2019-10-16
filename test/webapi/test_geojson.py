import os.path
from collections import OrderedDict
from unittest import TestCase

import fiona
import numpy as np
import pyproj

from cate.webapi.geojson import get_geometry_transform, write_feature_collection, simplify_geometry

source_prj = pyproj.Proj({'init': 'EPSG:4326'})
target_prj = pyproj.Proj({'init': 'EPSG:3395'})


class GeometryTransformTest(TestCase):
    def test_transform_point(self):
        transform = get_geometry_transform('Point')
        self.assertIsNotNone(transform)
        coordinates = (12.0, 53.0)
        transformed_coordinates = transform(source_prj, target_prj, 1.0, coordinates)
        self.assertAlmostEqual(transformed_coordinates[0], 1335833., delta=1e0)
        self.assertAlmostEqual(transformed_coordinates[1], 6948849., delta=1e0)

    def test_transform_line_string(self):
        transform = get_geometry_transform('LineString')
        self.assertIsNotNone(transform)
        coordinates = [(12.0, 53.0), (13.0, 54.0)]
        transformed_coordinates = transform(source_prj, target_prj, 1.0, coordinates)
        self.assertEqual(len(transformed_coordinates), 2)
        self.assertAlmostEqual(transformed_coordinates[0][0], 1335833., delta=1e0)
        self.assertAlmostEqual(transformed_coordinates[0][1], 6948849., delta=1e0)

    def test_transform_polygon(self):
        transform = get_geometry_transform('Polygon')
        self.assertIsNotNone(transform)
        coordinates = [[(12.0, 53.0), (13.0, 54.0), (13.0, 56.0), (12.0, 53.0)]]
        transformed_coordinates = transform(source_prj, target_prj, 1.0, coordinates)
        self.assertEqual(len(transformed_coordinates), 1)
        self.assertEqual(len(transformed_coordinates[0]), 4)
        self.assertAlmostEqual(transformed_coordinates[0][0][0], 1335833., delta=1e0)
        self.assertAlmostEqual(transformed_coordinates[0][0][1], 6948849., delta=1e0)

    def test_transform_multi_point(self):
        transform = get_geometry_transform('MultiPoint')
        self.assertIsNotNone(transform)
        coordinates = [(12.0, 53.0), (13.0, 54.0)]
        transformed_coordinates = transform(source_prj, target_prj, 1.0, coordinates)
        self.assertEqual(len(transformed_coordinates), 2)
        self.assertAlmostEqual(transformed_coordinates[0][0], 1335833., delta=1e0)
        self.assertAlmostEqual(transformed_coordinates[0][1], 6948849., delta=1e0)

    def test_transform_multi_line_string(self):
        transform = get_geometry_transform('MultiLineString')
        self.assertIsNotNone(transform)
        coordinates = [[(12.0, 53.0), (13.0, 54.0), (13.0, 56.0)],
                       [(16.0, 53.0), (17.0, 54.0), (17.0, 56.0)]]
        transformed_coordinates = transform(source_prj, target_prj, 1.0, coordinates)
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
        transformed_coordinates = transform(source_prj, target_prj, 1.0, coordinates)
        self.assertEqual(len(transformed_coordinates), 2)
        self.assertEqual(len(transformed_coordinates[0]), 1)
        self.assertEqual(len(transformed_coordinates[1]), 1)
        self.assertEqual(len(transformed_coordinates[0][0]), 4)
        self.assertEqual(len(transformed_coordinates[1][0]), 4)
        self.assertAlmostEqual(transformed_coordinates[0][0][0][0], 1335833., delta=1e0)
        self.assertAlmostEqual(transformed_coordinates[0][0][0][1], 6948849., delta=1e0)
        self.assertAlmostEqual(transformed_coordinates[1][0][0][0], 1781111., delta=1e0)
        self.assertAlmostEqual(transformed_coordinates[1][0][0][1], 6948849., delta=1e0)

    def test_transform_large_multi_polygon(self):
        transform = get_geometry_transform('MultiPolygon')
        self.assertIsNotNone(transform)
        transformed_coordinates = transform(None, None, 0.0, LARGE_MULTI_POLYGON)
        self.assertEqual(len(transformed_coordinates), 2)
        transformed_coordinates = transform(None, None, 0.5, LARGE_MULTI_POLYGON)
        self.assertEqual(len(transformed_coordinates), 13)
        transformed_coordinates = transform(None, None, 1.0, LARGE_MULTI_POLYGON)
        self.assertEqual(len(transformed_coordinates), 13)


class WriteFeatureCollectionTest(TestCase):
    def test_polygon(self):
        self.maxDiff = None

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
                         '{"type": "Feature", "geometry": {"type": "Polygon", '
                         '"coordinates": [[[12.0, 53.0], [13.0, 54.0], [13.0, 56.0], [12.0, 53.0]]]}, '
                         '"properties": {"id": "1", "a": 3, "b": true}, "_idx": 0, "id": 0},\n'
                         '{"type": "Feature", "geometry": {"type": "Polygon", '
                         '"coordinates": [[[12.0, 73.0], [13.0, 74.0], [13.0, 76.0], [12.0, 73.0]]]}, '
                         '"properties": {"id": "2", "a": 9, "b": false}, "_idx": 1, "id": 1}\n'
                         ']}\n')

    def test_polygon_with_simp(self):
        self.maxDiff = None

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
                                                    [[(12.0, 53.0), (13.0, 54.0), (13.3, 55.1), (13.0, 56.0),
                                                      (12.0, 53.0)]])])),
                         ("properties", OrderedDict([("id", "1"), ("a", 3), ("b", True)]))]),
            OrderedDict([("type", "Feature"),
                         ("geometry", OrderedDict([("type", "Polygon"),
                                                   ("coordinates",
                                                    [[(12.0, 73.0), (13.0, 74.0), (13.1, 75.4), (13.0, 76.0),
                                                      (12.0, 73.0)]])])),
                         ("properties", OrderedDict([("id", "2"), ("a", 9), ("b", False)]))]),
        ])

        from io import StringIO
        string_io = StringIO()
        num_written = write_feature_collection(collection, string_io, conservation_ratio=0.5)
        # print(num_written, string_io.getvalue())
        self.assertEqual(num_written, 2)
        self.assertEqual(string_io.getvalue(),
                         '{"type": "FeatureCollection", "features": [\n'
                         '{"type": "Feature", "geometry": {"type": "Polygon", '
                         '"coordinates": [[[12.0, 53.0], [13.0, 54.0], [13.0, 56.0], [12.0, 53.0]]]}, '
                         '"properties": {"id": "1", "a": 3, "b": true}, "_simp": 1, "_idx": 0, "id": 0},\n'
                         '{"type": "Feature", "geometry": {"type": "Polygon", '
                         '"coordinates": [[[12.0, 73.0], [13.0, 74.0], [13.0, 76.0], [12.0, 73.0]]]}, '
                         '"properties": {"id": "2", "a": 9, "b": false}, "_simp": 1, "_idx": 1, "id": 1}\n'
                         ']}\n')

    def test_countries_with_simp(self):
        self.maxDiff = None

        class Collection(list):
            pass

        file = os.path.join(os.path.dirname(__file__), '..', '..', 'cate', 'ds', 'data', 'countries',
                            'countries.geojson')

        collection = fiona.open(file)

        from io import StringIO
        string_io = StringIO()
        num_written = write_feature_collection(collection, string_io, conservation_ratio=0)
        self.assertEqual(num_written, 179)


class SimplifyGeometryTest(TestCase):
    def test_simplify_none(self):
        # A triangle (ring)
        x_data = [1, 3, 3, 1]
        y_data = [1, 1, 3, 1]

        x = np.array(x_data)
        y = np.array(y_data)
        sx, sy = simplify_geometry(x, y, 1.0)
        self.assertIs(sx, x)
        self.assertIs(sy, y)

        # A square (ring)
        x_data = [1, 3, 3, 1, 1]
        y_data = [1, 1, 3, 3, 1]

        x = np.array(x_data)
        y = np.array(y_data)
        sx, sy = simplify_geometry(x, y, 1.0)
        self.assertIs(sx, x)
        self.assertIs(sy, y)

    def test_simplify_min(self):
        # A line
        x_data = [1, 2]
        y_data = [1, 2]

        x = np.array(x_data)
        y = np.array(y_data)
        sx, sy = simplify_geometry(x, y, 0.0)
        self.assertIs(sx, x)
        self.assertIs(sy, y)

        # A line string
        x_data = [1, 2, 3]
        y_data = [1, 2, 3]

        x = np.array(x_data)
        y = np.array(y_data)
        sx, sy = simplify_geometry(x, y, 0.0)
        self.assertEqual((sx.size, sy.size), (2, 2))
        self.assertEqual(list(sx), [1, 3])
        self.assertEqual(list(sy), [1, 3])

        # A square (ring)
        x_data = [1, 3, 3, 1, 1]
        y_data = [1, 1, 3, 3, 1]

        x = np.array(x_data)
        y = np.array(y_data)
        sx, sy = simplify_geometry(x, y, 0.0)
        self.assertEqual((sx.size, sy.size), (4, 4))
        self.assertEqual(list(sx), [1, 3, 1, 1])
        self.assertEqual(list(sy), [1, 3, 3, 1])

    def test_simplify_square(self):
        x_data = [1, 2, 3, 3, 3, 2, 1, 1, 1]
        y_data = [1, 1, 1, 2, 3, 3, 3, 2, 1]

        #
        #  3  o----o----o         3  o---------o
        #     |         |            |         |
        #  2  o         o   ==>   2  |         |
        #     |         |            |         |
        #  1  o----o----o         1  o---------o
        #     1    2    3            1    2    3
        #
        x = np.array(x_data)
        y = np.array(y_data)
        sx, sy = simplify_geometry(x, y, 5. / 9.)
        self.assertEqual((sx.size, sy.size), (5, 5))
        self.assertEqual(list(sx), [1, 3, 3, 1, 1])
        self.assertEqual(list(sy), [1, 1, 3, 3, 1])

        #
        #  3  o----o----o         3  o----o----o
        #     |         |            |         |
        #  2  o         o   ==>   2  o         |
        #     |         |            |         |
        #  1  o----o----o         1  o---------o
        #     1    2    3            1    2    3
        #
        x = np.array(x_data)
        y = np.array(y_data)
        sx, sy = simplify_geometry(x, y, 7. / 9.)
        self.assertEqual((sx.size, sy.size), (7, 7))
        self.assertEqual(list(sx), [1, 3, 3, 2, 1, 1, 1])
        self.assertEqual(list(sy), [1, 1, 3, 3, 3, 2, 1])

        #
        #  3  o----o----o         3  o----------o
        #     |         |            |       _/
        #  2  o         o   ==>   2  |    _/
        #     |         |            | _/
        #  1  o----o----o         1  o
        #     1    2    3            1     2    3
        #
        x = np.array(x_data)
        y = np.array(y_data)
        sx, sy = simplify_geometry(x, y, 4. / 9.)
        self.assertEqual((sx.size, sy.size), (4, 4))
        self.assertEqual(list(sx), [1, 3, 1, 1])
        self.assertEqual(list(sy), [1, 3, 3, 1])


LARGE_MULTI_POLYGON = [
    [
        [
            (143.648007, 50.7476),
            (144.654148, 48.976391),
            (143.173928, 49.306551),
            (142.558668, 47.861575),
            (143.533492, 46.836728),
            (143.505277, 46.137908),
            (142.747701, 46.740765),
            (142.09203, 45.966755),
            (141.906925, 46.805929),
            (142.018443, 47.780133),
            (141.904445, 48.859189),
            (142.1358, 49.615163),
            (142.179983, 50.952342),
            (141.594076, 51.935435),
            (141.682546, 53.301966),
            (142.606934, 53.762145),
            (142.209749, 54.225476),
            (142.654786, 54.365881),
            (142.914616, 53.704578),
            (143.260848, 52.74076),
            (143.235268, 51.75666),
            (143.648007, 50.7476)]
    ],
    [
        [
            (22.731099, 54.327537),
            (20.892245, 54.312525),
            (19.66064, 54.426084),
            (19.888481, 54.86616),
            (21.268449, 55.190482),
            (22.315724, 55.015299),
            (22.757764, 54.856574),
            (22.651052, 54.582741),
            (22.731099, 54.327537)
        ]
    ],
    [
        [
            (-175.01425, 66.58435),
            (-174.33983, 66.33556),
            (-174.57182, 67.06219),
            (-171.85731, 66.91308),
            (-169.89958, 65.97724),
            (-170.89107, 65.54139),
            (-172.53025, 65.43791),
            (-172.555, 64.46079),
            (-172.95533, 64.25269),
            (-173.89184, 64.2826),
            (-174.65392, 64.63125),
            (-175.98353, 64.92288),
            (-176.20716, 65.35667),
            (-177.22266, 65.52024),
            (-178.35993, 65.39052),
            (-178.90332, 65.74044),
            (-178.68611, 66.11211),
            (-179.88377, 65.87456),
            (-179.43268, 65.40411),
            (-180.0, 64.979709),
            (-180.0, 68.963636),
            (-177.55, 68.2),
            (-174.92825, 67.20589),
            (-175.01425, 66.58435)
        ]
    ],
    [
        [
            (180.0, 70.832199),
            (178.903425, 70.78114),
            (178.7253, 71.0988),
            (180.0, 71.515714),
            (180.0, 70.832199)
        ]
    ],
    [
        [
            (-178.69378, 70.89302),
            (-180.0, 70.832199),
            (-180.0, 71.515714),
            (-179.871875, 71.55762),
            (-179.02433, 71.55553),
            (-177.577945, 71.26948),
            (-177.663575, 71.13277),
            (-178.69378, 70.89302)
        ]
    ],
    [
        [
            (143.60385, 73.21244),
            (142.08763, 73.20544),
            (140.038155, 73.31692),
            (139.86312, 73.36983),
            (140.81171, 73.76506),
            (142.06207, 73.85758),
            (143.48283, 73.47525),
            (143.60385, 73.21244)
        ]
    ],
    [
        [
            (150.73167, 75.08406),
            (149.575925, 74.68892),
            (147.977465, 74.778355),
            (146.11919, 75.17298),
            (146.358485, 75.49682),
            (148.22223, 75.345845),
            (150.73167, 75.08406)
        ]
    ],
    [
        [
            (145.086285, 75.562625),
            (144.3, 74.82),
            (140.61381, 74.84768),
            (138.95544, 74.61148),
            (136.97439, 75.26167),
            (137.51176, 75.94917),
            (138.831075, 76.13676),
            (141.471615, 76.09289),
            (145.086285, 75.562625)
        ]
    ],
    [
        [
            (57.535693, 70.720464),
            (56.944979, 70.632743),
            (53.677375, 70.762658),
            (53.412017, 71.206662),
            (51.601895, 71.474759),
            (51.455754, 72.014881),
            (52.478275, 72.229442),
            (52.444169, 72.774731),
            (54.427614, 73.627548),
            (53.50829, 73.749814),
            (55.902459, 74.627486),
            (55.631933, 75.081412),
            (57.868644, 75.60939),
            (61.170044, 76.251883),
            (64.498368, 76.439055),
            (66.210977, 76.809782),
            (68.15706, 76.939697),
            (68.852211, 76.544811),
            (68.180573, 76.233642),
            (64.637326, 75.737755),
            (61.583508, 75.260885),
            (58.477082, 74.309056),
            (56.986786, 73.333044),
            (55.419336, 72.371268),
            (55.622838, 71.540595),
            (57.535693, 70.720464)
        ]
    ],
    [
        [
            (106.97013, 76.97419),
            (107.24, 76.48),
            (108.1538, 76.72335),
            (111.07726, 76.71),
            (113.33151, 76.22224),
            (114.13417, 75.84764),
            (113.88539, 75.32779),
            (112.77918, 75.03186),
            (110.15125, 74.47673),
            (109.4, 74.18),
            (110.64, 74.04),
            (112.11919, 73.78774),
            (113.01954, 73.97693),
            (113.52958, 73.33505),
            (113.96881, 73.59488),
            (115.56782, 73.75285),
            (118.77633, 73.58772),
            (119.02, 73.12),
            (123.20066, 72.97122),
            (123.25777, 73.73503),
            (125.38, 73.56),
            (126.97644, 73.56549),
            (128.59126, 73.03871),
            (129.05157, 72.39872),
            (128.46, 71.98),
            (129.71599, 71.19304),
            (131.28858, 70.78699),
            (132.2535, 71.8363),
            (133.85766, 71.38642),
            (135.56193, 71.65525),
            (137.49755, 71.34763),
            (138.23409, 71.62803),
            (139.86983, 71.48783),
            (139.14791, 72.41619),
            (140.46817, 72.84941),
            (149.5, 72.2),
            (150.35118, 71.60643),
            (152.9689, 70.84222),
            (157.00688, 71.03141),
            (158.99779, 70.86672),
            (159.83031, 70.45324),
            (159.70866, 69.72198),
            (160.94053, 69.43728),
            (162.27907, 69.64204),
            (164.05248, 69.66823),
            (165.94037, 69.47199),
            (167.83567, 69.58269),
            (169.57763, 68.6938),
            (170.81688, 69.01363),
            (170.0082, 69.65276),
            (170.45345, 70.09703),
            (173.64391, 69.81743),
            (175.72403, 69.87725),
            (178.6, 69.4),
            (180.0, 68.963636),
            (180.0, 64.979709),
            (179.99281, 64.97433),
            (178.7072, 64.53493),
            (177.41128, 64.60821),
            (178.313, 64.07593),
            (178.90825, 63.25197),
            (179.37034, 62.98262),
            (179.48636, 62.56894),
            (179.22825, 62.3041),
            (177.3643, 62.5219),
            (174.56929, 61.76915),
            (173.68013, 61.65261),
            (172.15, 60.95),
            (170.6985, 60.33618),
            (170.33085, 59.88177),
            (168.90046, 60.57355),
            (166.29498, 59.78855),
            (165.84, 60.16),
            (164.87674, 59.7316),
            (163.53929, 59.86871),
            (163.21711, 59.21101),
            (162.01733, 58.24328),
            (162.05297, 57.83912),
            (163.19191, 57.61503),
            (163.05794, 56.15924),
            (162.12958, 56.12219),
            (161.70146, 55.28568),
            (162.11749, 54.85514),
            (160.36877, 54.34433),
            (160.02173, 53.20257),
            (158.53094, 52.95868),
            (158.23118, 51.94269),
            (156.78979, 51.01105),
            (156.42, 51.7),
            (155.99182, 53.15895),
            (155.43366, 55.38103),
            (155.91442, 56.76792),
            (156.75815, 57.3647),
            (156.81035, 57.83204),
            (158.36433, 58.05575),
            (160.15064, 59.31477),
            (161.87204, 60.343),
            (163.66969, 61.1409),
            (164.47355, 62.55061),
            (163.25842, 62.46627),
            (162.65791, 61.6425),
            (160.12148, 60.54423),
            (159.30232, 61.77396),
            (156.72068, 61.43442),
            (154.21806, 59.75818),
            (155.04375, 59.14495),
            (152.81185, 58.88385),
            (151.26573, 58.78089),
            (151.33815, 59.50396),
            (149.78371, 59.65573),
            (148.54481, 59.16448),
            (145.48722, 59.33637),
            (142.19782, 59.03998),
            (138.95848, 57.08805),
            (135.12619, 54.72959),
            (136.70171, 54.60355),
            (137.19342, 53.97732),
            (138.1647, 53.75501),
            (138.80463, 54.25455),
            (139.90151, 54.18968),
            (141.34531, 53.08957),
            (141.37923, 52.23877),
            (140.59742, 51.23967),
            (140.51308, 50.04553),
            (140.06193, 48.44671),
            (138.55472, 46.99965),
            (138.21971, 46.30795),
            (136.86232, 45.1435),
            (135.51535, 43.989),
            (134.86939, 43.39821),
            (133.53687, 42.81147),
            (132.90627, 42.79849),
            (132.27807, 43.28456),
            (130.93587, 42.55274),
            (130.78, 42.22),
            (130.64, 42.395),
            (130.633866, 42.903015),
            (131.144688, 42.92999),
            (131.288555, 44.11152),
            (131.02519, 44.96796),
            (131.883454, 45.321162),
            (133.09712, 45.14409),
            (133.769644, 46.116927),
            (134.11235, 47.21248),
            (134.50081, 47.57845),
            (135.026311, 48.47823),
            (133.373596, 48.183442),
            (132.50669, 47.78896),
            (130.98726, 47.79013),
            (130.582293, 48.729687),
            (129.397818, 49.4406),
            (127.6574, 49.76027),
            (127.287456, 50.739797),
            (126.939157, 51.353894),
            (126.564399, 51.784255),
            (125.946349, 52.792799),
            (125.068211, 53.161045),
            (123.57147, 53.4588),
            (122.245748, 53.431726),
            (121.003085, 53.251401),
            (120.177089, 52.753886),
            (120.725789, 52.516226),
            (120.7382, 51.96411),
            (120.18208, 51.64355),
            (119.27939, 50.58292),
            (119.288461, 50.142883),
            (117.879244, 49.510983),
            (116.678801, 49.888531),
            (115.485695, 49.805177),
            (114.96211, 50.140247),
            (114.362456, 50.248303),
            (112.89774, 49.543565),
            (111.581231, 49.377968),
            (110.662011, 49.130128),
            (109.402449, 49.292961),
            (108.475167, 49.282548),
            (107.868176, 49.793705),
            (106.888804, 50.274296),
            (105.886591, 50.406019),
            (104.62158, 50.27532),
            (103.676545, 50.089966),
            (102.25589, 50.51056),
            (102.06521, 51.25991),
            (100.88948, 51.516856),
            (99.981732, 51.634006),
            (98.861491, 52.047366),
            (97.82574, 51.010995),
            (98.231762, 50.422401),
            (97.25976, 49.72605),
            (95.81402, 49.97746),
            (94.815949, 50.013433),
            (94.147566, 50.480537),
            (93.10421, 50.49529),
            (92.234712, 50.802171),
            (90.713667, 50.331812),
            (88.805567, 49.470521),
            (87.751264, 49.297198),
            (87.35997, 49.214981),
            (86.829357, 49.826675),
            (85.54127, 49.692859),
            (85.11556, 50.117303),
            (84.416377, 50.3114),
            (83.935115, 50.889246),
            (83.383004, 51.069183),
            (81.945986, 50.812196),
            (80.568447, 51.388336),
            (80.03556, 50.864751),
            (77.800916, 53.404415),
            (76.525179, 54.177003),
            (76.8911, 54.490524),
            (74.38482, 53.54685),
            (73.425679, 53.48981),
            (73.508516, 54.035617),
            (72.22415, 54.376655),
            (71.180131, 54.133285),
            (70.865267, 55.169734),
            (69.068167, 55.38525),
            (68.1691, 54.970392),
            (65.66687, 54.60125),
            (65.178534, 54.354228),
            (61.4366, 54.00625),
            (60.978066, 53.664993),
            (61.699986, 52.979996),
            (60.739993, 52.719986),
            (60.927269, 52.447548),
            (59.967534, 51.96042),
            (61.588003, 51.272659),
            (61.337424, 50.79907),
            (59.932807, 50.842194),
            (59.642282, 50.545442),
            (58.36332, 51.06364),
            (56.77798, 51.04355),
            (55.71694, 50.62171),
            (54.532878, 51.02624),
            (52.328724, 51.718652),
            (50.766648, 51.692762),
            (48.702382, 50.605128),
            (48.577841, 49.87476),
            (47.54948, 50.454698),
            (46.751596, 49.356006),
            (47.043672, 49.152039),
            (46.466446, 48.394152),
            (47.31524, 47.71585),
            (48.05725, 47.74377),
            (48.694734, 47.075628),
            (48.59325, 46.56104),
            (49.10116, 46.39933),
            (48.64541, 45.80629),
            (47.67591, 45.64149),
            (46.68201, 44.6092),
            (47.59094, 43.66016),
            (47.49252, 42.98658),
            (48.58437, 41.80888),
            (47.987283, 41.405819),
            (47.815666, 41.151416),
            (47.373315, 41.219732),
            (46.686071, 41.827137),
            (46.404951, 41.860675),
            (45.7764, 42.09244),
            (45.470279, 42.502781),
            (44.537623, 42.711993),
            (43.93121, 42.55496),
            (43.75599, 42.74083),
            (42.3944, 43.2203),
            (40.92219, 43.38215),
            (40.076965, 43.553104),
            (39.955009, 43.434998),
            (38.68, 44.28),
            (37.53912, 44.65721),
            (36.67546, 45.24469),
            (37.40317, 45.40451),
            (38.23295, 46.24087),
            (37.67372, 46.63657),
            (39.14767, 47.04475),
            (39.1212, 47.26336),
            (38.223538, 47.10219),
            (38.255112, 47.5464),
            (38.77057, 47.82562),
            (39.738278, 47.898937),
            (39.89562, 48.23241),
            (39.67465, 48.78382),
            (40.080789, 49.30743),
            (40.06904, 49.60105),
            (38.594988, 49.926462),
            (38.010631, 49.915662),
            (37.39346, 50.383953),
            (36.626168, 50.225591),
            (35.356116, 50.577197),
            (35.37791, 50.77394),
            (35.022183, 51.207572),
            (34.224816, 51.255993),
            (34.141978, 51.566413),
            (34.391731, 51.768882),
            (33.7527, 52.335075),
            (32.715761, 52.238465),
            (32.412058, 52.288695),
            (32.15944, 52.06125),
            (31.78597, 52.10168),
            (31.540018, 52.742052),
            (31.305201, 53.073996),
            (31.49764, 53.16743),
            (32.304519, 53.132726),
            (32.693643, 53.351421),
            (32.405599, 53.618045),
            (31.731273, 53.794029),
            (31.791424, 53.974639),
            (31.384472, 54.157056),
            (30.757534, 54.811771),
            (30.971836, 55.081548),
            (30.873909, 55.550976),
            (29.896294, 55.789463),
            (29.371572, 55.670091),
            (29.229513, 55.918344),
            (28.176709, 56.16913),
            (27.855282, 56.759326),
            (27.770016, 57.244258),
            (27.288185, 57.474528),
            (27.716686, 57.791899),
            (27.42015, 58.72457),
            (28.131699, 59.300825),
            (27.98112, 59.47537),
            (29.1177, 60.02805),
            (28.07, 60.50352),
            (30.211107, 61.780028),
            (31.139991, 62.357693),
            (31.516092, 62.867687),
            (30.035872, 63.552814),
            (30.444685, 64.204453),
            (29.54443, 64.948672),
            (30.21765, 65.80598),
            (29.054589, 66.944286),
            (29.977426, 67.698297),
            (28.445944, 68.364613),
            (28.59193, 69.064777),
            (29.39955, 69.15692),
            (31.10108, 69.55811),
            (32.13272, 69.90595),
            (33.77547, 69.30142),
            (36.51396, 69.06342),
            (40.29234, 67.9324),
            (41.05987, 67.45713),
            (41.12595, 66.79158),
            (40.01583, 66.26618),
            (38.38295, 65.99953),
            (33.91871, 66.75961),
            (33.18444, 66.63253),
            (34.81477, 65.90015),
            (34.878574, 65.436213),
            (34.94391, 64.41437),
            (36.23129, 64.10945),
            (37.01273, 63.84983),
            (37.14197, 64.33471),
            (36.539579, 64.76446),
            (37.17604, 65.14322),
            (39.59345, 64.52079),
            (40.4356, 64.76446),
            (39.7626, 65.49682),
            (42.09309, 66.47623),
            (43.01604, 66.41858),
            (43.94975, 66.06908),
            (44.53226, 66.75634),
            (43.69839, 67.35245),
            (44.18795, 67.95051),
            (43.45282, 68.57079),
            (46.25, 68.25),
            (46.82134, 67.68997),
            (45.55517, 67.56652),
            (45.56202, 67.01005),
            (46.34915, 66.66767),
            (47.89416, 66.88455),
            (48.13876, 67.52238),
            (50.22766, 67.99867),
            (53.71743, 68.85738),
            (54.47171, 68.80815),
            (53.48582, 68.20131),
            (54.72628, 68.09702),
            (55.44268, 68.43866),
            (57.31702, 68.46628),
            (58.802, 68.88082),
            (59.94142, 68.27844),
            (61.07784, 68.94069),
            (60.03, 69.52),
            (60.55, 69.85),
            (63.504, 69.54739),
            (64.888115, 69.234835),
            (68.51216, 68.09233),
            (69.18068, 68.61563),
            (68.16444, 69.14436),
            (68.13522, 69.35649),
            (66.93008, 69.45461),
            (67.25976, 69.92873),
            (66.72492, 70.70889),
            (66.69466, 71.02897),
            (68.54006, 71.9345),
            (69.19636, 72.84336),
            (69.94, 73.04),
            (72.58754, 72.77629),
            (72.79603, 72.22006),
            (71.84811, 71.40898),
            (72.47011, 71.09019),
            (72.79188, 70.39114),
            (72.5647, 69.02085),
            (73.66787, 68.4079),
            (73.2387, 67.7404),
            (71.28, 66.32),
            (72.42301, 66.17267),
            (72.82077, 66.53267),
            (73.92099, 66.78946),
            (74.18651, 67.28429),
            (75.052, 67.76047),
            (74.46926, 68.32899),
            (74.93584, 68.98918),
            (73.84236, 69.07146),
            (73.60187, 69.62763),
            (74.3998, 70.63175),
            (73.1011, 71.44717),
            (74.89082, 72.12119),
            (74.65926, 72.83227),
            (75.15801, 72.85497),
            (75.68351, 72.30056),
            (75.28898, 71.33556),
            (76.35911, 71.15287),
            (75.90313, 71.87401),
            (77.57665, 72.26717),
            (79.65202, 72.32011),
            (81.5, 71.75),
            (80.61071, 72.58285),
            (80.51109, 73.6482),
            (82.25, 73.85),
            (84.65526, 73.80591),
            (86.8223, 73.93688),
            (86.00956, 74.45967),
            (87.16682, 75.11643),
            (88.31571, 75.14393),
            (90.26, 75.64),
            (92.90058, 75.77333),
            (93.23421, 76.0472),
            (95.86, 76.14),
            (96.67821, 75.91548),
            (98.92254, 76.44689),
            (100.75967, 76.43028),
            (101.03532, 76.86189),
            (101.99084, 77.28754),
            (104.3516, 77.69792),
            (106.06664, 77.37389),
            (104.705, 77.1274),
            (106.97013, 76.97419)
        ]
    ],
    [
        [
            (105.07547, 78.30689),
            (99.43814, 77.921),
            (101.2649, 79.23399),
            (102.08635, 79.34641),
            (102.837815, 79.28129),
            (105.37243, 78.71334),
            (105.07547, 78.30689)
        ]
    ],
    [
        [
            (51.136187, 80.54728),
            (49.793685, 80.415428),
            (48.894411, 80.339567),
            (48.754937, 80.175468),
            (47.586119, 80.010181),
            (46.502826, 80.247247),
            (47.072455, 80.559424),
            (44.846958, 80.58981),
            (46.799139, 80.771918),
            (48.318477, 80.78401),
            (48.522806, 80.514569),
            (49.09719, 80.753986),
            (50.039768, 80.918885),
            (51.522933, 80.699726),
            (51.136187, 80.54728)
        ]
    ],
    [
        [
            (99.93976, 78.88094),
            (97.75794, 78.7562),
            (94.97259, 79.044745),
            (93.31288, 79.4265),
            (92.5454, 80.14379),
            (91.18107, 80.34146),
            (93.77766, 81.0246),
            (95.940895, 81.2504),
            (97.88385, 80.746975),
            (100.186655, 79.780135),
            (99.93976, 78.88094)
        ]
    ]
]
