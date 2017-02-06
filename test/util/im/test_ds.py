from unittest import TestCase

import cate.util.im.ds as ds


class NaturalEarth2Test(TestCase):
    def test_natural_earth_2_pyramid(self):
        pyramid = ds.NaturalEarth2Image.get_pyramid()

        tile = pyramid.get_tile(0, 0, 0)
        self.assertIsNotNone(tile)
        self.assertEqual(12067, len(tile))

        tile = pyramid.get_tile(7, 3, 2)
        self.assertIsNotNone(tile)
        self.assertEqual(9032, len(tile))

# import time
# import h5py
# from cate.util.im.image import ColorMappedRgbaImage, ImagePyramid
#
# class H5PyDatasetImageTest(TestCase):
#     def setUp(self):
#         import ccitbxws.main as main
#         import os
#         data_root = main.CONFIG.get('DATA_ROOT', '.')
#         file_path = os.path.join(data_root, 'ESACCI-OC-L3S-CHLOR_A-MERGED-1M_MONTHLY_4km_GEO_PML_OC4v6-201301-fv2.0.nc')
#         self.file = h5py.File(file_path, 'r')
#         self.dataset = self.file['chlor_a']
#         self.assertIsNotNone(self.dataset)
#
#     def tearDown(self):
#         self.file.close()
#
#     def test_h5py_raw_image(self):
#         image = ds.H5PyDatasetImage(self.dataset)
#         self.assertEqual('ndarray', image.format)
#         self.assertEqual('float32', image.mode)
#         self.assertEqual((8640, 4320), image.size)
#         self.assertEqual((270, 270), image.tile_size)
#         self.assertEqual((32, 16), image.num_tiles)
#
#         tile00 = image.get_tile(0, 0)
#         self.assertIsNotNone(tile00)
#         self.assertEqual((1, 270, 270), tile00.shape)
#         tileNN = image.get_tile(33, 16)
#         self.assertIsNotNone(tileNN)
#         self.assertEqual((1, 270, 270), tileNN.shape)
#
#     def test_h5py_raw_pyramid(self):
#         image = ds.H5PyDatasetImage(self.dataset)
#         pyramid = image.create_pyramid()
#         self.assertEqual((270, 270), pyramid.tile_size)
#         self.assertEqual((2, 1), pyramid.num_level_zero_tiles)
#         self.assertEqual(5, pyramid.num_levels)
#
#         level_image = pyramid.get_level_image(0)
#
#         t1 = time.clock()
#         tile00 = level_image.get_tile(0, 0)
#         tile10 = level_image.get_tile(1, 0)
#         t2 = time.clock()
#         print("ndarray pyramid took: ", t2 - t1)
#
#     def test_h5py_raw_pyramid_fast(self):
#         pyramid = ImagePyramid.create_from_array(self.dataset, tile_size=(270, 270))
#         self.assertEqual((270, 270), pyramid.tile_size)
#         self.assertEqual((2, 1), pyramid.num_level_zero_tiles)
#         self.assertEqual(5, pyramid.num_levels)
#
#         level_image = pyramid.get_level_image(0)
#
#         t1 = time.clock()
#         tile00 = level_image.get_tile(0, 0)
#         tile10 = level_image.get_tile(1, 0)
#         t2 = time.clock()
#         print("ndarray fast pyramid took: ", t2 - t1)
#
#     def test_h5py_rgba_image(self):
#         image = ColorMappedRgbaImage(ds.H5PyDatasetImage(self.dataset))
#         self.assertEqual('ndarray', image.format)
#         self.assertEqual('RGBA', image.mode)
#         self.assertEqual((8640, 4320), image.size)
#         self.assertEqual((270, 270), image.tile_size)
#         self.assertEqual((32, 16), image.num_tiles)
#
#         tile00 = image.get_tile(0, 0)
#         self.assertIsNotNone(tile00)
#         tileNN = image.get_tile(33, 16)
#         self.assertIsNotNone(tileNN)
#
#     def test_h5py_rgba_pyramid(self):
#         image = ColorMappedRgbaImage(ds.H5PyDatasetImage(self.dataset, tile_size=(270, 270)))
#         pyramid = image.create_pyramid()
#         self.assertEqual((270, 270), pyramid.tile_size)
#         self.assertEqual((2, 1), pyramid.num_level_zero_tiles)
#         self.assertEqual(5, pyramid.num_levels)
#
#         level_image = pyramid.get_level_image(0)
#
#         t1 = time.clock()
#         tile00 = level_image.get_tile(0, 0)
#         tile10 = level_image.get_tile(1, 0)
#         t2 = time.clock()
#         print("RGBA pyramid took: ", t2 - t1)
#
#         t1 = time.clock()
#         num_tiles_x, num_tiles_y = image.num_tiles
#         for tile_y in range(num_tiles_y):
#             for tile_x in range(num_tiles_x):
#                 tile = image.get_tile(tile_x, tile_y)
#         t2 = time.clock()
#         print("max level tiles took: ", t2 - t1)
#
#     def test_h5py_raw_to_rgba_pyramid(self):
#         image = ds.H5PyDatasetImage(self.dataset, tile_size=(270, 270))
#         pyramid = image.create_pyramid()
#         pyramid = pyramid.apply(lambda image: ColorMappedRgbaImage(image,
#                                                                    value_range=(0.0, 2.0),
#                                                                    no_data_value=self.dataset.fillvalue))
#         self.assertEqual((270, 270), pyramid.tile_size)
#         self.assertEqual((2, 1), pyramid.num_level_zero_tiles)
#         self.assertEqual(5, pyramid.num_levels)
#
#         level_image = pyramid.get_level_image(0)
#
#         t1 = time.clock()
#         tile00 = level_image.get_tile(0, 0)
#         tile10 = level_image.get_tile(1, 0)
#         t2 = time.clock()
#         print("opt RGBA pyramid took: ", t2 - t1)
#
#         t1 = time.clock()
#         num_tiles_x, num_tiles_y = image.num_tiles
#         for tile_y in range(num_tiles_y):
#             for tile_x in range(num_tiles_x):
#                 tile = image.get_tile(tile_x, tile_y)
#         t2 = time.clock()
#         print("opt max level tiles took: ", t2 - t1)
