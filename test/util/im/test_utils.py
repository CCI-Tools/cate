from unittest import TestCase

import numpy as np

import cate.util.im.utils as utils


class ArrayTest(TestCase):
    def test_numpy_reduce(self):
        nan = np.nan
        a = np.zeros((8, 6))
        a[0::2, 0::2] = 1.1
        a[0::2, 1::2] = 2.2
        a[1::2, 0::2] = 3.3
        a[1::2, 1::2] = 4.4
        a[6, 4] = np.nan

        self.assertEqual(a.shape, (8, 6))
        np.testing.assert_equal(a, np.array([[1.1, 2.2, 1.1, 2.2, 1.1, 2.2],
                                             [3.3, 4.4, 3.3, 4.4, 3.3, 4.4],
                                             [1.1, 2.2, 1.1, 2.2, 1.1, 2.2],
                                             [3.3, 4.4, 3.3, 4.4, 3.3, 4.4],
                                             [1.1, 2.2, 1.1, 2.2, 1.1, 2.2],
                                             [3.3, 4.4, 3.3, 4.4, 3.3, 4.4],
                                             [1.1, 2.2, 1.1, 2.2, nan, 2.2],
                                             [3.3, 4.4, 3.3, 4.4, 3.3, 4.4]]))

        b = utils.downsample_ndarray(a)
        self.assertEqual(b.shape, (4, 3))
        np.testing.assert_equal(b, np.array([[2.75, 2.75, 2.75],
                                             [2.75, 2.75, 2.75],
                                             [2.75, 2.75, 2.75],
                                             [2.75, 2.75, nan]]))

        b = utils.downsample_ndarray(a, aggregator=utils.aggregate_ndarray_first)
        self.assertEqual(b.shape, (4, 3))
        np.testing.assert_equal(b, np.array([[1.1, 1.1, 1.1],
                                             [1.1, 1.1, 1.1],
                                             [1.1, 1.1, 1.1],
                                             [1.1, 1.1, nan]]))


class CardinalDivRoundTest(TestCase):
    def test_num_0(self):
        self.assertEqual(2, utils.cardinal_div_round(0, -1))
        self.assertRaises(ZeroDivisionError, utils.cardinal_div_round, 0, 0)
        self.assertEqual(0, utils.cardinal_div_round(0, 1))
        self.assertEqual(0, utils.cardinal_div_round(0, 110))

    def test_num_10(self):
        self.assertEqual(10, utils.cardinal_div_round(10, 1))
        self.assertEqual(5, utils.cardinal_div_round(10, 2))
        self.assertEqual(4, utils.cardinal_div_round(10, 3))
        self.assertEqual(3, utils.cardinal_div_round(10, 4))
        self.assertEqual(2, utils.cardinal_div_round(10, 5))
        self.assertEqual(2, utils.cardinal_div_round(10, 6))
        self.assertEqual(2, utils.cardinal_div_round(10, 7))
        self.assertEqual(2, utils.cardinal_div_round(10, 8))
        self.assertEqual(2, utils.cardinal_div_round(10, 9))
        self.assertEqual(1, utils.cardinal_div_round(10, 10))
        self.assertEqual(1, utils.cardinal_div_round(10, 11))
        self.assertEqual(1, utils.cardinal_div_round(10, 11))
        self.assertEqual(1, utils.cardinal_div_round(10, 110))


class TileSizeTest(TestCase):
    def test_int_div(self):
        # print('----------- test_int_div:')
        for s in range(270, 64 * 270, 270):
            ts = utils.compute_tile_size(s, int_div=True)
            n = utils.cardinal_div_round(s, ts)
            l2 = utils.cardinal_log2(n * ts)
            # print(s, ts, n, n * ts - s, l2)

    def test_num_levels_min(self):
        # print('----------- test_num_levels_min:')
        for s in range(270, 64 * 270, 270):
            ts = utils.compute_tile_size(s, num_levels_min=2)
            n = utils.cardinal_div_round(s, ts)
            l2 = utils.cardinal_log2(n * ts)
            # print(s, ts, n, n * ts - s, l2)

    def test_chunk_size(self):
        # print('----------- test_chunk_size:')
        for s in range(270, 64 * 270, 270):
            ts = utils.compute_tile_size(s, chunk_size=256)
            n = utils.cardinal_div_round(s, ts)
            l2 = utils.cardinal_log2(n * ts)
            # print(s, ts, n, n * ts - s, l2)


class GetChunkSizeTest(TestCase):
    def test_any_obj(self):
        any_obj = object()
        self.assertEqual(utils.get_chunk_size(any_obj), None)

    def test_xarray_var(self):
        class X:
            pass

        xarray_var = X()
        xarray_var.encoding = dict(chunksizes=(1, 256, 256))
        self.assertEqual(utils.get_chunk_size(xarray_var), (1, 256, 256))

    def test_netcdf4_var(self):
        class X:
            pass

        netcdf4_var = X()
        netcdf4_var.chunks = (1, 900, 1800)
        self.assertEqual(utils.get_chunk_size(netcdf4_var), (1, 900, 1800))
