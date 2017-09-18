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
    def test_lc_cci(self):
        self.assertEqual(180, utils.compute_tile_size(129600, chunk_size=3600, int_div=True))

    def test_int_div(self):
        # print('----------- test_int_div:')
        for s in range(270, 64 * 270, 270):
            ts = utils.compute_tile_size(s, int_div=True)
            n = utils.cardinal_div_round(s, ts)
            utils.cardinal_log2(n * ts)
            # print(s, ts, n, n * ts - s, l2)

    def test_num_levels_min(self):
        # print('----------- test_num_levels_min:')
        for s in range(270, 64 * 270, 270):
            ts = utils.compute_tile_size(s, num_levels_min=2)
            n = utils.cardinal_div_round(s, ts)
            utils.cardinal_log2(n * ts)
            # print(s, ts, n, n * ts - s, l2)

    def test_f_and_g(self):
        import numba as nb
        from numpy.testing import assert_almost_equal

        @nb.jit(nopython=True)
        def pyramid_subdivision_count(s_max: int, ts: int, ntl0_max: int = 1):
            """
            Compute number of times *w* can be divided by 2 without remainder and while the result is still
            integer-dividable by *ts*.
            """
            count = 0
            s = s_max
            while s % 2 == 0 and s % ts == 0 and (s // ts) % 2 == 0 and (s // ts) > ntl0_max:
                s //= 2
                count += 1
            return count

        @nb.jit(nopython=True)
        def pyramid_subdivision(w_max: int, h_max: int,
                                ts_min: int, ts_max: int,
                                tw_out, th_out,
                                ntl0x_max: int = 1,
                                ntl0y_max: int = 1):
            size = ts_max - ts_min + 1

            cx = np.empty(ts_max - ts_min + 1, dtype=np.int32)
            cy = np.empty(ts_max - ts_min + 1, dtype=np.int32)
            for i in range(size):
                ts = ts_min + i
                cx[i] = pyramid_subdivision_count(w_max, ts, ntl0_max=ntl0x_max)
                cy[i] = pyramid_subdivision_count(h_max, ts, ntl0_max=ntl0y_max)

            cx_max = -1
            cy_max = -1
            for i in range(size):
                cx_max = max(cx[i], cx_max)
                cy_max = max(cy[i], cy_max)

            c = min(cx_max, cy_max)

            for ix in range(tw_out.size):
                tw_out[ix] = 0
            for iy in range(th_out.size):
                th_out[iy] = 0

            if c <= 0:
                return 0

            ix = 0
            iy = 0
            for i in range(size):
                if cx[i] >= c and ix < tw_out.size:
                    tw_out[ix] = ts_min + i
                    ix += 1
                if cy[i] >= c and iy < th_out.size:
                    th_out[iy] = ts_min + i
                    iy += 1

            return c

        w = 129600
        h = w // 2

        self.assertEqual(pyramid_subdivision_count(w, 45), 6)
        self.assertEqual(pyramid_subdivision_count(w, 90), 5)
        self.assertEqual(pyramid_subdivision_count(w, 100), 4)
        self.assertEqual(pyramid_subdivision_count(w, 180), 4)
        self.assertEqual(pyramid_subdivision_count(w, 225), 6)
        self.assertEqual(pyramid_subdivision_count(w, 256), 0)
        self.assertEqual(pyramid_subdivision_count(w, 405), 6)
        self.assertEqual(pyramid_subdivision_count(w, 675), 6)
        self.assertEqual(pyramid_subdivision_count(w, 1024), 0)
        self.assertEqual(pyramid_subdivision_count(w, 1800), 3)
        self.assertEqual(pyramid_subdivision_count(w, 2048), 0)

        tw_out = np.zeros(10, dtype=np.int32)
        th_out = np.zeros(10, dtype=np.int32)

        c = pyramid_subdivision(w, h, 180, 2048, tw_out, th_out, ntl0x_max=1, ntl0y_max=1)
        self.assertEqual(c, 5)
        assert_almost_equal(tw_out, np.array([225, 270, 405, 450, 675, 810, 1350, 2025, 0, 0]))
        assert_almost_equal(th_out, np.array([225, 405, 675, 2025, 0, 0, 0, 0, 0, 0]))

        c = pyramid_subdivision(w, h, 180, 2048, tw_out, th_out, ntl0x_max=2, ntl0y_max=1)
        self.assertEqual(c, 5)
        assert_almost_equal(tw_out, np.array([225, 270, 405, 450, 675, 810, 1350, 2025, 0, 0]))
        assert_almost_equal(th_out, np.array([225, 405, 675, 2025, 0, 0, 0, 0, 0, 0]))

    def test_chunk_size(self):
        # print('----------- test_chunk_size:')
        for s in range(270, 64 * 270, 270):
            ts = utils.compute_tile_size(s, chunk_size=256)
            n = utils.cardinal_div_round(s, ts)
            utils.cardinal_log2(n * ts)
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
