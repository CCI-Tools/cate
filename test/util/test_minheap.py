from unittest import TestCase

import numba

import cate.util.minheap as minheap


class MinHeapTest(TestCase):
    def test_build(self):
        # self.assertEqual(minheap.build([]), [])
        self.assertEqual(minheap.build([1]), [1])
        self.assertEqual(minheap.build([2, 1]), [1, 2])
        self.assertEqual(minheap.build([3, 2, 1]), [1, 2, 3])
        self.assertEqual(minheap.build([4, 3, 2, 1]), [1, 3, 2, 4])
        self.assertEqual(minheap.build([5, 4, 3, 2, 1]), [1, 2, 3, 5, 4])
        self.assertEqual(minheap.build([6, 5, 4, 3, 2, 1]), [1, 2, 4, 3, 5, 6])
        self.assertEqual(minheap.build([7, 6, 5, 4, 3, 2, 1]), [1, 3, 2, 4, 6, 7, 5])

    def test_get_min(self):
        self.assertEqual(minheap.get_min([1.1, 1.2, 1.3], 3), 1.1)

    def test_add(self):
        heap = [0.] * 10
        size = 0

        size = minheap.add(heap, size, 4.)
        self.assertEqual(size, 1)
        self.assertEqual(heap[:size], [4.])

        size = minheap.add(heap, size, 2.)
        self.assertEqual(size, 2)
        self.assertEqual(heap[:size], [2, 4])

        size = minheap.add(heap, size, 6.)
        self.assertEqual(size, 3)
        self.assertEqual(heap[:size], [2, 4, 6])

        size = minheap.add(heap, size, 5.)
        self.assertEqual(size, 4)
        self.assertEqual(heap[:size], [2., 4., 6., 5.])

        size = minheap.add(heap, size, 1.)
        self.assertEqual(size, 5)
        self.assertEqual(heap[:size], [1., 2., 6., 5., 4.])

        size = minheap.add(heap, size, 3.)
        self.assertEqual(size, 6)
        self.assertEqual(heap[:size], [1., 2., 3., 5., 4., 6.])

    def test_remove(self):
        heap = [1., 2., 4., 3., 5., 6., 0., 0., 0., 0.]
        size = 6

        size = minheap.remove(heap, size, 4)
        self.assertEqual(size, 5)
        self.assertEqual(heap[:size], [1., 2., 4., 3., 6.])

        size = minheap.remove(heap, size, 3)
        self.assertEqual(size, 4)
        self.assertEqual(heap[:size], [1., 2., 4., 6.])

        size = minheap.remove(heap, size, 0)
        self.assertEqual(size, 3)
        self.assertEqual(heap[:size], [2., 6., 4.])

        size = minheap.remove(heap, size, 2)
        self.assertEqual(size, 2)
        self.assertEqual(heap[:size], [2., 6.])

        size = minheap.remove(heap, size, 1)
        self.assertEqual(size, 1)
        self.assertEqual(heap[:size], [2.])

        size = minheap.remove(heap, size, 0)
        self.assertEqual(size, 0)
        self.assertEqual(heap[:size], [])

    def test_remove_min(self):
        heap = [1., 2., 4., 3., 5., 6., 7.]
        size = 7

        size = minheap.remove_min(heap, size)
        self.assertEqual(size, 6)
        self.assertEqual(heap[:size], [2., 3., 4., 7., 5., 6.])

        size = minheap.remove_min(heap, size)
        self.assertEqual(size, 5)
        self.assertEqual(heap[:size], [3., 5., 4., 7., 6.])

        size = minheap.remove_min(heap, size)
        self.assertEqual(size, 4)
        self.assertEqual(heap[:size], [4., 5., 6., 7.])

        size = minheap.remove_min(heap, size)
        self.assertEqual(size, 3)
        self.assertEqual(heap[:size], [5., 7., 6.])

        size = minheap.remove_min(heap, size)
        self.assertEqual(size, 2)
        self.assertEqual(heap[:size], [6., 7.])

        size = minheap.remove_min(heap, size)
        self.assertEqual(size, 1)
        self.assertEqual(heap[:size], [7.])

    def test_can_pass_numba_function(self):
        @numba.jit(nopython=True)
        def g(x):
            return 3 * x

        def f(g, a):
            return g(a)

        self.assertAlmostEqual(f(g, 1.4), 4.2)

    def test_can_use_numba_function_in_numba_function(self):
        @numba.jit(nopython=True)
        def g(x: float) -> float:
            return 3. * x

        @numba.jit(nopython=True)
        def f(a):
            return g(a)

        self.assertAlmostEqual(f(1.4), 4.2)

# import numba
# import numpy as np
#
# @numba.jit(nopython=True)
# def area(x1: float, y1: float, x2: float, y2: float) -> float:
#     return 0.5 * abs(x1 * y2 - y1 * x2)
#
# @numba.jit(nopython=True)
# def simplify(n, x, y):
#     if n < 4:
#         return
#     m = n - 2
#     areas = np.empty(m)
#     min_area = 1e10
#     min_area_i = -1
#     for i in range(m):
#         x0 = x[i]
#         y0 = y[i]
#         a = area(x[i + 1] - x0, y[i + 1] - y0, x[i + 2] - x0, y[i + 2] - y0)
#         if a < min_area:
#             min_area = a
#             min_area_i = i
#         areas[i] = a
#
#     if min_area_i >= 0:
#         pass
