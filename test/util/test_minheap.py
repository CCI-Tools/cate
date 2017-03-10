from unittest import TestCase

import numpy as np
from numpy.testing import assert_almost_equal

from cate.util.minheap import MinHeap


class MinHeapTest(TestCase):
    def test_init_and_properties(self):
        h = MinHeap(np.zeros(4, dtype=np.float), values=None, size=0)
        self.assertEqual(h.size, 0)
        assert_almost_equal(h.keys, [0., 0., 0., 0.])
        assert_almost_equal(h.values, [0, 1, 2, 3])

        h = MinHeap(np.array([1], dtype=np.float))
        self.assertEqual(h.size, 1)
        assert_almost_equal(h.keys, [1.])
        assert_almost_equal(h.values, [0])

        h = MinHeap(np.array([2, 1], dtype=np.float))
        self.assertEqual(h.size, 2)
        assert_almost_equal(h.keys, [1., 2.])
        assert_almost_equal(h.values, [1, 0])

        h = MinHeap(np.array([3, 2, 1], dtype=np.float))
        self.assertEqual(h.size, 3)
        assert_almost_equal(h.keys, [1., 2., 3.])
        assert_almost_equal(h.values, [2, 1, 0])

        h = MinHeap(np.array([4, 3, 2, 1], dtype=np.float))
        self.assertEqual(h.size, 4)
        assert_almost_equal(h.keys, [1., 3., 2., 4.])
        assert_almost_equal(h.values, [3, 1, 2, 0])

        h = MinHeap(np.array([5, 4, 3, 2, 1], dtype=np.float))
        self.assertEqual(h.size, 5)
        assert_almost_equal(h.keys, [1., 2., 3., 5., 4.])
        assert_almost_equal(h.values, [4, 3, 2, 0, 1])

        h = MinHeap(np.array([6, 5, 4, 3, 2, 1], dtype=np.float))
        self.assertEqual(h.size, 6)
        assert_almost_equal(h.keys, [1., 2., 4., 3., 5., 6.])
        assert_almost_equal(h.values, [5, 4, 2, 3, 1, 0])

        h = MinHeap(np.array([7, 6, 5, 4, 3, 2, 1], dtype=np.float))
        self.assertEqual(h.size, 7)
        assert_almost_equal(h.keys, [1., 3., 2., 4., 6., 7., 5.])
        assert_almost_equal(h.values, [6, 4, 5, 3, 1, 0, 2])

    def test_min(self):
        h = MinHeap(np.array([1.2, 1.3, 1.1]), values=np.array([10, 25, 19]))
        self.assertAlmostEqual(h.min[0], 1.1)
        self.assertEqual(h.min[1], 19)
        self.assertEqual(h.min[0], h.min_key)
        self.assertEqual(h.min[1], h.min_value)

    def test_get(self):
        h = MinHeap(np.array([1.2, 1.3, 1.1]), values=np.array([10, 25, 19]))
        self.assertAlmostEqual(h.get(2)[0], 1.2)
        self.assertEqual(h.get(2)[1], 10)
        self.assertEqual(h.get(2)[0], h.get_key(2))
        self.assertEqual(h.get(2)[1], h.get_value(2))

    def test_add(self):
        h = MinHeap(np.zeros(6, dtype=np.float), values=None, size=0)

        h.add(4., 0)
        self.assertEqual(h.size, 1)
        assert_almost_equal(h.keys, [4., 0., 0., 0., 0., 0.])
        assert_almost_equal(h.values, [0, 1, 2, 3, 4, 5])

        h.add(2., 1)
        self.assertEqual(h.size, 2)
        assert_almost_equal(h.keys, [2., 4., 0., 0., 0., 0.])
        assert_almost_equal(h.values, [1, 0, 2, 3, 4, 5])

        h.add(6.)
        self.assertEqual(h.size, 3)
        assert_almost_equal(h.keys, [2., 4., 6., 0., 0., 0.])
        assert_almost_equal(h.values, [1, 0, 2, 3, 4, 5])

        h.add(5.)
        self.assertEqual(h.size, 4)
        assert_almost_equal(h.keys, [2., 4., 6., 5., 0., 0.])
        assert_almost_equal(h.values, [1, 0, 2, 3, 4, 5])

        h.add(1., 4)
        self.assertEqual(h.size, 5)
        assert_almost_equal(h.keys, [1., 2., 6., 5., 4., 0.])
        assert_almost_equal(h.values, [4, 1, 2, 3, 0, 5])

        h.add(3., 67)
        self.assertEqual(h.size, 6)
        assert_almost_equal(h.keys, [1., 2., 3., 5., 4., 6.])
        assert_almost_equal(h.values, [4, 1, 67, 3, 0, 2])

    def test_remove(self):
        h = MinHeap(np.array([1., 2., 4., 3., 5., 6.]))
        self.assertEqual(h.size, 6)
        assert_almost_equal(h.keys, [1., 2., 4., 3., 5., 6.])
        assert_almost_equal(h.values, [0, 1, 2, 3, 4, 5])

        removed_entry = h.remove(4)
        self.assertEqual(removed_entry, (5., 4))
        self.assertEqual(h.size, 5)
        assert_almost_equal(h.keys, [1.,  2.,  4.,  3.,  6.,  5.])
        assert_almost_equal(h.values, [0, 1, 2, 3, 5, 4])

        removed_entry = h.remove(3)
        self.assertEqual(removed_entry, (3., 3))
        self.assertEqual(h.size, 4)
        assert_almost_equal(h.keys, [1.,  2.,  4.,  6.,  3.,  5.])
        assert_almost_equal(h.values, [0, 1, 2, 5, 3, 4])

        removed_entry = h.remove(0)
        self.assertEqual(removed_entry, (1., 0))
        self.assertEqual(h.size, 3)
        assert_almost_equal(h.keys, [2.,  6.,  4.,  1.,  3.,  5.])
        assert_almost_equal(h.values, [1, 5, 2, 0, 3, 4])

        removed_entry = h.remove(2)
        self.assertEqual(removed_entry, (4., 2))
        self.assertEqual(h.size, 2)
        assert_almost_equal(h.keys, [2.,  6.,  4.,  1.,  3.,  5.])
        assert_almost_equal(h.values, [1, 5, 2, 0, 3, 4])

        removed_entry = h.remove(1)
        self.assertEqual(removed_entry, (6.0, 5))
        self.assertEqual(h.size, 1)
        assert_almost_equal(h.keys, [2.,  6.,  4.,  1.,  3.,  5.])
        assert_almost_equal(h.values, [1, 5, 2, 0, 3, 4])

        removed_entry = h.remove(0)
        self.assertEqual(removed_entry, (2., 1))
        self.assertEqual(h.size, 0)
        assert_almost_equal(h.keys, [2.,  6.,  4.,  1.,  3.,  5.])
        assert_almost_equal(h.values, [1, 5, 2, 0, 3, 4])

    def test_remove_min(self):
        h = MinHeap(np.array([1., 2., 4., 3., 5., 6., 7.]))
        self.assertEqual(h.size, 7)
        assert_almost_equal(h.keys, [1., 2., 4., 3., 5., 6., 7.])
        assert_almost_equal(h.values, [0, 1, 2, 3, 4, 5, 6])

        removed_entry = h.remove_min()
        self.assertEqual(removed_entry, (1., 0))
        self.assertEqual(h.size, 6)
        assert_almost_equal(h.keys, [2.,  3.,  4.,  7.,  5.,  6.,  1.])
        assert_almost_equal(h.values, [1, 3, 2, 6, 4, 5, 0])


