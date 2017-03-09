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
Fast min-heap implementation using ``numba``.

See

* https://en.wikipedia.org/wiki/Binary_heap
* (german version is more detailed: https://de.wikipedia.org/wiki/Bin%C3%A4rer_Heap)

"""

__author__ = "Norman Fomferra (Brockmann Consult GmbH)"

import math

import numba


@numba.jit(nopython=True)
def build(array):
    """
    Turn the given array into a min-heap.

    :param array: The array to be heapified in place.
    :return: The heapified array.
    """
    size = len(array)
    if size > 1:
        n = size >> 1
        for i in range(0, n):
            index = n - i - 1
            _heapify(array, size, index)
    return array


@numba.jit(nopython=True)
def get_min(heap, size):
    """
    Get the minimum value.

    :param heap: The heap array.
    :param size: The heap size, ``1 <= size <= len(heap)``.
    :return: The minimum value.
    """
    assert 1 <= size <= len(heap), "size out of bounds"
    return heap[0]


@numba.jit(nopython=True)
def remove_min(heap, size):
    """
    Remove the minimum value.

    :param heap: The heap array.
    :param size: The heap size, ``1 <= size <= len(heap)``.
    :return:
    """
    assert 1 <= size <= len(heap), "size out of bounds"
    return remove(heap, size, 0)


@numba.jit(nopython=True)
def add(heap, size, new_value):
    """
    Add a new value to the heap.

    :param heap: The heap array.
    :param size: The heap size, ``0 <= size < len(heap)``.
    :param new_value: The new value to be inserted.
    :return: The new heap size.
    """
    assert 0 <= size < len(heap), "size out of bounds"
    index = size
    size += 1
    heap[index] = math.inf
    _decrease(heap, size, index, new_value)
    return size


@numba.jit(nopython=True)
def remove(heap, size, index):
    """
    Remove a value from the heap.

    :param heap: The heap array.
    :param size: The heap size, ``0 <= size <= len(heap)``.
    :param index: The index.
    :return: The new heap size
    """
    assert 0 <= size <= len(heap), "size out of bounds"
    assert 0 <= index < size, "index out of bounds"
    last_i = size - 1
    _swap(heap, index, last_i)
    size = last_i
    if index != last_i:
        if index == 0 or heap[index] > heap[_parent(index)]:
            _heapify(heap, size, index)
        else:
            # decrease does nothing, if h[i] == h[parent(i)]
            _decrease(heap, size, index, -math.inf)
    return size


@numba.jit(nopython=True)
def _heapify(heap, size, index):
    """
    :param heap: The heap array.
    :param size: The heap size, ``0 <= size <= len(heap)``.
    :param index: The index, ``0 <= index < size``.
    """
    assert 0 <= size <= len(heap), "size out of bounds"
    assert 0 <= index < size, "index out of bounds"
    i = index
    while True:
        # assert isheap(h, left(i)) and isheap(h, right(i))
        min_i = i
        left_i = _left(i)
        if left_i < size and heap[left_i] < heap[min_i]:
            min_i = left_i
        right_i = _right(i)
        if right_i < size and heap[right_i] < heap[min_i]:
            min_i = right_i
        if min_i == i:
            break
        _swap(heap, i, min_i)
        i = min_i


@numba.jit(nopython=True)
def _decrease(heap, size, index, new_value):
    """
    :param heap: The heap array.
    :param size: The heap size, ``1 <= size <= len(heap)``.
    :param index: The index, ``0 <= index < size``.
    :param new_value: The new value, ``heap[index] >= new_value``.
    """
    assert 1 <= size <= len(heap), "size out of bounds"
    assert 0 <= index < size, "index out of bounds"
    assert heap[index] >= new_value
    heap[index] = new_value
    while index > 0:
        parent_i = _parent(index)
        if heap[index] >= heap[parent_i]:
            break
        _swap(heap, index, parent_i)
        index = parent_i


@numba.jit(nopython=True)
def _parent(i):
    return (i - 1) >> 1


@numba.jit(nopython=True)
def _left(i):
    return (i << 1) + 1


@numba.jit(nopython=True)
def _right(i):
    return (i << 1) + 2


@numba.jit(nopython=True)
def _swap(h, i, j):
    t = h[i]
    h[i] = h[j]
    h[j] = t
