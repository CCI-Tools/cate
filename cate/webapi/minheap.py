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
Fast binary heap (min-heap) implementation using ``numba``.

See https://en.wikipedia.org/wiki/Binary_heap
(implementation is based on german version at https://de.wikipedia.org/wiki/Bin%C3%A4rer_Heap)

"""

__author__ = "Norman Fomferra (Brockmann Consult GmbH)"

from typing import Union, Tuple, Optional

import numba
import numpy as np

KeyType = Union[int, float]
ValueType = Union[bool, int, float]
KeyArray = np.ndarray
ValueArray = np.ndarray


@numba.jit(nopython=True)
def build(keys: KeyArray, values: ValueArray, size: int) -> None:
    """
    Turn the given array into a min-heap.
    """
    assert 0 <= size <= keys.size, "size out of bounds"
    if size > 1:
        n = size >> 1
        for i in range(0, n):
            index = n - i - 1
            _heapify(keys, values, size, index)


@numba.jit(nopython=True)
def add(keys: KeyArray, values: ValueArray, size: int,
        max_key: KeyType, new_key: KeyType, new_value: ValueType) -> int:
    """
    Add a new value to the heap.

    :param keys: The heap's keys, ``0 <= size <= keys.size``.
    :param values: The heap's values, ``0 <= size <= values.size``.
    :param size: The heap's current size.
    :param max_key: The maximum key value.
    :param new_key: The new key to be inserted.
    :return: The new heap size.
    """
    assert 0 <= size < keys.size, "size out of bounds"
    index = size
    size += 1
    keys[index] = max_key
    values[index] = 0
    _decrease(keys, values, size, index, new_key, new_value)
    return size


@numba.jit(nopython=True)
def remove(keys: KeyArray, values: ValueArray, size: int,
           min_key: KeyType, index: int) -> int:
    """
    Remove a value from the heap.

    :param keys: The heap's keys, ``0 <= size <= keys.size``.
    :param values: The heap's values, ``0 <= size <= values.size``.
    :param size: The heap's current size.
    :param min_key: The minimum key value.
    :param index: The index of the element to be removed.
    :return: The new heap size
    """
    assert 0 <= size <= keys.size, "size out of bounds"
    assert 0 <= index < size, "index out of bounds"
    last_i = size - 1
    size = last_i
    if index != last_i:
        _swap(keys, values, index, last_i)
        # TODO (forman): make sure (test!) that size arg is correct here. Is it the old size (size + 1)?
        if index == 0 or keys[index] > keys[_parent(index)]:
            _heapify(keys, values, size, index)
        else:
            # decrease does nothing, if h[i] == h[parent(i)]
            _decrease(keys, values, size, index, min_key, 0)
    return size


@numba.jit(nopython=True)
def remove_min(keys: KeyArray, values: ValueArray, size: int,
               min_key: KeyType) -> int:
    """
    Remove the heap's current minimum value (element at index 0).

    :param keys: The heap's keys, ``0 <= size <= keys.size``.
    :param values: The heap's values, ``0 <= size <= values.size``.
    :param size: The heap's current size.
    :param min_key: The minimum key value.
    :return: The new heap size
    """
    return remove(keys, values, size, min_key, 0)


@numba.jit(nopython=True)
def _heapify(keys: KeyArray, values: ValueArray, size: int, index: int) -> None:
    """
    :param keys: The heap's keys, ``0 <= size <= keys.size``.
    :param values: The heap's values, ``0 <= size <= values.size``.
    :param size: The heap's current size.
    :param index: The index, ``0 <= index < size``.
    """
    assert 0 <= size <= keys.size, "size out of bounds"
    assert 0 <= index < size, "index out of bounds"
    i = index
    while True:
        min_i = i
        left_i = _left(i)
        if left_i < size and keys[left_i] < keys[min_i]:
            min_i = left_i
        right_i = _right(i)
        if right_i < size and keys[right_i] < keys[min_i]:
            min_i = right_i
        if min_i == i:
            break
        _swap(keys, values, i, min_i)
        i = min_i


@numba.jit(nopython=True)
def _decrease(keys: KeyArray, values: ValueArray, size: int, index: int,
              new_key: KeyType, new_value: ValueType):
    """
    :param keys: The heap's keys, ``1 <= size <= keys.size``.
    :param values: The heap's values, ``1 <= size <= values.size``.
    :param size: The heap's current size.
    :param index: The index, ``0 <= index < size``.
    :param new_key: The new key, ``keys[index] >= new_key``.
    :param new_value: Any new value
    """
    assert 1 <= size <= keys.size, "size out of bounds of keys"
    assert 1 <= size <= values.size, "size out of bounds of values"
    assert 0 <= index < size, "index out of bounds"
    assert keys[index] >= new_key
    keys[index] = new_key
    values[index] = new_value
    while index > 0:
        parent_i = _parent(index)
        if keys[index] >= keys[parent_i]:
            break
        _swap(keys, values, index, parent_i)
        index = parent_i


@numba.jit(nopython=True)
def _swap(keys: KeyArray, values: ValueArray, index1: int, index2: int) -> None:
    key1 = keys[index1]
    keys[index1] = keys[index2]
    keys[index2] = key1
    value1 = values[index1]
    values[index1] = values[index2]
    values[index2] = value1


@numba.jit(nopython=True)
def _parent(index: int) -> int:
    return (index - 1) >> 1


@numba.jit(nopython=True)
def _left(index: int) -> int:
    return (index << 1) + 1


@numba.jit(nopython=True)
def _right(index: int) -> int:
    return (index << 1) + 2


class MinHeap:
    """
    A min-heap.

    :param keys: Initial key array. The size of this array determines the maximum capacity of the min-heap.
    :param values: Initial values. This is usually an array of indices into the actual values.
    :param size: Initial heap size.
    :param min_key: The smallest possible key value.
    :param max_key: The largest possible key value.
    """

    def __init__(self,
                 keys: KeyArray,
                 values: Optional[ValueArray] = None,
                 size: Optional[int] = None,
                 min_key: Optional[KeyType] = None,
                 max_key: Optional[KeyType] = None):

        if keys is None:
            raise ValueError('keys must be given')
        if keys.size == 0:
            raise ValueError('keys must not be empty')
        if values is not None and values.size < keys.size:
            raise ValueError('values.size must greater than or equal to keys.size')
        if size is not None and size > keys.size:
            raise ValueError('size must be less than or equal to keys.size')
        if min_key is not None and max_key is not None and max_key <= min_key:
            raise ValueError('min_key must be less than max_key')

        if values is None:
            values = np.arange(keys.size, dtype=np.uint32)
        if size is None:
            size = keys.size

        if min_key is None:
            dtype = keys.dtype
            min_key = np.iinfo(dtype).min if np.issubdtype(dtype, np.int) else np.finfo(dtype).min
        if max_key is None:
            dtype = keys.dtype
            max_key = np.iinfo(dtype).max if np.issubdtype(dtype, np.int) else np.finfo(dtype).max

        build(keys, values, size)

        self._keys = keys
        self._values = values
        self._size = size
        self._min_key = min_key
        self._max_key = max_key

    @property
    def keys(self) -> KeyArray:
        return self._keys

    @property
    def values(self) -> ValueArray:
        return self._values

    @property
    def size(self) -> int:
        return self._size

    @property
    def min(self) -> Tuple[KeyType, ValueType]:
        return self._keys[0], self._values[0]

    @property
    def min_key(self) -> KeyType:
        return self._keys[0]

    @property
    def min_value(self) -> ValueType:
        return self._values[0]

    def get(self, index: int) -> Tuple[KeyType, ValueType]:
        return self._keys[index], self._values[index]

    def get_key(self, index: int) -> KeyType:
        return self._keys[index]

    def get_value(self, index: int) -> ValueType:
        return self._values[index]

    def add(self, new_key: KeyType, new_value: Optional[ValueType] = None) -> None:
        if new_value is None:
            new_value = self._size
        self._size = add(self._keys, self._values, self._size, self._max_key, new_key, new_value)

    def remove(self, index: int) -> Tuple[KeyType, ValueType]:
        old_entry = self._keys[index], self._values[index]
        self._size = remove(self._keys, self._values, self._size, self._min_key, index)
        return old_entry

    def remove_min(self) -> Tuple[KeyType, ValueType]:
        return self.remove(0)
