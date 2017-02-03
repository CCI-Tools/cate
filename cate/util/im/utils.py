# The MIT License (MIT)
# Copyright (c) 2017 by the Cate Development Team and contributors
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

__author__ = "Norman Fomferra (Brockmann Consult GmbH)"

def aggregate_ndarray_first(a1, a2, a3, a4):
    return a1


def aggregate_ndarray_min(a1, a2, a3, a4):
    a = np.fmin(a1, a2)
    a = np.fmin(a, a3, out=a)
    a = np.fmin(a, a4, out=a)
    return a


def aggregate_ndarray_max(a1, a2, a3, a4):
    a = np.fmax(a1, a2)
    a = np.fmax(a, a3, out=a)
    a = np.fmax(a, a4, out=a)
    return a


def aggregate_ndarray_sum(a1, a2, a3, a4):
    return a1 + a2 + a3 + a4


def aggregate_ndarray_mean(a1, a2, a3, a4):
    return (a1 + a2 + a3 + a4) / 4.


def downsample_ndarray(a, aggregator=aggregate_ndarray_mean):
    if aggregator is aggregate_ndarray_first:
        # Optimization
        return a[..., 0::2, 0::2]
    else:
        a1 = a[..., 0::2, 0::2]
        a2 = a[..., 0::2, 1::2]
        a3 = a[..., 1::2, 0::2]
        a4 = a[..., 1::2, 1::2]
        return aggregator(a1, a2, a3, a4)


def cardinal_div_round(num, denom):
    return int(num + denom - 1) // int(denom)


def cardinal_log2(x):
    n = 0
    while x % 2 == 0:
        n += 1
        x //= 2
    return n


# check - we can make this method faster - it is a silly brute-force implementation (nf)
def compute_tile_size(total_size,
                      tile_size_min=180,
                      tile_size_max=512,
                      tile_size_step=2,
                      chunk_size=None,
                      num_levels_min=None,
                      int_div=False):
    """
    Compute a suitable tile size.
    :param total_size:
    :param tile_size_min:
    :param tile_size_max:
    :param tile_size_step:
    :param chunk_size:
    :param num_levels_min:
    :param int_div:
    :return: best size (an int) w.r.t. the given constraints
    """

    ts = total_size
    num_levels = 0
    while ts % 2 == 0:
        ts2 = ts // 2
        if ts2 < tile_size_min:
            break
        ts = ts2
        num_levels += 1

    if ts <= tile_size_max and (not num_levels_min or num_levels >= num_levels_min):
        return ts

    min_penalty = 10 * total_size
    best_tile_size = None
    for ts in range(tile_size_min, tile_size_max + 1, tile_size_step):

        if int_div and total_size % ts:
            continue

        num_tiles = cardinal_div_round(total_size, ts)
        if num_levels_min:
            num_levels = cardinal_log2(num_tiles * ts)
            if num_levels < num_levels_min:
                continue

        total_size_excess = ts * num_tiles - total_size
        penalty = total_size_excess

        if chunk_size:
            num_chunks = cardinal_div_round(ts, chunk_size)
            tile_size_excess = ts * num_chunks - ts
            penalty += tile_size_excess

        if penalty < min_penalty:
            min_penalty = penalty
            best_tile_size = ts

    if not best_tile_size:
        raise ValueError('tile size could not be computed')

    return best_tile_size


def get_chunk_size(array):
    chunk_size = None
    try:
        # xarray DataArray
        chunk_size = array.encoding['chunksizes']
    except:
        pass
    if not chunk_size:
        try:
            # netCDF4 data array
            chunk_size = array.chunks
        except:
            pass
    return chunk_size