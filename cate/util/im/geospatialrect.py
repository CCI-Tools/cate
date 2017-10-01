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


from typing import Tuple

import numpy as np

GeoSpatialRect = Tuple[float, float, float, float]


def get_geo_spatial_rect(x: np.ndarray, y: np.ndarray, eps: float = 1e-6) -> GeoSpatialRect:
    if x.ndim > 1:
        x = x[(0,) * (x.ndim - 1)]

    if y.ndim > 1:
        y = y[(0,) * (y.ndim - 2) + (..., 0)]

    dx = None
    if x.size > 1:
        dx = np.gradient(x)
        if (dx.max() - dx.min()) >= eps:
            x = np.where(x < 0., 360. + x, x)
            dx = np.gradient(x)
            if (dx.max() - dx.min()) >= eps:
                raise ValueError('coordinate variable "lon" not is not equi-distant')
        dx = dx[0]

    dy = None
    if y.size > 1:
        dy = np.gradient(y)
        if (dy.max() - dy.min()) >= eps:
            raise ValueError('coordinate variable "lat" not is not equi-distant')
        dy = dy[0]

    if dx is None:
        if dy is None:
            raise ValueError('cannot determine cell size')
        dx = dy
    if dy is None:
        dy = dx

    # Outer boundaries are +/- half a cell size
    x1 = x[0] - 0.5 * dx
    x2 = x[-1] + 0.5 * dx
    x1 = _adjust(x1, -180., -180., eps)
    x1 = _adjust(x1, +180., -180., eps)
    x2 = _adjust(x2, -180., +180., eps)
    x2 = _adjust(x2, +180., +180., eps)

    y1 = y[0] - 0.5 * dy
    y2 = y[-1] + 0.5 * dy
    y1 = _adjust(y1, -90., -90., eps)
    y1 = _adjust(y1, +90., +90., eps)
    y2 = _adjust(y2, -90., -90., eps)
    y2 = _adjust(y2, +90., +90., eps)

    return x1, y1, x2, y2


def _adjust(x1, x2, x3, eps):
    return x3 if abs(x2 - x1) < eps else x1
