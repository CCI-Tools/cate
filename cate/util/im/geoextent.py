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


from typing import Tuple, Any

import numpy as np

__author__ = "Norman Fomferra (Brockmann Consult GmbH)"

EPS = 1e-04


class GeoExtent:
    """
    A geographical extent given by *west*, *south*, *east*, and *north*.

    :param west: West coordinate
    :param south: South coordinate
    :param east: East coordinate
    :param north: North coordinate
    :param inv_y: Whether the image's y axis (latitude dimension) is flipped
    :param eps: Epsilon for coordinate comparisons
    """

    def __init__(self, west=-180., south=-90., east=180., north=90., inv_y=False, eps=EPS):
        west = _adjust_lon_1(float(west), eps)
        south = _adjust_lat(float(south), eps)
        east = _adjust_lon_2(float(east), eps)
        north = _adjust_lat(float(north), eps)
        if not _valid_lon(west):
            raise ValueError('west out of bounds: %s' % west)
        if not _valid_lat(south):
            raise ValueError('south out of bounds: %s' % south)
        if not _valid_lon(east):
            raise ValueError('east out of bounds: %s' % east)
        if not _valid_lat(north):
            raise ValueError('north out of bounds: %s' % north)
        if abs(east - west) < eps:
            raise ValueError('east and west are almost equal: %s, %s' % (east, west))
        if abs(south - north) < eps:
            raise ValueError('south and north are almost equal: %s, %s' % (south, north))
        if south > north:
            raise ValueError('south is greater than north: %s > %s' % (south, north))
        self._west = west
        self._south = south
        self._east = east
        self._north = north
        self._inv_y = inv_y
        self._eps = eps

    @property
    def west(self):
        return self._west

    @property
    def south(self):
        return self._south

    @property
    def east(self):
        return self._east

    @property
    def north(self):
        return self._north

    @property
    def inv_y(self):
        return self._inv_y

    @property
    def eps(self):
        return self._eps

    @property
    def crosses_antimeridian(self):
        return self._west > self._east

    @property
    def coords(self) -> Tuple[float, float, float, float]:
        return self._west, self._south, self._east, self._north

    def __str__(self):
        return ', '.join([str(c) for c in self.coords])

    def __repr__(self):
        args = []
        if self._west != -180.:
            args.append('west=%s' % self._west)
        if self._south != -90.:
            args.append('south=%s' % self._south)
        if self._east != 180.:
            args.append('east=%s' % self._east)
        if self._north != 90.:
            args.append('north=%s' % self._north)
        if self._inv_y:
            args.append('inv_y=%s' % self._inv_y)
        if self._eps != EPS:
            args.append('eps=%s' % self._eps)
        return 'GeoExtend(%s)' % ', '.join(args)

    def __hash__(self) -> int:
        # we silently ignore self.eps
        return hash(self.coords) + self.inv_y

    def __eq__(self, o: Any) -> bool:
        # we silently ignore self.eps
        try:
            return self.coords == o.coords and self.inv_y == o.inv_y
        except AttributeError:
            return False

    @classmethod
    def from_coord_arrays(cls, x: np.ndarray, y: np.ndarray, eps: float = EPS) -> 'GeoExtent':
        if x.ndim > 1:
            x = x[(0,) * (x.ndim - 1)]

        if y.ndim > 1:
            y = y[(0,) * (y.ndim - 2) + (..., 0)]

        dx = None
        if x.size > 1:
            dx = np.gradient(x)
            if (dx.max() - dx.min()) >= eps:
                fail = True
                # this may happened because we cross the antimeridian
                if x[0] > x[-1]:
                    # normalize x
                    x = np.where(x < 0., 360. + x, x)
                    # and test once more
                    dx = np.gradient(x)
                    fail = (dx.max() - dx.min()) >= eps
                if fail:
                    raise ValueError('coordinate variable "lon" is not equi-distant')
            dx = dx[0]

        dy = None
        if y.size > 1:
            dy = np.gradient(y)
            if (dy.max() - dy.min()) >= eps:
                # print('dy =', dy)
                raise ValueError('coordinate variable "lat" is not equi-distant')
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
        y1 = y[0] - 0.5 * dy
        y2 = y[-1] + 0.5 * dy

        if x2 > 180.0:
            x2 -= 360.0

        if y1 < y2:
            return GeoExtent(west=x1, south=y1, east=x2, north=y2, inv_y=True, eps=eps)
        else:
            return GeoExtent(west=x1, south=y2, east=x2, north=y1, inv_y=False, eps=eps)


def _adjust_lat(lat, eps):
    lat = _adjust(lat, -90., -90., eps)
    lat = _adjust(lat, +90., +90., eps)
    return lat


def _adjust_lon_1(lon, eps):
    lon = _adjust(lon, -180., -180., eps)
    lon = _adjust(lon, +180., -180., eps)
    return lon


def _adjust_lon_2(lon, eps):
    lon = _adjust(lon, -180., +180., eps)
    lon = _adjust(lon, +180., +180., eps)
    return lon


def _adjust(x1, x2, x3, eps):
    return x3 if abs(x2 - x1) < eps else x1


def _valid_lon(x):
    return -180. <= x <= 180.


def _valid_lat(y):
    return -90. <= y <= 90.
