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

import matplotlib
import matplotlib.cm as cm

__author__ = "Norman Fomferra (Brockmann Consult GmbH)"

LAND_COVER_CCI_CMAP = 'land_cover_cci'


def register_lc_color_map():
    lc_color_mappings = [
        (0, dict(r=0, g=0, b=0)),
        (10, dict(r=255, g=255, b=100)),
        (11, dict(r=255, g=255, b=100)),
        (12, dict(r=255, g=255, b=0)),
        (20, dict(r=170, g=240, b=240)),
        (30, dict(r=220, g=240, b=100)),
        (40, dict(r=200, g=200, b=100)),
        (50, dict(r=0, g=100, b=0)),
        (60, dict(r=0, g=160, b=0)),
        (61, dict(r=0, g=160, b=0)),
        (62, dict(r=170, g=200, b=0)),
        (70, dict(r=0, g=60, b=0)),
        (71, dict(r=0, g=60, b=0)),
        (72, dict(r=0, g=80, b=0)),
        (80, dict(r=40, g=80, b=0)),
        (81, dict(r=40, g=80, b=0)),
        (82, dict(r=40, g=100, b=0)),
        (90, dict(r=120, g=130, b=0)),
        (100, dict(r=140, g=160, b=0)),
        (110, dict(r=190, g=150, b=0)),
        (120, dict(r=150, g=100, b=0)),
        (121, dict(r=120, g=75, b=0)),
        (122, dict(r=150, g=100, b=0)),
        (130, dict(r=255, g=180, b=50)),
        (140, dict(r=255, g=220, b=210)),
        (150, dict(r=255, g=235, b=175)),
        (151, dict(r=255, g=205, b=120)),
        (152, dict(r=255, g=210, b=120)),
        (153, dict(r=255, g=235, b=175)),
        (160, dict(r=0, g=120, b=190)),
        (170, dict(r=0, g=150, b=120)),
        (180, dict(r=0, g=220, b=130)),
        (190, dict(r=195, g=20, b=0)),
        (200, dict(r=255, g=245, b=215)),
        (201, dict(r=220, g=220, b=220)),
        (202, dict(r=255, g=245, b=215)),
        (210, dict(r=0, g=70, b=200)),
        (220, dict(r=255, g=255, b=255)),
    ]

    classes = {lc: color for lc, color in lc_color_mappings}

    invalid_rgba = (0, 0, 0, 0.5)
    class_0_rgba = (0, 0, 0, 0)

    rgba_list = []
    num_entries = 256
    last_rgba = invalid_rgba
    for i in range(num_entries):
        color = classes.get(i)
        if color:
            last_rgba = (color['r'] / 255, color['g'] / 255, color['b'] / 255, 1.0)
        rgba_list.append(last_rgba)
    rgba_list[0] = class_0_rgba

    cmap = matplotlib.colors.ListedColormap(rgba_list, name=LAND_COVER_CCI_CMAP, N=num_entries)
    cm.register_cmap(cmap=cmap)
