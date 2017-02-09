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

import base64
import io
from threading import Lock

import matplotlib
import matplotlib.cm as cm
import numpy as np
from PIL import Image

# Have colormaps separated into categories:
# (taken from http://matplotlib.org/examples/color/colormaps_reference.html)

_CMAPS = (('Perceptually Uniform Sequential',
           'For many applications, a perceptually uniform colormap is the best choice - '
           'one in which equal steps in data are perceived as equal steps in the color space',
           ('viridis', 'inferno', 'plasma', 'magma')),
          ('Sequential 1',
           'These colormaps are approximately monochromatic colormaps varying smoothly '
           'between two color tones - usually from low saturation (e.g. white) to high '
           'saturation (e.g. a bright blue). Sequential colormaps are ideal for '
           'representing most scientific data since they show a clear progression from '
           'low-to-high values.',
           ('Blues', 'BuGn', 'BuPu',
            'GnBu', 'Greens', 'Greys', 'Oranges', 'OrRd',
            'PuBu', 'PuBuGn', 'PuRd', 'Purples', 'RdPu',
            'Reds', 'YlGn', 'YlGnBu', 'YlOrBr', 'YlOrRd')),
          ('Sequential 2',
           'Many of the values from the Sequential 2 plots are monotonically increasing.',
           ('afmhot', 'autumn', 'bone', 'cool',
            'copper', 'gist_heat', 'gray', 'hot',
            'pink', 'spring', 'summer', 'winter')),
          ('Diverging',
           'These colormaps have a median value (usually light in color) and vary '
           'smoothly to two different color tones at high and low values. Diverging '
           'colormaps are ideal when your data has a median value that is significant '
           '(e.g.  0, such that positive and negative values are represented by '
           'different colors of the colormap).',
           ('BrBG', 'bwr', 'coolwarm', 'PiYG', 'PRGn', 'PuOr',
            'RdBu', 'RdGy', 'RdYlBu', 'RdYlGn', 'Spectral',
            'seismic')),
          ('Qualitative',
           'These colormaps vary rapidly in color. Qualitative colormaps are useful for '
           'choosing a set of discrete colors.',
           ('Accent', 'Dark2', 'Paired', 'Pastel1',
            'Pastel2', 'Set1', 'Set2', 'Set3')),
          ('Miscellaneous',
           'Colormaps that don\'t fit into the categories above.',
           ('gist_earth', 'terrain', 'ocean', 'gist_stern',
            'brg', 'CMRmap', 'cubehelix',
            'gnuplot', 'gnuplot2', 'gist_ncar',
            'nipy_spectral', 'jet', 'rainbow',
            'gist_rainbow', 'hsv', 'flag', 'prism')))

_CBARS_LOADED = False
_LOCK = Lock()


def get_cmaps():
    """
    Return a JSON-serializable tuple containing records of the form: (<cmap-category>, <cmap-category-description>, <cmap-tuples>),
    where <cmap-tuples> is a tuple containing records of the form (<cmap-name>, <cbar-png-bytes>), and where
    <cbar-png-bytes> are encoded PNG images of size 256 x 2 pixels,
    :return: all known matplotlib color maps
    """
    global _CBARS_LOADED, _CMAPS
    if not _CBARS_LOADED:
        _LOCK.acquire()
        _CBARS_LOADED = True
        new_cmaps = []
        for cmap_category, cmap_description, cmap_names in _CMAPS:
            cbar_list = []
            for cmap_name in cmap_names:
                try:
                    cmap = cm.get_cmap(cmap_name)
                except:
                    print("WARNING: detected invalid colormap '" + cmap_name + "'")
                    continue

                # Add extra colormaps with alpha gradient
                # see http://matplotlib.org/api/colors_api.html
                if type(cmap) == matplotlib.colors.LinearSegmentedColormap:
                    new_name = cmap.name + '_alpha'
                    new_segmentdata = dict(cmap._segmentdata)
                    # let alpha increase from 0.0 to 0.5
                    new_segmentdata['alpha'] = ((0.0, 0.0, 0.0),
                                                (0.5, 1.0, 1.0),
                                                (1.0, 1.0, 1.0))
                    new_cmap = matplotlib.colors.LinearSegmentedColormap(new_name, new_segmentdata)
                    cm.register_cmap(cmap=new_cmap)
                    # print("INFO: new colormap '" + new_name + "'")
                elif type(cmap) == matplotlib.colors.ListedColormap:
                    new_name = cmap.name + '_alpha'
                    # print("TODO: create colormap '" + new_name + "'")

                gradient = np.linspace(0, 1, 256)
                gradient = np.vstack((gradient, gradient))
                image_data = cmap(gradient, bytes=True)
                image = Image.fromarray(image_data, 'RGBA')

                # ostream = io.FileIO('../cmaps/' + cmap_name + '.png', 'wb')
                # image.save(ostream, format='PNG')
                # ostream.close()

                ostream = io.BytesIO()
                image.save(ostream, format='PNG')
                cbar_png_bytes = ostream.getvalue()
                ostream.close()

                cbar_png_data = base64.b64encode(cbar_png_bytes)
                cbar_png_bytes = cbar_png_data.decode('unicode_escape')

                cbar_list.append((cmap_name, cbar_png_bytes))
            new_cmaps.append((cmap_category, cmap_description, tuple(cbar_list)))
        _CMAPS = tuple(new_cmaps)
        _LOCK.release()
        # import pprint
        # pprint.pprint(_CMAPS)
    return _CMAPS


def main():
    cmaps = get_cmaps()

    html_head = '<!DOCTYPE html>\n' + \
                '<html lang="en">\n' + \
                '<head>' + \
                '<meta charset="UTF-8">' + \
                '<title>matplotlib Color Maps</title>' + \
                '</head>\n' + \
                '<body style="padding: 0.2em">\n'

    html_body = ''

    html_foot = '</body>\n' + \
                '</html>\n'

    for cmap_cat, cmap_desc, cmap_bars in cmaps:
        html_body += '    <h2>%s</h2>\n' % cmap_cat
        html_body += '    <p><i>%s</i></p>\n' % cmap_desc
        html_body += '    <table style=border: 0">\n'
        for cmap_bar in cmap_bars:
            cmap_name, cmap_data = cmap_bar
            cmap_image = '<img src="data:image/png;base64,%s" width="100%%" height="100%%"/>' % cmap_data

            html_body += '        <tr><td style="width: 5em">%s:</td><td style="width: 40em">%s</td></tr>\n' % (
                cmap_name, cmap_image)
        html_body += '    </table>\n'

    html_page = html_head + html_body + html_foot

    html_filename = 'test_cmaps.html'
    with open(html_filename, 'w') as fp:
        fp.write(html_page)

    import webbrowser
    webbrowser.open_new_tab(html_filename)


if __name__ == '__main__':
    main()
