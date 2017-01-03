from unittest import TestCase

from cate.ui.imaging.cmaps import get_cmaps


class CmapsTest(TestCase):
    def test_get_cmaps(self):
        cmaps = get_cmaps()

        self.assertIs(cmaps, get_cmaps())
        self.assertIs(cmaps, get_cmaps())

        self.assertEqual(len(cmaps), 6)
        rec_len = len(cmaps[0])
        self.assertEqual(rec_len, 3)
        for cmap in cmaps:
            self.assertEqual(len(cmap), rec_len)

        self.assertEqual(cmaps[0][0], 'Perceptually Uniform Sequential')
        self.assertEqual(cmaps[1][0], 'Sequential 1')
        self.assertEqual(cmaps[2][0], 'Sequential 2')
        self.assertEqual(cmaps[3][0], 'Diverging')
        self.assertEqual(cmaps[4][0], 'Qualitative')
        self.assertEqual(cmaps[5][0], 'Miscellaneous')

        self.assertEqual(cmaps[0][1], 'For many applications, a perceptually uniform colormap is the best choice - '
                                      'one in which equal steps in data are perceived as equal steps in the color '
                                      'space')

    def test_gen_html(self):

        cmaps = get_cmaps()

        html_head = '<!DOCTYPE html>\n' + \
                    '<html lang="en">\n' + \
                    '<head>' + \
                    '<meta charset="UTF-8">' + \
                    '<title>matplotlib Color Maps</title>' + \
                    '</head>\n' + \
                    '<body style="padding: 0.2em">\n'

        html_body = ''

        html_foot = '</body>\n'+ \
                    '</html>\n'

        for cmap_cat, cmap_desc, cmap_bars in cmaps:
            html_body += '    <h2>%s</h2>\n' % cmap_cat
            html_body += '    <p><i>%s</i></p>\n' % cmap_desc
            html_body += '    <table style=border: 0">\n'
            for cmap_bar in cmap_bars:
                cmap_name, cmap_data = cmap_bar
                cmap_image = '<img src="data:image/png;base64,%s" width="100%%" height="100%%"/>' % cmap_data

                html_body += '        <tr><td style="width: 5em">%s:</td><td style="width: 40em">%s</td></tr>\n' % (cmap_name, cmap_image)
            html_body += '    </table>\n'

        html_page = html_head + html_body + html_foot

        with open('test_cmaps.html', 'w') as fp:
            fp.write(html_page)
