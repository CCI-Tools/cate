from unittest import TestCase

from cate.util.im.cmaps import get_cmaps


class CmapsTest(TestCase):
    def test_get_cmaps_returns_singleton(self):
        cmaps = get_cmaps()
        self.assertIs(cmaps, get_cmaps())
        self.assertIs(cmaps, get_cmaps())

    def test_get_cmaps_retruns_equal_size_recs(self):
        cmaps = get_cmaps()
        rec_len = len(cmaps[0])
        self.assertEqual(rec_len, 3)
        for cmap in cmaps:
            self.assertEqual(len(cmap), rec_len)

    def test_get_cmaps_categories(self):
        cmaps = get_cmaps()
        self.assertGreaterEqual(len(cmaps), 6)
        self.assertEqual(cmaps[0][0], 'Perceptually Uniform Sequential')
        self.assertEqual(cmaps[1][0], 'Sequential 1')
        self.assertEqual(cmaps[2][0], 'Sequential 2')
        self.assertEqual(cmaps[3][0], 'Diverging')
        self.assertEqual(cmaps[4][0], 'Qualitative')
        self.assertEqual(cmaps[5][0], 'Miscellaneous')

    def test_get_cmaps_category_descr(self):
        cmaps = get_cmaps()
        self.assertEqual(cmaps[0][1], 'For many applications, a perceptually uniform colormap is the best choice - '
                                      'one in which equal steps in data are perceived as equal steps in the color '
                                      'space')

    def test_get_cmaps_category_tuples(self):
        cmaps = get_cmaps()
        category_tuple = cmaps[0][2]
        self.assertEqual(len(category_tuple), 4)
        self.assertEqual(category_tuple[0][0], 'viridis')
        self.assertEqual(category_tuple[0][1],
                         'iVBORw0KGgoAAAANSUhEUgAAAQAAAAACCAYAAAC3zQLZAAAAzklEQVR4nO2TQZLFIAhEX7dXmyPM/Y8SZwEqMcnU3/9QZTU8GszC6Ee/HQlk5FAsJIENqVGv/piZ3uqf3nX6Vtd+l8D8UwNOLhZL3+BLh796OXvMdWaqtrrqnZ/tjvuZT/0XxnN/5f25z9X7tIMTKzV7/5yrME3NHoPlUzvplgOevOcz6ZO5eCqzOmark1nHDQveHuuYaazZkTcdmE110HJu6doR3tgfPHyL51zNc0fd2xjf0vPukUPL36YBTcpcWArFyY0RTca88cYbXxt/gUOJC8yRF1kAAAAASUVORK5CYII=')

        self.assertEqual(category_tuple[1][0], 'inferno')
        self.assertEqual(category_tuple[2][0], 'plasma')
        self.assertEqual(category_tuple[3][0], 'magma')
