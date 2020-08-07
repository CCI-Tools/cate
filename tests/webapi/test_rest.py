import unittest

from cate.webapi.rest import ensure_str


class TestEnsureStr(unittest.TestCase):
    def test_ensure_str(self):
        expected = 'doofer'

        value = 'doofer'
        res = ensure_str(value)

        self.assertEqual(expected, res)

        value = b'doofer'
        res = ensure_str(value)

        self.assertEqual(expected, res)

        value = [b'doofer']
        res = ensure_str(value)

        self.assertEqual(expected, res)

        expected = '1'
        value = 1
        res = ensure_str(value)

        self.assertEqual(expected, res)


if __name__ == '__main__':
    unittest.main()
