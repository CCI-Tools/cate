import unittest
from cate.util.cli import CommandError


class CommandErrorTest(unittest.TestCase):
    def test_plain(self):
        try:
            raise CommandError("haha")
        except CommandError as e:
            self.assertEqual(str(e), "haha")
            self.assertEqual(e.cause, None)

    def test_with_cause(self):
        e1 = ValueError("a > 5")
        try:
            raise CommandError("hoho") from e1
        except CommandError as e2:
            self.assertEqual(str(e2), "hoho")
            self.assertEqual(e2.cause, e1)
