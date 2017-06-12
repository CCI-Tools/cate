import cmath
import math
from unittest import TestCase

from cate.util.safe import get_safe_globals, safe_eval


class SafeTest(TestCase):
    def test_get_safe_globals(self):
        globals = get_safe_globals()

        self.assertIsNotNone(globals)

        self.assertEqual(globals.get('min'), min)
        self.assertEqual(globals.get('max'), max)
        self.assertEqual(globals.get('cmath'), cmath)
        self.assertEqual(globals.get('math'), math)

        self.assertEqual(globals.get('__builtins__'), None)
        self.assertEqual(globals.get('eval'), None)
        self.assertEqual(globals.get('exec'), None)

    def test_safe_eval_ok(self):
        self.assertEqual(safe_eval('2 + 1'), 3)
        self.assertEqual(safe_eval('x + 1', dict(x=2)), 3)
        self.assertEqual(safe_eval('"Ha%s" % "Ha"'), "HaHa")

    def test_safe_eval_forbidden(self):
        with self.assertRaises(TypeError) as e:
            safe_eval('eval("3+1")')

        with self.assertRaises(TypeError) as e:
            safe_eval('open("test.txt", "w")')
