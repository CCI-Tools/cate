from collections import OrderedDict
from unittest import TestCase

from cate.util.namespace import Namespace


# noinspection PyUnusedLocal
class NamespaceTest(TestCase):
    def test_empty(self):
        namespace = Namespace()
        self.assertEqual(len(namespace), 0)
        self.assertEqual(str(namespace), 'Namespace()')
        self.assertEqual(repr(namespace), 'Namespace()')
        self.assertFalse('a' in namespace)
        self.assertEqual(list(namespace), [])
        with self.assertRaisesRegex(KeyError, "'a'"):
            v = namespace['a']
        with self.assertRaisesRegex(AttributeError, "attribute 'a' not found"):
            v = namespace.a
        with self.assertRaisesRegex(IndexError, "list index out of range"):
            v = namespace[0]
        with self.assertRaisesRegex(IndexError, "list index out of range"):
            namespace[0] = True

    def test_set_items(self):
        namespace = Namespace()
        namespace['z'] = 10
        namespace.a = 20
        namespace.p = 30
        self.assertEqual(len(namespace), 3)
        self.assertEqual(namespace['z'], 10)
        self.assertEqual(namespace['a'], 20)
        self.assertEqual(namespace['p'], 30)
        self.assertEqual(namespace.z, 10)
        self.assertEqual(namespace.a, 20)
        self.assertEqual(namespace.p, 30)
        self.assertEqual(namespace[0], 10)
        self.assertEqual(namespace[1], 20)
        self.assertEqual(namespace[2], 30)
        self.assertEqual(namespace[:], [10, 20, 30])
        self.assertEqual(list(namespace), [('z', 10), ('a', 20), ('p', 30)])
        del namespace.a
        self.assertEqual(len(namespace), 2)
        self.assertEqual(namespace['z'], 10)
        self.assertEqual(namespace['p'], 30)
        self.assertEqual(namespace.z, 10)
        self.assertEqual(namespace.p, 30)
        self.assertEqual(namespace[0], 10)
        self.assertEqual(namespace[1], 30)
        self.assertEqual(namespace[:], [10, 30])
        self.assertEqual(list(namespace), [('z', 10), ('p', 30)])
        del namespace[0]
        self.assertEqual(len(namespace), 1)
        self.assertEqual(namespace['p'], 30)
        self.assertEqual(namespace.p, 30)
        self.assertEqual(namespace[0], 30)
        self.assertEqual(namespace[:], [30])
        self.assertEqual(list(namespace), [('p', 30)])
        del namespace['p']
        self.assertEqual(len(namespace), 0)
        self.assertEqual(namespace[:], [])
        self.assertEqual(list(namespace), [])

    def test_non_empty(self):
        namespace = Namespace([('a', 10), ('b', 20), ('c', 30)])
        self.assertEqual(len(namespace), 3)
        self.assertEqual(str(namespace), "Namespace([('a', 10), ('b', 20), ('c', 30)])")
        self.assertEqual(repr(namespace), "Namespace([('a', 10), ('b', 20), ('c', 30)])")
        self.assertTrue('a' in namespace)
        self.assertTrue('b' in namespace)
        self.assertTrue('c' in namespace)
        self.assertEqual(namespace['a'], 10)
        self.assertEqual(namespace['b'], 20)
        self.assertEqual(namespace['c'], 30)
        self.assertEqual(namespace.a, 10)
        self.assertEqual(namespace.b, 20)
        self.assertEqual(namespace.c, 30)
        self.assertEqual(namespace[0], 10)
        self.assertEqual(namespace[1], 20)
        self.assertEqual(namespace[2], 30)
        self.assertEqual(list(namespace), [('a', 10), ('b', 20), ('c', 30)])

    def test_for_in(self):
        namespace = Namespace([('a', 10), ('b', 20), ('c', 30)])
        items = [(name, value) for name, value in namespace]
        self.assertEqual(items, [('a', 10), ('b', 20), ('c', 30)])

    def test_to_dict(self):
        namespace = Namespace([('a', 10), ('b', 20), ('c', 30)])
        self.assertEqual(OrderedDict(namespace), OrderedDict([('a', 10), ('b', 20), ('c', 30)]))
        self.assertEqual(dict(namespace), {'a': 10, 'b': 20, 'c': 30})
