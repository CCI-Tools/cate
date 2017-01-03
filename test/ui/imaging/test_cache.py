from unittest import TestCase

from cate.ui.imaging.cache import CacheStore, Cache


class TestCacheStore(CacheStore):
    def __init__(self):
        self.trace = ''

    def store_value(self, key, value):
        self.trace += 'store(%s, %s);' % (key, value)
        return 'S/' + value, 100 * len(value)

    def restore_value(self, key, stored_value):
        self.trace += 'restore(%s, %s);' % (key, stored_value)
        return stored_value[2:]

    def discard_value(self, key, stored_value):
        self.trace += 'discard(%s, %s);' % (key, stored_value)


class CacheTest(TestCase):
    def setUp(self):
        pass

    def test_it(self):
        cache_store = TestCacheStore()
        cache = Cache(store=cache_store, capacity=1000)

        self.assertIs(cache.store, cache_store)
        self.assertEqual(cache.size, 0)
        self.assertEqual(cache.max_size, 750)

        cache_store.trace = ''
        self.assertEqual(cache.get_value('k1'), None)
        self.assertEqual(cache.size, 0)
        self.assertEqual(cache_store.trace, '')

        cache_store.trace = ''
        cache.put_value('k1', 'x')
        self.assertEqual(cache.get_value('k1'), 'x')
        self.assertEqual(cache.size, 100)
        self.assertEqual(cache_store.trace, 'store(k1, x);restore(k1, S/x);')

        cache_store.trace = ''
        cache.remove_value('k1')
        self.assertEqual(cache.size, 0)
        self.assertEqual(cache_store.trace, 'discard(k1, S/x);')
        cache_store.trace = ''

        cache_store.trace = ''
        cache.put_value('k1', 'x')
        cache.put_value('k1', 'xx')
        self.assertEqual(cache.get_value('k1'), 'xx')
        self.assertEqual(cache.size, 200)
        self.assertEqual(cache_store.trace, 'store(k1, x);discard(k1, S/x);store(k1, xx);restore(k1, S/xx);')

        cache_store.trace = ''
        cache.remove_value('k1')
        self.assertEqual(cache.size, 0)
        self.assertEqual(cache_store.trace, 'discard(k1, S/xx);')

        cache_store.trace = ''
        cache.put_value('k1', 'x')
        cache.put_value('k2', 'xxx')
        cache.put_value('k3', 'xx')
        self.assertEqual(cache.get_value('k1'), 'x')
        self.assertEqual(cache.get_value('k2'), 'xxx')
        self.assertEqual(cache.get_value('k3'), 'xx')
        self.assertEqual(cache.size, 600)
        self.assertEqual(cache_store.trace, 'store(k1, x);store(k2, xxx);store(k3, xx);'
                                            'restore(k1, S/x);restore(k2, S/xxx);restore(k3, S/xx);')

        cache_store.trace = ''
        cache.put_value('k4', 'xxxx')
        self.assertEqual(cache.size, 600)
        self.assertEqual(cache_store.trace, 'store(k4, xxxx);discard(k1, S/x);discard(k2, S/xxx);')

        cache_store.trace = ''
        cache.clear()
        self.assertEqual(cache.size, 0)
