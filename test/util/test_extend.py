from unittest import TestCase

from cate.util.extend import extend


# noinspection PyUnresolvedReferences
class ExtendTest(TestCase):
    def test_extension_property(self):
        # noinspection PyMethodMayBeStatic
        class Api:
            def m1(self, x):
                return 2 * x

        @extend(Api)
        class MyApiExt:
            """My API class extension"""

            def __init__(self, api0):
                self.api = api0

            def m2(self, x):
                return self.api.m1(x) + 2

        self.assertTrue(hasattr(Api, 'my_api_ext'))
        api = Api()
        self.assertEqual(api.my_api_ext.m2(8), 2 * 8 + 2)
        self.assertEqual(api.my_api_ext.__doc__, "My API class extension")
