import os
import os.path
import os.path
from unittest import TestCase

from ect.core.io import DATA_STORE_REGISTRY
from ect.ds.esa_cci_ftp import set_default_data_store


class EsaCciPortalFtpTest(TestCase):
    def test_set_default_data_store(self):
        if not DATA_STORE_REGISTRY.get_data_store('default'):
            set_default_data_store()

        data_store = DATA_STORE_REGISTRY.get_data_store('default')
        self.assertIsNotNone(data_store)
        self.assertEqual(data_store.root_dir,
                         os.path.expanduser(os.path.join('~', '.ect', 'data_stores', 'esa_cci_portal_ftp')))
