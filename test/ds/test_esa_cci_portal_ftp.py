import os
import os.path
import pkgutil

from ect.core.io import FileSetCatalogue, CATALOGUE_REGISTRY

from unittest import TestCase

from ect.ds.esa_cci_portal_ftp import add_default_file_catalogue

import os.path

class EsaCciPortalFtpTest(TestCase):
    def test_default_file_catalogue(self):
        if not CATALOGUE_REGISTRY.get_catalogue('default'):
            add_default_file_catalogue()

        catalog = CATALOGUE_REGISTRY.get_catalogue('default')
        self.assertIsNotNone(catalog)
        self.assertEqual(catalog.root_dir, os.path.expanduser(os.path.join('~', '.ect', 'data_sources', 'esa_cci_portal_ftp')))

