import json
import tempfile
import unittest
import unittest.mock

from cate.core.ds import DATA_STORE_REGISTRY
from cate.ds.config import LocalDataSourceConfiguration
from cate.ds.local import LocalDataStore


class LocalDataSourceConfigurationTest(unittest.TestCase):
    def setUp(self):

        self.tmp_dir = tempfile.mkdtemp()

        self.data_store_name = 'local'
        self.data_source_name = 'data_source_test'
        self.variables_name = ['lat', 'lon', 'time', 'variable_test']

        DATA_STORE_REGISTRY.add_data_store(LocalDataStore(self.data_store_name, self.tmp_dir))

        self.config = LocalDataSourceConfiguration(self.data_source_name, self.data_store_name, 'local')

    def test_load_file(self):

        test_data = {
            'name': 'local.test_name',
            'meta_data': {
                'type': "FILE_PATTERN",
                'data_store': 'local',
                'temporal_coverage': "2001-01-01,2001-01-31",
                'spatial_coverage': "0,10,20,30",
                'variables': ['var_test_1'],
                'source': 'local.previous_test',
                'last_update': None,
                'last_source_update': None
            },
            'files': [['file_1', '2002-02-01', '2002-02-01'],
                      ['file_2', '2002-03-01', '2002-03-01']]
        }

        with unittest.mock.patch('builtins.open', unittest.mock.mock_open(read_data=json.dumps(test_data))):
            with unittest.mock.patch('os.path.isfile', return_value=True):
                config = LocalDataSourceConfiguration.load('test.test_name')

        self.assertEqual(test_data.get('name'), config.name)
        self.assertEqual(test_data.get('meta_data').get('type'), config.type)
        self.assertEqual(test_data.get('meta_data').get('source'), config.source)
