import json
import unittest
import unittest.mock
from collections import OrderedDict
from tempfile import mkdtemp
from time import time

from cate.core.types import GeometryLike, TimeRangeLike, VariableNamesLike
from cate.ds.config import LocalDataSourceConfiguration


class LocalDataSourceConfigurationTest(unittest.TestCase):

    def setUp(self):
        self.tmp_dir = mkdtemp()
        self.data_store_name = 'local'
        self.data_source_name = 'data_source_test_{}'.format(time())

        self.config = LocalDataSourceConfiguration(self.data_source_name, self.data_store_name, 'local',
                                                   None, None, None, None, None,
                                                   OrderedDict({
                                                       'file_1': TimeRangeLike.convert("2001-01-01,2001-01-01")
                                                   }))

    def test_load_config(self):

        test_data = {
            'name': 'local.test_name',
            'meta_data': {
                'type': "FILE_PATTERN",
                'data_store': 'local',
                'temporal_coverage': "2001-01-01 00:00:00,2001-01-31 23:59:59",
                'spatial_coverage': "0,10,20,30",
                'variables': ['var_test_1', 'var_test_2'],
                'source': 'local.previous_test',
                'last_update': None
            },
            'files': [['file_1', '2002-02-01 00:00:00', '2002-02-01 23:59:59'],
                      ['file_2', '2002-03-01 00:00:00', '2002-03-01 23:59:59']]
        }

        with unittest.mock.patch('builtins.open', unittest.mock.mock_open(read_data=json.dumps(test_data))):
            with unittest.mock.patch('os.path.isfile', return_value=True):
                config = LocalDataSourceConfiguration.load('test.test_name')

        self.assertEqual(test_data.get('name'), config.name)
        self.assertEqual(test_data.get('meta_data').get('type'), config.type)
        self.assertEqual(test_data.get('meta_data').get('source'), config.source)

        self.assertEqual(test_data.get('meta_data').get('type'), config.type)
        self.assertEqual(test_data.get('meta_data').get('source'), config.source)

        self.assertEqual(TimeRangeLike.convert(test_data.get('meta_data').get('temporal_coverage')
                                               if test_data.get('meta_data').get('temporal_coverage') else None),
                         config.temporal_coverage)
        self.assertEqual(GeometryLike.convert(test_data.get('meta_data').get('spatial_coverage')
                                              if test_data.get('meta_data').get('spatial_coverage') else None),
                         config.region)
        self.assertEqual(VariableNamesLike.convert(test_data.get('meta_data').get('variables')
                                                   if test_data.get('meta_data').get('variables') else None),
                         config.var_names)

        for file in test_data.get('files'):
            self.assertIn(file[0], config.files.keys())
            self.assertEquals(TimeRangeLike.convert("{},{}".format(file[1], file[2])) if file[1] and file[2] else None,
                              config.files.get(file[0]))

    def test_save_and_load_config(self):
        pass

    def test_add_file(self):
        files_number = len(self.config.files.items())

        self.config.add_file('file_2', TimeRangeLike.convert("2001-01-02,2001-01-02"))
        self.assertEqual(len(self.config.files.items()), files_number + 1)

    def test_remove_file(self):
        files_number = len(self.config.files.items())
        self.assertGreaterEqual(files_number, 1)

        file_name = list(self.config.files.keys())[0]
        self.config.remove_file(file_name)
        self.assertEqual(len(self.config.files.items()), files_number - 1)
