import os
import unittest
import xcube.core.store as xcube_store

from cate.core.ds import DATA_STORE_POOL


def _create_test_data_store_config(name: str):
    local_test_store_path = \
        os.path.join(os.path.dirname(__file__), 'ds', 'resources', 'datasources', name)
    local_test_store_dict = {
        "store_id": "directory",
        "store_params": {
            "base_dir": local_test_store_path,
        },
        "title": f"Local Test Store '{name}'"
    }
    local_test_store = xcube_store.DataStoreConfig.from_dict(local_test_store_dict)
    return local_test_store


class StoreTest(unittest.TestCase):
    _orig_store_configs = None

    @classmethod
    def setUpClass(cls):
        cls._orig_store_configs = {instance_id: DATA_STORE_POOL.get_store_config(instance_id)
                                   for instance_id in DATA_STORE_POOL.store_instance_ids}
        for instance_id in DATA_STORE_POOL.store_instance_ids:
            DATA_STORE_POOL.remove_store_config(instance_id)
        DATA_STORE_POOL.add_store_config('local_test_store_1',
                                         _create_test_data_store_config('local'))
        DATA_STORE_POOL.add_store_config('local_test_store_2',
                                         _create_test_data_store_config('local2'))

    @classmethod
    def tearDownClass(cls):
        for instance_id in DATA_STORE_POOL.store_instance_ids:
            DATA_STORE_POOL.remove_store_config(instance_id)
        for instance_id, config in cls._orig_store_configs.items():
            DATA_STORE_POOL.add_store_config(instance_id, config)
