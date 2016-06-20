import os
import os.path
import pkgutil

from ect.core.io import FileSetDataStore, DATA_STORE_REGISTRY

DEFAULT_DATA_SOURCES_DIR = os.path.expanduser(os.path.join('~', '.ect', 'data_stores'))
DEFAULT_DATA_ROOT = os.path.join(DEFAULT_DATA_SOURCES_DIR, 'esa_cci_portal_ftp')


def set_default_data_store():
    ect_data_root_dir = os.environ.get('ECT_DATA_ROOT', DEFAULT_DATA_ROOT)
    json_data = pkgutil.get_data('ect.ds', 'esa_cci_portal_ftp.json')
    cat = FileSetDataStore.from_json(ect_data_root_dir, json_data.decode('utf-8'))
    DATA_STORE_REGISTRY.add_data_store('default', cat)
