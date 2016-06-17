import os
import os.path
import pkgutil

from ect.core.io import FileSetCatalogue, CATALOGUE_REGISTRY

DEFAULT_DATA_SOURCES_DIR = os.path.expanduser(os.path.join('~', '.ect', 'data_sources'))
DEFAULT_DATA_ROOT = os.path.join(DEFAULT_DATA_SOURCES_DIR, 'esa_cci_portal_ftp')


def add_default_file_catalogue():
    ect_data_root_dir = os.environ.get('ECT_DATA_ROOT', DEFAULT_DATA_ROOT)
    json_data = pkgutil.get_data('ect.ds', 'esa_cci_portal_ftp.json')
    cat = FileSetCatalogue.from_json(ect_data_root_dir, json_data.decode('utf-8'))
    CATALOGUE_REGISTRY.add_catalogue('default', cat)
