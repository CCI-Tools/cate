import os
import pkgutil

from ect.core.io import FileSetCatalogue, CATALOGUE_REGISTRY


def add_default_file_catalogue():
    ect_root_dir = 'ECT_DATA_ROOT'
    if 'ECT_DATA_ROOT' in os.environ:
        ect_root_dir = os.environ['ECT_DATA_ROOT']
    json_data = pkgutil.get_data('ect.ds', 'esa_cci_portal_ftp.json')
    cat = FileSetCatalogue.from_json(ect_root_dir, json_data.decode('utf-8'))
    CATALOGUE_REGISTRY.add_catalogue('default', cat)


