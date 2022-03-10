# The MIT License (MIT)
# Copyright (c) 2016, 2017 by the ESA CCI Toolbox development team and contributors
#
# Permission is hereby granted, free of charge, to any person obtaining a copy of
# this software and associated documentation files (the "Software"), to deal in
# the Software without restriction, including without limitation the rights to
# use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies
# of the Software, and to permit persons to whom the Software is furnished to do
# so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

from xcube.util.assertions import assert_given


def cate_init():
    # Plugin initializer.
    import yaml
    import os
    from xcube.core.store import DataStoreConfig
    from xcube.core.store import get_data_store_params_schema

    from cate.conf import get_data_stores_path
    from cate.conf.defaults import STORES_CONF_FILE
    from cate.core.common import default_user_agent
    from cate.core.ds import DATA_STORE_POOL

    dir_path = os.path.dirname(os.path.abspath(__file__))
    default_stores_file = os.path.join(dir_path, 'data/stores.yml')

    if os.path.exists(STORES_CONF_FILE):
        with open(STORES_CONF_FILE, 'r') as fp:
            store_configs = yaml.safe_load(fp)
    else:
        with open(default_stores_file, 'r') as fp:
            store_configs = yaml.safe_load(fp)

    for store_name, store_config in store_configs.items():
        store_id = store_config.get('store_id')
        assert_given(store_id, name='store_id', exception_type=RuntimeError)

        if store_id == 'file' \
                and 'store_params' in store_config \
                and store_config.get('store_params', {}).get('root') is None:
            root = os.environ.get('CATE_LOCAL_DATA_STORE_PATH',
                                  os.path.join(get_data_stores_path(),
                                               store_name))
            # Note: even if the root directory doesn't exist yet,
            # the xcube "file" data store will create it for us.
            store_config['store_params']['root'] = root

        store_params_schema = get_data_store_params_schema(store_id)
        if 'user_agent' in store_params_schema.properties:
            if 'store_params' not in store_config:
                store_config['store_params'] = {}
            store_config['store_params']['user_agent'] = default_user_agent()

        store_config = DataStoreConfig.from_dict(store_config)
        DATA_STORE_POOL.add_store_config(store_name, store_config)
