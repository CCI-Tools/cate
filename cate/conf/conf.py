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

__author__ = "Norman Fomferra (Brockmann Consult GmbH)"

import logging
import os.path
import sys
from typing import Any, Dict, Optional, Sequence, Union

from .defaults import GLOBAL_CONF_FILE, LOCAL_CONF_FILE, LOCATION_FILE, VERSION_CONF_FILE, \
    VARIABLE_DISPLAY_SETTINGS, DEFAULT_DATA_PATH, DEFAULT_VERSION_DATA_PATH, DEFAULT_COLOR_MAP, DEFAULT_RES_PATTERN, \
    WEBAPI_USE_WORKSPACE_IMAGERY_CACHE

_CONFIG = None


def get_config_path(name: str, default=None) -> str:
    """
    Get the ``str`` value of the configuration parameter *name* which is expected to be a path.
    Any tilde character '~' in the value will be expanded to the current user's home directory.

    :param name: The name of the configuration parameter.
    :param default: The default value, if *name* is not defined.
    :return: The value
    """
    value = get_config_value(name, default=default)
    return os.path.expanduser(str(value)) if value is not None else None


def get_config_value(name: str, default=None) -> Any:
    """
    Get the value of the configuration parameter *name*.

    :param name: The name of the configuration parameter.
    :param default: The default value, if *name* is not defined.
    :return: The value
    """
    if not name:
        raise ValueError('name argument must be given')
    return get_config().get(name, default)


def get_data_stores_path() -> str:
    """
    Get the default path to where Cate stores local data store information and stores data files synchronized with their
    remote versions.

    :return: Effectively reads the value of the configuration parameter ``data_stores_path``, if any. Otherwise return
             the default value ``~/.cate/data_stores``.
    """
    return get_config_path('data_stores_path', os.path.join(DEFAULT_DATA_PATH, 'data_stores'))


def get_use_workspace_imagery_cache() -> bool:
    return get_config_value('use_workspace_imagery_cache', WEBAPI_USE_WORKSPACE_IMAGERY_CACHE)


def get_default_res_pattern() -> str:
    """
    Get the default prefix for names generated for new workspace resources originating from opening data sources
    or executing workflow steps.
    This prefix is used only if no specific prefix is defined for a given operation.
    :return: default resource name prefix.
    """
    default_res_pattern = get_config().get('default_res_pattern')
    if default_res_pattern:
        default_res_pattern = default_res_pattern.strip()
    if not default_res_pattern:
        default_res_pattern = DEFAULT_RES_PATTERN
    return default_res_pattern


def get_variable_display_settings(var_name: str) -> Optional[Dict[str, Any]]:
    """
    Get the global variable display settings which is a combination of defaults.
    :return:
    """
    settings_dict = get_config().get('variable_display_settings', None)
    if settings_dict and var_name in settings_dict:
        return settings_dict[var_name]

    settings = VARIABLE_DISPLAY_SETTINGS.get(var_name)
    if settings:
        return settings

    return dict(color_map=get_config_value('default_color_map', DEFAULT_COLOR_MAP))


def get_config() -> dict:
    """
    Get the global Cate configuration dictionary.

    :return: A mutable dictionary containing any Python objects.
    """
    global _CONFIG
    if _CONFIG is None:
        _init_config([GLOBAL_CONF_FILE,
                      VERSION_CONF_FILE,
                      LOCAL_CONF_FILE],
                     [DEFAULT_DATA_PATH,
                      DEFAULT_VERSION_DATA_PATH])
    return _CONFIG


def _init_config(config_files: Sequence[str], config_dirs, template_module: str = 'cate.conf.template') -> None:
    """
    Initialize the Cate configuration.

    :param config_files: A non-empty sequence of configuration files, for example
           ``["~/.cate/conf.py", "~/.cate/<version>/conf.py", "./cate-conf.py"]``.
           Configuration files are read in the given order. If the first file does not exist, it will be created
           and its content will be taken from the given *template_module*.
    :param config_dirs: list of directories in which a location file will be written.
    :param template_module: Qualified name of a Python module that serves as a configuration template file.
           If given, this file will be copied into the parent directory of *default_config_file*.
    """
    _write_location_files(config_dirs)
    new_config = _read_config_files(config_files, template_module=template_module)
    global _CONFIG
    if new_config is not None:
        _CONFIG = new_config
    else:
        _CONFIG = {}


def _read_config_files(config_files: Sequence[str],
                       template_module: str = 'cate.conf.template') -> Union[str, None]:
    """
    Initialize the Cate configuration.

    :param config_files: A non-empty sequence of configuration files, for example
           ``["~/.cate/conf.py", "~/.cate/<version>/conf.py", "./cate-conf.py"]``.
           Configuration files are read in the given order. If the first file does not exist, it will be created
           and its content will be taken from the given *template_module*.
    :param template_module: Qualified name of a Python module that serves as a configuration template file.
           If given, this file will be copied into the parent directory of *default_config_file*.
    """
    default_config_file = config_files[0]
    if default_config_file and template_module:
        if not os.path.exists(default_config_file):
            try:
                _write_default_config_file(default_config_file, template_module)
            except (IOError, OSError) as error:
                logging.warning('failed writing %s: %s' % (default_config_file, str(error)))

    new_config = None
    for config_file in config_files:
        if config_file and os.path.isfile(config_file):
            config = None
            try:
                config = _read_python_config(config_file)
            except Exception as error:
                logging.warning('failed reading %s: %s' % (config_file, str(error)))
            if config is not None:
                if new_config is None:
                    new_config = config
                else:
                    new_config.update(config)
    return new_config


def _write_location_files(config_dirs: Sequence[str]):
    """
    Write a location file into the given directories.

    :param config_dirs: Sequence of directories paths.
    """
    new_config = None
    for config_dir in config_dirs:
        if not os.path.isdir(config_dir):
            try:
                os.makedirs(config_dir, exist_ok=True)
            except Exception as error:
                logging.warning('failed creating %s: %s' % (config_dir, str(error)))
        location_file = os.path.join(config_dir, LOCATION_FILE)
        try:
            with open(location_file, 'w') as fp:
                fp.write(sys.prefix)
        except Exception as error:
            logging.warning('failed writing %s: %s' % (location_file, str(error)))
    return new_config


def _read_python_config(file):
    """
    Reads a configuration *file* which may contain any Python code.

    :param file: Either a configuration file path or a file pointer.
    :return: A dictionary with all the variable assignments made in the configuration file.
    """

    fp = open(file, 'r') if isinstance(file, str) else file
    try:
        config = {}
        code = compile(fp.read(), file if isinstance(file, str) else '<NO FILE>', 'exec')
        exec(code, None, config)
        return config
    finally:
        if fp is not file:
            fp.close()


def _write_default_config_file(config_file: str, template_module: str) -> str:
    config_file = os.path.expanduser(config_file)
    config_dir = os.path.dirname(config_file)
    if config_dir and not os.path.exists(config_dir):
        os.makedirs(config_dir, exist_ok=True)

    with open(config_file, 'w', newline='') as fp:
        import pkgutil
        parts = template_module.split('.')
        template_package = '.'.join(parts[:-1])
        template_file = parts[-1] + '.py'
        template_data = pkgutil.get_data(template_package, template_file)
        text = template_data.decode('utf-8')
        fp.write(text)

    return config_file
