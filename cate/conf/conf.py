# The MIT License (MIT)
# Copyright (c) 2017 by the Cate Development Team and contributors
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
from typing import Any

__author__ = "Norman Fomferra (Brockmann Consult GmbH)"

import os.path

from .defaults import DEFAULT_CONF_FILE, LOCAL_CONF_FILE

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


def get_config() -> dict:
    """
    Get the global Cate configuration dictionary.

    :return: A mutable dictionary containing any Python objects.
    """
    global _CONFIG
    if _CONFIG is None:
        set_config(default_config_file=os.path.expanduser(DEFAULT_CONF_FILE),
                   local_config_file=os.path.expanduser(LOCAL_CONF_FILE),
                   template_module='cate.conf.template')

    return _CONFIG


def set_config(config: dict = None,
               default_config_file: str = None,
               local_config_file: str = None,
               template_module: str = None) -> None:
    """
    Set the global Cate configuration dictionary.

    :param config: Optional configuration object
    :param default_config_file: Location of the default configuration Python file
    :param local_config_file: Location of a local configuration Python file
    :param template_module: Qualified name of a Python module that serves as a configuration template file.
                            If given, this file will be copied into the parent directory of *default_config_file*.
    :return:
    """
    global _CONFIG
    _CONFIG = config or {}

    if default_config_file and template_module:
        if not os.path.exists(default_config_file):
            try:
                _write_default_config_file(default_config_file, template_module)
            except (IOError, OSError) as error:
                print('warning: failed to create %s: %s' % (default_config_file, str(error)))

    if default_config_file and os.path.isfile(default_config_file):
        try:
            _CONFIG = _read_python_config(default_config_file)
        except Exception as error:
            print('warning: failed to read %s: %s' % (default_config_file, str(error)))

    if local_config_file and os.path.isfile(local_config_file):
        try:
            local_config = _read_python_config(local_config_file)
            _CONFIG.update(local_config)
        except Exception as e:
            print('warning: failed to read %s: %s' % (local_config_file, str(e)))


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


def _write_default_config_file(default_config_file: str, template_module: str) -> str:
    default_config_file = os.path.expanduser(default_config_file)
    default_config_dir = os.path.dirname(default_config_file)
    if default_config_dir and not os.path.exists(default_config_dir):
        os.mkdir(default_config_dir)
    with open(default_config_file, 'w') as fp:
        import pkgutil
        parts = template_module.split('.')
        template_package = '.'.join(parts[:-1])
        template_file = parts[-1] + '.py'
        template_data = pkgutil.get_data(template_package, template_file)
        text = template_data.decode('utf-8')
        fp.write(text)
    return default_config_file
