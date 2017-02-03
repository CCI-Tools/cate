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

__author__ = "Norman Fomferra (Brockmann Consult GmbH)"

"""
Description
===========

This module provides Cate's configuration API.

Functions
=========
"""

import os.path

from .defaults import DEFAULT_CONF_FILE, LOCAL_CONF_FILE

_CONFIG = None


def get_config_path(name: str, default=None):
    """
    Get the ``str`` value of the configuration parameter *name* which is expected to be a path.
    Any tilde character '~' in the value will be expanded to the current user's home directory.

    :param name: The name of the configuration parameter.
    :param default: The default value, if *name* is not defined.
    :return: The value
    """
    value = get_config_value(name, default=default)
    return os.path.expanduser(str(value)) if value is not None else None


def get_config_value(name: str, default=None):
    """
    Get the value of the configuration parameter *name*.

    :param name: The name of the configuration parameter.
    :param default: The default value, if *name* is not defined.
    :return: The value
    """
    if not name:
        raise ValueError('name argument must be given')
    return get_config().get(name, default)


def get_config():
    """
    Get the global Cate configuration dictionary.

    :return: A mutable dictionary containing any Python objects.
    """
    global _CONFIG
    if _CONFIG is None:
        _CONFIG = {}

        default_config_file = os.path.expanduser(DEFAULT_CONF_FILE)
        if not os.path.exists(default_config_file):
            try:
                _write_default_config_file()
            except (IOError, OSError) as error:
                print('warning: failed to create %s: %s' % (default_config_file, str(error)))

        if os.path.isfile(default_config_file):
            try:
                _CONFIG = _read_python_config(default_config_file)
            except Exception as error:
                print('warning: failed to read %s: %s' % (default_config_file, str(error)))

        local_config_file = os.path.expanduser(LOCAL_CONF_FILE)
        if os.path.isfile(local_config_file):
            try:
                local_config = _read_python_config(local_config_file)
                _CONFIG.update(local_config)
            except Exception as e:
                print('warning: failed to read %s: %s' % (local_config_file, str(e)))

    return _CONFIG


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


def _write_default_config_file() -> str:
    default_config_file = os.path.expanduser(DEFAULT_CONF_FILE)
    default_config_dir = os.path.dirname(default_config_file)
    if default_config_dir and not os.path.exists(default_config_dir):
        os.mkdir(default_config_dir)
    with open(default_config_file, 'w') as fp:
        import pkgutil
        template_data = pkgutil.get_data('cate.core', 'template.py')
        text = template_data.decode('utf-8')
        fp.write(text)
    return default_config_file
