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

from cate.conf import get_config_value

__author__ = "Chris Bernat (Telespazio VEGA UK Ltd)"


_CONFIG_KEY_HTTP_PROXY = 'http_proxy'
_CONFIG_KEY_HTTPS_PROXY = 'https_proxy'


def initialize_proxy():
    """
    Initialize user defined proxy settings, read proxy setting from config file.
    Populates value to 3rd-party libraries using proper environment variables.
    """
    from os import environ

    http_proxy_config = get_config_value(_CONFIG_KEY_HTTP_PROXY)
    https_proxy_config = get_config_value(_CONFIG_KEY_HTTPS_PROXY)
    if http_proxy_config:
        environ['http_proxy'] = http_proxy_config
    if https_proxy_config:
        environ['https_proxy'] = https_proxy_config
