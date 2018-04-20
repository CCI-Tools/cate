# The MIT License (MIT)
# Copyright (c) 2018 by the ESA CCI Toolbox development team and contributors
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

from cate.conf import get_http_proxy

__author__ = "Chris Bernat (Telespazio VEGA UK Ltd), " \
             "Hans Permana (Brockmann Consult GmbH), " \
             "Norman Fomferra (Brockmann Consult GmbH)"


def initialize_proxy():
    """
    Initialize user defined proxy settings, read proxy setting from config file.
    Populates value to 3rd-party libraries using proper environment variables.
    """
    http_proxy = get_http_proxy()
    if not http_proxy:
        return

    from os import environ
    if http_proxy.startswith('https:'):
        environ['https_proxy'] = http_proxy
    elif http_proxy.startswith('http:'):
        environ['http_proxy'] = http_proxy


def configure_user_agent():
    """
    Sets proper Cate HTTP User-Agent in 3rd-party HTTP libraries. Allows to mark HTTP requests sent from Cate
    application via 3rd-party libraries as a part of Cate application flow rather than showing them as an independent
    calls.
    """
    import requests.utils

    requests.utils.old_user_agent = requests.utils.default_user_agent
    requests.utils.default_user_agent = lambda x=None: default_user_agent(requests.utils.old_user_agent(x)) if x \
        else default_user_agent(requests.utils.old_user_agent())


def default_user_agent(ext: str = ""):
    """
    Default Cate HTTP User-Agent.
    :param ext:
    :return:
    """
    from ..version import __title__, __version__
    from platform import machine, python_version, system

    return "{title}/{version} (Python {python}; {system} {arch}) {ext}".format(
        title=__title__,
        version=__version__,
        python=python_version(),
        system=system(),
        arch=machine(),
        ext=ext)
