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

import json
import os
import socket
import urllib.request
from typing import Optional, Union


def read_service_info(service_info_file: str) -> Union[dict, None]:
    """
    Get a dictionary with WebAPI service information:::

        {
            "port": service-port-number (int)
            "address": service-address (str)
            "caller": caller-name (str)
            "started": service-start-time (str)
        }

    :return: dictionary with WebAPI service information or ``None`` if it does not exist
    :raise OSError, IOError: if information file exists, but could not be loaded
    """
    if not service_info_file:
        raise ValueError('service_info_file argument must be given')
    if os.path.isfile(service_info_file):
        with open(service_info_file) as fp:
            return json.load(fp=fp) or {}
    return None


def write_service_info(service_info: dict, service_info_file: str) -> None:
    if not service_info:
        raise ValueError('service_info argument must be given')
    if not service_info_file:
        raise ValueError('service_info_file argument must be given')
    dir_path = os.path.dirname(service_info_file)
    if dir_path:
        os.makedirs(dir_path, exist_ok=True)
    with open(service_info_file, 'w') as fp:
        json.dump(service_info, fp, indent='  ')


def is_service_compatible(port: Optional[int], address: Optional[str], caller: Optional[str],
                          service_info: dict) -> bool:
    if not port and not service_info.get('port'):
        # This means we have a service_info without port, should actually never happen,
        # but who knows, service_info_file may have been modified.
        return False
    port_ok = port == service_info.get('port') if port and port > 0 else True
    address_ok = address == service_info.get('address') if address else True
    caller_ok = caller == service_info.get('caller') if caller else True
    return port_ok and address_ok and caller_ok


def is_service_running(port: int, address: str, timeout: float = 10.0) -> bool:
    url = 'http://%s:%s/' % (address or '127.0.0.1', port)
    # noinspection PyBroadException
    try:
        with urllib.request.urlopen(url, timeout=timeout) as response:
            json_text = response.read()
    except:
        return False
    json_response = json.loads(json_text.decode('utf-8'))
    return json_response.get('status') == 'ok'


def find_free_port():
    s = socket.socket()
    # Bind to a free port provided by the host.
    s.bind(('', 0))
    free_port = s.getsockname()[1]
    s.close()
    # Return the port number assigned.
    return free_port
