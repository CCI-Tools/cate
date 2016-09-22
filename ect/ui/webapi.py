# The MIT License (MIT)
# Copyright (c) 2016 by the ECT Development Team and contributors
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

import argparse
import json
import os.path
import socket
import subprocess
import sys
import time
import urllib.request
from datetime import date, datetime

from ect.ui.workspace import FSWorkspaceManager
from ect.version import __version__
from tornado.ioloop import IOLoop
from tornado.log import enable_pretty_logging
from tornado.web import RequestHandler, Application

# Explicitly load ECT-internal plugins.
__import__('ect.ds')
__import__('ect.ops')

CLI_NAME = 'ect-webapi'

DEFAULT_ADDRESS = '127.0.0.1'
DEFAULT_PORT = 8888


# All JSON responses should have same structure, namely a dictionary as follows:
#
# {
#    "status": "ok" | "error",
#    "error": optional error-details,
#    "content": optional content, if status "ok"
# }

def get_application():
    application = Application([
        (url_pattern('/'), VersionHandler),
        (url_pattern('/ws/init'), WorkspaceInitHandler),
        (url_pattern('/ws/get/{{base_dir}}'), WorkspaceGetHandler),
        (url_pattern('/ws/del/{{base_dir}}'), WorkspaceDeleteHandler),
        (url_pattern('/ws/res/set/{{base_dir}}/{{res_name}}'), ResourceSetHandler),
        (url_pattern('/ws/res/write/{{base_dir}}/{{res_name}}'), ResourceWriteHandler),
        (url_pattern('/exit'), ExitHandler)
    ])
    application.workspace_manager = FSWorkspaceManager()
    return application


def main(args=None):
    if args is None:
        args = sys.argv[1:]

    parser = argparse.ArgumentParser(prog=CLI_NAME,
                                     description='ESA CCI Toolbox WebAPI tool, version %s' % __version__)
    parser.add_argument('--version', '-V', action='version', version='%s %s' % (CLI_NAME, __version__))
    parser.add_argument('--port', '-p', dest='port', metavar='PORT', type=int,
                        help='run WebAPI service on port number PORT')
    parser.add_argument('--address', '-a', dest='address', metavar='ADDRESS',
                        help='run WebAPI service using address ADDRESS', default='')
    parser.add_argument('--caller', '-c', dest='caller', default=CLI_NAME,
                        help='name of the calling application')
    parser.add_argument('--file', '-f', dest='file', metavar='FILE',
                        help="if given, service information will be written to (start) or read from (stop) FILE")
    parser.add_argument('command', choices=['start', 'stop'],
                        help='start or stop the service')
    args_obj = parser.parse_args(args)

    kwargs = dict(port=args_obj.port,
                  address=args_obj.address,
                  caller=args_obj.caller,
                  service_info_file=args_obj.file)

    if args_obj.command == 'start':
        start_service(**kwargs)
    else:
        stop_service(**kwargs)


def start_service_subprocess(port: int = None,
                             address: str = None,
                             caller: str = None,
                             service_info_file: str = None,
                             timeout: float = 10.0) -> int:
    port = port or find_free_port()
    command = _get_command_base(port, address, caller, service_info_file) + ' start'
    webapi = subprocess.Popen(command, shell=True)
    webapi_url = 'http://%s:%s/' % (address or DEFAULT_ADDRESS, port)
    t0 = time.clock()
    while True:
        return_code = webapi.poll()
        if return_code is not None:
            # Process terminated, we can return now, as there will be no running service
            if return_code:
                return return_code
            return -9998
        # noinspection PyBroadException
        try:
            urllib.request.urlopen(webapi_url, timeout=2)
            return 0
        except Exception:
            pass
        time.sleep(0.1)
        t1 = time.clock()
        if t1 - t0 > timeout:
            return -9999


def stop_service_subprocess(port: int = None,
                            address: str = None,
                            caller: str = None,
                            service_info_file: str = None,
                            timeout: float = 10.0) -> int:
    command = _get_command_base(port, address, caller, service_info_file) + ' stop'
    return subprocess.call(command, shell=True, timeout=timeout)


def _get_command_base(port, address, caller, service_info_file):
    command = '"%s" -m ect.ui.webapi' % sys.executable
    if port:
        command += ' -p %d' % port
    if address:
        command += ' -a "%s"' % address
    if caller:
        command += ' -c "%s"' % caller
    if service_info_file:
        command += ' -f "%s"' % service_info_file
    return command


def start_service(port: int = None, address: str = None, caller: str = None, service_info_file: str = None) -> dict:
    """
    Start a WebAPI service.

    :param port: the port number
    :param address: the address
    :param caller: the name of the calling application (informal)
    :param service_info_file: If not ``None``, a service information JSON file will be written to *service_info_file*.
    :return: service information dictionary
    """
    if service_info_file and os.path.exists(service_info_file):
        raise ValueError('service info file exists: %s' % service_info_file)
    enable_pretty_logging()
    application = get_application()
    application.service_info_file = service_info_file
    port = port or find_free_port()
    address_and_port = '%s:%s' % (address or DEFAULT_ADDRESS, port)
    print('starting ECT WebAPI on %s' % address_and_port)
    application.listen(port, address=address or '')
    io_loop = IOLoop()
    io_loop.make_current()
    service_info = dict(port=port,
                        address=address,
                        caller=caller,
                        started=datetime.now().isoformat(sep=' '))
    if service_info_file:
        _write_service_info(service_info, service_info_file)
    IOLoop.instance().start()
    return service_info


def stop_service(port=None, address=None, caller: str = None, service_info_file: str = None) -> dict:
    """
    Stop a WebAPI service.

    :param port:
    :param address:
    :param caller:
    :param service_info_file:
    :return: service information dictionary
    """
    service_info = {}
    if service_info_file:
        service_info = read_service_info(service_info_file)
        service_info = service_info or {}

    port = port or service_info.get('port', None)
    address = address or service_info.get('address', None)
    caller = caller or service_info.get('caller', None)

    if not port:
        raise ValueError('cannot stop WebAPI service for unknown port number (caller: %s)' % caller)

    address_and_port = '%s:%s' % (address or DEFAULT_ADDRESS, port)
    print('stopping ECT WebAPI on %s' % address_and_port)
    urllib.request.urlopen('http://%s/exit' % address_and_port)

    return dict(port=port, address=address, caller=caller, started=service_info.get('started', None))


def find_free_port():
    s = socket.socket()
    # Bind to a free port provided by the host.
    s.bind(('', 0))
    free_port = s.getsockname()[1]
    s.close()
    # Return the port number assigned.
    return free_port


def read_service_info(service_info_file: str) -> dict:
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


def _write_service_info(service_info: dict, service_info_file: str) -> None:
    if not service_info:
        raise ValueError('service_info argument must be given')
    if not service_info_file:
        raise ValueError('service_info_file argument must be given')
    os.makedirs(os.path.dirname(service_info_file), exist_ok=True)
    with open(service_info_file, 'w') as fp:
        json.dump(service_info, fp, indent='  ')


def url_pattern(pattern: str):
    """
    Convert a string *pattern* where any occurrences of ``{{NAME}}`` are replaced by an equivalent
    regex expression which will assign matching character groups to NAME. Characters match until
    one of the RFC 2396 reserved characters is found or the end of the *pattern* is reached.

    RFC 2396 Uniform Resource Identifiers (URI): Generic Syntax lists
    the following reserved characters::

        reserved    = ";" | "/" | "?" | ":" | "@" | "&" | "=" | "+" | "$" | ","

    :param pattern: URL pattern
    :return: equivalent regex pattern
    :raise ValueError: if *pattern* is invalid
    """
    name_pattern = '(?P<%s>[^\;\/\?\:\@\&\=\+\$\,]+)'
    reg_expr = ''
    pos = 0
    while True:
        pos1 = pattern.find('{{', pos)
        if pos1 >= 0:
            pos2 = pattern.find('}}', pos1 + 2)
            if pos2 > pos1:
                name = pattern[pos1 + 2:pos2]
                if not name.isidentifier():
                    raise ValueError('name in {{name}} must be a valid identifier, but got "%s"' % name)
                reg_expr += pattern[pos:pos1] + (name_pattern % name)
                pos = pos2 + 2
            else:
                raise ValueError('no matching "}}" after "{{" in "%s"' % pattern)

        else:
            reg_expr += pattern[pos:]
            break
    return reg_expr


def _status_ok(content: object = None):
    return dict(status='ok', content=content)


def _status_error(exception: Exception = None, type_name: str = None, message: str = None):
    type_name = type_name or (type(exception).__name__ if exception else 'unknown')
    message = message or (str(exception) if exception else None)
    return dict(status='error', error=dict(type=type_name, message=message))


# noinspection PyAbstractClass
class WorkspaceGetHandler(RequestHandler):
    def get(self, base_dir):
        workspace_manager = self.application.workspace_manager
        try:
            workspace = workspace_manager.get_workspace(base_dir)
            self.write(_status_ok(content=workspace.to_json_dict()))
        except Exception as e:
            self.write(_status_error(exception=e))


# noinspection PyAbstractClass
class WorkspaceInitHandler(RequestHandler):
    def get(self):
        base_dir = self.get_query_argument('base_dir')
        description = self.get_query_argument('description', default=None)
        workspace_manager = self.application.workspace_manager
        try:
            workspace = workspace_manager.init_workspace(base_dir, description=description)
            self.write(_status_ok(workspace.to_json_dict()))
        except Exception as e:
            self.write(_status_error(exception=e))


# noinspection PyAbstractClass
class WorkspaceDeleteHandler(RequestHandler):
    def get(self, base_dir):
        workspace_manager = self.application.workspace_manager
        try:
            workspace_manager.delete_workspace(base_dir)
            self.write(_status_ok())
        except Exception as e:
            self.write(_status_error(exception=e))


# noinspection PyAbstractClass
class ResourceSetHandler(RequestHandler):
    def post(self, base_dir, res_name):
        op_name = self.get_body_argument('op_name')
        op_args = self.get_body_argument('op_args', default=None)
        op_args = json.loads(op_args) if op_args else None
        workspace_manager = self.application.workspace_manager
        try:
            workspace_manager.set_workspace_resource(base_dir, res_name, op_name, op_args=op_args)
            self.write(_status_ok())
        except Exception as e:
            self.write(_status_error(exception=e))


# noinspection PyAbstractClass
class ResourceWriteHandler(RequestHandler):
    def post(self, base_dir, res_name):
        file_path = self.get_body_argument('file_path')
        format_name = self.get_body_argument('format_name', default=None)
        workspace_manager = self.application.workspace_manager
        try:
            workspace_manager.write_workspace_resource(base_dir, res_name, file_path, format_name=format_name)
            self.write(_status_ok())
        except Exception as e:
            self.write(_status_error(exception=e))


# noinspection PyAbstractClass
class VersionHandler(RequestHandler):
    def get(self):
        self.write(_status_ok(content={'name': CLI_NAME,
                                       'version': __version__,
                                       'timestamp': date.today().isoformat()}))


# noinspection PyAbstractClass
class ExitHandler(RequestHandler):
    def get(self):
        self.write(_status_ok(content='Bye!'))
        IOLoop.instance().stop()
        # IOLoop.instance().add_callback(IOLoop.instance().stop)

        service_info_file = self.application.service_info_file
        if service_info_file and os.path.exists(service_info_file):
            os.remove(service_info_file)


if __name__ == "__main__":
    main()
