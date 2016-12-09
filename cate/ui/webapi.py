# The MIT License (MIT)
# Copyright (c) 2016 by the Cate Development Team and contributors
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
import signal
import socket
import subprocess
import sys
import threading
import time
import traceback
import urllib.request
from datetime import date, datetime
from typing import Optional, Union

from tornado.ioloop import IOLoop
from tornado.log import enable_pretty_logging
from tornado.web import RequestHandler, Application

from cate.core.monitor import Monitor, ConsoleMonitor
from cate.core.util import cwd
from cate.version import __version__
from cate.ui.conf import WEBAPI_ON_INACTIVITY_AUTO_EXIT_AFTER, WEBAPI_ON_ALL_CLOSED_AUTO_EXIT_AFTER, WEBAPI_LOG_FILE
from cate.ui.websock import AppWebSocketHandler
from cate.ui.wsmanag import FSWorkspaceManager

# Explicitly load Cate-internal plugins.
__import__('cate.ds')
__import__('cate.ops')

CLI_NAME = 'cate-webapi'

LOCALHOST = '127.0.0.1'


class WebAPIServiceError(Exception):
    def __init__(self, cause, *args, **kwargs):
        if isinstance(cause, Exception):
            super(WebAPIServiceError, self).__init__(str(cause), *args, **kwargs)
            _, _, tb = sys.exc_info()
            self.with_traceback(tb)
        elif isinstance(cause, str):
            super(WebAPIServiceError, self).__init__(cause, *args, **kwargs)
        else:
            super(WebAPIServiceError, self).__init__(*args, **kwargs)
        self._cause = cause

    @property
    def cause(self):
        return self._cause


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
        (url_pattern('/app'), AppWebSocketHandler),
        (url_pattern('/ws/new'), WorkspaceNewHandler),
        (url_pattern('/ws/get_open'), WorkspaceGetOpenHandler),
        (url_pattern('/ws/get/{{base_dir}}'), WorkspaceGetHandler),
        (url_pattern('/ws/open/{{base_dir}}'), WorkspaceOpenHandler),
        (url_pattern('/ws/close/{{base_dir}}'), WorkspaceCloseHandler),
        (url_pattern('/ws/close_all'), WorkspaceCloseAllHandler),
        (url_pattern('/ws/save/{{base_dir}}'), WorkspaceSaveHandler),
        (url_pattern('/ws/save_all'), WorkspaceSaveAllHandler),
        (url_pattern('/ws/del/{{base_dir}}'), WorkspaceDeleteHandler),
        (url_pattern('/ws/clean/{{base_dir}}'), WorkspaceCleanHandler),
        (url_pattern('/ws/run_op/{{base_dir}}'), WorkspaceRunOpHandler),
        (url_pattern('/ws/res/set/{{base_dir}}/{{res_name}}'), ResourceSetHandler),
        (url_pattern('/ws/res/del/{{base_dir}}/{{res_name}}'), ResourceDeleteHandler),
        (url_pattern('/ws/res/write/{{base_dir}}/{{res_name}}'), ResourceWriteHandler),
        (url_pattern('/ws/res/plot/{{base_dir}}/{{res_name}}'), ResourcePlotHandler),
        (url_pattern('/ws/res/print/{{base_dir}}'), ResourcePrintHandler),
        (url_pattern('/exit'), ExitHandler)
    ])
    application.workspace_manager = FSWorkspaceManager()
    application.auto_exit_enabled = False
    application.auto_exit_timer = None
    application.service_info_file = None
    application.time_of_last_activity = time.clock()
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
        stop_service(kill_after=5.0, timeout=5.0, **kwargs)


def start_service_subprocess(port: int = None,
                             address: str = None,
                             caller: str = None,
                             service_info_file: str = None,
                             timeout: float = 10.0) -> None:
    port = port or find_free_port()
    command = _join_command('start', port, address, caller, service_info_file)
    webapi = subprocess.Popen(command, shell=True)
    webapi_url = 'http://%s:%s/' % (address or LOCALHOST, port)
    t0 = time.clock()
    while True:
        exit_code = webapi.poll()
        if exit_code is not None:
            # Process terminated, we can return now, as there will be no running service
            raise WebAPIServiceError('Cate WebAPI service terminated with exit code %d' % exit_code)
        # noinspection PyBroadException
        try:
            urllib.request.urlopen(webapi_url, timeout=2)
            # Success!
            return
        except Exception:
            pass
        time.sleep(0.1)
        t1 = time.clock()
        if t1 - t0 > timeout:
            raise TimeoutError('Cate WebAPI service timeout, exceeded %d sec' % timeout)


def stop_service_subprocess(port: int = None,
                            address: str = None,
                            caller: str = None,
                            service_info_file: str = None,
                            timeout: float = 10.0) -> None:
    command = _join_command('stop', port, address, caller, service_info_file)
    exit_code = subprocess.call(command, shell=True, timeout=timeout)
    if exit_code != 0:
        raise WebAPIServiceError('Cate WebAPI service terminated with exit code %d' % exit_code)


def _join_command(sub_command, port, address, caller, service_info_file):
    command = '"%s" -m cate.ui.webapi' % sys.executable
    if port:
        command += ' -p %d' % port
    if address:
        command += ' -a "%s"' % address
    if caller:
        command += ' -c "%s"' % caller
    if service_info_file:
        command += ' -f "%s"' % service_info_file
    return command + ' ' + sub_command


def start_service(port: int = None, address: str = None, caller: str = None, service_info_file: str = None) -> dict:
    """
    Start a WebAPI service.

    The *service_info_file*, if given, represents the service in the filesystem, similar to
    the ``/var/run/`` directory on Linux systems.

    If the service file exist and its information is compatible with the requested *port*, *address*, *caller*, then
    this function simply returns without taking any other actions.

    :param port: the port number
    :param address: the address
    :param caller: the name of the calling application (informal)
    :param service_info_file: If not ``None``, a service information JSON file will be written to *service_info_file*.
    :return: service information dictionary
    """
    if service_info_file and os.path.isfile(service_info_file):
        service_info = read_service_info(service_info_file)
        if is_service_compatible(port, address, caller, service_info):
            port = service_info.get('port')
            address = service_info.get('address') or LOCALHOST
            if is_service_running(port, address):
                print('Cate WebAPI service already running on %s:%s, reusing it' % (address, port))
                return service_info
            else:
                # Try shutting down the service, even violently
                stop_service(service_info_file, kill_after=5.0, timeout=5.0)
        else:
            print('warning: Cate WebAPI service info file exists: %s, removing it' % service_info_file)
            os.remove(service_info_file)
    import tornado.options
    options = tornado.options.options
    # Check, we should better use a log file per caller, e.g. "~/.cate/webapi-%s.log" % caller
    options.log_file_prefix = WEBAPI_LOG_FILE
    options.log_to_stderr = None
    enable_pretty_logging()
    application = get_application()
    application.service_info_file = service_info_file
    application.auto_exit_enabled = caller == 'cate'
    port = port or find_free_port()
    print('starting Cate WebAPI on %s:%s' % (address or LOCALHOST, port))
    application.listen(port, address=address or '')
    io_loop = IOLoop()
    io_loop.make_current()
    service_info = dict(port=port,
                        address=address,
                        caller=caller,
                        started=datetime.now().isoformat(sep=' '),
                        process_id=os.getpid())
    if service_info_file:
        _write_service_info(service_info, service_info_file)
    # IOLoop.call_later(delay, callback, *args, **kwargs)
    if application.auto_exit_enabled:
        _install_next_inactivity_check(application)
    IOLoop.instance().start()
    return service_info


def stop_service(port=None,
                 address=None,
                 caller: str = None,
                 service_info_file: str = None,
                 kill_after: float = None,
                 timeout: float = 10.0) -> dict:
    """
    Stop a WebAPI service.

    :param port:
    :param address:
    :param caller:
    :param service_info_file:
    :param kill_after: if not ``None``, the number of seconds to wait after a hanging service process will be killed
    :param timeout:
    :return: service information dictionary
    """
    service_info = {}
    if service_info_file:
        service_info = read_service_info(service_info_file)
        if service_info is None and port is None:
            raise RuntimeWarning('Cate WebAPI service not running')
        service_info = service_info or {}

    port = port or service_info.get('port')
    address = address or service_info.get('address')
    caller = caller or service_info.get('caller')
    pid = service_info.get('process_id')

    if not port:
        raise WebAPIServiceError('cannot stop Cate WebAPI service on unknown port (caller: %s)' % caller)

    address_and_port = '%s:%s' % (address or LOCALHOST, port)
    print('stopping Cate WebAPI on %s' % address_and_port)

    # noinspection PyBroadException
    try:
        with urllib.request.urlopen('http://%s/exit' % address_and_port, timeout=timeout * 0.3) as response:
            response.read()
    except:
        # Either process does not exist, or timeout, or some other error
        pass

    # give the service a bit time to shut down before testing
    time.sleep(kill_after * 0.5)

    # Note: is_service_running() should be replaced by is_process_active(pid)
    if kill_after and pid and is_service_running(port, address, timeout=timeout * 0.3):
        # If we have a PID and the service runs
        time.sleep(kill_after * 0.5)
        # Note: is_service_running() should be replaced by is_process_active(pid)
        if is_service_running(port, address, timeout=timeout * 0.3):
            # noinspection PyBroadException
            try:
                os.kill(pid, signal.SIGTERM)
            except:
                pass
            if os.path.isfile(service_info_file):
                os.remove(service_info_file)

    return dict(port=port, address=address, caller=caller, started=service_info.get('started', None))


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


def _write_service_info(service_info: dict, service_info_file: str) -> None:
    if not service_info:
        raise ValueError('service_info argument must be given')
    if not service_info_file:
        raise ValueError('service_info_file argument must be given')
    dir_path = os.path.dirname(service_info_file)
    if dir_path:
        os.makedirs(dir_path, exist_ok=True)
    with open(service_info_file, 'w') as fp:
        json.dump(service_info, fp, indent='  ')


def _exit(application: Application):
    # noinspection PyUnresolvedReferences
    service_info_file = application.service_info_file
    if service_info_file and os.path.isfile(service_info_file):
        # noinspection PyBroadException
        try:
            os.remove(service_info_file)
        except:
            pass
    IOLoop.instance().stop()


def _install_next_inactivity_check(application):
    IOLoop.instance().call_later(WEBAPI_ON_INACTIVITY_AUTO_EXIT_AFTER, _check_inactivity, application)


def _check_inactivity(application: Application):
    # noinspection PyUnresolvedReferences
    time_of_last_activity = application.time_of_last_activity
    inactivity_time = time.clock() - time_of_last_activity
    if inactivity_time > WEBAPI_ON_INACTIVITY_AUTO_EXIT_AFTER:
        print('stopping WebAPI service after %.1f seconds of inactivity' % inactivity_time)
        _auto_exit(application)
    else:
        _install_next_inactivity_check(application)


def _auto_exit(application: Application):
    IOLoop.instance().add_callback(_exit, application)


def _on_workspace_closed(application: Application):
    # noinspection PyUnresolvedReferences
    if not application.auto_exit_enabled:
        return
    # noinspection PyUnresolvedReferences
    workspace_manager = application.workspace_manager
    num_open_workspaces = workspace_manager.num_open_workspaces()
    _check_auto_exit(application, num_open_workspaces == 0, WEBAPI_ON_ALL_CLOSED_AUTO_EXIT_AFTER)


def _check_auto_exit(application: Application, condition: bool, interval: float):
    if application.auto_exit_timer is not None:
        # noinspection PyBroadException
        try:
            application.auto_exit_timer.cancel()
        except:
            pass
    if condition:
        application.auto_exit_timer = threading.Timer(interval, _auto_exit, [application])
        application.auto_exit_timer.start()
    else:
        application.auto_exit_timer = None


def _new_monitor() -> Monitor:
    return ConsoleMonitor(stay_in_line=True, progress_bar_size=30)


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
    trace_back = None
    if exception is not None:
        trace_back = traceback.format_exc()
        type_name = type_name or type(exception).__name__
        message = message or str(exception)
    error_details = {}
    if trace_back is not None:
        error_details['traceback'] = trace_back
    if type_name:
        error_details['type'] = type_name
    if message:
        error_details['message'] = message
    response = dict(status='error', error=dict(type=type_name, message=message))
    if exception is not None:
        response['traceback'] = traceback.format_exc()
    return dict(status='error', error=error_details) if error_details else dict(status='error')


# noinspection PyAbstractClass
class BaseRequestHandler(RequestHandler):
    def on_finish(self):
        self.application.time_of_last_activity = time.clock()


# noinspection PyAbstractClass
class WorkspaceGetHandler(BaseRequestHandler):
    def get(self, base_dir):
        workspace_manager = self.application.workspace_manager
        do_open = self.get_query_argument('do_open', default='False').lower() == 'true'
        try:
            workspace = workspace_manager.get_workspace(base_dir, do_open=do_open)
            self.write(_status_ok(content=workspace.to_json_dict()))
        except Exception as e:
            self.write(_status_error(exception=e))


# noinspection PyAbstractClass
class WorkspaceGetOpenHandler(BaseRequestHandler):
    def get(self):
        workspace_manager = self.application.workspace_manager
        try:
            workspace_list = workspace_manager.get_open_workspaces()
            self.write(_status_ok(content=[workspace.to_json_dict() for workspace in workspace_list]))
        except Exception as e:
            self.write(_status_error(exception=e))


# noinspection PyAbstractClass
class WorkspaceNewHandler(BaseRequestHandler):
    def get(self):
        base_dir = self.get_query_argument('base_dir')
        do_save = self.get_query_argument('do_save', default='False').lower() == 'true'
        description = self.get_query_argument('description', default='')
        workspace_manager = self.application.workspace_manager
        try:
            workspace = workspace_manager.new_workspace(base_dir, do_save=do_save, description=description)
            self.write(_status_ok(workspace.to_json_dict()))
        except Exception as e:
            self.write(_status_error(exception=e))


# noinspection PyAbstractClass
class WorkspaceOpenHandler(BaseRequestHandler):
    def get(self, base_dir):
        workspace_manager = self.application.workspace_manager
        try:
            workspace = workspace_manager.open_workspace(base_dir)
            self.write(_status_ok(workspace.to_json_dict()))
        except Exception as e:
            self.write(_status_error(exception=e))


# noinspection PyAbstractClass
class WorkspaceCloseHandler(BaseRequestHandler):
    def get(self, base_dir):
        do_save = self.get_query_argument('do_save', default='False').lower() == 'true'
        workspace_manager = self.application.workspace_manager
        try:
            workspace_manager.close_workspace(base_dir, do_save)
            _on_workspace_closed(self.application)
            self.write(_status_ok())
        except Exception as e:
            self.write(_status_error(exception=e))


# noinspection PyAbstractClass
class WorkspaceCloseAllHandler(BaseRequestHandler):
    def get(self):
        do_save = self.get_query_argument('do_save', default='False').lower() == 'true'
        workspace_manager = self.application.workspace_manager
        try:
            workspace_manager.close_all_workspaces(do_save)
            _on_workspace_closed(self.application)
            self.write(_status_ok())
        except Exception as e:
            self.write(_status_error(exception=e))


# noinspection PyAbstractClass
class WorkspaceSaveHandler(BaseRequestHandler):
    def get(self, base_dir):
        workspace_manager = self.application.workspace_manager
        try:
            workspace_manager.save_workspace(base_dir)
            self.write(_status_ok())
        except Exception as e:
            self.write(_status_error(exception=e))


# noinspection PyAbstractClass
class WorkspaceSaveAllHandler(BaseRequestHandler):
    def get(self):
        workspace_manager = self.application.workspace_manager
        try:
            workspace_manager.save_all_workspaces()
            self.write(_status_ok())
        except Exception as e:
            self.write(_status_error(exception=e))


# noinspection PyAbstractClass
class WorkspaceDeleteHandler(BaseRequestHandler):
    def get(self, base_dir):
        workspace_manager = self.application.workspace_manager
        try:
            workspace_manager.delete_workspace(base_dir)
            self.write(_status_ok())
        except Exception as e:
            self.write(_status_error(exception=e))


# noinspection PyAbstractClass
class WorkspaceCleanHandler(BaseRequestHandler):
    def get(self, base_dir):
        workspace_manager = self.application.workspace_manager
        try:
            workspace_manager.clean_workspace(base_dir)
            self.write(_status_ok())
        except Exception as e:
            self.write(_status_error(exception=e))


# noinspection PyAbstractClass
class WorkspaceRunOpHandler(BaseRequestHandler):
    def post(self, base_dir):
        op_name = self.get_body_argument('op_name')
        op_args = self.get_body_argument('op_args', default=None)
        op_args = json.loads(op_args) if op_args else None
        workspace_manager = self.application.workspace_manager
        try:
            with cwd(base_dir):
                workspace_manager.run_op_in_workspace(base_dir, op_name, op_args=op_args,
                                                      monitor=_new_monitor())
            self.write(_status_ok())
        except Exception as e:
            self.write(_status_error(exception=e))


# noinspection PyAbstractClass
class ResourceDeleteHandler(BaseRequestHandler):
    def get(self, base_dir, res_name):
        workspace_manager = self.application.workspace_manager
        try:
            with cwd(base_dir):
                workspace_manager.delete_workspace_resource(base_dir, res_name)
            self.write(_status_ok())
        except Exception as e:
            self.write(_status_error(exception=e))


# noinspection PyAbstractClass
class ResourceSetHandler(BaseRequestHandler):
    def post(self, base_dir, res_name):
        op_name = self.get_body_argument('op_name')
        op_args = self.get_body_argument('op_args', default=None)
        op_args = json.loads(op_args) if op_args else None
        workspace_manager = self.application.workspace_manager
        try:
            with cwd(base_dir):
                workspace_manager.set_workspace_resource(base_dir, res_name, op_name, op_args=op_args,
                                                         monitor=_new_monitor())
            self.write(_status_ok())
        except Exception as e:
            self.write(_status_error(exception=e))


# noinspection PyAbstractClass
class ResourceWriteHandler(BaseRequestHandler):
    def get(self, base_dir, res_name):
        file_path = self.get_query_argument('file_path')
        format_name = self.get_query_argument('format_name', default=None)
        workspace_manager = self.application.workspace_manager
        try:
            with cwd(base_dir):
                workspace_manager.write_workspace_resource(base_dir, res_name, file_path, format_name=format_name)
            self.write(_status_ok())
        except Exception as e:
            self.write(_status_error(exception=e))


# noinspection PyAbstractClass
class ResourcePlotHandler(BaseRequestHandler):
    def get(self, base_dir, res_name):
        var_name = self.get_query_argument('var_name', default=None)
        file_path = self.get_query_argument('file_path', default=None)
        workspace_manager = self.application.workspace_manager
        try:
            with cwd(base_dir):
                workspace_manager.plot_workspace_resource(base_dir, res_name, var_name=var_name, file_path=file_path)
            self.write(_status_ok())
        except Exception as e:
            self.write(_status_error(exception=e))


# noinspection PyAbstractClass
class ResourcePrintHandler(BaseRequestHandler):
    def get(self, base_dir):
        res_name_or_expr = self.get_query_argument('res_name_or_expr', default=None)
        workspace_manager = self.application.workspace_manager
        try:
            with cwd(base_dir):
                workspace_manager.print_workspace_resource(base_dir, res_name_or_expr)
            self.write(_status_ok())
        except Exception as e:
            self.write(_status_error(exception=e))


# noinspection PyAbstractClass
class VersionHandler(BaseRequestHandler):
    def get(self):
        self.write(_status_ok(content={'name': CLI_NAME,
                                       'version': __version__,
                                       'timestamp': date.today().isoformat()}))


# noinspection PyAbstractClass
class ExitHandler(RequestHandler):
    def get(self):
        self.write(_status_ok(content='Bye!'))
        IOLoop.instance().add_callback(_exit, self.application)


if __name__ == "__main__":
    main()
