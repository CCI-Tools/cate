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

import argparse
import asyncio
import logging
import os.path
import signal
import subprocess
import sys
import threading
import time
import traceback
import urllib.request
from datetime import datetime
from typing import List, Callable, Optional, Tuple

# from tornado.platform.asyncio import AnyThreadEventLoopPolicy
import tornado.options
from tornado.ioloop import IOLoop
from tornado.log import enable_pretty_logging
from tornado.web import RequestHandler, Application

from .common import exception_to_json
from .serviceinfo import read_service_info, write_service_info, \
    find_free_port, is_service_compatible, is_service_running, join_address_and_port

__author__ = "Norman Fomferra (Brockmann Consult GmbH), " \
             "Marco Zühlke (Brockmann Consult GmbH)"

_LOG = logging.getLogger('cate')

ApplicationFactory = Callable[[], Application]


def _get_common_cli_parser(name: str,
                           description: str,
                           version: str) -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument('--version', '-V', action='version', version=version)
    parser.add_argument('--port', '-p', dest='port', metavar='PORT', type=int,
                        help='start/stop WebAPI service on port number PORT')
    parser.add_argument('--address', '-a', dest='address', metavar='ADDRESS',
                        help='start/stop WebAPI service using address ADDRESS', default='localhost')
    parser.add_argument('--caller', '-c', dest='caller', default=name,
                        help='name of the calling application')
    parser.add_argument('--file', '-f', dest='file', metavar='FILE',
                        help="if given, service information will be written to (start) or read from (stop) FILE")
    return parser


def run_start(name: str,
              description: str,
              version: str,
              application_factory: ApplicationFactory,
              log_file_prefix=None,
              args: List[str] = None) -> int:
    """
    Run the WebAPI command-line interface.

    :param name: The service name.
    :param description: The CLI's description.
    :param version: The CLI's version string.
    :param application_factory: A no-arg function that creates a Tornado web application instance.
    :param log_file_prefix: Log file prefix name.
    :param args: The command-line arguments, may be None.
    :return: the exit code, zero on success.
    """
    if args is None:
        args = sys.argv[1:]

    parser = _get_common_cli_parser(name, description, version)
    parser.add_argument('--auto-stop-after', '-s', dest='auto_stop_after', metavar='AUTO_STOP_AFTER', type=float,
                        help="if given, service will stop after AUTO_STOP_AFTER seconds of inactivity")
    parser.add_argument('--verbose', '-v', dest='verbose', action='store_true',
                        help="if given, logging will be delegated to the console (stderr)")
    try:
        args_obj = parser.parse_args(args)

        if not os.path.isdir(os.path.dirname(log_file_prefix)):
            os.makedirs(os.path.dirname(log_file_prefix), exist_ok=True)

        service = WebAPI()
        service.start(name, application_factory,
                      log_file_prefix=log_file_prefix,
                      log_to_stderr=args_obj.verbose,
                      port=args_obj.port,
                      address=args_obj.address,
                      caller=args_obj.caller,
                      service_info_file=args_obj.file,
                      auto_stop_after=args_obj.auto_stop_after)

        return 0
    except Exception as e:
        print('error: %s' % e)
        return 1


def run_stop(name: str,
             description: str,
             version: str,
             args: List[str] = None) -> int:
    """
    Run the WebAPI command-line interface.

    :param name: The service name.
    :param description: The CLI's description.
    :param version: The CLI's version string.
    :param args: The command-line arguments, may be None.
    :return: the exit code, zero on success.
    """
    if args is None:
        args = sys.argv[1:]

    parser = _get_common_cli_parser(name, description, version)
    parser.add_argument('--kill-after', '-k', dest='kill_after', metavar='KILL_AFTER', type=float, default=5.,
                        help="Service will be killed (SIGTERM) after KILL_AFTER seconds of inactivity")
    parser.add_argument('--timeout', '-t', dest='timeout', metavar='TIMEOUT', type=float, default=5.,
                        help="Service will stop after TIME seconds of inactivity")

    try:
        args_obj = parser.parse_args(args)

        WebAPI.stop(name,
                    port=args_obj.port,
                    address=args_obj.address,
                    caller=args_obj.caller,
                    service_info_file=args_obj.file,
                    kill_after=args_obj.kill_after,
                    timeout=args_obj.timeout)

        return 0
    except Exception as e:
        print('error: %s' % e)
        return 1


class WebAPI:
    """
    A web service that provides a remote API to some application.
    """

    def __init__(self):
        self.name = None
        self.application = None
        self.server = None
        self.auto_stop_enabled = None
        self.auto_stop_timer = None
        self.auto_stop_after = None
        self.service_info_file = None
        self.service_info = None

    @classmethod
    def get_webapi(cls, application: Application) -> 'WebAPI':
        """
        Retrieves the associated WebAPI service from the given Tornado web application.

        :param application: The Tornado web application
        :return: The WebAPI instance, or None
        """
        return application.webapi if application and hasattr(application, 'webapi') else None

    def start(self,
              name: str,
              application_factory: ApplicationFactory,
              log_file_prefix: str = None,
              log_to_stderr: bool = False,
              auto_stop_after: float = None,
              port: int = None,
              address: str = None,
              caller: str = None,
              service_info_file: str = None) -> dict:

        """
        Start a WebAPI service.

        The *service_info_file*, if given, represents the service in the filesystem, similar to
        the ``/var/run/`` directory on Linux systems.

        If the service file exist and its information is compatible with the requested *port*, *address*, *caller*, then
        this function simply returns without taking any other actions.

        :param name: The (CLI) name of this service.
        :param application_factory: no-arg function which is used to create
        :param log_file_prefix: Log file prefix, default is "webapi.log"
        :param log_to_stderr: Whether logging should be shown on stderr
        :param auto_stop_after: if not-None, time of idleness in seconds before service is terminated
        :param port: the port number
        :param address: the address
        :param caller: the name of the calling application (informal)
        :param service_info_file: If not ``None``, a service information JSON file will
               be written to *service_info_file*.
        :return: service information dictionary
        """
        if service_info_file and os.path.isfile(service_info_file):
            service_info = read_service_info(service_info_file)
            if is_service_compatible(port, address, caller, service_info):
                port = service_info.get('port')
                address = service_info.get('address') or address
                if is_service_running(port, address):
                    print(f'{name}: detected service running on {join_address_and_port(address, port)}')
                    return service_info
                else:
                    # Try shutting down the service, even violently
                    self.stop(name, service_info_file=service_info_file, kill_after=5.0, timeout=5.0)
            else:
                print(f'{name}: service info file exists:{service_info_file}, removing it')
                os.remove(service_info_file)

        options = tornado.options.options
        # Check, we should better use a log file per caller, e.g. "~/.cate/webapi-%s.log" % caller
        options.log_file_prefix = log_file_prefix or f'{name}.log'
        options.log_to_stderr = log_to_stderr
        enable_pretty_logging()

        port = port or find_free_port()

        self.name = name
        self.auto_stop_enabled = not not auto_stop_after
        self.auto_stop_after = auto_stop_after
        self.auto_stop_timer = None
        self.service_info_file = service_info_file
        self.service_info = dict(port=port,
                                 address=address,
                                 caller=caller,
                                 started=datetime.now().isoformat(sep=' '),
                                 pid=os.getpid())

        application = application_factory()
        application.webapi = self
        application.time_of_last_activity = time.clock()
        self.application = application

        print(f'{name}: started service, listening on {join_address_and_port(address, port)}')

        self.server = application.listen(port, address='' if address == 'localhost' else address)
        # Ensure we have the same event loop in all threads
        asyncio.set_event_loop_policy(_GlobalEventLoopPolicy(asyncio.get_event_loop()))
        # Register handlers for common termination signals
        signal.signal(signal.SIGINT, self._sig_handler)
        signal.signal(signal.SIGTERM, self._sig_handler)
        if service_info_file:
            write_service_info(self.service_info, service_info_file)
        if self.auto_stop_enabled:
            self._install_next_inactivity_check()
        IOLoop.current().start()
        return self.service_info

    @classmethod
    def stop(cls,
             name: str,
             port=None,
             address=None,
             caller: str = None,
             service_info_file: str = None,
             kill_after: float = None,
             timeout: float = 10.0) -> dict:
        """
        Stop a WebAPI service.

        :param name: The name of this service.
        :param port: port number
        :param address: service address
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
                raise RuntimeWarning('%s service not running' % name)
            service_info = service_info or {}

        port = port or service_info.get('port')
        address = address or service_info.get('address')
        caller = caller or service_info.get('caller')
        pid = service_info.get('pid')

        if not port:
            raise WebAPIServiceError('cannot stop %s service on unknown port (caller: %s)' % (name, caller))

        if service_info_file and service_info:
            print(f'{name}: service information file found: {service_info_file}')

        print(f'{name}: trying to stop any service on {join_address_and_port(address, port)}')

        # noinspection PyBroadException
        try:
            with urllib.request.urlopen(f'http://{join_address_and_port(address, port)}/exit',
                                        timeout=timeout * 0.3) as response:
                response.read()
        except Exception:
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
                except Exception:
                    pass
                if os.path.isfile(service_info_file):
                    os.remove(service_info_file)

        return dict(port=port, address=address, caller=caller, started=service_info.get('started', None))

    def check_for_auto_stop(self, condition: bool, interval: float = 100):
        """
        If *condition* is True, the WebAPI service will end after *interval* seconds.

        :param condition: The condition
        :param interval: The time in seconds before the service is shut down.
        :return:
        """
        # noinspection PyUnresolvedReferences
        if not self.auto_stop_enabled:
            return
        if self.auto_stop_timer is not None:
            # noinspection PyBroadException
            try:
                self.auto_stop_timer.cancel()
            except Exception:
                pass
        if condition:
            self.auto_stop_timer = threading.Timer(interval, self.shut_down)
            self.auto_stop_timer.start()
        else:
            self.auto_stop_timer = None

    def shut_down(self):
        """
        Stops the Tornado web server.
        """
        IOLoop.current().add_callback(self._on_shut_down)

    def _on_shut_down(self):
        # noinspection PyUnresolvedReferences
        service_info_file = self.service_info_file
        if service_info_file and os.path.isfile(service_info_file):
            # noinspection PyBroadException
            try:
                os.remove(service_info_file)
            except Exception:
                pass

        if self.server:
            self.server.stop()
            self.server = None

        IOLoop.current().stop()

    # noinspection PyUnusedLocal
    def _sig_handler(self, sig, frame):
        _LOG.warning('Caught signal: %s', sig)
        IOLoop.current().add_callback_from_signal(self._on_shut_down)

    def _install_next_inactivity_check(self):
        IOLoop.current().call_later(self.auto_stop_after, self._check_inactivity)

    def _check_inactivity(self):
        # noinspection PyUnresolvedReferences
        time_of_last_activity = self.application.time_of_last_activity
        inactivity_time = time.clock() - time_of_last_activity
        if inactivity_time > self.auto_stop_after:
            _LOG.info('stopping %s service after %.1f seconds of inactivity' % (self.name, inactivity_time))
            self.shut_down()
        else:
            self._install_next_inactivity_check()

    @classmethod
    def start_subprocess(cls,
                         module: str,
                         port: int = None,
                         address: str = None,
                         caller: str = None,
                         service_info_file: str = None,
                         auto_stop_after: float = None,
                         timeout: float = 10.0) -> None:
        """
        Start the Web API service as a sub-process.

        :param module: the name of the Python main module to be executed.
        :param port: the port number, if not given, a new free port will be searched.
        :param address: the service address, if not given, "localhost" will be used.
        :param caller: the caller's program name
        :param service_info_file: optional path to a (JSON) file, where service info will be stored
        :param auto_stop_after: if not-None, time of idleness in seconds before service will automatically stop
        :param timeout: timeout in seconds
        """
        port = port or find_free_port()
        command = cls._join_subprocess_command(module, port, address, caller, service_info_file,
                                               auto_stop_after)
        webapi = subprocess.Popen(command, shell=True)
        webapi_url = f'http://{join_address_and_port(address, port)}/'
        t0 = time.clock()
        while True:
            exit_code = webapi.poll()
            if exit_code is not None:
                # Process terminated, we can return now, as there will be no running service
                raise ValueError('WebAPI service terminated with exit code %d' % exit_code)
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
                raise TimeoutError('WebAPI service timeout, exceeded %d sec' % timeout)

    @classmethod
    def stop_subprocess(cls,
                        module: str,
                        port: int = None,
                        address: str = None,
                        caller: str = None,
                        service_info_file: str = None,
                        timeout: float = 10.0) -> None:
        """
        Stop a Web API service running as a sub-process.

        :param module: the name of the Python main module to be executed.
        :param port: the port number.
        :param address: the service address, if not given, "localhost" will be used.
        :param caller: the caller's program name
        :param service_info_file: optional path to a (JSON) file, where service info will be stored
        :param timeout: timeout in seconds
        """
        command = cls._join_subprocess_command(module, port, address, caller, service_info_file, None)
        exit_code = subprocess.call(command, shell=True, timeout=timeout)
        if exit_code != 0:
            raise ValueError('WebAPI service terminated with exit code %d' % exit_code)

    @classmethod
    def _join_subprocess_command(cls, module, port, address, caller, service_info_file, auto_stop_after):
        command = '"%s" -m %s' % (sys.executable, module)
        if port:
            command += ' -p %d' % port
        if address:
            command += ' -a "%s"' % address
        if caller:
            command += ' -c "%s"' % caller
        if service_info_file:
            command += ' -f "%s"' % service_info_file
        if auto_stop_after:
            command += ' -s %s' % auto_stop_after
        return command


def check_for_auto_stop(application: Application, condition: bool,
                        interval: float = 100):
    webapi = WebAPI.get_webapi(application)
    if webapi:
        webapi.check_for_auto_stop(condition, interval)


# noinspection PyAbstractClass
class WebAPIRequestHandler(RequestHandler):
    """
    Base class for REST API requests.
    All JSON REST responses should have same structure, namely a dictionary as follows:

    {
       "status": "ok" | "error",
       "error": optional error-details,
       "content": optional content, if status "ok"
    }

    See methods write_status_ok() and write_status_error().
    """

    def __init__(self, application, request, **kwargs):
        super(WebAPIRequestHandler, self).__init__(application, request, **kwargs)

    @property
    def webapi(self) -> WebAPI:
        return WebAPI.get_webapi(self.application)

    @classmethod
    def to_int(cls, name: str, value: str) -> int:
        """
        Convert str value to int.
        :param name: Name of the value
        :param value: The string value
        :return: The int value
        :raise: WebAPIRequestError
        """
        if value is None:
            raise WebAPIRequestError('%s must be an integer, but was None' % name)
        try:
            return int(value)
        except ValueError as e:
            raise WebAPIRequestError('%s must be an integer, but was "%s"' % (name, value)) from e

    @classmethod
    def to_int_tuple(cls, name: str, value: str) -> Tuple[int, ...]:
        """
        Convert str value to int.
        :param name: Name of the value
        :param value: The string value
        :return: The int value
        :raise: WebAPIRequestError
        """
        if value is None:
            raise WebAPIRequestError('%s must be a list of integers, but was None' % name)
        try:
            return tuple(map(int, value.split(','))) if value else ()
        except ValueError as e:
            raise WebAPIRequestError('%s must be a list of integers, but was "%s"' % (name, value)) from e

    @classmethod
    def to_float(cls, name: str, value: str) -> float:
        """
        Convert str value to float.
        :param name: Name of the value
        :param value: The string value
        :return: The float value
        :raise: WebAPIRequestError
        """
        if value is None:
            raise WebAPIRequestError('%s must be a number, but was None' % name)
        try:
            return float(value)
        except ValueError as e:
            raise WebAPIRequestError('%s must be a number, but was "%s"' % (name, value)) from e

    def get_query_argument_int(self, name: str, default: int) -> Optional[int]:
        """
        Get query argument of type int.
        :param name: Query argument name
        :param default: Default value.
        :return: int value
        :raise: WebAPIRequestError
        """
        value = self.get_query_argument(name, default=None)
        return self.to_int(name, value) if value is not None else default

    def get_query_argument_int_tuple(self, name: str, default: Tuple[int, ...]) -> Optional[Tuple[int, ...]]:
        """
        Get query argument of type int list.
        :param name: Query argument name
        :param default: Default value.
        :return: int list value
        :raise: WebAPIRequestError
        """
        value = self.get_query_argument(name, default=None)
        return self.to_int_tuple(name, value) if value is not None else default

    def get_query_argument_float(self, name: str, default: float) -> Optional[float]:
        """
        Get query argument of type float.
        :param name: Query argument name
        :param default: Default value.
        :return: float value
        :raise: WebAPIRequestError
        """
        value = self.get_query_argument(name, default=None)
        return self.to_float(name, value) if value is not None else default

    def on_finish(self):
        """
        Store time of last activity so we can measure time of inactivity and then optionally auto-exit.
        """
        self.application.time_of_last_activity = time.clock()

    def write_status_ok(self, content: object = None):
        self.write(dict(status='ok', content=content))

    def write_status_error(self, message: str = None, exc_info=None):
        if message is not None:
            _LOG.error(message)
        if exc_info is not None:
            _LOG.info(''.join(traceback.format_exception(exc_info[0], exc_info[1], exc_info[2])))
        self.write(self._to_status_error(message=message, exc_info=exc_info))

    @classmethod
    def _to_status_error(cls, message: str = None, exc_info=None):
        error_data = None
        if exc_info is not None:
            message = message or str(exc_info[1])
            error_data = exception_to_json(exc_info)
        if message:
            if error_data:
                return dict(status='error', error=dict(message=message, data=error_data))
            else:
                return dict(status='error', error=dict(message=message))
        return dict(status='error')


class _GlobalEventLoopPolicy(asyncio.DefaultEventLoopPolicy):
    """
    Event loop policy that has one fixed global loop for all threads.

    We use it for the following reason: As of Tornado 5 IOLoop.current() no longer has
    a single global instance. It is a thread-local instance, but only on the main thread.
    Other threads have no IOLoop instance by default.

    _GlobalEventLoopPolicy is a fix that allows us to access the same IOLoop
    in all threads.

    Usage::

        asyncio.set_event_loop_policy(_GlobalEventLoopPolicy(asyncio.get_event_loop()))

    """

    def __init__(self, global_loop):
        super().__init__()
        self._global_loop = global_loop

    def get_event_loop(self):
        return self._global_loop


# noinspection PyAbstractClass
class WebAPIExitHandler(WebAPIRequestHandler):
    """
    A request handler that exits the Tornado webs service.
    """

    def get(self):
        self.write_status_ok(content='Bye!')
        IOLoop.current().add_callback(self.webapi.shut_down)


class WebAPIError(Exception):
    """
    WepAPI error base class.
    Exceptions thrown by the Cate WebAPI.
    """

    def __init__(self, message: str):
        super().__init__(message)

    @property
    def cause(self):
        return self.__cause__


class WebAPIServiceError(WebAPIError):
    """
    Exception which may be raised by the WebAPI service class.
    """


class WebAPIRequestError(WebAPIError):
    """
    Exception which may be raised and handled(!) by WebAPI service requests.
    """


def url_pattern(pattern: str):
    """
    Convert a string *pattern* where any occurrences of ``{{NAME}}`` are replaced by an equivalent
    regex expression which will assign matching character groups to NAME. Characters match until
    one of the RFC 2396 reserved characters is found or the end of the *pattern* is reached.

    The function can be used to map URLs patterns to request handlers as desired by the Tornado web server, see
    http://www.tornadoweb.org/en/stable/web.html

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
