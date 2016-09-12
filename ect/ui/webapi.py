import argparse
import json
import sys
import urllib.request
from datetime import date

from ect.ui.workspace import FSWorkspaceManager
from ect.version import __version__
from tornado.ioloop import IOLoop
from tornado.log import enable_pretty_logging
from tornado.web import RequestHandler, Application

CLI_NAME = 'ect-webapi'

DEFAULT_ADDRESS = '127.0.0.1'
DEFAULT_PORT = 8888


# noinspection PyAbstractClass
class WorkspaceInitHandler(RequestHandler):
    def get(self):
        base_dir = self.get_query_argument('base_dir')
        description = self.get_query_argument('description', default=None)
        workspace_manager = self.application.workspace_manager
        try:
            workspace = workspace_manager.init_workspace(base_dir, description=description)
            self.write(workspace.to_json_dict())
        except Exception as e:
            self.write(dict(status='error', error=type(e), message=str(e)))


# noinspection PyAbstractClass
class WorkspaceGetHandler(RequestHandler):
    def get(self, base_dir):
        workspace_manager = self.application.workspace_manager
        try:
            workspace = workspace_manager.get_workspace(base_dir)
            self.write(workspace.to_json_dict())
        except Exception as e:
            self.write(dict(status='error', error=type(e), message=str(e)))


# noinspection PyAbstractClass
class WorkspaceResourceSetHandler(RequestHandler):
    def post(self, base_dir, res_name):
        op_name = self.get_body_argument('op_name')
        op_args = self.get_body_argument('op_args', default=None)
        op_args = json.loads(op_args) if op_args else None
        workspace_manager = self.application.workspace_manager
        try:
            workspace_manager.set_workspace_resource(base_dir, res_name, op_name, op_args=op_args)
            self.write(dict(status='ok'))
        except Exception as e:
            self.write(dict(status='error', error=type(e), message=str(e)))


# noinspection PyAbstractClass
class VersionHandler(RequestHandler):
    def get(self):
        response = {'name': CLI_NAME,
                    'version': __version__,
                    'timestamp': date.today().isoformat()}
        self.write(response)


# noinspection PyAbstractClass
class ExitHandler(RequestHandler):
    def get(self):
        self.write(dict(status='ok', message='Bye!'))
        IOLoop.instance().stop()
        # IOLoop.instance().add_callback(IOLoop.instance().stop)


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


def get_application():
    application = Application([
        (url_pattern('/'), VersionHandler),
        (url_pattern('/ws/init'), WorkspaceInitHandler),
        (url_pattern('/ws/get/{{base_dir}}'), WorkspaceGetHandler),
        (url_pattern('/ws/{{base_dir}}/res/{{res_name}}/set'), WorkspaceResourceSetHandler),
        (url_pattern('/exit'), ExitHandler)
    ])
    application.workspace_manager = FSWorkspaceManager()
    return application


def start_service(port=None, address=None):
    enable_pretty_logging()

    application = get_application()
    port = port or DEFAULT_PORT
    address_and_port = '%s:%s' % (address or DEFAULT_ADDRESS, port)
    print('starting ECT WebAPI on %s' % address_and_port)
    application.listen(port, address=address or '')
    io_loop = IOLoop()
    io_loop.make_current()
    IOLoop.instance().start()


def stop_service(port=None, address=None):
    port = port or DEFAULT_PORT
    address = address or DEFAULT_ADDRESS
    address_and_port = '%s:%s' % (address, port)
    print('stopping ECT WebAPI on %s' % address_and_port)
    urllib.request.urlopen('http://%s/exit' % address_and_port)


def main(args=None):
    if args is None:
        args = sys.argv[1:]

    parser = argparse.ArgumentParser(prog=CLI_NAME,
                                     description='ESA CCI Toolbox Web API, version %s' % __version__)
    parser.add_argument('command', choices=['start', 'stop'], help='start or stop the service')
    parser.add_argument('--version', '-V', action='version', version='%s %s' % (CLI_NAME, __version__))
    parser.add_argument('--port', '-p', dest='port', metavar='PORT',
                        help='run service on port number PORT', default=DEFAULT_PORT)
    parser.add_argument('--address', '-a', dest='address', metavar='ADDRESS',
                        help='run service using address ADDRESS', default='')
    args_obj = parser.parse_args(args)

    if args_obj.command == 'start':
        start_service(port=args_obj.port, address=args_obj.address)
    else:
        stop_service(port=args_obj.port, address=args_obj.address)


if __name__ == "__main__":
    main()
