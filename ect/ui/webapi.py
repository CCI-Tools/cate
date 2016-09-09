import argparse
import sys
from datetime import date

from ect.version import __version__
from tornado.ioloop import IOLoop
from tornado.web import RequestHandler, Application

CLI_NAME = 'ect-webapi'

DEFAULT_ADDRESS = 'localhost'
DEFAULT_PORT = 8888


class GetHandler(RequestHandler):
    def get(self, id2=None, id1=None):
        response = {'id1': int(id1),
                    'id2': int(id2),
                    'timestamp': date.today().isoformat()}
        print('arguments =', self.request.arguments)
        print('query_arguments =', self.request.query_arguments)
        self.write(response)


class VersionHandler(RequestHandler):
    def get(self):
        response = {'name': CLI_NAME,
                    'version': __version__,
                    'timestamp': date.today().isoformat()}
        self.write(response)


class ExitHandler(RequestHandler):
    def get(self):
        self.write('Bye!')
        IOLoop.instance().stop()


def url_pattern(pattern: str):
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
                reg_expr += pattern[pos:pos1] + ('(?P<%s>[^\/\?\=\&]+)' % name)
                pos = pos2 + 2
            else:
                raise ValueError('no matching "}}" after "{{" in "%s"' % pattern)

        else:
            reg_expr += pattern[pos:]
            break
    return reg_expr


def start_service(port=None, address=None):
    application = Application([
        (url_pattern('/get/{{id1}}/{{id2}}'), GetHandler),
        (url_pattern('/version'), VersionHandler),
        (url_pattern('/exit'), ExitHandler)
    ])

    port = port or DEFAULT_PORT
    print('starting ECT WebAPI on %s:%s' % (address or DEFAULT_ADDRESS, port))
    application.listen(port, address=address or '')
    IOLoop.instance().start()


def stop_service(port=None, address=None):
    import urllib.request
    port = port or DEFAULT_PORT
    address = address or DEFAULT_ADDRESS
    print('stopping ECT WebAPI on %s:%s' % (address, port))
    urllib.request.urlopen('http://%s:%s/exit' % (address, port))


def main(args=None):
    if args is None:
        args = sys.argv[1:]

    parser = argparse.ArgumentParser(prog=CLI_NAME,
                                     description='ESA CCI Toolbox Web API, version %s' % __version__)
    parser.add_argument('command', choices=['start', 'stop'], help='start or stop the service')
    parser.add_argument('--version', action='version', version='%s %s' % (CLI_NAME, __version__))
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
