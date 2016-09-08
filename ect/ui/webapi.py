import argparse
import sys
from datetime import date

from tornado.ioloop import IOLoop
from tornado.web import RequestHandler, Application

from ect.version import __version__

CLI_NAME = 'ect-webapi'
DEFAULT_PORT = 8888


class GetHandler(RequestHandler):
    def get(self, id2=None, id1=None, **kwargs):
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
        print('stopping service')
        self.write('Bye!')
        IOLoop.instance().stop()


application = Application([
    ("/get/(?P<id1>[0-9]+)/(?P<id2>[0-9]+)", GetHandler),
    ("/version", VersionHandler),
    ("/exit", ExitHandler)
])


def main(args=None):
    if args is None:
        args = sys.argv[1:]

    parser = argparse.ArgumentParser(prog=CLI_NAME,
                                     description='ESA CCI Toolbox Web API, version %s' % __version__)
    parser.add_argument('-v', action='version', version='%s %s' % (CLI_NAME, __version__))
    parser.add_argument('-s', dest='stop', action='store_true', help='stop the service and exit')
    parser.add_argument('-p', dest='port', metavar='PORT', help='run service on port number PORT', default=DEFAULT_PORT)
    parser.add_argument('-a', dest='address', metavar='ADDRESS', help='run service using address ADDRESS', default='')
    args_obj = parser.parse_args(args)

    if not args_obj.stop:
        print('listening on port %s' % args_obj.port)
        application.listen(args_obj.port, address=args_obj.address)
        print('starting service')
        IOLoop.instance().start()
    else:
        import urllib.request
        urllib.request.urlopen('http://%s:%s/exit' % (args_obj.address or 'localhost', args_obj.port))


if __name__ == "__main__":
    main()
