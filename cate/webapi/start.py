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

"""
Description
===========

This module provides Cate's WebAPI Start executable.

To use the WebAPI executable, invoke the module file as a script,
type ``python3 cate/webapi/start.py [ARGS] [OPTIONS]``.
Type `python3 cate/webapi/start.py --help`` for usage help.

Verification
============

The module's unit-tests are located in
`test/webapi <https://github.com/CCI-Tools/cate/blob/master/test/webapi>`_
and may be executed using ``$ py.test test/webapi --cov=cate/webapi``
for extra code coverage information.

Components
==========
"""
import os
import warnings

warnings.filterwarnings("ignore")  # never print any warnings to users
import sys
from datetime import date

from tornado.web import Application, StaticFileHandler
from matplotlib.backends.backend_webagg_core import FigureManagerWebAgg

from cate.conf.defaults import WEBAPI_LOG_FILE_PREFIX, WEBAPI_PROGRESS_DEFER_PERIOD
from cate.core.pathmanag import PathManager
from cate.core.types import ValidationError
from cate.core.wsmanag import FSWorkspaceManager, RelativeFSWorkspaceManager
from cate.util.web import JsonRpcWebSocketHandler
from cate.util.web.webapi import run_start, url_pattern, WebAPIRequestHandler, WebAPIExitHandler
from cate.version import __version__
from cate.webapi.rest import ResourcePlotHandler, CountriesGeoJSONHandler, ResVarTileHandler, \
    ResFeatureCollectionHandler, ResFeatureHandler, ResVarCsvHandler, ResVarHtmlHandler, NE2Handler
from cate.webapi.mpl import MplJavaScriptHandler, MplDownloadHandler, MplWebSocketHandler
from cate.webapi.websocket import WebSocketService
from cate.webapi.service import SERVICE_NAME, SERVICE_TITLE

# Explicitly load Cate-internal plugins.
__import__('cate.ds')
__import__('cate.ops')

__author__ = "Norman Fomferra (Brockmann Consult GmbH), " \
             "Marco Zühlke (Brockmann Consult GmbH)"


# noinspection PyAbstractClass
class WebAPIInfoHandler(WebAPIRequestHandler):
    def get(self):
        app = self.application
        self.write_status_ok(content={'name': SERVICE_NAME,
                                      'version': __version__,
                                      'timestamp': date.today().isoformat(),
                                      'workspace_manager': app.workspace_manager.__class__.__name__})

        self.finish()


def service_factory(application):
    return WebSocketService(application.workspace_manager)


# All JSON REST responses should have same structure, namely a dictionary as follows:
#
# {
#    "status": "ok" | "error",
#    "error": optional error-details,
#    "content": optional content, if status "ok"
# }

def create_application(user_root_path: str = None):
    application = Application([
        ('/_static/(.*)', StaticFileHandler, {'path': FigureManagerWebAgg.get_static_file_path()}),
        ('/mpl.js', MplJavaScriptHandler),

        (url_pattern('/mpl/download/{{base_dir}}/{{figure_id}}/{{format_name}}'), MplDownloadHandler),
        (url_pattern('/mpl/figures/{{base_dir}}/{{figure_id}}'), MplWebSocketHandler),

        (url_pattern('/'), WebAPIInfoHandler),
        (url_pattern('/exit'), WebAPIExitHandler),
        (url_pattern('/api'), JsonRpcWebSocketHandler, dict(
            service_factory=service_factory,
            validation_exception_class=ValidationError,
            report_defer_period=WEBAPI_PROGRESS_DEFER_PERIOD)
         ),
        (url_pattern('/ws/res/plot/{{base_dir}}/{{res_name}}'), ResourcePlotHandler),
        (url_pattern('/ws/res/geojson/{{base_dir}}/{{res_id}}'), ResFeatureCollectionHandler),
        (url_pattern('/ws/res/geojson/{{base_dir}}/{{res_id}}/{{feature_index}}'), ResFeatureHandler),
        (url_pattern('/ws/res/csv/{{base_dir}}/{{res_id}}'), ResVarCsvHandler),
        (url_pattern('/ws/res/html/{{base_dir}}/{{res_id}}'), ResVarHtmlHandler),
        (url_pattern('/ws/res/tile/{{base_dir}}/{{res_id}}/{{z}}/{{y}}/{{x}}.png'), ResVarTileHandler),
        (url_pattern('/ws/ne2/tile/{{z}}/{{y}}/{{x}}.jpg'), NE2Handler),
        (url_pattern('/ws/countries'), CountriesGeoJSONHandler),
    ])

    default_user_root_path = os.environ.get('CATE_USER_ROOT')
    if user_root_path is None:
        user_root_path = default_user_root_path
    elif default_user_root_path:
        print(f"warning: user root path given by environment variable CATE_USER_ROOT superseded by {user_root_path}")

    if user_root_path is None:
        application.workspace_manager = FSWorkspaceManager()
    else:
        application.workspace_manager = RelativeFSWorkspaceManager(PathManager(user_root_path))

    return application


def main(args=None) -> int:
    return run_start(SERVICE_NAME,
                     'Starts a new {}'.format(SERVICE_TITLE),
                     __version__,
                     application_factory=create_application,
                     log_file_prefix=WEBAPI_LOG_FILE_PREFIX,
                     args=args)


if __name__ == "__main__":
    sys.exit(main())
