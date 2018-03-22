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

__author__ = "Norman Fomferra (Brockmann Consult GmbH), " \
             "Marco Zühlke (Brockmann Consult GmbH)"

"""
Description
===========

This module provides Cate's WebAPI executable.

To use the WebAPI executable, invoke the module file as a script, type ``python3 cate/webapi/main.py [ARGS] [OPTIONS]``.
Type `python3 cate/webapi/main.py --help`` for usage help.

The WebAPI has two sub-commands, ``start`` and ``stop``.

Verification
============

The module's unit-tests are located in
`test/webapi <https://github.com/CCI-Tools/cate/blob/master/test/webapi>`_
and may be executed using ``$ py.test test/webapi --cov=cate/webapi``
for extra code coverage information.

Components
==========
"""

import warnings

warnings.filterwarnings("ignore")  # never print any warnings to users
import os.path
import sys
import urllib.parse
from datetime import date

from tornado.web import Application, StaticFileHandler
from matplotlib.backends.backend_webagg_core import FigureManagerWebAgg

from cate.conf.defaults import WEBAPI_LOG_FILE_PREFIX, \
    WEBAPI_PROGRESS_DEFER_PERIOD
from cate.core.wsmanag import FSWorkspaceManager
from cate.util.web import JsonRpcWebSocketHandler
from cate.util.web.webapi import run_main, url_pattern, WebAPIRequestHandler, WebAPIExitHandler
from cate.version import __version__
from cate.webapi.rest import ResourcePlotHandler, CountriesGeoJSONHandler, ResVarTileHandler, \
    ResFeatureCollectionHandler, ResFeatureHandler, ResVarCsvHandler, ResVarHtmlHandler, NE2Handler
from cate.webapi.mpl import MplJavaScriptHandler, MplDownloadHandler, MplWebSocketHandler
from cate.webapi.websocket import WebSocketService

# Explicitly load Cate-internal plugins.
__import__('cate.ds')
__import__('cate.ops')

CLI_NAME = 'cate-webapi'
CLI_DESCRIPTION = 'ESA CCI Toolbox (Cate) Web API'


# noinspection PyAbstractClass
class WebAPIVersionHandler(WebAPIRequestHandler):
    def get(self):
        self.write_status_ok(content={'name': CLI_NAME,
                                      'version': __version__,
                                      'timestamp': date.today().isoformat()})


def service_factory(application):
    return WebSocketService(application.workspace_manager)


# All JSON REST responses should have same structure, namely a dictionary as follows:
#
# {
#    "status": "ok" | "error",
#    "error": optional error-details,
#    "content": optional content, if status "ok"
# }

def create_application():
    user_prefix = '/' + urllib.parse.quote(os.path.basename(os.path.expanduser('~')))

    application = Application([
        ('/_static/(.*)', StaticFileHandler, {'path': FigureManagerWebAgg.get_static_file_path()}),
        ('/mpl.js', MplJavaScriptHandler),

        (url_pattern(user_prefix + '/mpl/download/{{base_dir}}/{{figure_id}}/{{format_name}}'), MplDownloadHandler),
        (url_pattern(user_prefix + '/mpl/figures/{{base_dir}}/{{figure_id}}'), MplWebSocketHandler),

        (url_pattern(user_prefix + '/'), WebAPIVersionHandler),
        (url_pattern(user_prefix + '/exit'), WebAPIExitHandler),
        (url_pattern(user_prefix + '/api'), JsonRpcWebSocketHandler, dict(
            service_factory=service_factory,
            report_defer_period=WEBAPI_PROGRESS_DEFER_PERIOD)
         ),
        (url_pattern(user_prefix + '/ws/res/plot/{{base_dir}}/{{res_name}}'), ResourcePlotHandler),
        (url_pattern(user_prefix + '/ws/res/geojson/{{base_dir}}/{{res_id}}'), ResFeatureCollectionHandler),
        (url_pattern(user_prefix + '/ws/res/geojson/{{base_dir}}/{{res_id}}/{{feature_index}}'), ResFeatureHandler),
        (url_pattern(user_prefix + '/ws/res/csv/{{base_dir}}/{{res_id}}'), ResVarCsvHandler),
        (url_pattern(user_prefix + '/ws/res/html/{{base_dir}}/{{res_id}}'), ResVarHtmlHandler),
        (url_pattern(user_prefix + '/ws/res/tile/{{base_dir}}/{{res_id}}/{{z}}/{{y}}/{{x}}.png'), ResVarTileHandler),
        (url_pattern(user_prefix + '/ws/ne2/tile/{{z}}/{{y}}/{{x}}.jpg'), NE2Handler),
        (url_pattern(user_prefix + '/ws/countries'), CountriesGeoJSONHandler),

    ])
    application.workspace_manager = FSWorkspaceManager()
    return application


def main(args=None) -> int:
    return run_main(CLI_NAME, CLI_DESCRIPTION, __version__,
                    application_factory=create_application,
                    log_file_prefix=WEBAPI_LOG_FILE_PREFIX,
                    args=args)


if __name__ == "__main__":
    sys.exit(main())
