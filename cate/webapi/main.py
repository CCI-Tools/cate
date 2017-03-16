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
`test/webapi <https://github.com/CCI-Tools/cate-core/blob/master/test/webapi>`_
and may be executed using ``$ py.test test/webapi --cov=cate/webapi``
for extra code coverage information.

Components
==========
"""

import sys
from datetime import date

from tornado.web import Application

from cate.conf.defaults import WEBAPI_LOG_FILE_PREFIX,  \
    WEBAPI_PROGRESS_DEFER_PERIOD
from cate.core.wsmanag import FSWorkspaceManager
from cate.util.web import JsonRcpWebSocketHandler
from cate.util.web.webapi import run_main, url_pattern, WebAPIRequestHandler, WebAPIExitHandler
from cate.version import __version__
from cate.webapi.rest import WorkspaceGetHandler, WorkspaceNewHandler, WorkspaceOpenHandler, \
    WorkspaceCloseHandler, WorkspaceGetOpenHandler, WorkspaceCleanHandler, \
    WorkspaceCloseAllHandler, WorkspaceDeleteHandler, WorkspaceRunOpHandler, \
    WorkspaceSaveAllHandler, WorkspaceSaveAsHandler, WorkspaceSaveHandler, \
    ResourceSetHandler, ResourceDeleteHandler, ResourcePlotHandler, \
    ResourcePrintHandler, ResourceWriteHandler, CountriesGeoJSONHandler, \
    ResVarTileHandler, ResVarGeoJSONHandler, NE2Handler
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
    application = Application([
        (url_pattern('/'), WebAPIVersionHandler),
        (url_pattern('/exit'), WebAPIExitHandler),
        (url_pattern('/app'), JsonRcpWebSocketHandler, dict(service_factory=service_factory,
                                                            report_defer_period=WEBAPI_PROGRESS_DEFER_PERIOD)),
        (url_pattern('/ws/new'), WorkspaceNewHandler),
        (url_pattern('/ws/get_open'), WorkspaceGetOpenHandler),
        (url_pattern('/ws/get/{{base_dir}}'), WorkspaceGetHandler),
        (url_pattern('/ws/open/{{base_dir}}'), WorkspaceOpenHandler),
        (url_pattern('/ws/close/{{base_dir}}'), WorkspaceCloseHandler),
        (url_pattern('/ws/close_all'), WorkspaceCloseAllHandler),
        (url_pattern('/ws/save/{{base_dir}}'), WorkspaceSaveHandler),
        (url_pattern('/ws/save_as/{{base_dir}}'), WorkspaceSaveAsHandler),
        (url_pattern('/ws/save_all'), WorkspaceSaveAllHandler),
        (url_pattern('/ws/del/{{base_dir}}'), WorkspaceDeleteHandler),
        (url_pattern('/ws/clean/{{base_dir}}'), WorkspaceCleanHandler),
        (url_pattern('/ws/run_op/{{base_dir}}'), WorkspaceRunOpHandler),
        (url_pattern('/ws/res/set/{{base_dir}}/{{res_name}}'), ResourceSetHandler),
        (url_pattern('/ws/res/del/{{base_dir}}/{{res_name}}'), ResourceDeleteHandler),
        (url_pattern('/ws/res/write/{{base_dir}}/{{res_name}}'), ResourceWriteHandler),
        (url_pattern('/ws/res/plot/{{base_dir}}/{{res_name}}'), ResourcePlotHandler),
        (url_pattern('/ws/res/print/{{base_dir}}'), ResourcePrintHandler),
        (url_pattern('/ws/countries/{{zoom}}'), CountriesGeoJSONHandler),
        (url_pattern('/ws/res/geojson/{{base_dir}}/{{res_name}}/{{zoom}}'), ResVarGeoJSONHandler),
        (url_pattern('/ws/res/tile/{{base_dir}}/{{res_name}}/{{z}}/{{y}}/{{x}}.png'), ResVarTileHandler),
        (url_pattern('/ws/ne2/tile/{{z}}/{{y}}/{{x}}.jpg'), NE2Handler),
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
