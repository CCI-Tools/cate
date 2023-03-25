# The MIT License (MIT)
# Copyright (c) 2016-2023 by the ESA CCI Toolbox team and contributors
#
# Permission is hereby granted, free of charge, to any person obtaining a
# copy of this software and associated documentation files (the "Software"),
# to deal in the Software without restriction, including without limitation
# the rights to use, copy, modify, merge, publish, distribute, sublicense,
# and/or sell copies of the Software, and to permit persons to whom the
# Software is furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
# FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
# DEALINGS IN THE SOFTWARE.

"""
Implements the Tornado REST and WebSocket handlers for working with
interactive ``matplotlib`` figures in a web frontend.

Code bases on an example taken from
https://matplotlib.org/examples/user_interfaces/embedding_webagg.html
"""

import io
import json
import sys
from typing import Optional

from matplotlib.backends.backend_webagg_core import FigureManagerWebAgg
# noinspection PyUnresolvedReferences
from matplotlib.backends.backend_webagg_core import \
    new_figure_manager_given_figure
from matplotlib.figure import Figure
from tornado.web import RequestHandler
from tornado.websocket import WebSocketHandler

from cate.core.workspace import Workspace
from cate.core.wsmanag import WorkspaceManager
from cate.util.web.common import log_debug, is_debug_mode
from cate.util.web.webapi import WebAPIRequestHandler

__author__ = "Norman Fomferra (Brockmann Consult GmbH)"

# The following is the content of the web page.  You would normally
# generate this using some sort of template facility in your web
# framework, but here we just use Python string formatting.

html_content = """
<html>
  <head>
    <!-- TODO: There should be a way to include all of the required javascript
               and CSS so matplotlib can add to the set in the future if it
               needs to. -->
    <link rel="stylesheet" href="_static/css/page.css" type="text/css"/>
    <link rel="stylesheet" href="_static/css/boilerplate.css" type="text/css"/>
    <link rel="stylesheet" href="_static/css/fbm.css" type="text/css"/>
    <link rel="stylesheet" href="_static/jquery/css/themes/base/jquery-ui.min.css"/>
    <script src="_static/jquery/js/jquery-1.11.3.min.js"></script>
    <script src="_static/jquery/js/jquery-ui.min.js"></script>
    <script src="mpl.js"></script>

    <script>
      /* This is a callback that is called when the user saves
         (downloads) a file.  Its purpose is really to map from a
         figure and file format to a url in the application. */
      function ondownload(figure, format) {
        window.open('download.' + format, '_blank');
      };

      $(document).ready(
        function() {
          /* It is up to the application to provide a websocket that the 
             figure will use to communicate to the server. 
             This websocket object can also be a "fake" websocket that 
             underneath multiplexes messages from multiple figures, 
             if necessary. 
          */
          var websocket_type = mpl.get_websocket_type();
          var websocket = new websocket_type("%(ws_uri)sws");

          // mpl.figure creates a new figure on the webpage.
          var fig = new mpl.figure(
              // A unique numeric identifier for the figure
              %(fig_id)s,
              // A websocket object (or something that behaves like one)
              websocket,
              // A function called when a file type is selected for download
              ondownload,
              // The HTML element in which to place the figure
              $('div#figure'));
        }
      );
    </script>

    <title>matplotlib</title>
  </head>

  <body>
    <div id="figure">
    </div>
  </body>
</html>
"""


# noinspection PyAbstractClass
class MplJavaScriptHandler(RequestHandler):
    """
    Serves the generated matplotlib javascript file.  The content
    is dynamically generated based on which toolbar functions the
    user has defined.  Call `FigureManagerWebAgg` to get its
    content.
    """

    def get(self):
        self.set_header('Content-Type', 'application/javascript')
        js_content = FigureManagerWebAgg.get_javascript()
        self.write(js_content)


# noinspection PyAbstractClass
class MplDownloadHandler(WebAPIRequestHandler):
    """
    Handles downloading of the figure in various file formats.
    """

    def get(self, workspace_id: str, figure_id: str, format_name: str):

        workspace_id = int(workspace_id)
        figure_id = int(figure_id)

        # noinspection PyUnresolvedReferences
        workspace_manager: WorkspaceManager = \
            self.application.workspace_manager
        assert workspace_manager

        base_dir = workspace_manager.resolve_path(workspace_id)
        workspace = workspace_manager.get_workspace(base_dir)
        assert workspace

        try:
            figure_manager = _get_figure_manager(workspace, figure_id)
        except ValueError:
            self.write_status_error(exc_info=sys.exc_info())
            self.finish()
            return

        mime_types = {
            'ps': 'application/postscript',
            'eps': 'application/postscript',
            'pdf': 'application/pdf',
            'svg': 'image/svg+xml',
            'png': 'image/png',
            'jpeg': 'image/jpeg',
            'tif': 'image/tiff',
            'emf': 'application/emf'
        }

        self.set_header('Content-Type', mime_types.get(format_name, 'binary'))

        buff = io.BytesIO()
        figure_manager.canvas.print_figure(buff, format=format_name)
        self.write(buff.getvalue())


# noinspection PyAbstractClass
class MplWebSocketHandler(WebSocketHandler):
    """
    A websocket for interactive communication between the plot in
    the browser and the server.

    In addition to the methods required by tornado, it is required to
    have two callback methods:

        - ``send_json(json_content)`` is called by matplotlib when
          it needs to send json to the browser.  `json_content` is
          a JSON tree (Python dictionary), and it is the responsibility
          of this implementation to encode it as a string to send over
          the socket.

        - ``send_binary(blob)`` is called to send binary image data
          to the browser.
    """
    supports_binary = True

    def __init__(self, application, request, **kwargs):
        super(MplWebSocketHandler, self).__init__(application,
                                                  request,
                                                  **kwargs)
        self.workspace = None
        self.workspace_id = None
        self.figure_id = None
        self.figure_manager = None

    def open(self, workspace_id: str, figure_id: str):
        if hasattr(self, 'set_nodelay'):
            self.set_nodelay(True)

        self.workspace_id = int(workspace_id)
        self.figure_id = int(figure_id)
        # print('MplWebSocketHandler.open', workspace_id, figure_id)

        # noinspection PyUnresolvedReferences
        workspace_manager: WorkspaceManager = \
            self.application.workspace_manager
        assert workspace_manager

        base_dir = workspace_manager.resolve_path(workspace_id)
        self.workspace = workspace_manager.get_workspace(base_dir)
        assert self.workspace

        # print('got figure_manager for figure #%s' % figure_id)

    def on_close(self):
        # print('MplWebSocketHandler.on_close',
        #       self.workspace.base_dir, self.figure_id)
        self._remove_figure_manager()

    def on_message(self, message):
        if is_debug_mode():
            log_debug('MplWebSocketHandler.on_message(%s)' % repr(message))

        # Every message has a "type" and a "figure_id".
        message = json.loads(message)

        if message['type'] == 'supports_binary':
            # The 'supports_binary' message is relevant to the
            # WebSocket itself.  The other messages get passed along
            # to matplotlib as-is.
            self.supports_binary = message['value']
        else:
            figure_id = message['figure_id']
            if figure_id != self.figure_id:
                message = "received figure_id={}," \
                          " but expected figure_id={}".format(figure_id,
                                                              self.figure_id)
                self.send_json(dict(type='message', message=message))
                return

            figure_manager = self._get_or_create_figure_manager()
            if figure_manager:
                figure_manager.handle_json(message)
            else:
                message = "no figure found for figure_id={}".format(figure_id)
                self.send_json(dict(type='message', message=message))

    def send_json(self, content):
        """Method required by matplotlib's FigureManagerWebAgg"""
        if is_debug_mode():
            log_debug('MplWebSocketHandler.send_json(%s)' % repr(content))
        self.write_message(json.dumps(content))

    def send_binary(self, blob):
        """Method required by matplotlib's FigureManagerWebAgg"""
        if self.supports_binary:
            self.write_message(blob, binary=True)
        else:
            data_uri = "data:image/png;base64,{0}".format(
                blob.encode('base64').replace('\n', '')
            )
            self.write_message(data_uri)

    def check_origin(self, origin):
        """
        Overridden to to return True (= all origins are ok), otherwise we get
            WebSocket connection to 'ws://localhost:9090/app' failed:
            Error during WebSocket handshake:
            Unexpected response code: 403 (forbidden)

        :param origin: The request origin
        :return: True
        """
        return True

    def _get_or_create_figure_manager(self) -> Optional[FigureManagerWebAgg]:
        workspace = self.workspace
        figure_id = self.figure_id

        figure = workspace.resource_cache.get_value_by_id(figure_id)
        if isinstance(figure, Figure):
            figure_managers = workspace.user_data.get('figure_managers')
            figure_manager = figure_managers \
                             and figure_managers.get(figure_id)
            if figure_manager:
                if figure_manager.canvas.figure is figure:
                    # we have a figure_manager and it already
                    # manages our figure
                    return figure_manager
                # forget this manager, we have a new figure
                figure_manager.remove_web_socket(self)
            # create a new figure_manager for our figure
            figure_manager = new_figure_manager_given_figure(figure_id,
                                                             figure)
            figure_manager.add_web_socket(self)
            if not figure_managers:
                # store a new mapping of figure_id to figure_manager
                figure_managers = dict()
                workspace.user_data['figure_managers'] = figure_managers
            # register our figure_manager
            figure_managers[figure_id] = figure_manager
            return figure_manager
        return None

    def _remove_figure_manager(self) -> None:
        workspace = self.workspace
        figure_id = self.figure_id
        figure_managers = workspace.user_data.get('figure_managers')
        if figure_managers and figure_id in figure_managers:
            figure_manager = figure_managers[figure_id]
            figure_manager.remove_web_socket(self)
            del figure_managers[figure_id]


def _get_figure_manager(workspace: Workspace, figure_id: int) \
        -> FigureManagerWebAgg:
    figure_managers = workspace.user_data.get('figure_managers')
    if figure_managers and figure_id in figure_managers:
        return figure_managers[figure_id]
    raise ValueError("missing figure manager"
                     " for figure_id={}".format(figure_id))
