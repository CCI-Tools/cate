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
from cate.util.web.webapi import WebAPIRequestHandler

__author__ = "Norman Fomferra (Brockmann Consult GmbH)"

"""
Implements the Tornado REST and WebSocket handlers for working with interactive ``matplotlib`` 
figures in a web frontend.

Code bases on an example taken from https://matplotlib.org/examples/user_interfaces/embedding_webagg.html 
"""

import io
import json
from typing import Optional

from matplotlib.backends.backend_webagg_core import FigureManagerWebAgg
from matplotlib.backends.backend_webagg_core import new_figure_manager_given_figure
from tornado.web import RequestHandler
from tornado.websocket import WebSocketHandler

from cate.ops.plot import FIGURE_REGISTRY

# The following is the content of the web page.  You would normally
# generate this using some sort of template facility in your web
# framework, but here we just use Python string formatting.
html_content = """
<html>
  <head>
    <!-- TODO: There should be a way to include all of the required javascript
               and CSS so matplotlib can add to the set in the future if it
               needs to. -->
    <link rel="stylesheet" href="_static/css/page.css" type="text/css">
    <link rel="stylesheet" href="_static/css/boilerplate.css" type="text/css" />
    <link rel="stylesheet" href="_static/css/fbm.css" type="text/css" />
    <link rel="stylesheet" href="_static/jquery/css/themes/base/jquery-ui.min.css" >
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
          /* It is up to the application to provide a websocket that the figure
             will use to communicate to the server.  This websocket object can
             also be a "fake" websocket that underneath multiplexes messages
             from multiple figures, if necessary. */
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


def _get_figure_manager(figure_id: int) -> Optional[FigureManagerWebAgg]:
    if FIGURE_REGISTRY.has_entry(figure_id):
        _, data = FIGURE_REGISTRY.get_entry(figure_id)
        return data['figure_manager']
    return None


# noinspection PyAbstractClass
class MplMainPageHandler(RequestHandler):
    """
    Serves the main HTML page.
    """

    def get(self):
        manager = self.application.manager
        ws_uri = "ws://{req.host}/".format(req=self.request)
        content = html_content % {"ws_uri": ws_uri, "fig_id": manager.num}
        self.write(content)


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

    def get(self, figure_id: str, format: str):
        figure_id = int(figure_id)

        figure_manager = _get_figure_manager(figure_id)
        try:
            assert figure_manager is not None, "missing figure manager for figure_id={}".format(figure_id)
        except Exception as exception:
            self.write_status_error(exception)
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

        self.set_header('Content-Type', mime_types.get(format, 'binary'))

        buff = io.BytesIO()
        figure_manager.canvas.print_figure(buff, format=format)
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

    # We must override this to return True (= all origins are ok), otherwise we get
    #   WebSocket connection to 'ws://localhost:9090/app' failed:
    #   Error during WebSocket handshake:
    #   Unexpected response code: 403 (forbidden)
    def check_origin(self, origin):
        print("MplWebSocketHandler: check " + str(origin))
        return True

    def open(self):
        # Register the WebSocket with the FigureManagers.
        for fig_entry in FIGURE_REGISTRY.entries:
            self._register_with_figure_manager(fig_entry)
        # Start observing changes in figure registry
        FIGURE_REGISTRY.add_observer(self._on_figure_registry_change)

        if hasattr(self, 'set_nodelay'):
            self.set_nodelay(True)

    def on_close(self):
        # Stop observing changes in figure registry
        FIGURE_REGISTRY.remove_observer(self._on_figure_registry_change)
        # When closed, deregister the WebSocket with the FigureManagers.
        for fig_entry in FIGURE_REGISTRY.entries:
            self._deregister_with_figure_manager(fig_entry)

    def on_message(self, message):
        # The 'supports_binary' message is relevant to the
        # WebSocket itself.  The other messages get passed along
        # to matplotlib as-is.

        # Every message has a "type" and a "figure_id".
        message = json.loads(message)
        if message['type'] == 'supports_binary':
            self.supports_binary = message['value']
        else:
            figure_id = message['figure_id']
            if FIGURE_REGISTRY.has_entry(figure_id):
                _, data = FIGURE_REGISTRY.get_entry(figure_id)
                figure_manager = data['figure_manager']
                assert figure_manager is not None
                figure_manager.handle_json(message)

    def send_json(self, content):
        self.write_message(json.dumps(content))

    def send_binary(self, blob):
        if self.supports_binary:
            self.write_message(blob, binary=True)
        else:
            data_uri = "data:image/png;base64,{0}".format(
                blob.encode('base64').replace('\n', ''))
            self.write_message(data_uri)

    def _on_figure_registry_change(self, event: str, fig_entry):
        print('MplWebSocketHandler._on_figure_registry_change({}, data={}): '.format(event, fig_entry[1]))
        if event == 'entry_added' or event == 'entry_updated':
            self._register_with_figure_manager(fig_entry)
        elif event == 'entry_removed':
            self._deregister_with_figure_manager(fig_entry)

    def _register_with_figure_manager(self, fig_entry):
        figure, data = fig_entry
        figure_manager = data.get('figure_manager')
        if figure_manager is None:
            figure_id = data['figure_id']
            figure_manager = new_figure_manager_given_figure(figure_id, figure)
            data['figure_manager'] = figure_manager
        figure_manager.add_web_socket(self)

    def _deregister_with_figure_manager(self, fig_entry):
        _, data = fig_entry
        figure_manager = data.get('figure_manager')
        if figure_manager is not None:
            figure_manager.remove_web_socket(self)
