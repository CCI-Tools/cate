"""
This example demonstrates how to embed matplotlib WebAgg interactive
plotting in your own web application and framework.  It is not
necessary to do all this if you merely want to display a plot in a
browser or use matplotlib's built-in Tornado-based server "on the
side".

The framework being used must support web sockets.
"""
from typing import Optional

import tornado.web
import tornado.websocket

from matplotlib.figure import Figure
from matplotlib.backends.backend_webagg_core import (
    FigureManagerWebAgg, new_figure_manager_given_figure)

import seaborn as sns
import matplotlib.pyplot as plt

import io
import json

import numpy as np
import tornado


def create_figure():
    """
    Creates a simple example figure.
    """
    sns.set(style="dark")
    rs = np.random.RandomState(50)

    # Set up the matplotlib figure
    f, axes = plt.subplots(3, 3, figsize=(9, 9), sharex=True, sharey=True)

    # Rotate the starting point around the cubehelix hue circle
    for ax, s in zip(axes.flat, np.linspace(0, 3, 10)):
        # Create a cubehelix colormap to use with kdeplot
        cmap = sns.cubehelix_palette(start=s, light=1, as_cmap=True)

        # Generate and plot a random bivariate dataset
        x, y = rs.randn(2, 50)
        sns.kdeplot(x, y, cmap=cmap, shade=True, cut=5, ax=ax)
        ax.set(xlim=(-3, 3), ylim=(-3, 3))

    f.tight_layout()
    return f


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


class MplMainPageHander(tornado.web.RequestHandler):
    """
    Serves the main HTML page.
    """

    def get(self):
        manager = self.application.manager
        ws_uri = "ws://{req.host}/".format(req=self.request)
        content = html_content % {"ws_uri": ws_uri, "fig_id": manager.num}
        self.write(content)


class MplJsHander(tornado.web.RequestHandler):
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


class MplDownloadHander(tornado.web.RequestHandler):
    """
    Handles downloading of the figure in various file formats.
    """

    def get(self, format):
        manager = self.application.manager

        mimetypes = {
            'ps': 'application/postscript',
            'eps': 'application/postscript',
            'pdf': 'application/pdf',
            'svg': 'image/svg+xml',
            'png': 'image/png',
            'jpeg': 'image/jpeg',
            'tif': 'image/tiff',
            'emf': 'application/emf'
        }

        self.set_header('Content-Type', mimetypes.get(format, 'binary'))

        buff = io.BytesIO()
        manager.canvas.print_figure(buff, format=format)
        self.write(buff.getvalue())


class MplWebSocket(tornado.websocket.WebSocketHandler):
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

    def open(self):
        # Register the websocket with the FigureManagers.
        for figure_id, figure_manager in FIGURE_MANAGERS.items():
            figure_manager.add_web_socket(self)
        if hasattr(self, 'set_nodelay'):
            self.set_nodelay(True)

    def on_close(self):
        # When the socket is closed, deregister the websocket with
        # the FigureManagers.
        for figure_id, figure_manager in FIGURE_MANAGERS.items():
            figure_manager.remove_web_socket(self)

    def on_message(self, message):
        # The 'supports_binary' message is relevant to the
        # websocket itself.  The other messages get passed along
        # to matplotlib as-is.

        # Every message has a "type" and a "figure_id".
        message = json.loads(message)
        if message['type'] == 'supports_binary':
            self.supports_binary = message['value']
        else:
            figure_id = message['figure_id']
            if figure_id in FIGURE_MANAGERS:
                manager = FIGURE_MANAGERS[figure_id]
                manager.handle_json(message)

    def send_json(self, content):
        self.write_message(json.dumps(content))

    def send_binary(self, blob):
        if self.supports_binary:
            self.write_message(blob, binary=True)
        else:
            data_uri = "data:image/png;base64,{0}".format(
                blob.encode('base64').replace('\n', ''))
            self.write_message(data_uri)
