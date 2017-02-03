# The MIT License (MIT)
# Copyright (c) 2017 by the Cate Development Team and contributors
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

__author__ = "Norman Fomferra (Brockmann Consult GmbH)"

"""
Internal module that implements the representation of a :py:class:`Workflow` as SVG graphic.
"""

from abc import abstractmethod, ABCMeta
from collections import OrderedDict
from math import sqrt, atan2, degrees
from typing import List


class Box:
    """
    Represents a rectangular region.

    :param x: y-axis coordinate of the upper-left corner of the rectangular region
    :param y: y-axis coordinate of the upper-left corner of the bounding box
    :param width: width of the rectangular region
    :param height: height of the rectangular region
    """

    def __init__(self, x: float = 0, y: float = 0, width: float = 0, height: float = 0):
        self.x = x
        self.y = y
        self.width = width
        self.height = height


class Port(metaclass=ABCMeta):
    """
    A port.

    :param node: Owner node.
    :param name: Port name.
    """

    def __init__(self, node: 'Node', name: str):
        self.node = node
        self.name = name
        self.incoming_edge = None
        self.outgoing_edges = []
        self.cy = 0

    @abstractmethod
    def center(self):
        pass

    def connect(self, other_port: 'Port'):
        edge = Edge(self, other_port)
        other_port.incoming_edge = edge
        self.outgoing_edges.append(edge)

    def _debug_str_incoming_edge(self):
        if self.incoming_edge:
            return str(self.incoming_edge.port_1)
        else:
            return '-'

    def _debug_str_outgoing_edges(self):
        if self.outgoing_edges:
            edge_parts = []
            for edge in self.outgoing_edges:
                edge_parts.append(str(edge.port_2))
            return ', '.join(edge_parts)
        else:
            return '-'

    def __str__(self):
        return '%s.%s' % (self.node.name, self.name)


class InputPort(Port):
    """
    An input port.

    :param node: Owner node.
    :param name: Port name.
    """

    def __init__(self, node: 'Node', name: str):
        super(InputPort, self).__init__(node, name)

    def center(self):
        box = self.node.box
        return box.x, box.y + self.cy


class OutputPort(Port):
    """
    An output port.

    :param node: Owner node.
    :param name: Port name.
    """

    def __init__(self, node: 'Node', name: str):
        super(OutputPort, self).__init__(node, name)

    def center(self):
        box = self.node.box
        return box.x + box.width, box.y + self.cy


class Edge:
    """
    A directed edge connecting *port_1* and *port_2*.

    :param port_1:
    :param port_2:
    """

    def __init__(self, port_1: Port, port_2: Port):
        self.port_1 = port_1
        self.port_2 = port_2

    def __str__(self):
        return '%s -> %s' % (self.port_1, self.port_2)


class Node:
    """
    A graph's node.

    :param name: Node name
    :param input_names: Names of input ports.
    :param output_names: Names of output ports.
    """

    def __init__(self, name: str, input_names: List[str], output_names: List[str]):
        self.name = name
        self.input_ports = OrderedDict([(name, InputPort(self, name)) for name in input_names])
        self.output_ports = OrderedDict([(name, OutputPort(self, name)) for name in output_names])
        self.layer_index = 0
        self.box = None

    @property
    def nodes(self) -> List['Node']:
        return None

    def find_port(self, name) -> 'Port':
        """
        Find port with given name. Output ports are searched first, then input ports.
        :param name: The port name
        :return: The port, or ``None`` if it couldn't be found.
        """
        if name in self.output_ports:
            return self.output_ports[name]
        if name in self.input_ports:
            return self.input_ports[name]
        return None

    def style(self, drawing_config: 'DrawingConfig'):
        return drawing_config.node_style

    def to_svg(self, drawing_config: 'DrawingConfig'):
        svg_lines = []

        node_style = self.style(drawing_config)
        r = drawing_config.port_radius

        text_dx = r
        text_dy = r + drawing_config.node_font_size

        rect = '<rect x="%s" y="%s" rx="%s" ry="%s" width="%s" height="%s" style="%s"/>' % (self.box.x, self.box.y,
                                                                                            r, r,
                                                                                            self.box.width,
                                                                                            self.box.height,
                                                                                            node_style)
        svg_lines.append(rect)

        text = '<text x="%s" y="%s">%s</text>' % (self.box.x + text_dx, self.box.y + text_dy, self.name)
        svg_lines.append(text)

        for input_port in self.input_ports.values():
            for outgoing_edge in input_port.outgoing_edges:
                self._outgoing_edge_svg(drawing_config, outgoing_edge, svg_lines)

        for output_port in self.output_ports.values():
            for outgoing_edge in output_port.outgoing_edges:
                self._outgoing_edge_svg(drawing_config, outgoing_edge, svg_lines)

        text_dx = r
        text_dy = r
        text_size = drawing_config.port_font_size
        text_style = 'font-family: Verdana; font-size: %s;' % text_size

        for input_port in self.input_ports.values():
            cx, cy = input_port.center()
            circle = '<circle cx="%s" cy="%s" r="%s" style="%s"/>' % (cx, cy, r, node_style)
            text_width = text_size * len(input_port.name)
            text = '<text x="%s" y="%s" style="%s">%s</text>' % (
                cx - text_dx - text_width, cy - text_dy, text_style, input_port.name)
            svg_lines.append(circle)
            svg_lines.append(text)

        for output_port in self.output_ports.values():
            cx, cy = output_port.center()
            circle = '<circle cx="%s" cy="%s" r="%s" style="%s"/>' % (cx, cy, r, node_style)
            text = '<text x="%s" y="%s" style="%s">%s</text>' % (
                cx + text_dx, cy - text_dy, text_style, output_port.name)
            svg_lines.append(circle)
            svg_lines.append(text)

        return '\n  '.join(svg_lines)

    # noinspection PyMethodMayBeStatic
    def _outgoing_edge_svg(self, drawing_config: 'DrawingConfig', outgoing_edge: Edge, svg_lines):
        x1, y1 = outgoing_edge.port_1.center()
        x2, y2 = outgoing_edge.port_2.center()
        dx = x2 - x1
        dy = y2 - y1
        r = drawing_config.port_radius
        l = sqrt(dx * dx + dy * dy) - r
        a = degrees(atan2(dy, dx))
        ex = l
        ey = 0
        line = '<line x1="0" y1="0" x2="%s" y2="%s"/>' % (ex, ey)
        arrow_head = '<polygon points="%s,%s %s,%s %s,%s %s,%s"/>' % (ex, ey,
                                                                      ex - r, ey + r / 2,
                                                                      ex - r, ey - r / 2,
                                                                      ex, ey)

        svg_lines.append(
            '<g transform="translate(%s %s) rotate(%s)" style="%s">"' % (x1, y1, a, drawing_config.line_style))
        svg_lines.append(line)
        svg_lines.append(arrow_head)
        svg_lines.append('</g>')

    def layout(self, drawing_config: 'DrawingConfig'):
        r = drawing_config.port_radius
        pg = drawing_config.port_gap
        input_port_count = len(self.input_ports)
        output_port_count = len(self.output_ports)
        input_port_panel_height = input_port_count * 2 * r + (input_port_count - 1) * pg
        output_port_panel_height = output_port_count * 2 * r + (output_port_count - 1) * pg
        min_input_box_height = input_port_panel_height + 2 * pg
        min_output_box_height = output_port_panel_height + 2 * pg
        box_height = max(drawing_config.min_node_height, min_input_box_height, min_output_box_height)
        box = Box(width=drawing_config.min_node_width, height=box_height)

        cy = (box_height - input_port_panel_height) / 2 + r
        for input_port in self.input_ports.values():
            input_port.cy = cy
            cy += 2 * r + pg

        cy = (box_height - output_port_panel_height) / 2 + r
        for output_port in self.output_ports.values():
            output_port.cy = cy
            cy += 2 * r + pg

        self.box = box

    # noinspection PyMethodMayBeStatic
    def _debug_str_ports(self, ports):
        parts = []
        for port in ports:
            # noinspection PyProtectedMember
            parts.append('%s: incoming: %s, outgoing: %s' % (
                port.name, port._debug_str_incoming_edge(), port._debug_str_outgoing_edges()))
        return '\n    '.join(parts)

    def _debug_str_input_ports(self):
        return self._debug_str_ports(self.input_ports.values())

    def _debug_str_output_ports(self):
        return self._debug_str_ports(self.output_ports.values())

    def _debug_str(self):
        return '\n%s:' \
               '\n  input ports:\n    %s' \
               '\n  outputs ports:\n    %s' % (self.name,
                                               self._debug_str_input_ports(),
                                               self._debug_str_output_ports())

    def __str__(self):
        return self.name


class Graph(Node):
    """
    A graph.

    :param name: Graph name
    :param input_names: Names of input ports.
    :param output_names: Names of output ports.
    """

    def __init__(self, name: str, input_names: List[str], output_names: List[str], nodes: List[Node]):
        super(Graph, self).__init__(name, input_names, output_names)
        self._nodes = list(nodes)
        self.layers = None

    @property
    def nodes(self) -> List[Node]:
        return self._nodes

    def style(self, drawing_config: 'DrawingConfig'):
        return drawing_config.graph_style

    def _repr_svg_(self):
        drawing = Drawing(self)
        return drawing.to_svg()

    def to_svg(self, drawing_config: 'DrawingConfig'):
        self.layout(drawing_config)

        svg_elements = []

        svg = super(Graph, self).to_svg(drawing_config)
        svg_elements.append(svg)

        for node in self.nodes:
            svg_elements.append(node.to_svg(drawing_config))

        return '\n   '.join(svg_elements)

    def layout(self, drawing_config: 'DrawingConfig'):

        # ------------------------------------------------------------------
        # Step 1: arrange node in "layers" according to their call depths

        # assign layer_index according to call depth
        self._set_layer_index_of_connected_nodes(self, 0, [self])
        self.layers = self._compute_layers()

        # Initial layout of child nodes
        for node in self.nodes:
            node.layout(drawing_config)

        # ------------------------------------------------------------------
        # TODO (nf, 20160627): Step 2: for any outgoing edge that spans more than one layer, insert new DummyNode
        # ------------------------------------------------------------------
        # TODO (nf, 20160627): Step 3: swap nodes in every layer in order to minimize edge overlaps

        # Initial layout of this graph's box
        self.box = self._layout_box(drawing_config)

        # Naive graph input/output port layout
        r = drawing_config.port_radius
        g = drawing_config.port_gap
        cy_delta_min = 2 * r + g

        # The cy of an input port is the cy of the node pointed to by the first outgoing edge
        last_cy = 0
        for input_port in self.input_ports.values():
            if input_port.outgoing_edges:
                outgoing_edge = input_port.outgoing_edges[0]
                cx, cy = outgoing_edge.port_2.center()
                if cy - last_cy < cy_delta_min:
                    cy = last_cy + cy_delta_min
                input_port.cy = cy
                last_cy = cy

        # The cy of an output port is the cy of the node pointed to by the incoming edge
        last_cy = 0
        for output_port in self.output_ports.values():
            if output_port.incoming_edge:
                incoming_edge = output_port.incoming_edge
                cx, cy = incoming_edge.port_1.center()
                if cy - last_cy < cy_delta_min:
                    cy = last_cy + cy_delta_min
                output_port.cy = cy
                last_cy = cy

    def _compute_layers(self):
        layer_dict = dict()
        for node in self.nodes:
            layer_nodes = layer_dict.get(node.layer_index, [])
            layer_nodes.append(node)
            layer_dict[node.layer_index] = layer_nodes
        # sort by layer_index
        layer_items = sorted(layer_dict.items(), key=lambda item: item[0])
        layers = []
        for layer_index, layer_nodes in layer_items:
            layers.append(layer_nodes)
        return layers

    def _layout_box(self, drawing_config):
        cx = 0
        hg = drawing_config.node_horizontal_gap
        vg = drawing_config.node_vertical_gap
        box_height = 0
        for layer_nodes in self.layers:
            cx += hg

            max_width = 0
            for node in layer_nodes:
                max_width = max(max_width, node.box.width)

            cy = 0
            for node in layer_nodes:
                cy += vg
                node.box.x = cx
                node.box.y = cy
                node.box.width = max_width
                cy += node.box.height

            cx += max_width
            box_height = max(box_height, cy)
        box_width = cx + hg
        box_height += vg
        return Box(width=box_width, height=box_height)

    def _set_layer_index_of_connected_nodes(self, node: Node, layer_index: int, parents: List[Node]):
        if layer_index > node.layer_index:
            node.layer_index = layer_index
        # For all inputs of this node
        for input_port in node.input_ports.values():
            self._set_layer_of_outgoing_edges(input_port, layer_index, parents)
        # For all outputs of this node
        for output_port in node.output_ports.values():
            self._set_layer_of_outgoing_edges(output_port, layer_index, parents)

    def _set_layer_of_outgoing_edges(self, port: Port, layer_index: int, parents):
        # For all outgoing edges
        for outgoing_edge in port.outgoing_edges:
            target_node = outgoing_edge.port_2.node
            if target_node not in parents:
                self._set_layer_index_of_connected_nodes(target_node, layer_index + 1, parents + [target_node])

    @staticmethod
    def test_graph():
        node_1 = Node('step_1', ['a', 'b'], ['c'])
        node_2 = Node('step_2', ['x'], ['y'])
        node_3 = Node('step_3', ['p', 'q'], ['P', 'Q'])
        graph = Graph('workflow', ['a', 'b', 'c'], ['x', 'y', 'z'], [node_1, node_2, node_3])

        graph.input_ports['a'].connect(node_1.input_ports['a'])
        graph.input_ports['b'].connect(node_1.input_ports['b'])
        graph.input_ports['c'].connect(node_3.input_ports['q'])
        node_1.output_ports['c'].connect(node_2.input_ports['x'])
        node_1.output_ports['c'].connect(node_3.input_ports['p'])
        node_2.output_ports['y'].connect(graph.output_ports['x'])
        node_3.output_ports['P'].connect(graph.output_ports['y'])
        node_3.output_ports['Q'].connect(graph.output_ports['z'])

        return graph

    def __str__(self):
        return '%s(%s)' % (self.name, ' '.join([str(node) for node in self.nodes.keys()]))


class DrawingConfig:
    """
    Drawing configuration.

    :param min_node_width:
    :param max_node_width:
    :param min_node_height:
    :param port_radius:
    :param port_gap:
    :param node_horizontal_gap:
    :param node_vertical_gap:
    :param graph_style:
    :param node_style:
    :param line_style:
    :param node_font_size:
    :param port_font_size:
    """

    def __init__(self,
                 min_node_width=100,
                 max_node_width=200,
                 min_node_height=48,
                 port_radius=4,
                 port_gap=4,
                 node_horizontal_gap=48,
                 node_vertical_gap=18,
                 graph_style='fill:rgb(255,255,255); stroke-width:1; stroke:rgb(0,0,0)',
                 node_style='fill:rgb(220,220,255); stroke-width:1; stroke:rgb(0,0,0)',
                 line_style='fill:rgb(0,0,255); stroke-width:1; stroke:rgb(0,0,255)',
                 node_font_size=10,
                 port_font_size=8,
                 ):
        self.node_font_size = node_font_size
        self.port_font_size = port_font_size
        self.min_node_width = min_node_width
        self.max_node_width = max_node_width
        self.min_node_height = min_node_height
        self.port_radius = port_radius
        self.port_gap = port_gap
        self.node_horizontal_gap = node_horizontal_gap
        self.node_vertical_gap = node_vertical_gap
        self.graph_style = graph_style
        self.node_style = node_style
        self.line_style = line_style


class Drawing:
    """

    :param graph: A graph of type :py:class:`Graph`.
    :param config: Drawing configuration of type :py:class:`Config`.
    """

    def __init__(self, graph: Graph, config: DrawingConfig = DrawingConfig()):
        self.graph = graph
        self.config = config

    def layout(self):
        self.graph.layout(self.config)

    def to_svg(self):
        self.layout()
        width = self.graph.box.width
        height = self.graph.box.height
        r = self.config.port_radius
        view_box = '%s %s %s %s' % (- 2 * r, - 2 * r, width + 4 * r, height + 4 * r)
        style = 'font-family: Verdana; font-size: %s;' % self.config.node_font_size
        return '<svg width="%s" height="%s" viewBox="%s" style="%s">\n%s\n</svg>' % (
            width,
            height,
            view_box,
            style,
            self.graph.to_svg(self.config))

    def __str__(self):
        return 'drawing of ' % self.graph.name
