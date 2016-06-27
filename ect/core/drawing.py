from math import sqrt, atan2, degrees
from typing import List


class Box:
    def __init__(self, x: float = 0, y: float = 0, width: float = 0, height: float = 0):
        self.x = x
        self.y = y
        self.width = width
        self.height = height


class Circle:
    def __init__(self, cx: float = 0, cy: float = 0, r: float = 0):
        self.cx = cx
        self.cy = cy
        self.r = r


class Port:
    def __init__(self, node: 'Node', name: str):
        self.node = node
        self.name = name
        self.incoming_edge = None
        self.outgoing_edges = []
        self.cy = 0

    def center(self):
        return 0, 0

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
    def __init__(self, node: 'Node', name: str):
        super(InputPort, self).__init__(node, name)

    def center(self):
        box = self.node.box
        return box.x, box.y + self.cy


class OutputPort(Port):
    def __init__(self, node: 'Node', name: str):
        super(OutputPort, self).__init__(node, name)

    def center(self):
        box = self.node.box
        return box.x + box.width, box.y + self.cy


class Edge:
    def __init__(self, port_1: Port, port_2: Port):
        self.port_1 = port_1
        self.port_2 = port_2

    def __str__(self):
        return '%s -> %s' % (self.port_1, self.port_2)


class Node:
    def __init__(self, name: str, input_names: List[str], output_names: List[str]):
        self.name = name
        self.input_ports = []
        self.output_ports = []
        for name in input_names:
            self.input_ports.append(InputPort(self, name))
        for name in output_names:
            self.output_ports.append(OutputPort(self, name))
        self.layer_index = 0
        self.box = None

    @property
    def nodes(self) -> List['Node']:
        return None

    def style(self, drawing_config: 'Drawing.Config'):
        return drawing_config.node_style

    def to_svg(self, drawing_config: 'Drawing.Config'):
        svg_lines = []

        line_style = drawing_config.line_style
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

        for input_port in self.input_ports:
            for outgoing_edge in input_port.outgoing_edges:
                self.outgoing_edge_svg(drawing_config, outgoing_edge, svg_lines)

        for output_port in self.output_ports:
            for outgoing_edge in output_port.outgoing_edges:
                self.outgoing_edge_svg(drawing_config, outgoing_edge, svg_lines)

        text_dx = r
        text_dy = r
        text_size = drawing_config.port_font_size
        text_style = 'font-family: Verdana; font-size: %s;' % text_size

        for input_port in self.input_ports:
            cx, cy = input_port.center()
            circle = '<circle cx="%s" cy="%s" r="%s" style="%s"/>' % (cx, cy, r, node_style)
            text_width = text_size * len(input_port.name)
            text = '<text x="%s" y="%s" style="%s">%s</text>' % (
                cx - text_dx - text_width, cy - text_dy, text_style, input_port.name)
            svg_lines.append(circle)
            svg_lines.append(text)

        for output_port in self.output_ports:
            cx, cy = output_port.center()
            circle = '<circle cx="%s" cy="%s" r="%s" style="%s"/>' % (cx, cy, r, node_style)
            text = '<text x="%s" y="%s" style="%s">%s</text>' % (
                cx + text_dx, cy - text_dy, text_style, output_port.name)
            svg_lines.append(circle)
            svg_lines.append(text)

        return '\n  '.join(svg_lines)

    def outgoing_edge_svg(self, drawing_config: 'Drawing.Config', outgoing_edge: Edge, svg_lines):
        x1, y1 = outgoing_edge.port_1.center()
        x2, y2 = outgoing_edge.port_2.center()
        dx = x2 - x1
        dy = y2 - y1
        r = drawing_config.port_radius
        l = sqrt(dx * dx + dy * dy) - r
        a = degrees(atan2(dy, dx))
        ex = l
        ey = 0
        line = '<line x1="%s" y1="%s" x2="%s" y2="%s" style="%s"/>' % (x1, y1,
                                                                       x2, y2,
                                                                       drawing_config.line_style)
        # svg_lines.append(line)
        # return
        line = '<line x1="0" y1="0" x2="%s" y2="%s"/>' % (ex, ey)
        arrow_head = '<polygon points="%s,%s %s,%s %s,%s %s,%s"/>' % (ex, ey,
                                                                      ex - r, ey + r / 2,
                                                                      ex - r, ey - r / 2,
                                                                      ex, ey)

        #arrow_1 = '<line x1="%s" y1="%s" x2="%s" y2="%s"/>' % (ex - r, ey - r / 2, ex, ey)
        #arrow_2 = '<line x1="%s" y1="%s" x2="%s" y2="%s"/>' % (ex - r, ey + r / 2, ex, ey)
        # svg_lines.append('<g transform="rotate(%s %s %s)">"' % (a, x1, y1))
        svg_lines.append(
            '<g transform="translate(%s %s) rotate(%s)" style="%s">"' % (x1, y1, a, drawing_config.line_style))
        svg_lines.append(line)
        svg_lines.append(arrow_head)
        #svg_lines.append(arrow_1)
        #svg_lines.append(arrow_2)
        svg_lines.append('</g>')

    def layout(self, drawing_config: 'Drawing.Config'):
        input_port_count = len(self.input_ports)
        output_port_count = len(self.output_ports)
        input_port_panel_height = input_port_count * 2 * drawing_config.port_radius + (
                                                                                          input_port_count - 1) * drawing_config.port_gap
        output_port_panel_height = output_port_count * 2 * drawing_config.port_radius + (
                                                                                            output_port_count - 1) * drawing_config.port_gap
        min_input_box_height = input_port_panel_height + 2 * drawing_config.port_gap
        min_output_box_height = output_port_panel_height + 2 * drawing_config.port_gap
        box_height = max(drawing_config.min_node_height, min_input_box_height, min_output_box_height)

        box = Box(width=drawing_config.min_node_width, height=box_height)

        r = drawing_config.port_radius
        g = drawing_config.port_gap
        cy = (box_height - input_port_panel_height) / 2 + r
        for input_port in self.input_ports:
            input_port.cy = cy
            cy += 2 * r + g

        cy = (box_height - output_port_panel_height) / 2 + r
        for output_port in self.output_ports:
            output_port.cy = cy
            cy += 2 * r + g

        self.box = box

    def _debug_str_ports(self, ports):
        parts = []
        for port in ports:
            parts.append('%s: incoming: %s, outgoing: %s' % (
                port.name, port._debug_str_incoming_edge(), port._debug_str_outgoing_edges()))
        return '\n    '.join(parts)

    def _debug_str_input_ports(self):
        return self._debug_str_ports(self.input_ports)

    def _debug_str_output_ports(self):
        return self._debug_str_ports(self.output_ports)

    def _debug_str(self):
        return '\n%s:' \
               '\n  input ports:\n    %s' \
               '\n  outputs ports:\n    %s' % (self.name,
                                               self._debug_str_input_ports(),
                                               self._debug_str_output_ports())

    def __str__(self):
        return self.name


class Graph(Node):
    def __init__(self, name: str, input_names: List[str], output_names: List[str], nodes: List[Node]):
        super(Graph, self).__init__(name, input_names, output_names)
        self._nodes = nodes
        self.layers = None

    @property
    def nodes(self) -> List[Node]:
        return self._nodes

    def style(self, drawing_config: 'Drawing.Config'):
        return drawing_config.graph_style

    def layout(self, drawing_config: 'Drawing.Config'):
        self._set_layer_of_connected_nodes(self, 0, [self])

        layer_dict = dict()
        for node in self.nodes:
            layer_nodes = layer_dict.get(node.layer_index, [])
            layer_nodes.append(node)
            layer_dict[node.layer_index] = layer_nodes

        layer_list = sorted(layer_dict.items(), key=lambda item: item[0])
        self.layers = len(layer_list) * [None]
        for i in range(len(layer_list)):
            self.layers[i] = layer_list[i][1]

        # TODO (nf, 20160627): 2. for any outgoing edge that spans more than one layer, insert new DummyNode
        # TODO (nf, 20160627): 3. swap nodes in every layer in order to minimize edge overlaps

        for node in self.nodes:
            node.layout(drawing_config)

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
        self.box = Box(width=box_width, height=box_height)

        for input_port in self.input_ports:
            if input_port.outgoing_edges:
                outgoing_edge = input_port.outgoing_edges[0]
                input_port.cy = outgoing_edge.port_2.center()[1]

        for output_port in self.output_ports:
            if output_port.incoming_edge:
                incoming_edge = output_port.incoming_edge
                output_port.cy = incoming_edge.port_1.center()[1]

    def to_svg(self, drawing_config: 'Drawing.Config'):
        svg_elements = []

        svg = super(Graph, self).to_svg(drawing_config)
        svg_elements.append(svg)

        for node in self.nodes:
            svg_elements.append(node.to_svg(drawing_config))

        return '\n   '.join(svg_elements)

    def _set_layer_of_connected_nodes(self, node, layer_index, parents: List[Node]):
        if layer_index > node.layer_index:
            node.layer_index = layer_index
        # For all inputs of this node
        for input_port in node.input_ports:
            self._set_layer_of_outgoing_edges(input_port, layer_index, parents)
        # For all outputs of this node
        for output_port in node.output_ports:
            self._set_layer_of_outgoing_edges(output_port, layer_index, parents)

    def _set_layer_of_outgoing_edges(self, port: Port, layer_index: int, parents):
        # For all outgoing edges
        for outgoing_edge in port.outgoing_edges:
            target_node = outgoing_edge.port_2.node
            if target_node not in parents:
                self._set_layer_of_connected_nodes(target_node, layer_index + 1, parents + [target_node])

    def __str__(self):
        return '%s(%s)' % (self.name, ' '.join([str(node) for node in self.nodes]))


class Drawing:
    class Config:
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

    def __init__(self, graph, config=Config()):
        self.graph = graph
        self.config = config

    def layout(self):
        self.graph.layout(self.config)

    def to_svg(self):
        width = self.graph.box.width
        height = self.graph.box.height
        r = self.config.port_radius
        # view_box_attr = ''
        style = 'style="font-family: Verdana; font-size: %s;"' % self.config.node_font_size
        view_box_attr = 'viewBox="%s %s %s %s"' % (- 2 * r, - 2 * r, width + 4 * r, height + 4 * r)
        return '<svg width="%s" height="%s" %s %s>\n%s\n</svg>' % (
            width,
            height,
            view_box_attr,
            style,
            self.graph.to_svg(self.config))

    def __str__(self):
        return 'drawing of ' % self.graph.name
