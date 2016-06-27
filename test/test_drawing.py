from unittest import TestCase

from ect.core.drawing import Drawing, Graph, Node


class DrawingTest(TestCase):
    def test_it(self):
        node_1 = Node('step_1', ['a', 'b'], ['c'])
        node_2 = Node('step_2', ['x'], ['y'])
        node_3 = Node('step_3', ['p', 'q'], ['P', 'Q'])
        graph = Graph('workflow', ['a', 'b', 'c'], ['x', 'y', 'z'], [node_1, node_2, node_3])

        graph.input_ports[0].connect(node_1.input_ports[0])
        graph.input_ports[1].connect(node_1.input_ports[1])
        graph.input_ports[2].connect(node_3.input_ports[1])
        node_1.output_ports[0].connect(node_2.input_ports[0])
        node_1.output_ports[0].connect(node_3.input_ports[0])
        node_2.output_ports[0].connect(graph.output_ports[0])
        node_3.output_ports[0].connect(graph.output_ports[1])
        node_3.output_ports[1].connect(graph.output_ports[2])

        print()
        print(graph._debug_str())
        print(node_1._debug_str())
        print(node_2._debug_str())
        print(node_3._debug_str())

        drawing = Drawing(graph)
        drawing.layout()

        self.assertEqual(graph.layer_index, 0)
        self.assertEqual(node_1.layer_index, 1)
        self.assertEqual(node_2.layer_index, 2)
        self.assertEqual(node_3.layer_index, 2)

        layers = graph.layers
        self.assertEqual(len(layers), 2)
        self.assertEqual(layers[0], [node_1])
        self.assertEqual(layers[1], [node_2, node_3])

        svg = drawing.to_svg()
        print(svg)

        with open('./test.html', 'w') as fp:
            fp.write('<html>%s</html>' % svg)


