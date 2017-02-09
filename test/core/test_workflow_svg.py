from unittest import TestCase

from cate.core.workflow_svg import Drawing, Graph


class WorkflowSvgTest(TestCase):
    def test_routing(self):
        from cate.core.workflow import Workflow
        import os.path
        workflow = Workflow.load(os.path.join(os.path.dirname(__file__), 'workflows', 'four_steps_chain.json'))

        actual_svg = workflow._repr_svg_()
        self._write_svg_html(actual_svg)

        # self.assertEqual(actual_svg, expected_svg)

    def test_it(self):
        """
        By intention, not actually a rigorous test.
        The test compares some SVG output with some expected SVG output ('test_workflow_svg.svg')
        that has been verified by visual inspection.
        If you change the implementation of 'workflow_svg.py' or the styles used in there, feel free change
        'test_workflow_svg.svg' accordingly (after visual inspection).
        """
        graph = Graph.test_graph()
        node_1, node_2, node_3 = graph.nodes

        # print(graph._debug_str())
        # print(node_1._debug_str())
        # print(node_2._debug_str())
        # print(node_3._debug_str())

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

        actual_svg = drawing.to_svg()
        # self._write_svg_html(actual_svg)

        import os.path
        with open(os.path.join(os.path.dirname(__file__), '..', 'test_workflow_svg.svg'), 'r') as fp:
            expected_svg = fp.read()

        self.assertEqual(actual_svg, expected_svg)

    def _write_svg_html(self, svg):
        # print(svg)
        import os.path
        with open(os.path.join(os.path.dirname(__file__), '..', 'test_workflow_svg.html'), 'w') as fp:
            fp.write('<html>\n%s\n</html>' % svg)
