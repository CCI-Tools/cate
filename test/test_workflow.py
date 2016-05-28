from unittest import TestCase

from ect.core.op import op_input, op_output
from ect.core.util import object_to_qualified_name
from ect.core.workflow import InputConnector, OutputConnector, OpNode, Graph


@op_input('x')
@op_output('y')
class Op1:
    def __call__(self, x):
        return {'y': x + 1}


@op_input('a')
@op_output('b')
class Op2:
    def __call__(self, a):
        return {'b': 2 * a}


@op_input('u')
@op_input('v')
@op_output('w')
class Op3:
    def __call__(self, u, v):
        return {'w': 2 * u + 3 * v}


class InputConnectorTest(TestCase):
    def test_init(self):
        node = OpNode(Op1)
        input_connector = InputConnector(node, 'x')
        self.assertIs(input_connector.node, node)
        self.assertEqual(input_connector.name, 'x')
        self.assertEqual(input_connector.is_input, True)
        with self.assertRaisesRegex(ValueError, "'a' is not an input of operation '[a-z\\.]*test_workflow.Op1'"):
            InputConnector(node, 'a')
        with self.assertRaisesRegex(ValueError, "'y' is not an input of operation '[a-z\\.]*test_workflow.Op1'"):
            InputConnector(node, 'y')

    def test_eq(self):
        node1 = OpNode(Op3)
        node2 = OpNode(Op3)
        self.assertEqual(InputConnector(node1, 'u'), InputConnector(node1, 'u'))
        self.assertNotEqual(InputConnector(node2, 'u'), InputConnector(node1, 'u'))
        self.assertNotEqual(InputConnector(node1, 'v'), InputConnector(node1, 'u'))
        self.assertNotEqual(OutputConnector(node1, 'w'), InputConnector(node1, 'u'))


class OutputConnectorTest(TestCase):
    def test_init(self):
        node = OpNode(Op1)
        output_connector = OutputConnector(node, 'y')
        self.assertIs(output_connector.node, node)
        self.assertEqual(output_connector.name, 'y')
        self.assertEqual(output_connector.is_input, False)
        with self.assertRaisesRegex(ValueError, "'x' is not an output of operation '[a-z\\.]*test_workflow.Op1'"):
            OutputConnector(node, 'x')

    def test_eq(self):
        node1 = OpNode(Op3)
        node2 = OpNode(Op3)
        self.assertEqual(OutputConnector(node1, 'w'), OutputConnector(node1, 'w'))
        self.assertNotEqual(OutputConnector(node2, 'w'), OutputConnector(node1, 'w'))
        self.assertNotEqual(InputConnector(node1, 'u'), OutputConnector(node1, 'w'))


# noinspection PyUnresolvedReferences
class NodeTest(TestCase):
    def test_init(self):
        node = OpNode(Op3)

        self.assertRegex(node.id, '[a-z\\.]*test_workflow.Op3#[0-9]+')

        self.assertTrue(len(node.input), 2)
        self.assertTrue(len(node.output), 1)

        self.assertTrue(hasattr(node.input, 'u'))
        self.assertIsInstance(node.input.u, InputConnector)
        self.assertIs(node.input.u.node, node)
        self.assertEqual(node.input.u.name, 'u')
        self.assertEqual(node.input.u.value, None)
        self.assertTrue(node.input.u.is_input)

        self.assertTrue(hasattr(node.input, 'v'))
        self.assertIsInstance(node.input.v, InputConnector)
        self.assertIs(node.input.v.node, node)
        self.assertEqual(node.input.v.name, 'v')
        self.assertTrue(node.input.v.is_input)

        self.assertTrue(hasattr(node.output, 'w'))
        self.assertIsInstance(node.output.w, OutputConnector)
        self.assertIs(node.output.w.node, node)
        self.assertEqual(node.output.w.name, 'w')
        self.assertFalse(node.output.w.is_input)

    def test_init_operation_or_name_is_equivalent(self):
        node1 = OpNode(Op3)
        self.assertIsNotNone(node1.op)
        self.assertIsNotNone(node1.op_meta_info)
        node2 = OpNode(object_to_qualified_name(Op3))
        self.assertIs(node2.op, node1.op)
        self.assertIs(node2.op_meta_info, node1.op_meta_info)

    def test_node_invocation(self):
        node1 = OpNode(Op1)
        node1.input.x = 3
        return_value = node1.invoke()
        output_value = node1.output.y.value
        self.assertEqual(return_value, None)
        self.assertEqual(output_value, 3 + 1)

        node2 = OpNode(Op2)
        node2.input.a = 3
        return_value = node2.invoke()
        output_value = node2.output.b.value
        self.assertEqual(return_value, None)
        self.assertEqual(output_value, 2 * 3)

        node3 = OpNode(Op3)
        node3.input.u = 4
        node3.input.v = 5
        return_value = node3.invoke()
        output_value = node3.output.w.value
        self.assertEqual(return_value, None)
        self.assertEqual(output_value, 2 * 4 + 3 * 5)

    def test_init_failures(self):
        with self.assertRaises(ValueError):
            # "ValueError: operation with name 'test_node.NodeTest' not registered"
            OpNode(NodeTest)

        with self.assertRaises(ValueError):
            # "ValueError: operation with name 'X' not registered"
            OpNode('X')

        with self.assertRaises(ValueError):
            # "ValueError: operation with name 'X.Y' not registered"
            OpNode('X.Y')

        with self.assertRaises(ValueError):
            # "ValueError: operation must be given"
            OpNode(None)

    def test_join_output_with_input(self):
        node1 = OpNode(Op1)
        node2 = OpNode(Op2)
        node3 = OpNode(Op3)
        node1.output.y.join(node2.input.a)
        node1.output.y.join(node3.input.u)
        node2.output.b.join(node3.input.v)
        self.assertConnectionsAreOk(node1, node2, node3)
        node1 = OpNode(Op1)
        node2 = OpNode(Op2)
        with self.assertRaisesRegex(AttributeError, "'y' is an output and cannot be set"):
            node1.output.y = node2.input.a

    def test_join_input_with_output(self):
        node1 = OpNode(Op1)
        node2 = OpNode(Op2)
        node3 = OpNode(Op3)
        node2.input.a.join(node1.output.y)
        node3.input.u.join(node1.output.y)
        node3.input.v.join(node2.output.b)
        self.assertConnectionsAreOk(node1, node2, node3)

        node1 = OpNode(Op1)
        node2 = OpNode(Op2)
        node3 = OpNode(Op3)
        node2.input.a = node1.output.y
        node3.input.u = node1.output.y
        node3.input.v = node2.output.b
        self.assertConnectionsAreOk(node1, node2, node3)

        with self.assertRaisesRegex(AttributeError, "'a' is not an input"):
            node1.input.a = node3.input.u

        with self.assertRaisesRegex(AttributeError, "input 'a' expects an output"):
            node2.input.a = node3.input.u

    def test_disjoin(self):
        node1 = OpNode(Op1)
        node2 = OpNode(Op2)
        node3 = OpNode(Op3)

        node2.input.a.join(node1.output.y)
        node3.input.u.join(node1.output.y)
        node3.input.v.join(node2.output.b)
        self.assertConnectionsAreOk(node1, node2, node3)

        node3.input.v.disjoin()

        self.assertIs(node1.input.x.source, None)
        self.assertEqual(node1.output.y.targets, [node2.input.a, node3.input.u])
        self.assertIs(node2.input.a.source, node1.output.y)
        self.assertEqual(node2.output.b.targets, [])
        self.assertIs(node3.input.u.source, node1.output.y)
        self.assertIs(node3.input.v.source, None)
        self.assertEqual(node3.output.w.targets, [])

        node2.input.a.disjoin()

        self.assertIs(node1.input.x.source, None)
        self.assertEqual(node1.output.y.targets, [node3.input.u])
        self.assertIs(node2.input.a.source, None)
        self.assertEqual(node2.output.b.targets, [])
        self.assertIs(node3.input.u.source, node1.output.y)
        self.assertIs(node3.input.v.source, None)
        self.assertEqual(node3.output.w.targets, [])

        node3.input.u.disjoin()

        self.assertIs(node1.input.x.source, None)
        self.assertEqual(node1.output.y.targets, [])
        self.assertIs(node2.input.a.source, None)
        self.assertEqual(node2.output.b.targets, [])
        self.assertIs(node3.input.u.source, None)
        self.assertIs(node3.input.v.source, None)
        self.assertEqual(node3.output.w.targets, [])

    def assertConnectionsAreOk(self, node1, node2, node3):
        self.assertIs(node1.input.x.source, None)
        self.assertEqual(node1.output.y.targets, [node2.input.a, node3.input.u])

        self.assertIs(node2.input.a.source, node1.output.y)
        self.assertEqual(node2.output.b.targets, [node3.input.v])

        self.assertIs(node3.input.u.source, node1.output.y)
        self.assertIs(node3.input.v.source, node2.output.b)
        self.assertEqual(node3.output.w.targets, [])

    def test_node_json(self):
        node3 = OpNode(Op3, node_id='Op3')

        node3_dict = node3.to_json_dict()

        expected_json_text = """{
          "id": "Op3",
          "op": "test.test_workflow.Op3",
          "input": {
            "v": {"value": null},
            "u": {"value": null}
          }
        }
        """

        import json
        from io import StringIO

        actual_json_text = json.dumps(node3_dict)

        expected_json_obj = json.load(StringIO(expected_json_text))
        actual_json_obj = json.load(StringIO(actual_json_text))

        self.assertEqual(actual_json_obj, expected_json_obj)


class GraphTest(TestCase):
    def test_graph_init(self):
        node1 = OpNode(Op1, node_id='Op1')
        node2 = OpNode(Op2, node_id='Op2')
        node3 = OpNode(Op3, node_id='Op3')
        node2.input.a = node1.output.y
        node3.input.u = node1.output.y
        node3.input.v = node2.output.b
        graph = Graph(node1, node2, node3, graph_id='Workflow')

        self.assertEqual(graph.nodes, [node1, node2, node3])
        self.assertEqual(len(graph.input), 1)
        self.assertIs(graph.input[0], node1.input.x)
        self.assertEqual(len(graph.output), 1)
        self.assertIs(graph.output[0], node3.output.w)

    def test_graph_invocation(self):
        node1 = OpNode(Op1, node_id='Op1')
        node2 = OpNode(Op2, node_id='Op2')
        node3 = OpNode(Op3, node_id='Op3')
        node2.input.a = node1.output.y
        node3.input.u = node1.output.y
        node3.input.v = node2.output.b
        graph = Graph(node1, node2, node3, graph_id='Workflow')

        graph.input.x = 3
        return_value = graph.invoke()
        output_value = graph.output.w.value
        self.assertEqual(return_value, None)
        self.assertEqual(output_value, 2 * (3 + 1) + 3 * (2 * (3 + 1)))

    def test_graph_json(self):
        node1 = OpNode(Op1, node_id='Op1')
        node2 = OpNode(Op2, node_id='Op2')
        node3 = OpNode(Op3, node_id='Op3')
        node2.input.a = node1.output.y
        node3.input.u = node1.output.y
        node3.input.v = node2.output.b
        graph = Graph(node1, node2, node3, graph_id='Workflow')

        graph_dict = graph.to_json_dict()

        expected_json_text = """{
          "graph": [
            {
              "id": "Op1",
              "op": "test.test_workflow.Op1",
              "input": {
                "x": {"value": null}
              }
            },
            {
              "id": "Op2",
              "op": "test.test_workflow.Op2",
              "input": {
                "a": {"output_of": "Op1.y"}
              }
            },
            {
              "id": "Op3",
              "op": "test.test_workflow.Op3",
              "input": {
                "v": {"output_of": "Op2.b"},
                "u": {"output_of": "Op1.y"}
              }
            }
          ]
        }
        """

        import json
        from io import StringIO

        actual_json_text = json.dumps(graph_dict)

        expected_json_obj = json.load(StringIO(expected_json_text))
        actual_json_obj = json.load(StringIO(actual_json_text))

        self.assertEqual(actual_json_obj, expected_json_obj)
