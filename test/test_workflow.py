import json
from unittest import TestCase

from ect.core.op import op_input, op_output, OpRegistration, OpMetaInfo
from ect.core.util import object_to_qualified_name
from ect.core.workflow import NodeInput, NodeOutput, OpNode, ExternalInput, ConstantInput, Graph, GraphOutput, \
    UndefinedInput, NodeOutputProxy, UNDEFINED, GraphInputProxy, GraphInput


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


class NodeInputTest(TestCase):
    def test_init(self):
        node = OpNode(Op1, node_id='myop')
        node_input = NodeInput(node, 'x')
        self.assertIs(node_input.node, node)
        self.assertEqual(node_input.name, 'x')
        self.assertEqual(str(node_input), 'myop.x')
        with self.assertRaises(AssertionError):
            NodeInput(node, 'a')

    def test_source(self):
        node = OpNode(Op1, node_id='myop')
        node_input = NodeInput(node, 'x')
        self.assertIsInstance(node_input.source, UndefinedInput)
        source = ConstantInput(2.9)
        node_input.join(source)
        self.assertIs(node_input.source, source)
        with self.assertRaises(AssertionError):
            node_input.join(None)


class NodeOutputTest(TestCase):
    def test_init(self):
        node = OpNode(Op1, node_id='myop')
        node_output = NodeOutput(node, 'y')
        self.assertIs(node_output.node, node)
        self.assertEqual(node_output.name, 'y')
        self.assertEqual(str(node_output), 'myop.y')
        with self.assertRaises(AssertionError):
            NodeOutput(node, 'x')

    def test_targets(self):
        node = OpNode(Op1)
        node_output = NodeOutput(node, 'y')
        self.assertEqual(node_output.targets, [])
        node_output.add_target('A')
        node_output.add_target('B')
        self.assertEqual(node_output.targets, ['A', 'B'])
        self.assertEqual(node_output.targets, ['A', 'B'])
        node_output.remove_target('A')
        self.assertEqual(node_output.targets, ['B'])
        node_output.remove_target('B')
        self.assertEqual(node_output.targets, [])
        with self.assertRaises(AssertionError):
            node_output.add_target(None)
        with self.assertRaises(AssertionError):
            node_output.remove_target(None)


class GraphOutputTest(TestCase):
    def test_init(self):
        graph = Graph(graph_id='mygraph')
        graph_output = GraphOutput(graph, 'y')
        self.assertIs(graph_output.graph, graph)
        self.assertEqual(graph_output.name, 'y')
        self.assertEqual(str(graph_output), 'mygraph.y')

    def test_source(self):
        graph = Graph(graph_id='mygraph')
        graph_output = GraphOutput(graph, 'y')
        self.assertIsInstance(graph_output.source, UndefinedInput)
        source = ConstantInput(2.9)
        graph_output.join(source)
        self.assertIs(graph_output.source, source)
        with self.assertRaises(AssertionError):
            graph_output.join(None)


class NodeTest(TestCase):
    def test_init(self):
        node = OpNode(Op3)

        self.assertRegex(node.id, 'OpNode[0-9]+')

        self.assertTrue(len(node.input), 2)
        self.assertTrue(len(node.output), 1)

        self.assertTrue(hasattr(node.input, 'u'))
        self.assertIsInstance(node.input.u, NodeInput)
        self.assertIs(node.input.u.node, node)
        self.assertEqual(node.input.u.name, 'u')
        self.assertIsInstance(node.input.u.source, UndefinedInput)

        self.assertTrue(hasattr(node.input, 'v'))
        self.assertIsInstance(node.input.v, NodeInput)
        self.assertIs(node.input.v.node, node)
        self.assertEqual(node.input.v.name, 'v')
        self.assertIsInstance(node.input.v.source, UndefinedInput)

        self.assertTrue(hasattr(node.output, 'w'))
        self.assertIsInstance(node.output.w, NodeOutput)
        self.assertIs(node.output.w.node, node)
        self.assertEqual(node.output.w.name, 'w')
        self.assertEqual(node.output.w.targets, [])

    def test_init_operation_and_name_are_equivalent(self):
        node3 = OpNode(Op3)
        self.assertIsNotNone(node3.op)
        self.assertIsNotNone(node3.op_meta_info)
        node31 = OpNode(object_to_qualified_name(Op3))
        self.assertIs(node31.op, node3.op)
        self.assertIs(node31.op_meta_info, node3.op_meta_info)

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

    def test_join(self):
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

    def assertConnectionsAreOk(self, node1, node2, node3):
        self.assertIsInstance(node1.input.x.source, UndefinedInput)
        self.assertEqual(node1.output.y.targets, [node2.input.a, node3.input.u])

        self.assertIs(node2.input.a.source, node1.output.y)
        self.assertEqual(node2.output.b.targets, [node3.input.v])

        self.assertIs(node3.input.u.source, node1.output.y)
        self.assertIs(node3.input.v.source, node2.output.b)
        self.assertEqual(node3.output.w.targets, [])

    def test_disjoin(self):
        node1 = OpNode(Op1)
        node2 = OpNode(Op2)
        node3 = OpNode(Op3)

        node2.input.a.join(node1.output.y)
        node3.input.u.join(node1.output.y)
        node3.input.v.join(node2.output.b)
        self.assertConnectionsAreOk(node1, node2, node3)

        node3.input.v.disjoin()

        self.assertIsInstance(node1.input.x.source, UndefinedInput)
        self.assertEqual(node1.output.y.targets, [node2.input.a, node3.input.u])
        self.assertIs(node2.input.a.source, node1.output.y)
        self.assertEqual(node2.output.b.targets, [])
        self.assertIs(node3.input.u.source, node1.output.y)
        self.assertIsInstance(node3.input.v.source, UndefinedInput)
        self.assertEqual(node3.output.w.targets, [])

        node2.input.a.disjoin()

        self.assertIsInstance(node1.input.x.source, UndefinedInput)
        self.assertEqual(node1.output.y.targets, [node3.input.u])
        self.assertIsInstance(node2.input.a.source, UndefinedInput)
        self.assertEqual(node2.output.b.targets, [])
        self.assertIs(node3.input.u.source, node1.output.y)
        self.assertIsInstance(node3.input.v.source, UndefinedInput)
        self.assertEqual(node3.output.w.targets, [])

        node3.input.u.disjoin()

        self.assertIsInstance(node1.input.x.source, UndefinedInput)
        self.assertEqual(node1.output.y.targets, [])
        self.assertIsInstance(node2.input.a.source, UndefinedInput)
        self.assertEqual(node2.output.b.targets, [])
        self.assertIsInstance(node3.input.u.source, UndefinedInput)
        self.assertIsInstance(node3.input.v.source, UndefinedInput)
        self.assertEqual(node3.output.w.targets, [])

    def test_to_json_dict(self):
        node3 = OpNode(Op3, node_id='op3')
        node3.input.u = 2.8

        node3_dict = node3.to_json_dict()

        expected_json_text = """
        {
            "id": "op3",
            "op": "test.test_workflow.Op3",
            "input": {
                "v": {"undefined": true},
                "u": {"constant": 2.8}
            }
        }
        """

        actual_json_text = json.dumps(node3_dict)

        expected_json_obj = json.loads(expected_json_text)
        actual_json_obj = json.loads(actual_json_text)

        self.assertEqual(actual_json_obj, expected_json_obj,
                         msg='\n%sexpected:\n%s\n%s\nbut got:\n%s\n' %
                             (120 * '-', expected_json_text,
                              120 * '-', actual_json_text))

    def test_from_json_dict_const_param(self):
        json_text = """
        {
            "id": "op3",
            "op": "test.test_workflow.Op3",
            "input": {
                "u": {"undefined": true},
                "v": {"external": true}
            }
        }
        """

        json_dict = json.loads(json_text)

        node3 = OpNode.from_json_dict(json_dict)

        self.assertIsNotNone(node3)
        self.assertEqual(node3.id, "op3")
        self.assertIsInstance(node3.op, OpRegistration)
        self.assertIn('u', node3.input)
        self.assertIn('v', node3.input)
        self.assertIn('w', node3.output)
        self.assertIsInstance(node3.input.u, NodeInput)
        self.assertIsInstance(node3.input.v, NodeInput)
        self.assertIsInstance(node3.output.w, NodeOutput)
        self.assertIsInstance(node3.input.u.source, UndefinedInput)
        self.assertIsInstance(node3.input.v.source, ExternalInput)

        self.assertIs(node3.input.u.source.value, UNDEFINED)
        self.assertIs(node3.input.v.source.value, UNDEFINED)

    def test_from_json_dict_output_of_param(self):
        json_text = """
        {
            "id": "op3",
            "op": "test.test_workflow.Op3",
            "input": {
                "u": {"output_of": "stat_op.stats"},
                "v": {"constant": "nearest"}
            }
        }
        """

        json_dict = json.loads(json_text)

        node3 = OpNode.from_json_dict(json_dict)

        self.assertIsNotNone(node3)
        self.assertEqual(node3.id, "op3")
        self.assertIsInstance(node3.op, OpRegistration)
        self.assertIn('u', node3.input)
        self.assertIn('v', node3.input)
        self.assertIn('w', node3.output)
        self.assertIsInstance(node3.input.u, NodeInput)
        self.assertIsInstance(node3.input.v, NodeInput)
        self.assertIsInstance(node3.output.w, NodeOutput)
        self.assertIsInstance(node3.input.u.source, NodeOutputProxy)
        self.assertIsInstance(node3.input.v.source, ConstantInput)

        self.assertEqual(node3.input.u.source.node_id, 'stat_op')
        self.assertEqual(node3.input.u.source.node_output_name, 'stats')
        self.assertEqual(node3.input.v.source.value, 'nearest')


class GraphTest(TestCase):
    def test_init(self):
        node1 = OpNode(Op1, node_id='op1')
        node2 = OpNode(Op2, node_id='op2')
        node3 = OpNode(Op3, node_id='op3')
        graph = Graph(op_meta_info=OpMetaInfo('mygraph', input=dict(p={}), output=dict(q={})))
        graph.add_nodes(node1, node2, node3)
        node1.input.x = graph.input.p
        node2.input.a = node1.output.y
        node3.input.u = node1.output.y
        node3.input.v = node2.output.b
        graph.output.q = node3.output.w

        self.assertEqual(len(graph.input), 1)
        self.assertEqual(len(graph.output), 1)
        self.assertIn('p', graph.input)
        self.assertIn('q', graph.output)

        self.assertEqual(graph.nodes, [node1, node2, node3])

        self.assertIsInstance(graph.input.p, GraphInput)
        self.assertIsInstance(graph.input.p.source, ExternalInput)
        self.assertIsInstance(node1.input.x, NodeInput)
        self.assertIsInstance(node1.input.x.source, GraphInput)

        self.assertIsInstance(graph.output.q, GraphOutput)
        self.assertIsInstance(graph.output.q.source, NodeOutput)

    def test_graph_invocation(self):
        node1 = OpNode(Op1, node_id='op1')
        node2 = OpNode(Op2, node_id='op2')
        node3 = OpNode(Op3, node_id='op3')
        graph = Graph(op_meta_info=OpMetaInfo('mygraph', input=dict(p={}), output=dict(q={})))
        graph.add_nodes(node1, node2, node3)
        node1.input.x = graph.input.p
        node2.input.a = node1.output.y
        node3.input.u = node1.output.y
        node3.input.v = node2.output.b
        graph.output.q = node3.output.w

        graph.input.p = 3
        return_value = graph.invoke()
        output_value = graph.output.q.value
        self.assertEqual(return_value, None)
        self.assertEqual(output_value, 2 * (3 + 1) + 3 * (2 * (3 + 1)))

    def test_to_json_dict(self):
        node1 = OpNode(Op1, node_id='op1')
        node2 = OpNode(Op2, node_id='op2')
        node3 = OpNode(Op3, node_id='op3')
        graph = Graph(op_meta_info=OpMetaInfo('graph', input=dict(p={}), output=dict(q={})), graph_id='my_workflow')
        graph.add_nodes(node1, node2, node3)
        node1.input.x = graph.input.p
        node2.input.a = node1.output.y
        node3.input.u = node1.output.y
        node3.input.v = node2.output.b
        graph.output.q = node3.output.w

        graph_dict = graph.to_json_dict()

        expected_json_text = """
        {
            "id": "my_workflow",
            "input": {
                "p": {"external": true}
            },
            "output": {
                "q": {"output_of": "op3.w"}
            },
            "nodes": [
                {
                    "id": "op1",
                    "op": "test.test_workflow.Op1",
                    "input": {
                        "x": { "input_from": "p" }
                    }
                },
                {
                    "id": "op2",
                    "op": "test.test_workflow.Op2",
                    "input": {
                        "a": {"output_of": "op1.y"}
                    }
                },
                {
                    "id": "op3",
                    "op": "test.test_workflow.Op3",
                    "input": {
                        "v": {"output_of": "op2.b"},
                        "u": {"output_of": "op1.y"}
                    }
                }
            ]
        }
        """

        actual_json_text = json.dumps(graph_dict, indent=4)
        expected_json_obj = json.loads(expected_json_text)
        actual_json_obj = json.loads(actual_json_text)

        self.assertEqual(actual_json_obj, expected_json_obj,
                         msg='\nexpected:\n%s\n%s\nbut got:\n%s\n%s\n' %
                             (120 * '-', expected_json_text,
                              120 * '-', actual_json_text))

    def test_from_json_dict(self):
        graph_json_text = """
        {
            "id": "my_workflow",
            "input": {
                "p": {"external": true}
            },
            "output": {
                "q": {"output_of": "op3.w"}
            },
            "nodes": [
                {
                    "id": "op1",
                    "op": "test.test_workflow.Op1",
                    "input": {
                        "x": { "input_from": "p" }
                    }
                },
                {
                    "id": "op2",
                    "op": "test.test_workflow.Op2",
                    "input": {
                        "a": {"output_of": "op1.y"}
                    }
                },
                {
                    "id": "op3",
                    "op": "test.test_workflow.Op3",
                    "input": {
                        "v": {"output_of": "op2.b"},
                        "u": {"output_of": "op1.y"}
                    }
                }
            ]
        }
        """
        graph_json_dict = json.loads(graph_json_text)
        graph = Graph.from_json_dict(graph_json_dict)

        self.assertIsNotNone(graph)
        self.assertEqual(graph.id, "my_workflow")
        self.assertEqual(len(graph.nodes), 3)

        self.assertEqual(len(graph.input), 1)
        self.assertEqual(len(graph.output), 1)

        self.assertIn('p', graph.input)
        self.assertIn('q', graph.output)

        self.assertIsInstance(graph.input.p, NodeInput)
        self.assertIsInstance(graph.input.p.source, ExternalInput)
        self.assertIsInstance(graph.input.q, GraphOutput)
        self.assertIsInstance(graph.input.q.source, GraphOutput)

        node1 = graph.nodes[0]
        node2 = graph.nodes[1]
        node3 = graph.nodes[2]

        self.assertEqual(node1.id, 'op1')
        self.assertEqual(node2.id, 'op2')
        self.assertEqual(node3.id, 'op3')

