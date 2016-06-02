import json
import os.path
from collections import OrderedDict
from unittest import TestCase

from ect.core.graph import NodeInput, NodeOutput, OpNode, ParameterSource, ConstantSource, Graph, GraphOutput, \
    UndefinedSource, NodeOutputRef, GraphInput, GraphInputRef, GraphFileNode
from ect.core.op import op_input, op_output, OpRegistration, OpMetaInfo, UNDEFINED
from ect.core.util import object_to_qualified_name


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


class GraphFileNodeTest(TestCase):
    file_path = os.path.join(os.path.dirname(__file__), 'test_graph_g1.json').replace('\\', '/')

    def test_init(self):
        node = GraphFileNode(self.file_path, node_id='jojo_87')
        self.assertEqual(node.id, 'jojo_87')
        self.assertEqual(node.file_path, self.file_path)
        self.assertEqual(str(node), 'jojo_87')
        self.assertEqual(repr(node), "GraphFileNode('%s', node_id='jojo_87')" % self.file_path)

        self.assertIsNotNone(node.graph)
        self.assertIn('p', node.graph.input)
        self.assertIn('q', node.graph.output)

    def test_from_json_dict(self):
        json_text = """
        {
            "id": "graph_ref_89",
            "graph": "%s",
            "input": {
                "p": {"constant": 2.8}
            }
        }
        """ % self.file_path

        json_dict = json.loads(json_text)

        node = GraphFileNode.from_json_dict(json_dict)

        self.assertIsNotNone(node)
        self.assertEqual(node.id, "graph_ref_89")
        self.assertEqual(node.file_path, self.file_path)
        self.assertIn('p', node.input)
        self.assertIn('q', node.output)
        self.assertIsInstance(node.input.p, NodeInput)
        self.assertIsInstance(node.input.p.source, ConstantSource)
        self.assertEqual(node.input.p.source.value, 2.8)
        self.assertIsInstance(node.output.q, NodeOutput)
        self.assertEqual(node.output.q.value, UNDEFINED)

        self.assertIsNotNone(node.graph)
        self.assertIn('p', node.graph.input)
        self.assertIn('q', node.graph.output)

        self.assertIsInstance(node.graph.input.p, GraphInput)
        self.assertIsInstance(node.graph.input.p.source, NodeInput)
        self.assertIs(node.graph.input.p.source, node.input.p)
        self.assertIsInstance(node.graph.output.q, GraphOutput)

    def test_to_json_dict(self):
        node = GraphFileNode(self.file_path, node_id='jojo_87')
        actual_json_dict = node.to_json_dict()

        expected_json_text = """
        {
            "id": "jojo_87",
            "graph": "%s",
            "input": {
                "p": {"undefined": true}
            }
        }
        """ % self.file_path

        actual_json_text = json.dumps(actual_json_dict, indent=4)
        expected_json_dict = json.loads(expected_json_text)
        actual_json_dict = json.loads(actual_json_text)

        self.assertEqual(actual_json_dict, expected_json_dict,
                         msg='\n%sexpected:\n%s\n%s\nbut got:\n%s\n' %
                             (120 * '-', expected_json_text,
                              120 * '-', actual_json_text))

    def test_invoke(self):
        node = GraphFileNode(self.file_path, node_id='jojo_87')

        node.input.p = 3
        return_value = node.invoke()
        output_value = node.output.q.value
        self.assertEqual(return_value, None)
        self.assertEqual(output_value, 2 * (3 + 1) + 3 * (2 * (3 + 1)))


class OpNodeTest(TestCase):
    def test_init(self):
        node = OpNode(Op3)

        self.assertRegex(node.id, 'op_node_[0-9]+')

        self.assertTrue(len(node.input), 2)
        self.assertTrue(len(node.output), 1)

        self.assertTrue(hasattr(node.input, 'u'))
        self.assertIsInstance(node.input.u, NodeInput)
        self.assertIs(node.input.u.node, node)
        self.assertEqual(node.input.u.name, 'u')
        self.assertIsInstance(node.input.u.source, UndefinedSource)

        self.assertTrue(hasattr(node.input, 'v'))
        self.assertIsInstance(node.input.v, NodeInput)
        self.assertIs(node.input.v.node, node)
        self.assertEqual(node.input.v.name, 'v')
        self.assertIsInstance(node.input.v.source, UndefinedSource)

        self.assertTrue(hasattr(node.output, 'w'))
        self.assertIsInstance(node.output.w, NodeOutput)
        self.assertIs(node.output.w.node, node)
        self.assertEqual(node.output.w.name, 'w')

        self.assertEqual(str(node), node.id)
        self.assertEqual(repr(node), "OpNode(test.test_graph.Op3, node_id='%s')" % node.id)

    def test_init_operation_and_name_are_equivalent(self):
        node3 = OpNode(Op3)
        self.assertIsNotNone(node3.op)
        self.assertIsNotNone(node3.op_meta_info)
        node31 = OpNode(object_to_qualified_name(Op3))
        self.assertIs(node31.op, node3.op)
        self.assertIs(node31.op_meta_info, node3.op_meta_info)

    def test_invoke(self):
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
            OpNode(OpNodeTest)

        with self.assertRaises(ValueError):
            # "ValueError: operation with name 'X' not registered"
            OpNode('X')

        with self.assertRaises(ValueError):
            # "ValueError: operation with name 'X.Y' not registered"
            OpNode('X.Y')

        with self.assertRaises(ValueError):
            # "ValueError: operation must be given"
            OpNode(None)

    def test_connect_source(self):
        node1 = OpNode(Op1)
        node2 = OpNode(Op2)
        node3 = OpNode(Op3)
        node2.input.a.connect_source(node1.output.y)
        node3.input.u.connect_source(node1.output.y)
        node3.input.v.connect_source(node2.output.b)
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

    def test_disconnect_source(self):
        node1 = OpNode(Op1)
        node2 = OpNode(Op2)
        node3 = OpNode(Op3)

        node2.input.a.connect_source(node1.output.y)
        node3.input.u.connect_source(node1.output.y)
        node3.input.v.connect_source(node2.output.b)
        self.assertConnectionsAreOk(node1, node2, node3)

        node3.input.v.disconnect_source()

        self.assertIsInstance(node1.input.x.source, UndefinedSource)
        self.assertIsInstance(node2.input.a.source, NodeOutputRef)
        self.assertIsInstance(node3.input.u.source, NodeOutputRef)
        self.assertIs(node2.input.a.source.node_output, node1.output.y)
        self.assertIs(node3.input.u.source.node_output, node1.output.y)
        self.assertIsInstance(node3.input.v.source, UndefinedSource)

        node2.input.a.disconnect_source()

        self.assertIsInstance(node1.input.x.source, UndefinedSource)
        self.assertIsInstance(node2.input.a.source, UndefinedSource)
        self.assertIsInstance(node3.input.u.source, NodeOutputRef)
        self.assertIs(node3.input.u.source.node_output, node1.output.y)
        self.assertIsInstance(node3.input.v.source, UndefinedSource)

        node3.input.u.disconnect_source()

        self.assertIsInstance(node1.input.x.source, UndefinedSource)
        self.assertIsInstance(node2.input.a.source, UndefinedSource)
        self.assertIsInstance(node3.input.u.source, UndefinedSource)
        self.assertIsInstance(node3.input.v.source, UndefinedSource)

    def assertConnectionsAreOk(self, node1, node2, node3):
        self.assertIsInstance(node1.input.x.source, UndefinedSource)
        self.assertIsInstance(node2.input.a.source, NodeOutputRef)
        self.assertIsInstance(node3.input.u.source, NodeOutputRef)
        self.assertIsInstance(node3.input.v.source, NodeOutputRef)
        self.assertIs(node2.input.a.source.node_output, node1.output.y)
        self.assertIs(node3.input.u.source.node_output, node1.output.y)
        self.assertIs(node3.input.v.source.node_output, node2.output.b)

    def test_from_json_dict_const_param(self):
        json_text = """
        {
            "id": "op3",
            "op": "test.test_graph.Op3",
            "input": {
                "u": {"undefined": true},
                "v": {"parameter": true}
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
        self.assertIsInstance(node3.input.u.source, UndefinedSource)
        self.assertIsInstance(node3.input.v.source, ParameterSource)

        self.assertIs(node3.input.u.source.value, UNDEFINED)
        self.assertIs(node3.input.v.source.value, None)

    def test_from_json_dict_output_of_param(self):
        json_text = """
        {
            "id": "op3",
            "op": "test.test_graph.Op3",
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
        self.assertIsInstance(node3.input.u.source, NodeOutputRef)
        self.assertEqual(node3.input.u.source.node_id, 'stat_op')
        self.assertEqual(node3.input.u.source.name, 'stats')
        self.assertIsInstance(node3.input.v.source, ConstantSource)
        self.assertEqual(node3.input.v.source.value, 'nearest')

    def test_to_json_dict(self):
        node3 = OpNode(Op3, node_id='op3')
        node3.input.u = 2.8

        node3_dict = node3.to_json_dict()

        expected_json_text = """
        {
            "id": "op3",
            "op": "test.test_graph.Op3",
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


class GraphTest(TestCase):
    def test_init(self):
        node1 = OpNode(Op1, node_id='op1')
        node2 = OpNode(Op2, node_id='op2')
        node3 = OpNode(Op3, node_id='op3')
        graph = Graph(op_meta_info=OpMetaInfo('mygraph', input_dict=dict(p={}), output_dict=dict(q={})))
        graph.add_nodes(node1, node2, node3)
        node1.input.x = graph.input.p
        node2.input.a = node1.output.y
        node3.input.u = node1.output.y
        node3.input.v = node2.output.b
        graph.output.q = node3.output.w

        self.assertRegex(graph.id, 'graph_[0-9]+')
        self.assertEqual(len(graph.input), 1)
        self.assertEqual(len(graph.output), 1)
        self.assertIn('p', graph.input)
        self.assertIn('q', graph.output)

        self.assertEqual(graph.nodes, [node1, node2, node3])

        self.assertIsInstance(graph.input.p, GraphInput)
        self.assertIsInstance(graph.input.p.source, UndefinedSource)
        self.assertIsInstance(node1.input.x, NodeInput)
        self.assertIsInstance(node1.input.x.source, GraphInputRef)

        self.assertIsInstance(graph.output.q, GraphOutput)
        self.assertIsInstance(graph.output.q.source, NodeOutputRef)

        self.assertEqual(str(graph), graph.id)
        self.assertEqual(repr(graph), "Graph(OpMetaData('mygraph'), graph_id='%s')" % graph.id)

    def test_invoke(self):
        node1 = OpNode(Op1, node_id='op1')
        node2 = OpNode(Op2, node_id='op2')
        node3 = OpNode(Op3, node_id='op3')
        graph = Graph(op_meta_info=OpMetaInfo('mygraph', input_dict=dict(p={}), output_dict=dict(q={})))
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

    def test_from_json_dict(self):
        graph_json_text = """
        {
            "qualified_name": "my_workflow",
            "header": {
                "description": "My workflow is not bad."
            },
            "input": {
                "p": {"parameter": true, "description": "Input 'p'"}
            },
            "output": {
                "q": {"output_of": "op3.w", "description": "Output 'q'"}
            },
            "nodes": [
                {
                    "id": "op1",
                    "op": "test.test_graph.Op1",
                    "input": {
                        "x": { "input_from": "p" }
                    }
                },
                {
                    "id": "op2",
                    "op": "test.test_graph.Op2",
                    "input": {
                        "a": {"output_of": "op1.y"}
                    }
                },
                {
                    "id": "op3",
                    "op": "test.test_graph.Op3",
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

        self.assertEqual(graph.op_meta_info.qualified_name, graph.id)
        self.assertEqual(graph.op_meta_info.header, dict(description="My workflow is not bad."))
        self.assertEqual(len(graph.op_meta_info.input), 1)
        self.assertEqual(len(graph.op_meta_info.output), 1)
        self.assertEqual(graph.op_meta_info.input['p'], dict(parameter=True, description="Input 'p'"))
        self.assertEqual(graph.op_meta_info.output['q'], dict(output_of="op3.w", description="Output 'q'"))

        self.assertEqual(len(graph.input), 1)
        self.assertEqual(len(graph.output), 1)

        self.assertIn('p', graph.input)
        self.assertIn('q', graph.output)

        self.assertIsInstance(graph.input.p, NodeInput)
        self.assertIsInstance(graph.input.p.source, ParameterSource)
        self.assertIsInstance(graph.output.q, GraphOutput)
        self.assertIsInstance(graph.output.q.source, NodeOutputRef)

        self.assertEqual(len(graph.nodes), 3)
        node1 = graph.nodes[0]
        node2 = graph.nodes[1]
        node3 = graph.nodes[2]

        self.assertEqual(node1.id, 'op1')
        self.assertEqual(node2.id, 'op2')
        self.assertEqual(node3.id, 'op3')

    def test_to_json_dict(self):
        node1 = OpNode(Op1, node_id='op1')
        node2 = OpNode(Op2, node_id='op2')
        node3 = OpNode(Op3, node_id='op3')
        graph = Graph(op_meta_info=OpMetaInfo('my_workflow', input_dict=dict(p={}), output_dict=dict(q={})), graph_id='my_workflow')
        graph.add_nodes(node1, node2, node3)
        node1.input.x = graph.input.p
        node2.input.a = node1.output.y
        node3.input.u = node1.output.y
        node3.input.v = node2.output.b
        graph.output.q = node3.output.w

        graph_dict = graph.to_json_dict()

        expected_json_text = """
        {
            "qualified_name": "my_workflow",
            "input": {
                "p": {"undefined": true}
            },
            "output": {
                "q": {"output_of": "op3.w"}
            },
            "nodes": [
                {
                    "id": "op1",
                    "op": "test.test_graph.Op1",
                    "input": {
                        "x": { "input_from": "p" }
                    }
                },
                {
                    "id": "op2",
                    "op": "test.test_graph.Op2",
                    "input": {
                        "a": {"output_of": "op1.y"}
                    }
                },
                {
                    "id": "op3",
                    "op": "test.test_graph.Op3",
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


class NodeInputTest(TestCase):
    def test_init(self):
        node = OpNode(Op1, node_id='myop_17')
        node_input = NodeInput(node, 'x')
        self.assertIs(node_input.node, node)
        self.assertEqual(node_input.name, 'x')
        self.assertEqual(node_input.node_id, 'myop_17')
        self.assertEqual(str(node_input), 'myop_17.x')
        with self.assertRaises(AssertionError):
            NodeInput(node, 'a')

    def test_source(self):
        node = OpNode(Op1, node_id='myop_17')
        node_input = NodeInput(node, 'x')
        self.assertIsInstance(node_input.source, UndefinedSource)
        source = ConstantSource(2.9)
        node_input.connect_source(source)
        self.assertIs(node_input.source, source)
        node_input.connect_source(None)
        self.assertIsInstance(node_input.source, ConstantSource)
        self.assertIs(node_input.source.value, None)


class GraphInputTest(TestCase):
    def test_init(self):
        graph = Graph(OpMetaInfo('mygraph', input_dict=dict(x={})), graph_id='mygraph_10')
        graph_input = GraphInput(graph, 'x')
        self.assertIs(graph_input.node, graph)
        self.assertIs(graph_input.graph, graph)
        self.assertEqual(graph_input.name, 'x')
        self.assertEqual(graph_input.node_id, 'mygraph_10')
        self.assertEqual(str(graph_input), 'mygraph_10.x')
        with self.assertRaises(AssertionError):
            NodeInput(graph, 'a')


class NodeOutputTest(TestCase):
    def test_init(self):
        node = OpNode(Op1, node_id='myop')
        node_output = NodeOutput(node, 'y')
        self.assertIs(node_output.node, node)
        self.assertEqual(node_output.name, 'y')
        self.assertEqual(str(node_output), 'myop.y')
        with self.assertRaises(AssertionError):
            NodeOutput(node, 'x')


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
        self.assertIsInstance(graph_output.source, UndefinedSource)
        source = ConstantSource(2.9)
        graph_output.connect_source(source)
        self.assertIs(graph_output.source, source)
        graph_output.connect_source(None)
        self.assertIsInstance(graph_output.source, ConstantSource)
        self.assertIs(graph_output.source.value, None)


class UndefinedSourceTest(TestCase):
    def test_init(self):
        source = UndefinedSource()
        self.assertIs(source.value, UNDEFINED)
        self.assertEqual(str(source), 'UNDEFINED')
        self.assertEqual(repr(source), 'UndefinedSource()')


class ParameterSourceTest(TestCase):
    def test_init(self):
        source = ParameterSource()
        self.assertEqual(source.value, None)
        self.assertEqual(str(source), 'None')
        self.assertEqual(repr(source), 'ParameterSource(None)')

        source.set_value(3.14)
        self.assertEqual(source.value, 3.14)
        self.assertEqual(str(source), '3.14')
        self.assertEqual(repr(source), 'ParameterSource(3.14)')


class ConstantSourceTest(TestCase):
    def test_init(self):
        source = ConstantSource('ABC')
        self.assertEqual(source.value, 'ABC')
        self.assertEqual(str(source), 'ABC')
        self.assertEqual(repr(source), "ConstantSource('ABC')")

        source = ConstantSource(None)
        self.assertEqual(source.value, None)
        self.assertEqual(str(source), 'None')
        self.assertEqual(repr(source), 'ConstantSource(None)')
