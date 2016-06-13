import json
import os.path
from collections import OrderedDict
from unittest import TestCase

from ect.core.graph import OpNode, Graph, GraphNode, NodeConnector, ExprNode
from ect.core.op import op_input, op_output, OpRegistration, OpMetaInfo
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


def get_resource(rel_path):
    return os.path.join(os.path.dirname(__file__), rel_path).replace('\\', '/')


class GraphTest(TestCase):
    def test_init(self):
        node1 = OpNode(Op1, node_id='op1')
        node2 = OpNode(Op2, node_id='op2')
        node3 = OpNode(Op3, node_id='op3')
        graph = Graph(OpMetaInfo('mygraph', input_dict=OrderedDict(p={}), output_dict=OrderedDict(q={})))
        graph.add_nodes(node1, node2, node3)
        node1.input.x.source = graph.input.p
        node2.input.a.source = node1.output.y
        node3.input.u.source = node1.output.y
        node3.input.v.source = node2.output.b
        graph.output.q.source = node3.output.w

        self.assertEqual(graph.id, 'mygraph')
        self.assertEqual(len(graph.input), 1)
        self.assertEqual(len(graph.output), 1)
        self.assertIn('p', graph.input)
        self.assertIn('q', graph.output)

        self.assertEqual(graph.nodes, [node1, node2, node3])

        self.assertIsNone(graph.input.p.source)
        self.assertIsNone(graph.input.p.value)

        self.assertIs(graph.output.q.source, node3.output.w)
        self.assertIsNone(graph.output.q.value)

        self.assertEqual(str(graph), graph.id)
        self.assertEqual(repr(graph), "Graph('mygraph')")

    def test_invoke(self):
        node1 = OpNode(Op1, node_id='op1')
        node2 = OpNode(Op2, node_id='op2')
        node3 = OpNode(Op3, node_id='op3')
        graph = Graph(OpMetaInfo('mygraph', input_dict=OrderedDict(p={}), output_dict=OrderedDict(q={})))
        graph.add_nodes(node1, node2, node3)
        node1.input.x.source = graph.input.p
        node2.input.a.source = node1.output.y
        node3.input.u.source = node1.output.y
        node3.input.v.source = node2.output.b
        graph.output.q.source = node3.output.w

        graph.input.p.value = 3
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
                "p": {"description": "Input 'p'"}
            },
            "output": {
                "q": {"source": "op3.w", "description": "Output 'q'"}
            },
            "nodes": [
                {
                    "id": "op1",
                    "op": "test.test_graph.Op1",
                    "input": {
                        "x": { "source": ".p" }
                    }
                },
                {
                    "id": "op2",
                    "op": "test.test_graph.Op2",
                    "input": {
                        "a": {"source": "op1"}
                    }
                },
                {
                    "id": "op3",
                    "op": "test.test_graph.Op3",
                    "input": {
                        "u": {"source": "op1.y"},
                        "v": {"source": "op2.b"}
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
        self.assertEqual(graph.op_meta_info.input['p'], dict(description="Input 'p'"))
        self.assertEqual(graph.op_meta_info.output['q'], dict(source="op3.w", description="Output 'q'"))

        self.assertEqual(len(graph.input), 1)
        self.assertEqual(len(graph.output), 1)

        self.assertIn('p', graph.input)
        self.assertIn('q', graph.output)

        self.assertEqual(len(graph.nodes), 3)
        node1 = graph.nodes[0]
        node2 = graph.nodes[1]
        node3 = graph.nodes[2]

        self.assertEqual(node1.id, 'op1')
        self.assertEqual(node2.id, 'op2')
        self.assertEqual(node3.id, 'op3')

        self.assertIs(node1.input.x.source, graph.input.p)
        self.assertIs(node2.input.a.source, node1.output.y)
        self.assertIs(node3.input.u.source, node1.output.y)
        self.assertIs(node3.input.v.source, node2.output.b)
        self.assertIs(graph.output.q.source, node3.output.w)

    def test_from_json_dict_empty(self):
        json_dict = json.loads('{"qualified_name": "hello"}')
        graph = Graph.from_json_dict(json_dict)
        self.assertEqual(graph.id, 'hello')

    def test_from_json_dict_invalids(self):
        json_dict = json.loads('{"header": {}}')
        with self.assertRaises(ValueError) as cm:
            Graph.from_json_dict(json_dict)
        self.assertEqual(str(cm.exception), 'missing mandatory property "qualified_name" in graph JSON')

    def test_to_json_dict(self):
        node1 = OpNode(Op1, node_id='op1')
        node2 = OpNode(Op2, node_id='op2')
        node3 = OpNode(Op3, node_id='op3')
        graph = Graph(OpMetaInfo('my_workflow', input_dict=OrderedDict(p={}), output_dict=OrderedDict(q={})))
        graph.add_nodes(node1, node2, node3)
        node1.input.x.source = graph.input.p
        node2.input.a.source = node1.output.y
        node3.input.u.source = node1.output.y
        node3.input.v.source = node2.output.b
        graph.output.q.source = node3.output.w

        graph_dict = graph.to_json_dict()

        expected_json_text = """
        {
            "qualified_name": "my_workflow",
            "input": {
                "p": {}
            },
            "output": {
                "q": {"source": "op3.w"}
            },
            "nodes": [
                {
                    "id": "op1",
                    "op": "test.test_graph.Op1",
                    "input": {
                        "x": { "source": "my_workflow.p" }
                    },
                    "output": {
                        "y": {}
                    }
                },
                {
                    "id": "op2",
                    "op": "test.test_graph.Op2",
                    "input": {
                        "a": {"source": "op1.y"}
                    },
                    "output": {
                        "b": {}
                    }
                },
                {
                    "id": "op3",
                    "op": "test.test_graph.Op3",
                    "input": {
                        "v": {"source": "op2.b"},
                        "u": {"source": "op1.y"}
                    },
                    "output": {
                        "w": {}
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


class ExprNodeTest(TestCase):
    expression = "dict(x = 1 + 2 * a, y = 3 * b ** 2 + 4 * c ** 3)"

    def test_init(self):
        node = ExprNode(self.expression, OrderedDict(a={}, b={}, c={}), OrderedDict(x={}, y={}), node_id='bibo_8')
        self.assertEqual(node.id, 'bibo_8')
        self.assertEqual(node.expression, self.expression)
        self.assertEqual(str(node), 'bibo_8')
        self.assertEqual(repr(node), "ExprNode('%s', node_id='bibo_8')" % self.expression)

        node = ExprNode(self.expression)
        self.assertEqual(node.op_meta_info.input, {})
        self.assertEqual(node.op_meta_info.output, {'return': {}})

    def test_from_json_dict(self):
        json_text = """
        {
            "id": "bibo_8",
            "input": {
                "a": {},
                "b": {},
                "c": {}
            },
            "output": {
                "x": {},
                "y": {}
            },
            "expression": "%s"
        }
        """ % self.expression

        json_dict = json.loads(json_text)

        node = ExprNode.from_json_dict(json_dict)

        self.assertIsInstance(node, ExprNode)
        self.assertEqual(node.id, "bibo_8")
        self.assertEqual(node.expression, self.expression)
        self.assertIn('a', node.input)
        self.assertIn('b', node.input)
        self.assertIn('c', node.input)
        self.assertIn('x', node.output)
        self.assertIn('y', node.output)

    def test_to_json_dict(self):
        expected_json_text = """
        {
            "id": "bibo_8",
            "input": {
                "a": {},
                "b": {},
                "c": {}
            },
            "output": {
                "x": {},
                "y": {}
            },
            "expression": "%s"
        }
        """ % self.expression

        node = ExprNode(self.expression, OrderedDict(a={}, b={}, c={}), OrderedDict(x={}, y={}), node_id='bibo_8')
        actual_json_dict = node.to_json_dict()

        actual_json_text = json.dumps(actual_json_dict, indent=4)
        expected_json_dict = json.loads(expected_json_text)
        actual_json_dict = json.loads(actual_json_text)

        self.assertEqual(actual_json_dict, expected_json_dict,
                         msg='\n%sexpected:\n%s\n%s\nbut got:\n%s\n' %
                             (120 * '-', expected_json_text,
                              120 * '-', actual_json_text))

    def test_invoke(self):
        node = ExprNode(self.expression, OrderedDict(a={}, b={}, c={}), OrderedDict(x={}, y={}), node_id='bibo_8')
        a = 1.5
        b = -2.6
        c = 4.3
        node.input.a.value = a
        node.input.b.value = b
        node.input.c.value = c
        return_value = node.invoke()
        output_value_x = node.output.x.value
        output_value_y = node.output.y.value
        self.assertEqual(return_value, None)
        self.assertEqual(output_value_x, 1 + 2 * a)
        self.assertEqual(output_value_y, 3 * b ** 2 + 4 * c ** 3)

    def test_invoke_from_graph(self):
        resource = get_resource('graphs/test_graph_expr.json')
        graph = Graph.load(resource)
        a = 1.5
        b = -2.6
        c = 4.3
        graph.input.a.value = a
        graph.input.b.value = b
        graph.input.c.value = c
        return_value = graph.invoke()
        output_value_x = graph.output.x.value
        output_value_y = graph.output.y.value
        self.assertEqual(return_value, None)
        self.assertEqual(output_value_x, 1 + 2 * a)
        self.assertEqual(output_value_y, 3 * b ** 2 + 4 * c ** 3)


class GraphNodeTest(TestCase):
    def test_init(self):
        resource = get_resource('graphs/test_graph_3ops.json')
        graph = Graph.load(resource)
        node = GraphNode(graph, resource, node_id='jojo_87')
        self.assertEqual(node.id, 'jojo_87')
        self.assertEqual(node.resource, resource)
        self.assertEqual(str(node), 'jojo_87')
        self.assertEqual(repr(node), "GraphNode(Graph('cool_graph'), '%s', node_id='jojo_87')" % resource)

        self.assertIsNotNone(node.graph)
        self.assertIn('p', node.graph.input)
        self.assertIn('q', node.graph.output)

    def test_from_json_dict(self):
        resource = get_resource('graphs/test_graph_3ops.json')
        json_text = """
        {
            "id": "graph_ref_89",
            "graph": "%s",
            "input": {
                "p": {"value": 2.8}
            }
        }
        """ % resource

        json_dict = json.loads(json_text)

        node = GraphNode.from_json_dict(json_dict)

        self.assertIsInstance(node, GraphNode)
        self.assertEqual(node.id, "graph_ref_89")
        self.assertEqual(node.resource, resource)
        self.assertIn('p', node.input)
        self.assertIn('q', node.output)
        self.assertEqual(node.input.p.value, 2.8)
        self.assertEqual(node.output.q.value, None)

        self.assertIsNotNone(node.graph)
        self.assertIn('p', node.graph.input)
        self.assertIn('q', node.graph.output)

        self.assertIs(node.graph.input.p.source, node.input.p)

    def test_to_json_dict(self):
        resource = get_resource('graphs/test_graph_3ops.json')
        graph = Graph.load(resource)
        node = GraphNode(graph, resource, node_id='jojo_87')
        actual_json_dict = node.to_json_dict()

        expected_json_text = """
        {
            "id": "jojo_87",
            "graph": "%s",
            "input": {
                "p": {}
            },
            "output": {
                "q": {}
            }
        }
        """ % resource

        actual_json_text = json.dumps(actual_json_dict, indent=4)
        expected_json_dict = json.loads(expected_json_text)
        actual_json_dict = json.loads(actual_json_text)

        self.assertEqual(actual_json_dict, expected_json_dict,
                         msg='\n%sexpected:\n%s\n%s\nbut got:\n%s\n' %
                             (120 * '-', expected_json_text,
                              120 * '-', actual_json_text))

    def test_invoke(self):
        resource = get_resource('graphs/test_graph_3ops.json')
        graph = Graph.load(resource)
        node = GraphNode(graph, resource, node_id='jojo_87')

        node.input.p.value = 3
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
        self.assertIs(node.input.u.node, node)
        self.assertEqual(node.input.u.name, 'u')

        self.assertTrue(hasattr(node.input, 'v'))
        self.assertIs(node.input.v.node, node)
        self.assertEqual(node.input.v.name, 'v')

        self.assertTrue(hasattr(node.output, 'w'))
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
        node1.input.x.value = 3
        return_value = node1.invoke()
        output_value = node1.output.y.value
        self.assertEqual(return_value, None)
        self.assertEqual(output_value, 3 + 1)

        node2 = OpNode(Op2)
        node2.input.a.value = 3
        return_value = node2.invoke()
        output_value = node2.output.b.value
        self.assertEqual(return_value, None)
        self.assertEqual(output_value, 2 * 3)

        node3 = OpNode(Op3)
        node3.input.u.value = 4
        node3.input.v.value = 5
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
        node2.input.a.source = node1.output.y
        node3.input.u.source = node1.output.y
        node3.input.v.source = node2.output.b
        self.assertConnectionsAreOk(node1, node2, node3)

        with self.assertRaises(AttributeError) as cm:
            node1.input.a.source = node3.input.u
        self.assertEqual(str(cm.exception), "attribute 'a' not found")

    def test_disconnect_source(self):
        node1 = OpNode(Op1)
        node2 = OpNode(Op2)
        node3 = OpNode(Op3)

        node2.input.a.source = node1.output.y
        node3.input.u.source = node1.output.y
        node3.input.v.source = node2.output.b
        self.assertConnectionsAreOk(node1, node2, node3)

        node3.input.v.source = None

        self.assertIs(node2.input.a.source, node1.output.y)
        self.assertIs(node3.input.u.source, node1.output.y)

        node2.input.a.source = None

        self.assertIs(node3.input.u.source, node1.output.y)

        node3.input.u.source = None

    def assertConnectionsAreOk(self, node1, node2, node3):
        self.assertIs(node2.input.a.source, node1.output.y)
        self.assertIs(node3.input.u.source, node1.output.y)
        self.assertIs(node3.input.v.source, node2.output.b)

    def test_from_json_dict_value(self):
        json_text = """
        {
            "id": "op3",
            "op": "test.test_graph.Op3",
            "input": {
                "u": {"value": 647},
                "v": {"value": 2.9}
            }
        }
        """

        json_dict = json.loads(json_text)

        node3 = OpNode.from_json_dict(json_dict)

        self.assertIsInstance(node3, OpNode)
        self.assertEqual(node3.id, "op3")
        self.assertIsInstance(node3.op, OpRegistration)
        self.assertIn('u', node3.input)
        self.assertIn('v', node3.input)
        self.assertIn('w', node3.output)

        self.assertEqual(node3.input.u.value, 647)
        self.assertEqual(node3.input.v.value, 2.9)

    def test_from_json_dict_source(self):
        json_text = """
        {
            "id": "op3",
            "op": "test.test_graph.Op3",
            "input": {
                "u": {"source": "stat_op.stats"},
                "v": {"source": ".latitude"}
            }
        }
        """

        json_dict = json.loads(json_text)

        node3 = OpNode.from_json_dict(json_dict)

        self.assertIsInstance(node3, OpNode)
        self.assertEqual(node3.id, "op3")
        self.assertIsInstance(node3.op, OpRegistration)
        self.assertIn('u', node3.input)
        self.assertIn('v', node3.input)
        self.assertIn('w', node3.output)
        u_source = node3.input.u.source
        v_source = node3.input.v.source
        self.assertEqual(node3.input.u._source_ref, ('stat_op', 'stats'))
        self.assertEqual(node3.input.u.source, None)
        self.assertEqual(node3.input.v._source_ref, (None, 'latitude'))
        self.assertEqual(node3.input.v.source, None)

    def test_to_json_dict(self):
        node3 = OpNode(Op3, node_id='op3')
        node3.input.u.value = 2.8

        node3_dict = node3.to_json_dict()

        expected_json_text = """
        {
            "id": "op3",
            "op": "test.test_graph.Op3",
            "input": {
                "v": {},
                "u": {"value": 2.8}
            },
            "output": {
                "w": {}
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


class NodeConnectorTest(TestCase):
    def test_init(self):
        node = OpNode(Op1, node_id='myop')
        source = NodeConnector(node, 'x')

        self.assertIs(source.node, node)
        self.assertEqual(source.node_id, 'myop')
        self.assertEqual(source.name, 'x')
        self.assertEqual(source.source, None)
        self.assertEqual(source.value, None)
        self.assertEqual(str(source), 'myop.x')
        self.assertEqual(repr(source), "NodeConnector('myop', 'x')")

    def test_resolve_source_ref(self):
        node1 = OpNode(Op1, node_id='myop1')
        connector1 = node1.output.y

        node2 = OpNode(Op2, node_id='myop2')

        node2.input.a._source_ref = ('myop1', 'y')

        g = Graph(OpMetaInfo('mygraph', has_monitor=True, input_dict=OrderedDict(x={}), output_dict=OrderedDict(b={})))
        g.add_nodes(node1, node2)

        node2.input.a.resolve_source_ref()

        self.assertEqual(node2.input.a._source_ref, ('myop1', 'y'))
        self.assertIs(node2.input.a.source, node1.output.y)
        self.assertIs(node2.input.a.value, None)

    def test_from_json_dict(self):
        node2 = OpNode(Op2, node_id='myop2')
        connector2 = NodeConnector(node2, 'a')

        connector2.from_json_dict(json.loads('{"a": {"value": 2.6}}'))
        self.assertEqual(connector2._source_ref, None)
        self.assertEqual(connector2._source, None)
        self.assertEqual(connector2._value, 2.6)

        connector2.from_json_dict(json.loads('{"a": {"source": "myop1.y"}}'))
        self.assertEqual(connector2._source_ref, ('myop1', 'y'))
        self.assertEqual(connector2._source, None)
        self.assertEqual(connector2._value, None)

        # "myop1.y" is a shorthand for {"source": "myop1.y"}
        connector2.from_json_dict(json.loads('{"a": "myop1.y"}'))
        self.assertEqual(connector2._source_ref, ('myop1', 'y'))
        self.assertEqual(connector2._source, None)
        self.assertEqual(connector2._value, None)

        connector2.from_json_dict(json.loads('{"a": {"source": ".y"}}'))
        self.assertEqual(connector2._source_ref, (None, 'y'))
        self.assertEqual(connector2._source, None)
        self.assertEqual(connector2._value, None)

        # ".x" is a shorthand for {"source": ".x"}
        connector2.from_json_dict(json.loads('{"a": ".y"}'))
        self.assertEqual(connector2._source_ref, (None, 'y'))
        self.assertEqual(connector2._source, None)
        self.assertEqual(connector2._value, None)

        # "myop1" is a shorthand for {"source": "myop1"}
        connector2.from_json_dict(json.loads('{"a": "myop1"}'))
        self.assertEqual(connector2._source_ref, ('myop1', None))
        self.assertEqual(connector2._source, None)
        self.assertEqual(connector2._value, None)

        # if "a" is defined, but neither "source" nor "value" is given, it will neither have a source nor a value
        connector2.from_json_dict(json.loads('{"a": {}}'))
        self.assertEqual(connector2._source_ref, None)
        self.assertEqual(connector2._source, None)
        self.assertEqual(connector2._value, None)
        connector2.from_json_dict(json.loads('{"a": null}'))
        self.assertEqual(connector2._source_ref, None)
        self.assertEqual(connector2._source, None)
        self.assertEqual(connector2._value, None)

        # if "a" is not defined at all, it will neither have a source nor a value
        connector2.from_json_dict(json.loads('{}'))
        self.assertEqual(connector2._source_ref, None)
        self.assertEqual(connector2._source, None)
        self.assertEqual(connector2._value, None)

        with self.assertRaises(ValueError) as cm:
            connector2.from_json_dict(json.loads('{"a": {"value": 2.6, "source": "y"}}'))
        self.assertEqual(str(cm.exception),
                         "error decoding 'myop2.a' because \"source\" and \"value\" are mutually exclusive")

        expected_msg = "error decoding 'myop2.a' because the \"source\" value format is " \
                       "neither \"<node-id>.<name>\", \"<node-id>\", nor \".<name>\""

        with self.assertRaises(ValueError) as cm:
            connector2.from_json_dict(json.loads('{"a": {"source": ""}}'))
        self.assertEqual(str(cm.exception), expected_msg)

        with self.assertRaises(ValueError) as cm:
            connector2.from_json_dict(json.loads('{"a": {"source": "."}}'))
        self.assertEqual(str(cm.exception), expected_msg)

        with self.assertRaises(ValueError) as cm:
            connector2.from_json_dict(json.loads('{"a": {"source": "var."}}'))
        self.assertEqual(str(cm.exception), expected_msg)

    def test_to_json_dict(self):
        node1 = OpNode(Op1, node_id='myop1')
        node2 = OpNode(Op2, node_id='myop2')

        self.assertEqual(node2.input.a.to_json_dict(), dict())

        node2.input.a.value = 982
        self.assertEqual(node2.input.a.to_json_dict(), dict(value=982))

        node2.input.a.source = node1.output.y
        self.assertEqual(node2.input.a.to_json_dict(), dict(source='myop1.y'))

        node2.input.a.source = None
        self.assertEqual(node2.input.a.to_json_dict(), dict())
