from collections import OrderedDict
from unittest import TestCase

from ect.core.node import Connection, Connector, OpNode, Graph
from ect.core.op import op_input, op_output
from ect.core.util import object_to_qualified_name


@op_input('x')
@op_output('y')
class Op1:
    pass


@op_input('a')
@op_output('b')
class Op2:
    pass


@op_input('u')
@op_input('v')
@op_output('w')
class Op3:
    pass


class ConnectorTest(TestCase):
    def test_init(self):
        node = OpNode(Op1)
        input_connector = Connector(node, 'x', True)
        output_connector = Connector(node, 'y', False)
        self.assertIs(input_connector.node, node)
        self.assertEqual(input_connector.name, 'x')
        self.assertEqual(input_connector.is_input, True)
        self.assertIs(output_connector.node, node)
        self.assertEqual(output_connector.name, 'y')
        self.assertEqual(output_connector.is_input, False)
        with self.assertRaises(ValueError):
            Connector(node, 'a', True)
        with self.assertRaises(ValueError):
            Connector(node, 'y', True)
        with self.assertRaises(ValueError):
            Connector(node, 'x', False)

    def test_eq(self):
        node1 = OpNode(Op3)
        node2 = OpNode(Op3)
        self.assertEqual(Connector(node1, 'u', True), Connector(node1, 'u', True))
        self.assertNotEqual(Connector(node2, 'u', True), Connector(node1, 'u', True))
        self.assertNotEqual(Connector(node1, 'v', True), Connector(node1, 'u', True))
        self.assertNotEqual(Connector(node1, 'w', False), Connector(node1, 'u', True))


class ConnectionTest(TestCase):
    def test_init(self):
        node1 = OpNode(Op1)
        node2 = OpNode(Op2)
        output_connector = Connector(node1, 'y', False)
        input_connector = Connector(node2, 'a', True)
        connection = Connection(output_connector, input_connector)
        self.assertIs(connection.output_connector, output_connector)
        self.assertIs(connection.input_connector, input_connector)
        with self.assertRaises(ValueError):
            Connection(input_connector, input_connector)
        with self.assertRaises(ValueError):
            Connection(input_connector, output_connector)

    def test_eq(self):
        node1 = OpNode(Op1)
        node2 = OpNode(Op2)
        node3 = OpNode(Op2)
        self.assertEqual(Connection(Connector(node1, 'y', False), Connector(node2, 'a', True)),
                         Connection(Connector(node1, 'y', False), Connector(node2, 'a', True)))
        self.assertNotEqual(Connection(Connector(node1, 'y', False), Connector(node2, 'a', True)),
                            Connection(Connector(node1, 'y', False), Connector(node3, 'a', True)))


# noinspection PyUnresolvedReferences
class NodeTest(TestCase):
    def test_init(self):
        node = OpNode(Op3)

        self.assertTrue(len(node.id) >= 14)
        self.assertEqual(node.id[:14], 'test_node.Op3#')

        self.assertTrue(len(node.input), 2)
        self.assertTrue(len(node.output), 1)

        self.assertTrue(hasattr(node.input, 'u'))
        self.assertIsInstance(node.input.u, Connector)
        self.assertIs(node.input.u.node, node)
        self.assertEqual(node.input.u.name, 'u')
        self.assertEqual(node.input.u.value, None)
        self.assertTrue(node.input.u.is_input)

        self.assertTrue(hasattr(node.input, 'v'))
        self.assertIsInstance(node.input.v, Connector)
        self.assertIs(node.input.v.node, node)
        self.assertEqual(node.input.v.name, 'v')
        self.assertTrue(node.input.v.is_input)

        self.assertTrue(hasattr(node.output, 'w'))
        self.assertIsInstance(node.output.w, Connector)
        self.assertIs(node.output.w.node, node)
        self.assertEqual(node.output.w.name, 'w')
        self.assertFalse(node.output.w.is_input)


    def test_init_operation_or_name_is_equivalent(self):
        node1 = OpNode(Op3)
        self.assertIsNotNone(node1.operation)
        self.assertIsNotNone(node1.op_meta_info)
        node2 = OpNode(object_to_qualified_name(Op3))
        self.assertIs(node2.operation, node1.operation)
        self.assertIs(node2.op_meta_info, node1.op_meta_info)

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

    def test_link_to(self):
        node1 = OpNode(Op1)
        node2 = OpNode(Op2)
        node3 = OpNode(Op3)
        node1.output.y.link(node2.input.a)
        node1.output.y.link(node3.input.u)
        node2.output.b.link(node3.input.v)
        self.assert_links(node1, node2, node3)

        node1 = OpNode(Op1)
        node2 = OpNode(Op2)

        with self.assertRaises(AttributeError):
            # "AttributeError: 'y' is an output and cannot be set"
            node1.output.y = node2.input.a

        with self.assertRaises(AttributeError):
            # "AttributeError: 'a' is not an output"
            node1.input.x = node2.input.a

    def test_link_from(self):
        node1 = OpNode(Op1)
        node2 = OpNode(Op2)
        node3 = OpNode(Op3)
        node2.input.a.link(node1.output.y)
        node3.input.u.link(node1.output.y)
        node3.input.v.link(node2.output.b)
        self.assert_links(node1, node2, node3)

        node1 = OpNode(Op1)
        node2 = OpNode(Op2)
        node3 = OpNode(Op3)
        node2.input.a = node1.output.y
        node3.input.u = node1.output.y
        node3.input.v = node2.output.b
        self.assert_links(node1, node2, node3)

    def assert_links(self, node1, node2, node3):
        self.assertEqual(node1.input_connections,
                         [])
        self.assertEqual(node1.output_connections,
                         [Connection(Connector(node1, 'y', False),
                                     Connector(node2, 'a', True)),
                          Connection(Connector(node1, 'y', False),
                                     Connector(node3, 'u', True))])

        self.assertEqual(node2.input_connections,
                         [Connection(Connector(node1, 'y', False),
                                     Connector(node2, 'a', True))])
        self.assertEqual(node2.output_connections,
                         [Connection(Connector(node2, 'b', False),
                                     Connector(node3, 'v', True))])

        self.assertEqual(node3.input_connections,
                         [Connection(Connector(node1, 'y', False),
                                     Connector(node3, 'u', True)),
                          Connection(Connector(node2, 'b', False),
                                     Connector(node3, 'v', True))])
        self.assertEqual(node3.output_connections,
                         [])

    def test_graph(self):

        node1 = OpNode(Op1)
        node2 = OpNode(Op2)
        node3 = OpNode(Op3)
        node2.input.a = node1.output.y
        node3.input.u = node1.output.y
        node3.input.v = node2.output.b

        graph = Graph(node1, node2, node3)
        self.assertEqual(graph.nodes, [node1, node2, node3])

        graph.gen_io()


    def test_json(self):
        node1 = OpNode(Op1, node_id='Op1')
        node2 = OpNode(Op2, node_id='Op2')
        node3 = OpNode(Op3, node_id='Op3')
        node2.input.a = node1.output.y
        node3.input.u = node1.output.y
        node3.input.v = node2.output.b

        nodes = [node1, node2, node3]
        graph = []
        for node in nodes:

            inputs = OrderedDict()
            for ic in node.input_connections:
                icoc = ic.output_connector
                icic = ic.input_connector
                inputs[icic.name] = icoc.node.id + '.' + icoc.name
            graph.append({'node': {'id': node.id, 'inputs': inputs}})

        import pprint
        pprint.pprint(graph)
