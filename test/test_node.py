from unittest import TestCase

from ect.core.node import Connection, Connector, Node
from ect.core.op import Op, input, output


@input('x')
@output('y')
class Op1(Op):
    pass


@input('a')
@output('b')
class Op2(Op):
    pass


@input('u')
@input('v')
@output('w')
class Op3(Op):
    pass


class ConnectorTest(TestCase):
    def test_init(self):
        node = Node(Op1)
        input_connector = Connector(node, 'x', True)
        output_connector = Connector(node, 'y', False)
        self.assertIs(input_connector.node, node)
        self.assertEqual(input_connector.name, 'x')
        self.assertEqual(input_connector.is_input, True)
        self.assertIs(output_connector.node, node)
        self.assertEqual(output_connector.name, 'y')
        self.assertEqual(output_connector.is_input, False)
        with self.assertRaises(ValueError):
            connection = Connector(node, 'a', True)
        with self.assertRaises(ValueError):
            connection = Connector(node, 'y', True)
        with self.assertRaises(ValueError):
            connection = Connector(node, 'x', False)

    def test_eq(self):
        node1 = Node(Op3)
        node2 = Node(Op3)
        self.assertEqual(Connector(node1, 'u', True), Connector(node1, 'u', True))
        self.assertNotEqual(Connector(node2, 'u', True), Connector(node1, 'u', True))
        self.assertNotEqual(Connector(node1, 'v', True), Connector(node1, 'u', True))
        self.assertNotEqual(Connector(node1, 'w', False), Connector(node1, 'u', True))


class ConnectionTest(TestCase):
    def test_init(self):
        node1 = Node(Op1)
        node2 = Node(Op2)
        output_connector = Connector(node1, 'y', False)
        input_connector = Connector(node2, 'a', True)
        connection = Connection(output_connector, input_connector)
        self.assertIs(connection.output_connector, output_connector)
        self.assertIs(connection.input_connector, input_connector)
        with self.assertRaises(ValueError):
            connection = Connection(input_connector, input_connector)
        with self.assertRaises(ValueError):
            connection = Connection(input_connector, output_connector)

    def test_eq(self):
        node1 = Node(Op1)
        node2 = Node(Op2)
        node3 = Node(Op2)
        self.assertEqual(Connection(Connector(node1, 'y', False), Connector(node2, 'a', True)),
                         Connection(Connector(node1, 'y', False), Connector(node2, 'a', True)))
        self.assertNotEqual(Connection(Connector(node1, 'y', False), Connector(node2, 'a', True)),
                            Connection(Connector(node1, 'y', False), Connector(node3, 'a', True)))


class NodeTest(TestCase):
    def test_init(self):
        node = Node(Op3)

        self.assertIn('u', node.__dict__)
        self.assertIsInstance(node.u, Connector)
        self.assertIs(node.u.node, node)
        self.assertEqual(node.u.name, 'u')
        self.assertTrue(node.u.is_input)

        self.assertIn('v', node.__dict__)
        self.assertIsInstance(node.v, Connector)
        self.assertIs(node.v.node, node)
        self.assertEqual(node.v.name, 'v')
        self.assertTrue(node.v.is_input)

        self.assertIn('w', node.__dict__)
        self.assertIsInstance(node.w, Connector)
        self.assertIs(node.w.node, node)
        self.assertEqual(node.w.name, 'w')
        self.assertFalse(node.w.is_input)

        with self.assertRaises(TypeError):
            node = Node(NodeTest)

    def test_link_to(self):
        node1 = Node(Op1)
        node2 = Node(Op2)
        node3 = Node(Op3)
        node1.y.link(node2.a)
        node1.y.link(node3.u)
        node2.b.link(node3.v)
        self.assert_links(node1, node2, node3)

    def test_link_from(self):
        node1 = Node(Op1)
        node2 = Node(Op2)
        node3 = Node(Op3)
        node2.a.link(node1.y)
        node3.u.link(node1.y)
        node3.v.link(node2.b)
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
