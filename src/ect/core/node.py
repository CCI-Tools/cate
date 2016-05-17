from .op import Op


class Node:
    """
    Nodes can be used to construct networks or graphs of operations.
    Input and outputs of an operation are available as node attributes of type ``Connector``.

    :param op_class: A class derived from ``Op``
    """
    def __init__(self, op_class: Op):
        if not issubclass(op_class, Op):
            # Why would someone use issubclass() in Python, why not rely on duck typing?
            # Because it produces AttributeError all the way down, which are usually hard to trace.
            # We want type safety here. Just pass a subclass of Op, then you are fine.
            raise TypeError('op_class must be a subclass of %s:%s' % (Op.__module__, Op.__name__))
        self.op_class = op_class
        self.input_connections = []
        self.output_connections = []
        for name in op_class.get_inputs(deep=True).keys():
            self.__dict__[name] = Connector(self, name, True)
        for name in op_class.get_outputs(deep=True).keys():
            self.__dict__[name] = Connector(self, name, False)


class Connector:
    """
    An endpoint of a ``Connection`` between two ``Node``s.

    :param node: A ``Node`` instance
    :param name: Name of an input or output attribute of the node's ``op_class``.
    :param is_input:
    """
    def __init__(self, node: Node, name: str, is_input: bool):
        if is_input and name not in node.op_class.get_inputs(deep=True):
            raise ValueError('%s is not an input of operation %s' % (name, node.op_class.get_name()))
        if not is_input and name not in node.op_class.get_outputs():
            raise ValueError('%s is not an output of operation %s' % (name, node.op_class.get_name()))
        self._node = node
        self._name = name
        self._is_input = is_input

    @property
    def node(self) -> Node:
        return self._node

    @property
    def name(self) -> str:
        return self._name

    @property
    def is_input(self) -> bool:
        return self._is_input

    def link(self, other: 'Connector') -> 'Connection':
        if self.is_input:
            connection = Connection(other, self)
            self.node.input_connections.append(connection)
            other.node.output_connections.append(connection)
        else:
            connection = Connection(self, other)
            self.node.output_connections.append(connection)
            other.node.input_connections.append(connection)
        return connection

    def __hash__(self):
        return hash((self._node, self._name, self._is_input))

    def __eq__(self, other):
        return self._node is other.node \
               and self._name == other.name \
               and self._is_input == other.is_input

    def __ne__(self, other):
        # Not strictly necessary, but to avoid having both x==y and x!=y
        # True at the same time
        return not self.__eq__(other)

    def __str__(self):
        return '%s:%s.%s' % ('in' if self.is_input else 'out', self.node.op_class.get_name(), self.name)


class Connection:
    """
    A connection between two nodes.

    :param output_connector: The output connector of the first node.
    :param input_connector: The input connector of the second node.
    """

    def __init__(self,
                 output_connector: Connector,
                 input_connector: Connector):
        if output_connector.is_input:
            raise ValueError('output_connector must be an output')
        if not input_connector.is_input:
            raise ValueError('input_connector must be an input')
        self._output_connector = output_connector
        self._input_connector = input_connector

    @property
    def output_connector(self) -> Connector:
        return self._output_connector

    @property
    def input_connector(self) -> Connector:
        return self._input_connector

    def unlink(self):
        self.output_connector.node.output_connections.remove(self)
        self.input_connector.node.input_connections.remove(self)

    def __hash__(self):
        return hash((self._output_connector, self._input_connector))

    def __eq__(self, other):
        return self._output_connector == other.output_connector \
               and self._input_connector == other.input_connector

    def __ne__(self, other):
        # Not strictly necessary, but to avoid having both x==y and x!=y
        # True at the same time
        return not self.__eq__(other)

    def __str__(self):
        return '%s --> %s' % (self._output_connector, self._input_connector)
