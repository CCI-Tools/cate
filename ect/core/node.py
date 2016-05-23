"""
Module Description
==================

Provides classes that are used to construct processing networks / workflows from registered operations.

This module provides the following data types:
* A :py:class:`Net` has named input/output ``Connector``s and contains ``Node``s
* A :py:class:`Node` has named input/output ``Connector``s and contains input/output ``Connections``s
  from/to other ``Node``s
* A :py:class:`Connection` comprises two ``Connector``s from one ``Node`` to another
* A :py:class:`Connector` belongs to a `Node` or `Net` and is either an input or an output, and has a (parameter) name

Module Reference
================
"""

from abc import ABCMeta, abstractmethod
from collections import OrderedDict
from typing import List

from .op import REGISTRY, OpMetaInfo, OpRegistration


class _ConnectorsProperty(metaclass=ABCMeta):
    def __init__(self, connectors):
        object.__setattr__(self, '_connectors', OrderedDict())
        self.add_connectors(connectors)

    def add_connectors(self, new_connectors):
        connectors = object.__getattribute__(self, '_connectors')
        for new_connector in new_connectors:
            connectors[new_connector.name] = new_connector

    def __contains__(self, item):
        connectors = object.__getattribute__(self, '_connectors')
        return connectors.contains(item)

    def __len__(self):
        connectors = object.__getattribute__(self, '_connectors')
        return len(connectors)

    def __iter__(self):
        connectors = object.__getattribute__(self, '_connectors')
        return iter(connectors.values())

    def __setitem__(self, key, value):
        return self.__setattr__(key, value)

    def __getitem__(self, key):
        return self.__getattr__(key)

    @abstractmethod
    def __setattr__(self, key, value):
        pass

    @abstractmethod
    def __getattr__(self, key):
        pass


class _InputConnectorsProperty(_ConnectorsProperty):
    def __init__(self, connectors):
        super(_InputConnectorsProperty, self).__init__(connectors)

    def __setattr__(self, key, value):
        connectors = object.__getattribute__(self, '_connectors')
        if key in connectors:
            connector = connectors[key]
            if isinstance(value, Connector):
                if value.is_input:
                    raise AttributeError("'%s' is not an output" % value.name)
                value.link(connector)
            else:
                connector.value = value
        else:
            raise AttributeError("'%s' is not an input" % key)

    def __getattr__(self, key):
        connectors = object.__getattribute__(self, '_connectors')
        if key in connectors:
            return connectors[key]
        raise AttributeError("input '%s' not found" % key)


class _OutputConnectorsProperty(_ConnectorsProperty):
    def __init__(self, connectors):
        super(_OutputConnectorsProperty, self).__init__(connectors)

    def __setattr__(self, key, value):
        raise AttributeError("'%s' is an output and cannot be set" % key)

    def __getattr__(self, key):
        connectors = object.__getattribute__(self, '_connectors')
        if key in connectors:
            return connectors[key]
        raise AttributeError("output '%s' not found" % key)


class Node(metaclass=ABCMeta):
    """
    Nodes can be used to construct networks or graphs of operations.
    Input and outputs of an operation are available as node attributes of type :py:class:`Connector`.

    :param operation: A fully qualified operation name or operation object such as a class or callable.
    :param registry: An operation registry to be used to lookup the operation, if given by name..
    :param node_id: A node ID. If None, a unique ID will be generated.
    """

    def __init__(self, op_meta_info, node_id=None):
        if not op_meta_info:
            raise ValueError('op_meta_info must be given')
        node_id = node_id if node_id is not None else 'Node#' + str(id(self))
        self._id = node_id
        self._op_meta_info = op_meta_info
        self._input_connections = []
        self._output_connections = []
        self._input_connectors = _InputConnectorsProperty([Connector(self, name, True)
                                                           for name in op_meta_info.inputs.keys()])
        self._output_connectors = _OutputConnectorsProperty([Connector(self, name, False)
                                                             for name in op_meta_info.outputs.keys()])

    @property
    def id(self):
        """The node's ID."""
        return self._id

    @property
    def op_meta_info(self) -> OpMetaInfo:
        """The operation's meta-information."""
        return self._op_meta_info

    @property
    def input_connections(self) -> List['Connection']:
        """The node's input connections."""
        return self._input_connections

    @property
    def output_connections(self) -> List['Connection']:
        """The node's output connections."""
        return self._output_connections

    @property
    def input(self):
        """The node's input connectors."""
        return self._input_connectors

    @property
    def output(self):
        """The node's output connectors."""
        return self._output_connectors


class Graph(Node):
    def __init__(self, *nodes, graph_id=None, op_meta_info=None):
        graph_id = graph_id if graph_id is not None else 'Graph#' + str(id(self))
        op_meta_info = op_meta_info if op_meta_info is not None else OpMetaInfo(graph_id)
        super(Graph, self).__init__(op_meta_info, node_id=graph_id)
        self._nodes = list(nodes)

    @property
    def nodes(self):
        return self._nodes

    def gen_io(self):
        """
        Generate the inputs and outputs from unconnected inputs and outputs.
        """
        input_connectors = []
        output_connectors = []
        for node in self._nodes:
            for input_connector in node.input:
                name = input_connector.name
                if name not in self.op_meta_info.inputs:
                    self.op_meta_info.inputs[name] = node.op_meta_info.inputs[name]
                input_connectors.append(connector)
            for output_connector in node.output:
                name = output.name
                connector = node.output[name]
                if name not in self.op_meta_info.inputs:
                    self.op_meta_info.outputs[name] = node.op_meta_info.outputs[name]
                output_connectors.append(connector)
        self.input.add_connectors(input_connectors)
        self.output.add_connectors(output_connectors)


class OpNode(Node):
    """
    Nodes can be used to construct networks or graphs of operations.
    Input and outputs of an operation are available as node attributes of type :py:class:`Connector`.

    :param operation: A fully qualified operation name or operation object such as a class or callable.
    :param registry: An operation registry to be used to lookup the operation, if given by name..
    :param node_id: A node ID. If None, a unique ID will be generated.
    """

    def __init__(self, operation, registry=REGISTRY, node_id=None):
        if not operation:
            raise ValueError('operation must be given')
        if isinstance(operation, str):
            op_registration = registry.get_op(operation, fail_if_not_exists=True)
        elif isinstance(operation, OpRegistration):
            op_registration = operation
        else:
            op_registration = registry.get_op(operation, fail_if_not_exists=True)
        assert op_registration is not None
        op_meta_info = op_registration.meta_info
        node_id = node_id if node_id is not None else op_meta_info.qualified_name + '#' + str(id(self))
        super(OpNode, self).__init__(op_meta_info, node_id=node_id)
        self._op_registration = op_registration

    @property
    def operation(self):
        """The operation."""
        return self._op_registration.operation


class Connector:
    """
    An endpoint of a :py:class:`Connection` between two :py:class:`Node`s.

    :param node: The node
    :param name: Name of an input or output of the node's operation.
    :param is_input: ``True`` for input connectors, ``False`` for output connectors.
    """

    def __init__(self, node: Node, name: str, is_input: bool):
        meta_info = node.op_meta_info
        if is_input and name not in meta_info.inputs:
            raise ValueError(
                '%s is not an input of operation %s' % (name, meta_info.qualified_name))
        if not is_input and name not in meta_info.outputs:
            raise ValueError(
                '%s is not an output of operation %s' % (name, meta_info.qualified_name))
        self._node = node
        self._name = name
        self._is_input = is_input
        # experimental:
        self._value = None

    @property
    def node(self) -> Node:
        """The connector's node."""
        return self._node

    @property
    def name(self) -> str:
        """The connector's name."""
        return self._name

    @property
    def value(self):
        """The value."""
        return self._value

    @value.setter
    def value(self, value):
        """Set the value."""
        self._value = value

    @property
    def is_input(self) -> bool:
        """``True`` for input connectors, ``False`` for output connectors."""
        return self._is_input

    def link(self, other: 'Connector') -> 'Connection':
        """
        Create a connection by linking this connector with another :py:class:`Connector`.

        :param other: The other connector.
        :return: A new connection.
        """
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
        return '%s:%s.%s' % ('in' if self.is_input else 'out', self.node.op_meta_info.qualified_name, self.name)


class Connection:
    """
    A connection between the output connector of a first :py:class:`Node` and the input connector of
    a second :py:class:`Node`.

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
        """The output :py:class:`Connector` of the first node."""
        return self._output_connector

    @property
    def input_connector(self) -> Connector:
        """The input :py:class:`Connector` of the second node."""
        return self._input_connector

    def unlink(self):
        """
        Remove this connection between two :py:class:`Connector`s this connection is made of.
        """
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
