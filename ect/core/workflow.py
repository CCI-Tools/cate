"""
Module Description
==================

Provides classes that are used to construct processing workflows (networks) from registered operations and connected
graphs of such.

This module provides the following data types:
* A :py:class:`Node` has zero or more ``InputConnector``s and zero or more ``OutputConnector``s
* A :py:class:`OpNode` is a ``Node`` that wraps an executable operation.
* A :py:class:`Graph` is a ``Node``that contains other ``Node``s
* A :py:class:`InputConnector` belongs to a ``Node``, has a (parameter) name, and has a ``source`` property that points
  to a connected ``OutputConnector`` of a source node.
* A :py:class:`OutputConnector` belongs to a ``Node``, has a (parameter) name, and has a ``targets`` property (a list)
  that points to all connected ``InputConnector`` of other target nodes.

Module Reference
================
"""

from abc import ABCMeta, abstractproperty, abstractmethod
from collections import OrderedDict
from typing import List

from ect.core import Monitor
from .op import REGISTRY, OpMetaInfo, OpRegistration
from .util import Namespace


class Node(metaclass=ABCMeta):
    """
    Nodes can be used to construct networks or graphs of operations.
    Input and output of an operation are available as node attributes of type :py:class:`Connector`.

    :param op_meta_info: Meta-information about the operation, see :py:class:`OpMetaInfo`.
    :param node_id: A node ID. If None, a unique name will be generated.
    """

    def __init__(self, op_meta_info: OpMetaInfo, node_id=None):
        if not op_meta_info:
            raise ValueError('op_meta_info must be given')
        node_id = node_id if node_id is not None else 'Node#' + str(id(self))
        self._id = node_id
        self._op_meta_info = op_meta_info
        self._input_connectors = InputConnectors([InputConnector(self, name)
                                                  for name, _ in op_meta_info.input])
        self._output_connectors = OutputConnectors([OutputConnector(self, name)
                                                    for name, _ in op_meta_info.output])

    @property
    def id(self):
        """The node's ID."""
        return self._id

    @property
    def op_meta_info(self) -> OpMetaInfo:
        """The operation's meta-information."""
        return self._op_meta_info

    @property
    def input(self):
        """The node's input connectors."""
        return self._input_connectors

    @property
    def output(self):
        """The node's output connectors."""
        return self._output_connectors

    @abstractmethod
    def invoke(self, monitor: Monitor = Monitor.NULL):
        """
        Invoke this node's underlying operation with input values from
        :py:property:`input`. Output values in :py:property:`output` will
        be set from the underlying operation's return value(s).

        :param monitor: An optional progress monitor.
        """

    @abstractmethod
    def to_json_dict(self):
        """
        Return a JSON-serializable dictionary representation of this object.

        :return: A JSON-serializable dictionary
        """


class OpNode(Node):
    """
    Nodes can be used to construct networks or graphs of operations.
    Input and output of an operation are available as node attributes of type :py:class:`Connector`.

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
    def op(self):
        """The operation registration. See :py:class:`ect.core.op.OpRegistration`"""
        return self._op_registration

    def invoke(self, monitor: Monitor = Monitor.NULL):
        """
        Invoke this node's underlying operation :py:property:`op` with input values from
        :py:property:`input`. Output values in :py:property:`output` will
        be set from the underlying operation's return value(s).

        :param monitor: An optional progress monitor.
        """
        input_values = OrderedDict()
        for input_name, _ in self.op_meta_info.input:
            input_values[input_name] = None
        for input_connector in self.input[:]:
            if input_connector.source is not None:
                input_value = input_connector.source.value
            else:
                input_value = input_connector.value
            input_values[input_connector.name] = input_value

        return_value = self._op_registration(monitor=Monitor.NULL, **input_values)

        if self.op_meta_info.output_value_is_dict:
            for output_name, output_value in return_value.items():
                self.output[output_name].value = output_value
        else:
            self.output[OpMetaInfo.RETURN_OUTPUT_NAME].value = return_value

    def to_json_dict(self):
        """
        Return a JSON-serializable dictionary representation of this object.

        :return: A JSON-serializable dictionary
        """
        node_input_dict = OrderedDict()
        for input_connector in self.input[:]:
            source = input_connector.source
            value = input_connector.value
            if source is not None:
                node_input_dict[input_connector.name] = (source.node.id, source.name)
            else:
                # Care: input_connector.value may not be JSON-serializable!!!
                # Must add converter callback, or so.
                node_input_dict[input_connector.name] = value
        node_dict = OrderedDict()
        node_dict['id'] = self.id
        node_dict['op'] = self.op_meta_info.qualified_name
        node_dict['input'] = node_input_dict
        return node_dict

    def __str__(self):
        return "OpNode('%s')" % self.op_meta_info.qualified_name

    def __repr__(self):
        return "OpNode('%s')" % self.op_meta_info.qualified_name


class Graph(Node):
    """
    A graph of (connected) nodes.

    :param nodes: Nodes to be added. Must be compatible with the :py:class:`Node` class.
    :param graph_id: An optional ID for the graph.
    :param op_meta_info: An optional OpMetaInfo object. If not provided, a basic stump will be generated.
    """

    def __init__(self, *nodes, graph_id: str = None, op_meta_info: OpMetaInfo = None):
        graph_id = graph_id if graph_id is not None else 'Graph#' + str(id(self))
        op_meta_info = op_meta_info if op_meta_info is not None else OpMetaInfo(graph_id)
        super(Graph, self).__init__(op_meta_info, node_id=graph_id)
        self._nodes = list(nodes)
        for node in nodes:
            self._update_on_node_added(node)

    @property
    def nodes(self):
        return list(self._nodes)

    def invoke(self, monitor=Monitor.NULL):
        """
        Invoke this graph by invoking all all of its nodes.
        The node invocation order is determined by the input requirements of individual nodes.

        :param monitor: An optional progress monitor.
        """
        monitor.start('executing graph ' + self.id, len(self.nodes))
        for node in self.nodes:
            node.invoke(monitor.child(1))
        monitor.done()

    def to_json_dict(self):
        """
        Return a JSON-serializable dictionary representation of this object.

        :return: A JSON-serializable dictionary
        """
        graph_nodes_list = []
        for node in self._nodes:
            graph_nodes_list.append(node.to_json_dict())

        return {'graph': graph_nodes_list}

    def __str__(self):
        return "Graph('%s')" % self.op_meta_info.qualified_name

    def __repr__(self):
        return "Graph('%s')" % self.op_meta_info.qualified_name

    def _update_on_node_added(self, node):
        graph_meta_info = self.op_meta_info
        node_meta_info = node.op_meta_info
        for input_connector in node.input[:]:
            if not input_connector.source:
                print('input:', input_connector)
                name = input_connector.name
                # Make sure graph meta_info is correct
                if name not in graph_meta_info.input:
                    graph_meta_info.input[name] = dict(node_meta_info.input[name])
                # Add input
                self.input[name] = input_connector
        for output_connector in node.output[:]:
            if not output_connector.targets:
                print('output:', output_connector)
                name = output_connector.name
                # Make sure graph meta_info is correct
                if name not in graph_meta_info.output:
                    graph_meta_info.output[name] = dict(node_meta_info.output[name])
                # Add output
                self.output[name] = output_connector


class Connector(metaclass=ABCMeta):
    """
    An endpoint of a :py:class:`Connection` between two :py:class:`Node`s.

    :param node: The node
    :param name: Name of an input or output of the node's operation.
    """

    def __init__(self, node: Node, name: str):
        self._node = node
        self._name = name
        # experimental
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
        """Set the *value*."""
        self._value = value

    @abstractproperty
    def is_input(self) -> bool:
        """``True`` for input connectors, ``False`` for output connectors."""

    @abstractmethod
    def join(self, other: 'Connector'):
        """
        Create a connection by joining this connector with another one.

        :param other: The other connector.
        :raise ValueError: if *other* cannot be joined with this one.
        """

    @abstractmethod
    def disjoin(self):
        """
        Remove a connection which uses this connector.
        """

    def __hash__(self):
        return hash((self.node, self.name, self.is_input))

    def __eq__(self, other):
        return self.node is other.node \
               and self.name == other.name \
               and self.is_input == other.is_input

    def __ne__(self, other):
        # Not strictly necessary, but to avoid having both x==y and x!=y
        # True at the same time
        return not self.__eq__(other)


class InputConnector(Connector):
    """
    An endpoint of a :py:class:`Connection` between two :py:class:`Node`s.

    :param node: The node
    :param name: Name of an input or output of the node's operation.
    """

    def __init__(self, node: Node, name: str):
        meta_info = node.op_meta_info
        if name not in meta_info.input:
            raise ValueError(
                "'%s' is not an input of operation '%s'" % (name, meta_info.qualified_name))
        super(InputConnector, self).__init__(node, name)
        self._source = None

    @property
    def source(self) -> 'OutputConnector':
        """The source of this input connector, or ``None`` if it is not connected."""
        return self._source

    @property
    def is_input(self) -> bool:
        """Always ``True``."""
        return True

    def join(self, other: 'OutputConnector'):
        if other.is_input:
            raise ValueError('other must be an output connector')
        # Note: perform sanity checks here, e.g. to avoid close loops by self-referencing
        self._source = other
        other._targets.append(self)

    def disjoin(self):
        if self._source is not None:
            self._source._targets.remove(self)
            self._source = None

    def __str__(self):
        return '%s.input.%s' % (self.node.op_meta_info.qualified_name, self.name)

    def __repr__(self):
        return "InputConnector(%s, '%s')" % (self.node, self.name)


class OutputConnector(Connector):
    """
    An endpoint of a :py:class:`Connection` between two :py:class:`Node`s.

    :param node: The node
    :param name: Name of an input or output of the node's operation.
    """

    def __init__(self, node: Node, name: str):
        meta_info = node.op_meta_info
        if name not in meta_info.output:
            raise ValueError(
                "'%s' is not an output of operation '%s'" % (name, meta_info.qualified_name))
        super(OutputConnector, self).__init__(node, name)
        self._targets = []

    @property
    def targets(self) -> List[InputConnector]:
        """The targets of this output connector."""
        return list(self._targets)

    @property
    def is_input(self) -> bool:
        """Always ``False``."""
        return False

    def join(self, other: InputConnector):
        if not other.is_input:
            raise ValueError('other must be an input connector')
        # Note: perform sanity checks here, e.g. to avoid close loops by self-referencing
        other._source = self
        if other not in self._targets:
            self._targets.append(other)

    def disjoin(self):
        raise NotImplementedError()

    def __str__(self):
        return '%s.output.%s' % (self.node.op_meta_info.qualified_name, self.name)

    def __repr__(self):
        return "OutputConnector(%s, '%s')" % (self.node, self.name)


# todo nf - rename to InputConnectorNamespace to get rid of the plural

class InputConnectors(Namespace):
    def __init__(self, connectors):
        super(InputConnectors, self).__init__([(connector.name, connector) for connector in connectors])

    def __setattr__(self, name, value):
        connector = self.__getattr__(name)
        if isinstance(value, Connector):
            if value.is_input:
                raise AttributeError("input '%s' expects an output" % name)
            value.join(connector)
        else:
            connector.value = value

    def __getattr__(self, name) -> 'InputConnector':
        try:
            return super(InputConnectors, self).__getattr__(name)
        except AttributeError:
            raise AttributeError("'%s' is not an input" % name)

    def __delattr__(self, name):
        raise NotImplementedError()


# todo nf - rename to OutputConnectorNamespace to get rid of the plural

class OutputConnectors(Namespace):
    def __init__(self, connectors):
        super(OutputConnectors, self).__init__([(connector.name, connector) for connector in connectors])

    def __setattr__(self, name, value):
        raise AttributeError("'%s' is an output and cannot be set" % name)

    def __getattr__(self, name) -> 'OutputConnector':
        try:
            return super(OutputConnectors, self).__getattr__(name)
        except AttributeError:
            raise AttributeError("'%s' is not an output" % name)

    def __delattr__(self, name):
        raise NotImplementedError()
