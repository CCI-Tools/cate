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

from abc import ABCMeta, abstractproperty, abstractmethod
from typing import List

from .op import REGISTRY, OpMetaInfo, OpRegistration
from .util import Attributes


class Node(metaclass=ABCMeta):
    """
    Nodes can be used to construct networks or graphs of operations.
    Input and outputs of an operation are available as node attributes of type :py:class:`Connector`.

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
                                                  for name in op_meta_info.inputs.keys()])
        self._output_connectors = OutputConnectors([OutputConnector(self, name)
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
    def input(self):
        """The node's input connectors."""
        return self._input_connectors

    @property
    def output(self):
        """The node's output connectors."""
        return self._output_connectors


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

    def __str__(self):
        return "OpNode('%s')" % self.op_meta_info.qualified_name

    def __repr__(self):
        return "OpNode('%s')" % self.op_meta_info.qualified_name


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
        # input_connectors = []
        # output_connectors = []
        # for node in self._nodes:
        #     for input_connector in node.input:
        #         name = input_connector.name
        #         if name not in self.op_meta_info.inputs:
        #             self.op_meta_info.inputs[name] = node.op_meta_info.inputs[name]
        #         input_connectors.append(connector)
        #     for output_connector in node.output:
        #         name = output.name
        #         connector = node.output[name]
        #         if name not in self.op_meta_info.inputs:
        #             self.op_meta_info.outputs[name] = node.op_meta_info.outputs[name]
        #         output_connectors.append(connector)
        # self.input.add_connectors(input_connectors)
        # self.output.add_connectors(output_connectors)
        pass

    def __str__(self):
        return "Graph('%s')" % self.op_meta_info.qualified_name

    def __repr__(self):
        return "Graph('%s')" % self.op_meta_info.qualified_name


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
        pass

    @abstractmethod
    def join(self, other: 'Connector'):
        """
        Create a connection by joining this connector with another one.

        :param other: The other connector.
        :raise ValueError: if *other* cannot be joined with this one.
        """
        pass

    @abstractmethod
    def disjoin(self):
        """
        Remove a connection which uses this connector.

        :param other: The other connector.
        :raise ValueError: if this connector cannot be disjoined.
        """
        pass

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
        if name not in meta_info.inputs:
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
        if name not in meta_info.outputs:
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


class InputConnectors(Attributes):
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


class OutputConnectors(Attributes):
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
