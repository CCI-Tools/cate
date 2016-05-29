"""
Module Description
==================

Provides classes that are used to construct processing workflows (networks) from registered operations and connected
graphs of such.

This module provides the following data types:
* A :py:class:`Node` has zero or more ``NodeInput``s and zero or more ``NodeOutput``s
* A :py:class:`OpNode` is a ``Node`` that wraps an executable operation.
* A :py:class:`Graph` is a ``Node``that contains other ``Node``s
* A :py:class:`NodeInput` belongs to exactly one ``Node``, has a name, and has a ``source`` property which provides a
  the input value. A source may be a constant or a connected ``NodeOutput`` of another node. Basically anything
  can act as a source that has a ``value`` property.
* A :py:class:`NodeOutput` belongs to exactly one ``Node``, has a name, and has a ``targets`` property (a list)
  that points to all connected ``NodeInput``s of other nodes. A ``NodeOutput`` has a ``value`` property and
  can therefore act as a source for a ``NodeInput``.

Technical Requirements
======================

A Graph's inputs and outputs refer to dedicated graph node inputs and outputs. These are usually the
unconnected inputs and outputs within the graph.

A node's and a graph's input types may be:

* ``{"source": "parameter"}``: a value parsed from the command-line or provided by a GUI. No value given.
* ``{"source": "output":``, "output": *node-output-ref* ``}``: the output of another graph or node
* ``{"source": "constant":``, "constant":  *any-JSON* ``}``: a constant value, basically any JSON-serializable object
* ``{"source": "file":``, "file": *file-path* ``}``: an object loaded from a file in a given format,
  e.g. netCDF/xarray dataset, Shapefile, JSON, PNG image, numpy-binary
* ``{"source": "url":``, "url": *URL* ``}``: same as file but loaded from a URL

All ``{"source": *type*}`` other than ``{"source": "parameter"}`` are optional in a node's JSON, as the source
value names are unambiguous.

Graphs shall be callable by the CLI in the same way as single operations. The command line form for calling an
operation is currently:::

    ect run OP [ARGS]

Where *OP* shall be a registered operation or a graph.
_Implementation hint_: An ``OpResolver.find_op(op_name)`` may be utilized to resolve
operation names. If we move module ``workflow`` out of core, it may register a new OpResolver that can resolve
Graph file names (*.graph.json) as operations.

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
        self._node_input_namespace = NodeInputNamespace([NodeInput(self, name)
                                                         for name, _ in op_meta_info.input])
        self._node_output_namespace = NodeOutputNamespace([NodeOutput(self, name)
                                                           for name, _ in op_meta_info.output])

    @property
    def id(self):
        """The node's ID."""
        return self._id

    @property
    def op_meta_info(self) -> OpMetaInfo:
        """The node's operation meta-information."""
        return self._op_meta_info

    @property
    def input(self) -> 'NodeInputNamespace':
        """The node's inputs."""
        return self._node_input_namespace

    @property
    def output(self)-> 'NodeOutputNamespace':
        """The node's outputs."""
        return self._node_output_namespace

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
        for node_input in self.input[:]:
            if node_input.source is not None:
                input_value = node_input.source.value
            else:
                raise ValueError("unbound input '%s' of node '%s'" % (node_input.name, node_input.node.id))
            input_values[node_input.name] = input_value

        return_value = self._op_registration(monitor=Monitor.NULL, **input_values)

        if self.op_meta_info.has_named_outputs:
            for output_name, output_value in return_value.items():
                self.output[output_name].set_value(output_value)
        else:
            self.output[OpMetaInfo.RETURN_OUTPUT_NAME].set_value(return_value)

    def to_json_dict(self):
        """
        Return a JSON-serializable dictionary representation of this object.

        :return: A JSON-serializable dictionary
        """
        node_input_dict = OrderedDict()
        for node_input in self.input[:]:
            source = node_input.source
            if source is not None:
                node_input_dict[node_input.name] = source.to_json_dict()
        node_dict = OrderedDict()
        node_dict['id'] = self.id
        node_dict['op'] = self.op_meta_info.qualified_name
        if node_input_dict:
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
        input_dict = OrderedDict()
        for node_input in self._node_input_namespace[:]:
            input_dict[node_input.name] = node_input.to_json_dict()

        output_dict = OrderedDict()
        for node_output in self._node_output_namespace[:]:
            output_dict[node_output.name] = node_output.to_json_dict()

        graph_nodes_list = []
        for node in self._nodes:
            graph_nodes_list.append(node.to_json_dict())

        graph_dict = OrderedDict()
        graph_dict['input'] = input_dict
        graph_dict['output'] = output_dict
        graph_dict['nodes'] = graph_nodes_list

        return {'graph': graph_dict}

    def __str__(self):
        return "Graph('%s')" % self.op_meta_info.qualified_name

    def __repr__(self):
        return "Graph('%s')" % self.op_meta_info.qualified_name

    def _update_on_node_added(self, node):
        graph_meta_info = self.op_meta_info
        node_meta_info = node.op_meta_info
        for node_input in node.input[:]:
            if not node_input.source:
                name = node_input.name
                # Make sure graph meta_info is correct
                if name not in graph_meta_info.input:
                    graph_meta_info.input[name] = dict(node_meta_info.input[name])
                # Add input
                self.input[name] = node_input
        for node_output in node.output[:]:
            if not node_output.targets:
                name = node_output.name
                # Make sure graph meta_info is correct
                if name not in graph_meta_info.output:
                    graph_meta_info.output[name] = dict(node_meta_info.output[name])
                # Add output
                self.output[name] = node_output


class UndefinedValue:
    def __str__(self):
        return '<undefined>'

    def __repr__(self):
        return 'UNDEFINED'


class ValueSource(metaclass=ABCMeta):
    """
    The common interface for objects that can act as value sources for a :py:class`NodeInput`.
    """

    #: Special value returned by py:property:`value` indicating that a value has never been set.
    UNDEFINED = UndefinedValue()

    @abstractproperty
    def value(self):
        """Get the value of this source."""
        pass


class ConstantValueSource(ValueSource):
    def __init__(self, constant):
        self._constant = constant

    @property
    def value(self):
        return self._constant

    def to_json_dict(self):
        # Care: self._constant may not be JSON-serializable!
        # Must add converter callback, or so.
        return dict(constant=self._constant)

    def from_json_dict(self, dict):
        raise NotImplementedError()


class ParameterValueSource(ValueSource):
    def __init__(self):
        self._value = self.UNDEFINED

    @property
    def value(self):
        return self._value

    def set_value(self, value):
        self._value = value

    def to_json_dict(self):
        return dict(source='parameter')

    def from_json_dict(self, dict):
        raise NotImplementedError()


class NodeConnector(metaclass=ABCMeta):
    """
    Defines the common interface of :py:class:`NodeInput` and :py:class:`NodeOutput`.

    :param node: The node
    :param name: Name of an input or output of the node's operation.
    """

    def __init__(self, node: Node, name: str):
        self._node = node
        self._name = name

    @property
    def node(self) -> Node:
        """The connector's node."""
        return self._node

    @property
    def name(self) -> str:
        """The connector's name."""
        return self._name

    @abstractproperty
    def is_input(self) -> bool:
        """``True`` for input connectors, ``False`` for output connectors."""

    @abstractmethod
    def join(self, other: 'NodeConnector'):
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


class NodeInput(NodeConnector):
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
        super(NodeInput, self).__init__(node, name)
        self._source = None
        # todo (nd) self._source shall never be none
        #self._source = ParameterValueSource()

    @property
    def source(self) -> ValueSource:
        """The value source of this node input."""
        return self._source

    def set_source(self, source: ValueSource):
        """:param source: The new value source for this node input."""
        self._source = source

    @property
    def is_input(self) -> bool:
        """Always ``True``."""
        return True

    def join(self, other: 'NodeOutput'):
        if other.is_input:
            raise ValueError('other must be a node output')
        # Note: perform sanity checks here, e.g. to avoid close loops by self-referencing
        self.set_source(other)
        other.add_target(self)

    def disjoin(self):
        if isinstance(self._source, NodeOutput):
            self._source.remove_target(self)
            self._source = None

    def to_json_dict(self):
        return dict(input_for=self.node.id + '.' + self.name)

    def __str__(self):
        return '%s.input.%s' % (self.node.op_meta_info.qualified_name, self.name)

    def __repr__(self):
        return "InputConnector(%s, '%s')" % (self.node, self.name)


class NodeOutput(NodeConnector, ValueSource):
    """
    An endpoint of a :py:class:`Connection` between two :py:class:`Node`s.

    :param node: The node
    :param name: Name of an input or output of the node's operation.
    """

    def __init__(self, node: Node, name: str):
        op_meta_info = node.op_meta_info
        if name not in op_meta_info.output:
            raise ValueError(
                "'%s' is not an output of operation '%s'" % (name, op_meta_info.qualified_name))
        super(NodeOutput, self).__init__(node, name)
        self._targets = []
        self._value = ValueSource.UNDEFINED

    @property
    def targets(self) -> List[NodeInput]:
        """The targets of this node output."""
        return list(self._targets)

    def add_target(self, node_input: NodeInput):
        """:param node_input: The node input to add."""
        if node_input not in self._targets:
            self._targets.append(node_input)

    def remove_target(self, node_input: NodeInput):
        """:param node_input: The node input to remove."""
        self._targets.remove(node_input)

    @property
    def value(self):
        """The value."""
        return self._value

    def set_value(self, value):
        """Set the *value*."""
        self._value = value

    @property
    def is_input(self) -> bool:
        """Always ``False``."""
        return False

    def to_json_dict(self):
        return dict(output_of=self.node.id + '.' + self.name)

    def join(self, other: NodeInput):
        if not other.is_input:
            raise ValueError('other must be a node input')
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


class NodeInputNamespace(Namespace):
    def __init__(self, node_inputs):
        super(NodeInputNamespace, self).__init__([(node_input.name, node_input) for node_input in node_inputs])

    def __setattr__(self, name, value):
        node_input = self.__getattr__(name)
        if isinstance(value, NodeConnector):
            if value.is_input:
                raise AttributeError("input '%s' expects an output" % name)
            value.join(node_input)
        else:
            node_input.set_source(ConstantValueSource(value))

    def __getattr__(self, name) -> 'NodeInput':
        try:
            return super(NodeInputNamespace, self).__getattr__(name)
        except AttributeError:
            raise AttributeError("'%s' is not an input" % name)

    def __delattr__(self, name):
        raise NotImplementedError()


class NodeOutputNamespace(Namespace):
    def __init__(self, node_outputs):
        super(NodeOutputNamespace, self).__init__([(node_output.name, node_output) for node_output in node_outputs])

    def __setattr__(self, name, value):
        raise AttributeError("'%s' is an output and cannot be set" % name)

    def __getattr__(self, name) -> 'NodeOutput':
        try:
            return super(NodeOutputNamespace, self).__getattr__(name)
        except AttributeError:
            raise AttributeError("'%s' is not an output" % name)

    def __delattr__(self, name):
        raise NotImplementedError()
