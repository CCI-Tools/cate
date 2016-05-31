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

A Graph's inputs and outputs refer to dedicated inputs and outputs of nodes in the graph. These are usually
unconnected inputs and outputs.

Various source types should be supported for a node input:

* ``{"parameter": None}``: a value parsed from the command-line or provided by a GUI. No value given.
* ``{"output_of": *node-output-ref* ``}``: the output of another graph or node
* ``{"constant":  *any-JSON* ``}``: a constant value, basically any JSON-serializable object
* ``{"file": *file-path* ``}``: an object loaded from a file in a given format,
  e.g. netCDF/xarray dataset, Shapefile, JSON, PNG image, numpy-binary
* ``{"url":``, "url": *URL* ``}``: same as file but loaded from a URL

An attribute ``{"source": *source-type*}`` may be present in order to make the source type unambiguous.

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

from abc import ABCMeta, abstractmethod
from collections import OrderedDict
from typing import Sequence

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
        node_id = node_id if node_id is not None else type(self).__name__ + str(id(self))
        self._id = node_id
        self._op_meta_info = op_meta_info
        self._node_input_namespace = NodeInputNamespace(self)
        self._node_output_namespace = NodeOutputNamespace(self)

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
    def output(self) -> 'NodeOutputNamespace':
        """The node's outputs."""
        return self._node_output_namespace

    @abstractmethod
    def new_node_input(self, node_input_name: str):
        pass

    @abstractmethod
    def new_node_output(self, node_output_name: str):
        pass

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
        super(OpNode, self).__init__(op_meta_info, node_id=node_id)
        self._op_registration = op_registration

    @property
    def op(self):
        """The operation registration. See :py:class:`ect.core.op.OpRegistration`"""
        return self._op_registration

    def new_node_input(self, node_input_name: str):
        return NodeInput(self, node_input_name)

    def new_node_output(self, node_output_name: str):
        return NodeOutput(self, node_output_name)

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

    @classmethod
    def from_json_dict(cls, json_dict, registry=REGISTRY):
        node_id = json_dict.get('id', None)
        node_op_name = json_dict.get('op', None)
        op_node = OpNode(node_op_name, node_id=node_id, registry=registry)
        node_input_dict = json_dict.get('input', None)
        for node_input in op_node.input[:]:
            node_input_source_dict = node_input_dict.get(node_input.name, {})
            # if 'input_for' in node_input_source_dict:
            #     json_input_for = node_input_source_dict['input_for']
            #     raise ValueError("'input_for' value '%s' of node '%s' cannot be resolved" %
            #                      (json_input_for, op_node.id))
            if 'input_from' in node_input_source_dict:
                json_input_from = node_input_source_dict['input_from']
                node_input.join(GraphInputProxy(graph_input_name=json_input_from))
            elif 'output_of' in node_input_source_dict:
                json_output_of = node_input_source_dict['output_of']
                node_id, node_output_name = json_output_of.rsplit('.', maxsplit=1)
                node_input.join(NodeOutputProxy(node_id, node_output_name))
            elif 'constant' in node_input_source_dict:
                json_constant = node_input_source_dict['constant']
                # check (nf) - convert constant from its json_constant representation into a Python value
                node_input.join(ConstantInput(json_constant))
            elif 'undefined' in node_input_source_dict:
                # The value of 'undefined' is ignored
                node_input.join(_UNDEFINED_INPUT)
            elif 'external' in node_input_source_dict:
                # The value of 'external' is ignored
                node_input.join(ExternalInput())
            else:
                raise ValueError("failed to identify type of node '%s'" % node_id)
        # node_output_dict = json_dict.get('output', None)
        return op_node

    def to_json_dict(self):
        """
        Return a JSON-serializable dictionary representation of this object.

        :return: A JSON-serializable dictionary
        """
        node_input_dict = OrderedDict()
        for node_input in self.input[:]:
            node_input_dict[node_input.name] = node_input.to_json_dict()
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

    :param op_meta_info: An optional OpMetaInfo object. If not provided, a basic stump will be generated.
    :param graph_id: An optional ID for the graph.
    """

    def __init__(self, op_meta_info: OpMetaInfo = None, graph_id: str = None):
        op_meta_info = op_meta_info if op_meta_info is not None else OpMetaInfo('graph')
        graph_id = graph_id if graph_id else 'graph' + str(id(self))
        super(Graph, self).__init__(op_meta_info, node_id=graph_id)
        self._nodes = OrderedDict()

    @property
    def nodes(self) -> Sequence[Node]:
        return list(self._nodes.values())

    def find_node(self, node_id) -> Node:
        if node_id in self._nodes:
            return self._nodes[node_id]
        return None

    def add_nodes(self, *nodes: Sequence[Node]):
        for node in nodes:
            self._nodes[node.id] = node

    @property
    def remove_node(self, node: Node):
        return self._nodes.remove(node.id)

    def new_node_input(self, node_input_name: str):
        return GraphInput(self, node_input_name)

    def new_node_output(self, node_output_name: str):
        return GraphOutput(self, node_output_name)

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
        # Developer note: keep variable naming consistent with Graph.from_json_dict() method
        node_input_json_dict = OrderedDict()
        for node_input in self._node_input_namespace[:]:
            node_input_json_dict[node_input.name] = node_input.to_json_dict()

        node_output_json_dict = OrderedDict()
        for node_output in self._node_output_namespace[:]:
            node_output_json_dict[node_output.name] = node_output.to_json_dict()

        graph_nodes_list = []
        for node in self._nodes.values():
            graph_nodes_list.append(node.to_json_dict())

        graph_json_dict = OrderedDict()
        graph_json_dict['id'] = self.id
        graph_json_dict['input'] = node_input_json_dict
        graph_json_dict['output'] = node_output_json_dict
        graph_json_dict['nodes'] = graph_nodes_list

        return graph_json_dict

    @classmethod
    def from_json_dict(cls, graph_json_dict, registry=REGISTRY):
        # Developer note: keep variable naming consistent with Graph.to_json_dict() method

        graph_id = graph_json_dict.get('id', 'graph')
        node_input_json_dict = graph_json_dict.get('input', {})
        node_output_json_dict = graph_json_dict.get('output', {})
        graph_nodes_json_list = graph_json_dict.get('nodes', [])

        # Convert op_meta_info
        op_meta_info_json_dict = graph_json_dict.get('op_meta_info', None)
        if op_meta_info_json_dict is not None:
            op_meta_info = OpMetaInfo.from_json_dict(op_meta_info_json_dict)
        else:
            op_meta_info = OpMetaInfo(graph_id)
        for name, value in node_input_json_dict.items():
            if name not in op_meta_info.input:
                op_meta_info.input[name] = {}
        for name, value in node_output_json_dict.items():
            if name not in op_meta_info.output:
                op_meta_info.output[name] = {}

        graph = Graph(graph_id=graph_id, op_meta_info=op_meta_info)

        # Convert all nodes
        nodes = []
        for graph_node_json_dict in graph_nodes_json_list:
            if 'op' in graph_node_json_dict:
                node = OpNode.from_json_dict(graph_node_json_dict, registry=registry)
            elif 'graph' in graph_node_json_dict:
                node = Graph.from_json_dict(graph_node_json_dict, registry=registry)
            else:
                raise ValueError("either the 'op' or 'graph' property must be given")
            nodes.append(node)

        graph.add_nodes(*nodes)

        # Resolve GraphInputRefInput and NodeOutputRefInput sources of all node inputs
        for node in graph.nodes:
            for node_input in node.input[:]:
                if isinstance(node_input.source, GraphInputProxy):
                    other_graph_input_name = node_input.source.graph_input_name
                    if other_graph_input_name not in graph.input:
                        raise ValueError("undefined input '%s'", other_graph_input_name)
                    other_graph_input = graph.input[other_graph_input_name]
                    node_input.source.resolve(other_graph_input)
                    node_input.join(other_graph_input)
                if isinstance(node_input.source, NodeOutputProxy):
                    other_node_id = node_input.source.node_id
                    other_node_output_name = node_input.source.node_output_name
                    other_node = graph.find_node(other_node_id)
                    if other_node is None:
                        raise ValueError("unknown node '%s'" % other_node_id)
                    if other_node_output_name not in other_node.output:
                        raise ValueError("unknown output '%s' of node '%s'", (other_node_output_name, other_node_id))
                    other_node_output = other_node.output[other_node_output_name]
                    node_input.source.resolve(other_node_output)
                    node_input.join(other_node_output)

        return graph

    def __str__(self):
        return "Graph('%s')" % self.op_meta_info.qualified_name

    def __repr__(self):
        return "Graph('%s')" % self.op_meta_info.qualified_name


class _UndefinedValue:
    def __str__(self):
        return '<undefined>'

    def __repr__(self):
        return 'UNDEFINED'


#: Special value returned by py:property:`value` indicating that a value has never been set.
UNDEFINED = _UndefinedValue()


class Json(metaclass=ABCMeta):
    @abstractmethod
    def to_json_dict(self):
        pass


class Source(Json, metaclass=ABCMeta):
    @property
    @abstractmethod
    def value(self):
        """Get the value of this input."""
        pass


class Target(Json, metaclass=ABCMeta):
    @abstractmethod
    def set_value(self, value):
        """Set the *value* of this output."""
        pass


class ExternalInput(Source, Target):
    """
    Can be used as input for any node. Buffered.
    """

    def __init__(self):
        self._value = UNDEFINED

    @property
    def value(self):
        return self._value

    def set_value(self, value):
        self._value = value

    def to_json_dict(self):
        return dict(external=True)


class UndefinedInput(Source):
    @property
    def value(self):
        return UNDEFINED

    def to_json_dict(self):
        return dict(undefined=True)


_UNDEFINED_INPUT = UndefinedInput()


class ConstantInput(Source):
    """
    An input which is a constant value. Buffered.
    """

    def __init__(self, constant):
        self._constant = constant

    @property
    def value(self):
        return self._constant

    def to_json_dict(self):
        # Care: self._constant may not be JSON-serializable!
        # Must add converter callback, or so.
        return dict(constant=self._constant)


class GraphInputProxy(Source):
    """
    (2) Used both as input for nodes and as output for Graphs. Unbuffered.
    """

    def __init__(self, graph_input_name: str = None, graph_input: 'NodeInput' = None):
        assert not (graph_input_name is None and graph_input is None)
        graph_input_name = graph_input.name if graph_input else graph_input_name
        assert graph_input_name is not None
        self._graph_input_name = graph_input_name
        self._graph_input = graph_input

    @property
    def graph_input_name(self) -> str:
        return self._graph_input_name

    @property
    def graph_input(self) -> 'NodeInput':
        return self._graph_input

    def resolve(self, graph_input: 'NodeInput'):
        assert graph_input is not None
        assert graph_input.name == self._graph_input_name
        self._graph_input = graph_input

    @property
    def value(self):
        return self._graph_input.value

    def to_json_dict(self):
        return dict(input_from=self._graph_input_name)


class NodeOutputProxy(Source):
    """
    (5) Used both as input for nodes and as output for Graphs. Unbuffered.
    """

    def __init__(self, node_id: str, node_output_name: str):
        assert node_id is not None
        assert node_output_name is not None
        self._node_id = node_id
        self._node_output_name = node_output_name
        self._node_output = None

    @property
    def node_id(self) -> str:
        return self._node_id

    @property
    def node_output_name(self) -> str:
        return self._node_output_name

    def resolve(self, node_output: 'NodeOutput'):
        assert node_output.node.id == self._node_id
        assert node_output.name == self._node_output_name
        self._node_output = node_output

    @property
    def value(self):
        self._assert_resolved()
        return self._node_output.value

    def to_json_dict(self):
        self._assert_resolved()
        return dict(output_of="%s.%s" % (self._node_id, self._node_output_name))

    def _assert_resolved(self):
        if self._node_output is None:
            raise ValueError("unresolved output '%s' of node '%s'" % (self._node_output_name, self._node_id))


class SourceHolder(metaclass=ABCMeta):
    @property
    @abstractmethod
    def source(self) -> Source:
        """The current source."""
        pass

    @abstractmethod
    def join(self, source: Source):
        """Join with the given *source*."""
        pass

    @abstractmethod
    def disjoin(self):
        """Disjoin from the current source."""
        pass


class TargetTracker(metaclass=ABCMeta):
    @property
    @abstractmethod
    def targets(self) -> Sequence[SourceHolder]:
        """Get the sequence of targets."""
        pass

    @abstractmethod
    def add_target(self, target: SourceHolder):
        """Add a new *target*."""
        pass

    @abstractmethod
    def remove_target(self, target: SourceHolder):
        """Remove existing *target*."""
        pass


class NodeInput(Source, SourceHolder):
    def __init__(self, node: Node, name: str, source: Source = _UNDEFINED_INPUT):
        assert node is not None
        assert name is not None
        assert source is not None
        assert name in node.op_meta_info.input
        self._node = node
        self._name = name
        self._source = source

    @property
    def node(self) -> Node:
        return self._node

    @property
    def name(self) -> str:
        return self._name

    @property
    def source(self) -> Source:
        return self._source

    # todo (nf) - extract a mixin, see same code in GraphOutput
    # todo (nf) - rename 'join', e.g. 'connect_with' to indicate direction
    # todo (nf) - handle case where self is Target: if new_source is not a source, we could assign the value instead
    # todo (nf) - handle case where new_source is not a source --> convert to ConstantInput
    # todo (nf) - handle case where new_source is GraphInput --> convert to GraphInputProxy
    # todo (nf) - handle case where self is a GraphOutput --> convert to ?
    #
    def join(self, new_source: Source):
        assert new_source is not None
        old_source = self._source
        if isinstance(old_source, TargetTracker):
            old_source.remove_target(self)
        self._source = new_source
        if isinstance(new_source, TargetTracker):
            new_source.add_target(self)

    def disjoin(self):
        self.join(_UNDEFINED_INPUT)

    @property
    def value(self):
        return self._source.value

    def to_json_dict(self):
        return self._source.to_json_dict()

    def __str__(self):
        return "%s.%s" % (self._node.id, self._name)


class GraphInput(NodeInput):
    def __init__(self, graph: Graph, name: str, source: Source = None):
        if source is None:
            source = ExternalInput()
        super(GraphInput, self).__init__(graph, name, source=source)

    @property
    def graph(self) -> Graph:
        return self.node

    def to_json_dict(self):
        # return dict(source_from=self.name)
        return self.source.to_json_dict()


class NodeOutput(Source, Target, TargetTracker):
    def __init__(self, node: Node, name: str):
        assert node is not None
        assert name is not None
        assert name in node.op_meta_info.output
        self._node = node
        self._name = name
        self._value = UNDEFINED
        self._targets = []

    @property
    def node(self) -> Node:
        return self._node

    @property
    def name(self) -> str:
        return self._name

    @property
    def targets(self) -> Sequence[SourceHolder]:
        return list(self._targets)

    def add_target(self, target: SourceHolder):
        assert target is not None
        if target not in self._targets:
            self._targets.append(target)

    def remove_target(self, target: SourceHolder):
        assert target is not None
        if target in self._targets:
            self._targets.remove(target)

    @property
    def value(self):
        return self._value

    def set_value(self, value):
        self._value = value

    def to_json_dict(self):
        return dict(output_of='%s.%s' % (self._node.id, self._name))

    def __str__(self):
        return "%s.%s" % (self._node.id, self._name)


class GraphOutput(Source, SourceHolder):
    def __init__(self, graph: Graph, name: str, source: Source = UndefinedInput()):
        assert graph is not None
        assert name is not None
        assert source is not None
        self._graph = graph
        self._name = name
        self._source = source

    @property
    def graph(self) -> Graph:
        return self._graph

    @property
    def name(self) -> str:
        return self._name

    @property
    def source(self) -> Source:
        return self._source

    def join(self, new_source: Source):
        assert new_source is not None
        old_source = self._source
        if isinstance(old_source, TargetTracker):
            old_source.remove_target(self)
        self._source = new_source
        if isinstance(new_source, TargetTracker):
            new_source.add_target(self)

    def disjoin(self):
        self.join(_UNDEFINED_INPUT)

    @property
    def value(self):
        return self._source.value

    def to_json_dict(self):
        return self._source.to_json_dict()

    def __str__(self):
        return "%s.%s" % (self._graph.id, self._name)


class NodeInputNamespace(Namespace):
    def __init__(self, node: Node):
        inputs = [(input_name, node.new_node_input(input_name)) for input_name, _ in node.op_meta_info.input]
        super(NodeInputNamespace, self).__init__(inputs)

    def __setattr__(self, name, value):
        node_input = self.__getattr__(name)
        if not isinstance(value, Source):
            value = ConstantInput(value)
        node_input.join(value)

    def __getattr__(self, name):
        try:
            return super(NodeInputNamespace, self).__getattr__(name)
        except AttributeError:
            raise AttributeError("'%s' is not an input" % name)

    def __delattr__(self, input_name):
        raise NotImplementedError()


class NodeOutputNamespace(Namespace):
    def __init__(self, node: Node):
        outputs = [(output_name, node.new_node_output(output_name)) for output_name, _ in node.op_meta_info.output]
        super(NodeOutputNamespace, self).__init__(outputs)

    def __setattr__(self, name, value):
        node_output = self.__getattr__(name)
        if not isinstance(value, Source):
            raise AttributeError("invalid value for output '%s' of node '%s'" % (name, node_output.node.id))
        if not isinstance(node_output, SourceHolder):
            raise AttributeError("output '%s' of node '%s' cannot be set" % (name, node_output.node.id))
        node_output.join(value)

    def __getattr__(self, name):
        try:
            return super(NodeOutputNamespace, self).__getattr__(name)
        except AttributeError:
            raise AttributeError("'%s' is not an output" % name)

    def __delattr__(self, name):
        raise NotImplementedError()
