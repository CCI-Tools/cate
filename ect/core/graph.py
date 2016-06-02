"""
Module Description
==================

Provides classes that are used to construct processing graphs (workflows, networks) from registered operations
and connected graphs of such.

This module provides the following data types:

* A :py:class:`Node` has zero or more ``NodeInput`` and zero or more ``NodeOutput`` objects
* A :py:class:`OpNode` is a ``Node`` that wraps an executable operation.
* A :py:class:`GraphFileNode` is a ``Node`` that wraps an executable ``Graph`` loaded from a JSON file.
* A :py:class:`Graph` is a ``Node`` that contains other ``Node`` objects
* A :py:class:`NodeInput` belongs to exactly one ``Node``, has a name, and has a ``source`` property which provides
  the input value. A source may be a constant or a connected ``NodeOutput`` of another node. Basically anything
  can act as a source that has a ``value`` property.
* A :py:class:`NodeOutput` belongs to exactly one ``Node``, has a name and has a ``value`` property and
  can therefore act as a source for a ``NodeInput``.

Technical Requirements
======================

A graph is required to specify its inputs and outputs. Input source may be left unspecified, while it is mandatory to
connect the grap's outputs to outputs of contained child nodes.

A graph's child nodes are required to specify all of their input sources. Valid input sources for a child node
are the graph's inputs or other child node's outputs.

Child node input sources are indicated in the input specification of a node's JSON representation:

* ``{"input_from":`` *name* ``}``: the input named *name* of the current graph.
* ``{"output_of":`` *node-id* ``.`` *name* ``}``: the output named *name* of another graph or node given by *node-id*.
* ``{"parameter":`` *ignored-value* ``}``: parameter whose value is provided externally (CLI or GUI).
* ``{"constant":``  *value* ``}``: a constant value, where *value* my be any JSON-value.
* ``{"file":`` *file-path* ``}``: an object loaded from a file in a given format,
  e.g. netCDF/xarray dataset, Shapefile, JSON, PNG image, numpy-binary.
* ``{"url":`` *URL* ``}``: same as file but loaded from a URL.
* ``{"undefined":``  *ignored-value* ``}``: undefined input source. This is also used if no source type
  identifiers are present in the input JSON.

An attribute ``{"source": *source-type*}`` may be present in order to make the source type unambiguous.

Graphs shall be callable by the CLI in the same way as single operations. The command line form for calling an
operation is currently:::

    ect run OP [ARGS]

Where *OP* shall be a registered operation or a graph.

Implementation note: An ``OpResolver.find_op(op_name)`` may be utilized to resolve
operation names. If we move module ``workflow`` out of core, it may register a new OpResolver that can resolve
Graph file names (*.graph.json) as operations.

Module Reference
================
"""

from abc import ABCMeta, abstractmethod
from collections import OrderedDict
from io import IOBase
from typing import Sequence, Optional, Union

from ect.core import Monitor
from .op import REGISTRY, UNDEFINED, OpMetaInfo, OpRegistration
from .util import Namespace


class Node(metaclass=ABCMeta):
    """
    Base class for all nodes that may occur as building blocks of a processing graph.
    All nodes have inputs and outputs, and can be invoked to perform some operation.
    Inputs can be set, and outputs can be retrieved.

    Inputs are exposed as attributes of the :py:attr:`input` property and are of type :py:class:`NodeInput`.
    Outputs are exposed as attributes of the :py:attr:`output` property and are of type :py:class:`NodeOutput`.

    :param op_meta_info: Meta-information about the operation, see :py:class:`OpMetaInfo`.
    :param node_id: A node ID. If None, a unique name will be generated.
    """

    def __init__(self, op_meta_info: OpMetaInfo, node_id: str):
        assert op_meta_info
        assert node_id
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

    def new_node_input(self, node_input_name: str):
        """
        Create an appropriate instance of a node input object.

        :param node_input_name: The name of the input.
        :return: A node input object
        """
        return NodeInput(self, node_input_name)

    def new_node_output(self, node_output_name: str):
        """
        Create an appropriate instance of a node output object.

        :param node_output_name: The name of the output.
        :return: A node output object
        """
        return NodeOutput(self, node_output_name)

    @abstractmethod
    def invoke(self, monitor: Monitor = Monitor.NULL):
        """
        Invoke this node's underlying operation with input values from
        :py:attr:`input`. Output values in :py:attr:`output` will
        be set from the underlying operation's return value(s).

        :param monitor: An optional progress monitor.
        """

    @abstractmethod
    def to_json_dict(self):
        """
        Return a JSON-serializable dictionary representation of this object.

        :return: A JSON-serializable dictionary
        """

    def __str__(self):
        """String representation."""
        return self.id

    @abstractmethod
    def __repr__(self):
        """String representation for developers."""


class ChildNode(Node):
    """
    An inner node of a graph.

    :param op_meta_info: Meta-information about the operation, see :py:class:`OpMetaInfo`.
    :param node_id: A node ID. If None, a unique name will be generated.
    """

    def __init__(self, op_meta_info: OpMetaInfo, node_id: str):
        super(ChildNode, self).__init__(op_meta_info, node_id=node_id)

    @classmethod
    def from_json_dict(cls, json_dict, registry=REGISTRY) -> Optional['ChildNode']:
        node = cls.new_node_from_json_dict(json_dict, registry=registry)
        if node is None:
            return None
        # todo (nf) - address code duplication in Graph.from_json_dict
        node_input_dict = json_dict.get('input', None)
        source_classes = [GraphInputRef, NodeOutputRef, ConstantSource, UndefinedSource, ParameterSource]
        for node_input in node.input[:]:
            node_input_source_dict = node_input_dict.get(node_input.name, None)
            if node_input_source_dict is None:
                raise ValueError("missing specification for input '%s' of node '%s'" % (node_input.name, node.id))
            source = None
            for source_class in source_classes:
                source = source_class.from_json_dict(node_input_source_dict)
                if source is not None:
                    break
            if source is not None:
                node_input.connect_source(source)
            else:
                raise ValueError("unknown type for input '%s' of node '%s'" % (node_input.name, node.id))
        return node

    @classmethod
    @abstractmethod
    def new_node_from_json_dict(cls, json_dict, registry=REGISTRY):
        """Create a new child node instance from the given *json_dict*"""

    def to_json_dict(self):
        """
        Return a JSON-serializable dictionary representation of this object.

        :return: A JSON-serializable dictionary
        """
        node_input_dict = OrderedDict()
        for node_input in self.input[:]:
            source = node_input.source
            try:
                source_json_dict = source.to_json_dict()
            except AttributeError:
                raise ValueError("input '%s' of node '%s' is not JSON-serializable: source type: %s" %
                                 (node_input.name, node_input.node.id, str(type(source))))
            node_input_dict[node_input.name] = source_json_dict

        node_dict = OrderedDict()
        self.enhance_json_dict(node_dict)
        node_dict['id'] = self.id
        if node_input_dict:
            node_dict['input'] = node_input_dict

        return node_dict

    @abstractmethod
    def enhance_json_dict(self, node_dict: OrderedDict):
        """Enhance the given JSON-compatible *node_dict* by child node specific elements."""


class GraphNode(ChildNode):
    """
    A `GraphFileNode` is a child node that uses an externally stored :py:class:`Graph` for its computations.

    :param graph: The referenced graph.
    :param resource: A resource (e.g. file path, URL) from which the graph was loaded.
    :param node_id: A node ID. If None, a unique ID will be generated.
    """

    def __init__(self, graph, resource, node_id=None):
        if not graph:
            raise ValueError('graph must be given')
        if not resource:
            raise ValueError('resource must be given')
        node_id = node_id if node_id else 'graph_file_' + hex(id(self))[2:]
        super(GraphNode, self).__init__(graph.op_meta_info, node_id)
        self._graph = graph
        self._resource = resource
        # Connect the graph's inputs with this node's input sources
        for graph_input in self._graph.input[:]:
            name = graph_input.name
            assert name in self.input
            graph_input.connect_source(self.input[name])

    @property
    def graph(self) -> 'Graph':
        """The graph."""
        return self._graph

    @property
    def resource(self) -> str:
        """The graph's file path."""
        return self._resource

    def invoke(self, monitor: Monitor = Monitor.NULL):
        """
        Invoke this node's underlying :py:attr:`graph` with input values from
        :py:attr:`input`. Output values in :py:attr:`output` will
        be set from the underlying graph's return value(s).

        :param monitor: An optional progress monitor.
        """
        self._graph.invoke(monitor=monitor)
        # transfer graph output values into this node's output values
        for graph_output in self._graph.output[:]:
            assert graph_output.name in self.output
            node_output = self.output[graph_output.name]
            node_output.set_value(graph_output.value)

    @classmethod
    def new_node_from_json_dict(cls, json_dict, registry=REGISTRY):
        resource = json_dict.get('graph', None)
        if resource is None:
            return None
        graph = Graph.load(resource, registry=registry)
        return GraphNode(graph, resource, node_id=json_dict.get('id', None))

    def enhance_json_dict(self, node_dict: OrderedDict):
        node_dict['graph'] = self._resource

    def __repr__(self):
        return "GraphNode(%s, '%s', node_id='%s')" % (repr(self._graph), self.resource, self.id)


class OpNode(ChildNode):
    """
    Nodes can be used to construct networks or graphs of operations.
    Input and output of an operation are available as node attributes of type :py:class:`Connector`.

    :param operation: A fully qualified operation name or operation object such as a class or callable.
    :param registry: An operation registry to be used to lookup the operation, if given by name..
    :param node_id: A node ID. If None, a unique ID will be generated.
    """

    def __init__(self, operation, node_id=None, registry=REGISTRY):
        if not operation:
            raise ValueError('operation must be given')
        if isinstance(operation, str):
            op_registration = registry.get_op(operation, fail_if_not_exists=True)
        elif isinstance(operation, OpRegistration):
            op_registration = operation
        else:
            op_registration = registry.get_op(operation, fail_if_not_exists=True)
        assert op_registration is not None
        node_id = node_id if node_id else 'op_node_' + hex(id(self))[2:]
        op_meta_info = op_registration.meta_info
        super(OpNode, self).__init__(op_meta_info, node_id)
        self._op_registration = op_registration

    @property
    def op(self):
        """The operation registration. See :py:class:`ect.core.op.OpRegistration`"""
        return self._op_registration

    def invoke(self, monitor: Monitor = Monitor.NULL):
        """
        Invoke this node's underlying operation :py:attr:`op` with input values from
        :py:attr:`input`. Output values in :py:attr:`output` will
        be set from the underlying operation's return value(s).

        :param monitor: An optional progress monitor.
        """
        input_values = OrderedDict()
        for input_name, _ in self.op_meta_info.input:
            input_values[input_name] = None
        for node_input in self.input[:]:
            assert node_input is not None
            assert node_input.source is not None
            input_values[node_input.name] = node_input.source.value

        return_value = self._op_registration(monitor=monitor, **input_values)

        if self.op_meta_info.has_named_outputs:
            for output_name, output_value in return_value.items():
                self.output[output_name].set_value(output_value)
        else:
            self.output[OpMetaInfo.RETURN_OUTPUT_NAME].set_value(return_value)

    @classmethod
    def new_node_from_json_dict(cls, json_dict, registry=REGISTRY):
        op_name = json_dict.get('op', None)
        if op_name is None:
            return None
        return cls(op_name, node_id=json_dict.get('id', None), registry=registry)

    def enhance_json_dict(self, node_dict: OrderedDict):
        node_dict['op'] = self.op_meta_info.qualified_name

    def __repr__(self):
        return "OpNode(%s, node_id='%s')" % (self.op_meta_info.qualified_name, self.id)


class Graph(Node):
    """
    A graph of (connected) nodes.

    Inputs are exposed as attributes of the :py:attr:`input` property and are of type :py:class:`GraphInput`.
    Outputs are exposed as attributes of the :py:attr:`output` property and are of type :py:class:`GraphOutput`.

    :param op_meta_info: An optional OpMetaInfo object. If not provided, a basic stump will be generated.
    :param graph_id: An optional ID for the graph.
    """

    def __init__(self, op_meta_info: OpMetaInfo = None, graph_id: str = None):
        op_meta_info = op_meta_info if op_meta_info is not None else OpMetaInfo('graph')
        graph_id = graph_id if graph_id else 'graph_' + hex(id(self))[2:]
        super(Graph, self).__init__(op_meta_info, graph_id)
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
        Invoke this graph by invoking all all of its child nodes.
        The node invocation order is determined by the input requirements of individual nodes.

        :param monitor: An optional progress monitor.
        """
        nodes = self.nodes
        node_count = len(nodes)
        if node_count == 1:
            nodes[0].invoke(monitor)
        elif node_count > 1:
            monitor.start("Executing graph '%s'" % self.id, node_count)
            for node in nodes:
                node.invoke(monitor.child(1))
            monitor.done()

    @classmethod
    def load(cls, file_path_or_fp: Union[str, IOBase], registry=REGISTRY) -> 'Graph':
        """
        Load a graph from a file or file pointer. The format is expected to be "graph-JSON".

        :param file_path_or_fp: file path or file pointer
        :param registry: Operation registry
        :return: a graph
        """
        import json
        if isinstance(file_path_or_fp, str):
            file_path = file_path_or_fp
            with open(file_path) as fp:
                json_dict = json.load(fp)
        else:
            fp = file_path_or_fp
            json_dict = json.load(fp)
        return Graph.from_json_dict(json_dict, registry=registry)

    @classmethod
    def from_json_dict(cls, graph_json_dict, registry=REGISTRY):
        # Developer note: keep variable naming consistent with Graph.to_json_dict() method

        qualified_name = graph_json_dict.get('qualified_name', None)
        if qualified_name is None:
            raise ValueError('missing mandatory "qualified_name" property in graph JSON')
        header_json_dict = graph_json_dict.get('header', {})
        node_input_json_dict = graph_json_dict.get('input', {})
        node_output_json_dict = graph_json_dict.get('output', {})
        graph_nodes_json_list = graph_json_dict.get('nodes', [])

        # todo (nf) - convert 'data_type' values in node_input_json_dict & node_output_json_dict
        # todo (nf) - OpMetaInfo.input_dict: only add inputs whose source type is 'parameter'
        # todo (nf) - OpMetaInfo.output_dict: only add inputs whose source type is 'output_of'
        op_meta_info = OpMetaInfo(qualified_name,
                                  has_monitor=True,
                                  header_dict=header_json_dict,
                                  input_dict=node_input_json_dict,
                                  output_dict=node_output_json_dict)

        for name, value in node_input_json_dict.items():
            if name not in op_meta_info.input:
                op_meta_info.input[name] = {}
        for name, value in node_output_json_dict.items():
            if name not in op_meta_info.output:
                op_meta_info.output[name] = {}

        # todo (nf) - remove graph_id kw, make op_meta_info required arg, Graph.id == Graph.op_meta_info.qualified_name
        graph = Graph(graph_id=qualified_name, op_meta_info=op_meta_info)

        # todo (nf) - here we must somehow deal with the fact that only graph inputs of type 'parameter' can be altered,
        # as they are Targets (they have a set_value(value) method) by CLI or GUI.
        # Other inputs (other than 'undefined') shall be treated as "local" inputs to the graph's node inputs.

        # todo (nf) - address code duplication here and in ChildNode.from_json_dict
        source_classes = [ConstantSource, UndefinedSource, ParameterSource]
        default_source_class = ParameterSource
        for node_input in graph.input[:]:
            node_input_source_dict = node_input_json_dict.get(node_input.name, {})
            source = None
            for source_class in source_classes:
                source = source_class.from_json_dict(node_input_source_dict)
                if source is not None:
                    break
            if source is not None:
                node_input.connect_source(source)
            else:
                node_input.connect_source(default_source_class())
                #raise ValueError("unknown input type in graph '%s'" % qualified_name)

        source_classes = [GraphInputRef, NodeOutputRef, ConstantSource, UndefinedSource, ParameterSource]
        for node_output in graph.output[:]:
            node_output_source_dict = node_output_json_dict.get(node_output.name, {})
            source = None
            for source_class in source_classes:
                source = source_class.from_json_dict(node_output_source_dict)
                if source is not None:
                    break
            if source is not None:
                node_output.connect_source(source)
            else:
                raise ValueError("unknown output type in graph '%s'" % qualified_name)

        # Convert all nodes
        node_classes = [OpNode, GraphNode]
        nodes = []
        for graph_node_json_dict in graph_nodes_json_list:
            node = None
            for node_class in node_classes:
                node = node_class.from_json_dict(graph_node_json_dict, registry=registry)
                if node is not None:
                    break
            if node is not None:
                nodes.append(node)
            else:
                raise ValueError("unknown node type in graph '%s'" % qualified_name)

        graph.add_nodes(*nodes)

        # Resolve GraphInputSource and NodeOutputSource sources of node inputs
        for node in graph.nodes:
            for node_input in node.input[:]:
                graph._resolve_node_input(node_input)

        # Resolve GraphInputSource and NodeOutputSource sources of graph outputs
        for graph_input in graph.output[:]:
            graph._resolve_node_input(graph_input)

        return graph

    def _resolve_node_input(self, node_input: 'SourceHolder'):
        if isinstance(node_input.source, GraphInputRef):
            graph_input_ref = node_input.source
            other_graph_input_name = graph_input_ref.name
            if other_graph_input_name not in self.input:
                raise ValueError("undefined input '%s'", other_graph_input_name)
            other_graph_input = self.input[other_graph_input_name]
            graph_input_ref.resolve(other_graph_input)
            node_input.connect_source(other_graph_input)
        if isinstance(node_input.source, NodeOutputRef):
            node_output_ref = node_input.source
            other_node_id = node_output_ref.node_id
            other_node_output_name = node_output_ref.name
            other_node = self.find_node(other_node_id)
            if other_node is None:
                raise ValueError("unknown node '%s'" % other_node_id)
            if other_node_output_name not in other_node.output:
                raise ValueError("unknown output '%s' of node '%s'", (other_node_output_name, other_node_id))
            other_node_output = other_node.output[other_node_output_name]
            node_output_ref.resolve(other_node_output)
            node_input.connect_source(other_node_output)

    def to_json_dict(self):
        """
        Return a JSON-serializable dictionary representation of this object.

        :return: A JSON-serializable dictionary
        """
        # Developer note: keep variable naming consistent with Graph.from_json_dict() method
        input_json_dict = OrderedDict()
        for node_input in self._node_input_namespace[:]:
            source = node_input.source
            try:
                source_json_dict = source.to_json_dict()
            except AttributeError:
                raise ValueError("input '%s' of graph '%s' is not JSON-serializable: source type: %s" %
                                 (node_input.name, node_input.node.id, str(type(source))))
            input_json_dict[node_input.name] = source_json_dict

        output_json_dict = OrderedDict()
        for node_output in self._node_output_namespace[:]:
            source = node_output.source
            try:
                source_json_dict = source.to_json_dict()
            except AttributeError:
                raise ValueError("output '%s' of graph '%s' is not JSON-serializable: source type: %s" %
                                 (node_output.name, node_output.node.id, str(type(source))))
            output_json_dict[node_output.name] = source_json_dict

        nodes_json_list = []
        for node in self._nodes.values():
            nodes_json_list.append(node.to_json_dict())

        graph_json_dict = OrderedDict()
        graph_json_dict['qualified_name'] = self.op_meta_info.qualified_name
        graph_json_dict['input'] = input_json_dict
        graph_json_dict['output'] = output_json_dict
        graph_json_dict['nodes'] = nodes_json_list

        return graph_json_dict

    def __str__(self):
        return self.id

    def __repr__(self):
        return "Graph(OpMetaData(%s), graph_id=%s)" % (repr(self.op_meta_info.qualified_name), repr(self.id))


class Json(metaclass=ABCMeta):
    """Something that can convert from and to JSON-compatible dictionaries."""

    @classmethod
    @abstractmethod
    def from_json_dict(cls, json_dict: dict):
        """Convert JSON-compatible dictionary to an instance of this class."""

    @abstractmethod
    def to_json_dict(self):
        """Convert an instance of this class into a JSON-compatible dictionary."""


class Source(metaclass=ABCMeta):
    """Something that has a ``value`` property."""

    @property
    @abstractmethod
    def value(self):
        """Get the value of this input."""


class Target(metaclass=ABCMeta):
    """Something that has a ``set_value(value)`` method."""

    @abstractmethod
    def set_value(self, value):
        """Set the *value* of this output."""


class ParameterSource(Source, Target, Json):
    """A source representing a parameter whose value is provided externally (e.g. by CLI or GUI)."""

    def __init__(self, value=None):
        self._value = value

    @property
    def value(self):
        return self._value

    def set_value(self, value):
        self._value = value

    @classmethod
    def from_json_dict(cls, json_dict: dict):
        # The value of 'parameter' is ignored, should be 'true' or 'null'
        return cls() if 'parameter' in json_dict else None

    def to_json_dict(self):
        return dict(parameter=True)

    def __str__(self):
        return str(self._value)

    def __repr__(self):
        return "ParameterSource(%s)" % repr(self._value)


class UndefinedSource(Source, Json):
    """A source that returns the "value" ``UNDEFINED``."""

    @property
    def value(self):
        return UNDEFINED

    @classmethod
    def from_json_dict(cls, json_dict: dict):
        # The value of 'undefined' is ignored
        return UNDEFINED_SOURCE if 'undefined' in json_dict else None

    def to_json_dict(self):
        return dict(undefined=True)

    def __str__(self):
        return str(self.value)

    def __repr__(self):
        return "UndefinedSource()"


#: Use this constant instead of ``UndefinedSource()`` to avoid creating new instances.
UNDEFINED_SOURCE = UndefinedSource()


class ConstantSource(Source, Json):
    """A source that provides a constant value."""

    def __init__(self, constant=UNDEFINED):
        self._constant = constant

    @property
    def value(self):
        return self._constant

    @classmethod
    def from_json_dict(cls, json_dict: dict):
        if 'constant' in json_dict:
            constant = json_dict['constant']
            # Care: constant may be converted to a real Python value here
            # Must add converter callback, or so.
            return ConstantSource(constant)
        return None

    def to_json_dict(self):
        # Care: self._constant may not be JSON-serializable!
        # Must add converter callback, or so.
        return dict(constant=self._constant)

    def __str__(self):
        return str(self._constant)

    def __repr__(self):
        return "ConstantSource(%s)" % repr(self._constant)


class GraphInputRef(Source, Json):
    """Reference to a :py:class:`GraphInput` instance. Used as source for node inputs and graph outputs."""

    def __init__(self, name: str = None, graph_input: 'NodeInput' = None):
        assert not (name is None and graph_input is None)
        name = graph_input.name if graph_input else name
        assert name is not None
        self._name = name
        self._graph_input = graph_input

    @property
    def name(self) -> str:
        return self._name

    @property
    def graph_input(self) -> 'NodeInput':
        return self._graph_input

    def resolve(self, graph_input: 'NodeInput'):
        assert graph_input is not None
        assert graph_input.name == self._name
        self._graph_input = graph_input

    @property
    def value(self):
        return self._graph_input.value

    @classmethod
    def from_json_dict(cls, json_dict: dict):
        if 'input_from' in json_dict:
            input_from = json_dict['input_from']
            return cls(name=input_from)
        return None

    def to_json_dict(self):
        return dict(input_from=self._name)


class NodeOutputRef(Source, Json):
    """Reference to a :py:class:`NodeOutput` instance. Used as source for node inputs and graph outputs."""

    def __init__(self, node_id: str = None, name: str = None, node_output: 'NodeOutput' = None):
        assert not (node_id is None and node_output is None)
        assert not (name is None and node_output is None)
        node_id = node_output.node.id if node_output else node_id
        name = node_output.name if node_output else name
        assert node_id is not None
        assert name is not None
        self._node_id = node_id
        self._name = name
        self._node_output = node_output

    @property
    def node_id(self) -> str:
        return self._node_id

    @property
    def name(self) -> str:
        return self._name

    @property
    def node_output(self) -> 'NodeOutput':
        return self._node_output

    def resolve(self, node_output: 'NodeOutput'):
        assert node_output.node.id == self._node_id
        assert node_output.name == self._name
        self._node_output = node_output

    @property
    def value(self):
        self._assert_resolved()
        return self._node_output.value

    @classmethod
    def from_json_dict(cls, json_dict: dict):
        if 'output_of' in json_dict:
            output_of = json_dict['output_of']
            # todo (nf) - add test and code dealing with rsplit failures
            node_id, node_output_name = output_of.rsplit('.', maxsplit=1)
            return cls(node_id, node_output_name)
        return None

    def to_json_dict(self):
        self._assert_resolved()
        return dict(output_of="%s.%s" % (self._node_id, self._name))

    def _assert_resolved(self):
        if self._node_output is None:
            raise ValueError("unresolved output '%s' of node '%s'" % (self._name, self._node_id))


class SourceHolder(metaclass=ABCMeta):
    """Something that can be connected to and disconnected from a current :py:class:`Source`."""

    @property
    @abstractmethod
    def source(self) -> Source:
        """The current source."""

    @abstractmethod
    def connect_source(self, source: Source):
        """Connect with the given *source*."""

    @abstractmethod
    def disconnect_source(self):
        """Disconnect from the current source."""


# noinspection PyAttributeOutsideInit
class SourceHolderMixin(SourceHolder):
    """Mixin class for classes that hold a ``_source`` attribute of type :py:class`Source`. """

    @property
    def source(self) -> Source:
        return self._source

    def connect_source(self, new_source: Source):
        # convert 'new_source' so it matches our graph construction rules
        if new_source is UNDEFINED:
            new_source = UNDEFINED_SOURCE
        elif not isinstance(new_source, Source):
            new_source = ConstantSource(new_source)
        elif isinstance(new_source, NodeOutput):
            new_source = NodeOutputRef(node_output=new_source)
        elif isinstance(new_source, GraphInput):
            new_source = GraphInputRef(graph_input=new_source)
        # set new source
        self._source = new_source

    def disconnect_source(self):
        self.connect_source(UNDEFINED_SOURCE)


class NodeInput(Source, SourceHolderMixin):
    """Represents a named input for child nodes of type :py:class:`OpNode` and :py:class:`GraphFileNode`. """

    def __init__(self, node: Node, name: str, source: Source = UNDEFINED_SOURCE):
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
    def node_id(self) -> Node:
        return self._node.id

    @property
    def name(self) -> str:
        return self._name

    @property
    def value(self):
        return self._source.value

    def __str__(self):
        return "%s.%s" % (self._node.id, self._name)


class NodeOutput(Source, Target):
    """Represents a named output for child nodes of type :py:class:`OpNode` and :py:class:`GraphFileNode`. """

    def __init__(self, node: Node, name: str):
        assert node is not None
        assert name is not None
        assert name in node.op_meta_info.output
        self._node = node
        self._name = name
        self._value = UNDEFINED

    @property
    def node(self) -> Node:
        return self._node

    @property
    def name(self) -> str:
        return self._name

    @property
    def value(self):
        return self._value

    def set_value(self, value):
        self._value = value

    def __str__(self):
        return "%s.%s" % (self._node.id, self._name)


class GraphInput(NodeInput):
    """Represents a named input for nodes of type :py:class:`Graph`. """

    def __init__(self, graph: Graph, name: str, source: Source = UNDEFINED_SOURCE):
        super(GraphInput, self).__init__(graph, name, source=source)

    @property
    def graph(self) -> Graph:
        return self.node


class GraphOutput(Source, SourceHolderMixin):
    """Represents a named output for nodes of type :py:class:`Graph`. """

    def __init__(self, graph: Graph, name: str, source: Source = UNDEFINED_SOURCE):
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
    def value(self):
        return self._source.value

    def __str__(self):
        return "%s.%s" % (self._graph.id, self._name)


class NodeInputNamespace(Namespace):
    """
    Namespace for node inputs.
    See :py:class:`NodeOutputNamespace`.
    """

    def __init__(self, node: Node):
        inputs = [(input_name, node.new_node_input(input_name)) for input_name, _ in node.op_meta_info.input]
        super(NodeInputNamespace, self).__init__(inputs)

    def __setattr__(self, name, value):
        """
        Allows connecting node inputs (or any other :py:class:`SourceHolder` input)
        with sources by assignment operator. For example:::

            node1.input.x = node2.output.b

        connects input ``x`` of ``node1`` with output ``b`` of ``node2``, while::

            node1.input.x = 3.5

        connects input ``x`` of ``node1`` with a constant source of value ``3.5``.

        :param name: input name.
        :param value: a :py:class:`Source` or anything else which will be converted to a ``Source``.
        """
        node_input = self.__getattr__(name)
        if not isinstance(node_input, SourceHolder):
            raise AttributeError("input '%s' is not connectable" % name)
        node_input.connect_source(value)

    def __getattr__(self, name):
        try:
            return super(NodeInputNamespace, self).__getattr__(name)
        except AttributeError:
            raise AttributeError("'%s' is not an input" % name)

    def __delattr__(self, input_name):
        raise NotImplementedError()


class NodeOutputNamespace(Namespace):
    """
    Namespace for node outputs.
    See :py:class:`NodeInputNamespace`.
    """

    def __init__(self, node: Node):
        outputs = [(output_name, node.new_node_output(output_name)) for output_name, _ in node.op_meta_info.output]
        super(NodeOutputNamespace, self).__init__(outputs)

    def __setattr__(self, name, value):
        """
        Connect
        Allows connecting graph outputs (or any other :py:class:`SourceHolder` output)
        with sources by attribute assignment. For example:::

            graph.output.b = node2.output.w

        connects output ``b`` of ``graph`` with output ``w`` of ``node2``, while::

            graph.output.b = 3.5

        connects output ``b`` of ``graph`` with a constant source of value ``3.5``.

        :param name: output name.
        :param value: a :py:class:`Source` or anything else which will be converted to a ``Source``.
        """
        node_output = self.__getattr__(name)
        if not isinstance(node_output, SourceHolder):
            raise AttributeError("output '%s' is not connectable" % name)
        node_output.connect_source(value)

    def __getattr__(self, name):
        try:
            return super(NodeOutputNamespace, self).__getattr__(name)
        except AttributeError:
            raise AttributeError("'%s' is not an output" % name)

    def __delattr__(self, name):
        raise NotImplementedError()
