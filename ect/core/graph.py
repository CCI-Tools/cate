"""
Module Description
==================

Provides classes that are used to construct processing graphs (workflows, networks) from registered operations
and connected graphs of such.

This module provides the following data types:

* A :py:class:`Node` has zero or more ``NodeInput`` and zero or more ``NodeOutput`` objects
* A :py:class:`OpNode` is a ``Node`` that wraps an executable operation.
* A :py:class:`GraphNode` is a ``Node`` that wraps an executable ``Graph`` loaded from an external JSON resource.
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

* ``{"source":`` *node-id* ``.`` *name* ``}``: the output named *name* of another node given by *node-id*.
* ``{"source":`` *name* ``}``: a graph input or node output named *name* of this node or any parent node.
* ``{"value":``  *value* ``}``: a constant value, where *value* my be any JSON-value.

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
from .op import OP_REGISTRY, OpMetaInfo, OpRegistration
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

    def __init__(self,
                 op_meta_info: OpMetaInfo,
                 node_id: str,
                 input_is_source: bool,
                 output_is_source: bool):
        assert op_meta_info
        assert node_id
        self._parent = None
        self._id = node_id
        self._op_meta_info = op_meta_info
        self._input = self._new_input_namespace(input_is_source)
        self._output = self._new_output_namespace(output_is_source)

    @property
    def root(self) -> 'Node':
        """The root node."""
        node = self
        while node._parent:
            node = node._parent
        return node

    @property
    def parent(self) -> 'Node':
        """The node's parent node or ``None`` if this node has no parent."""
        return self._parent

    @property
    def id(self):
        """The node's ID."""
        return self._id

    @property
    def op_meta_info(self) -> OpMetaInfo:
        """The node's operation meta-information."""
        return self._op_meta_info

    @property
    def input(self) -> Namespace:
        """The node's inputs."""
        return self._input

    @property
    def output(self) -> Namespace:
        """The node's outputs."""
        return self._output

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

    def find_node(self, node_id) -> 'Node':
        """Find a (child) node with the given *node_id*."""
        return None

    def resolve_source_refs(self):
        """Resolve unresolved source references in inputs and outputs."""
        for node_output in self._output[:]:
            node_output.resolve_source_ref()
        for node_input in self._input[:]:
            node_input.resolve_source_ref()

    def find_connector(self, name):
        """Resolve unresolved source references in inputs and outputs."""
        if name in self._output:
            return self._output[name]
        if name in self._input:
            return self._input[name]
        return None

    def __str__(self):
        """String representation."""
        return self.id

    @abstractmethod
    def __repr__(self):
        """String representation for developers."""

    def _new_input_namespace(self, is_source: bool):
        return self._new_namespace(is_source, self.op_meta_info.input.keys())

    def _new_output_namespace(self, is_source: bool):
        return self._new_namespace(is_source, self.op_meta_info.output.keys())

    def _new_namespace(self, is_source: bool, names):
        return Namespace([(name, NodeConnector(self, name, is_source)) for name in names])


class ChildNode(Node):
    """
    An inner node of a graph.

    :param op_meta_info: Meta-information about the operation, see :py:class:`OpMetaInfo`.
    :param node_id: A node ID. If None, a unique name will be generated.
    """

    def __init__(self, op_meta_info: OpMetaInfo, node_id: str):
        super(ChildNode, self).__init__(op_meta_info, node_id, False, True)

    @classmethod
    def from_json_dict(cls, json_dict, registry=OP_REGISTRY) -> Optional['ChildNode']:
        node = cls.new_node_from_json_dict(json_dict, registry=registry)
        if node is None:
            return None
        node_input_dict = json_dict.get('input', None)
        for node_input in node.input[:]:
            node_input.from_json_dict(node_input_dict)
        return node

    @classmethod
    @abstractmethod
    def new_node_from_json_dict(cls, json_dict, registry=OP_REGISTRY):
        """Create a new child node instance from the given *json_dict*"""

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
        self.enhance_json_dict(node_dict)
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
        node_id = node_id if node_id else 'graph_node_' + hex(id(self))[2:]
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
    def new_node_from_json_dict(cls, json_dict, registry=OP_REGISTRY):
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

    def __init__(self, operation, node_id=None, registry=OP_REGISTRY):
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
        for node_input in self.input[:]:
            input_values[node_input.name] = node_input.value

        return_value = self._op_registration(monitor=monitor, **input_values)

        if self.op_meta_info.has_named_outputs:
            for output_name, output_value in return_value.items():
                self.output[output_name].value = output_value
        else:
            self.output[OpMetaInfo.RETURN_OUTPUT_NAME].value = return_value

    @classmethod
    def new_node_from_json_dict(cls, json_dict, registry=OP_REGISTRY):
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

    :param name_or_op_meta_info: Qualified operation name or meta-information object of type :py:class:`OpMetaInfo`.
    """

    def __init__(self, name_or_op_meta_info: Union[str, OpMetaInfo]):
        if isinstance(name_or_op_meta_info, str):
            op_meta_info = OpMetaInfo(name_or_op_meta_info)
        else:
            op_meta_info = name_or_op_meta_info
        super(Graph, self).__init__(op_meta_info, op_meta_info.qualified_name, True, False)
        self._nodes = OrderedDict()

    @property
    def nodes(self) -> Sequence[Node]:
        return list(self._nodes.values())

    def find_node(self, node_id) -> Node:
        # is it the ID of one of the direct children?
        if node_id in self._nodes:
            return self._nodes[node_id]
        # is it the ID of one of the children of the children?
        for node in self._nodes:
            other_node = node.find_node(node_id)
            if other_node:
                return other_node
        return None

    def add_nodes(self, *nodes: Sequence[Node]):
        for node in nodes:
            self._nodes[node.id] = node
            node._parent = self

    def remove_node(self, node: Node):
        if node.id in self._nodes:
            self._nodes.remove(node.id)
            node._parent = None

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
    def load(cls, file_path_or_fp: Union[str, IOBase], registry=OP_REGISTRY) -> 'Graph':
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
    def from_json_dict(cls, graph_json_dict, registry=OP_REGISTRY):
        # Developer note: keep variable naming consistent with Graph.to_json_dict() method

        qualified_name = graph_json_dict.get('qualified_name', None)
        if qualified_name is None:
            raise ValueError('missing mandatory property "qualified_name" in graph JSON')
        header_json_dict = graph_json_dict.get('header', {})
        input_json_dict = graph_json_dict.get('input', {})
        output_json_dict = graph_json_dict.get('output', {})
        nodes_json_list = graph_json_dict.get('nodes', [])

        # convert 'data_type' entries to Python types in op_meta_info_input_json_dict & node_output_json_dict
        input_obj_dict = OpMetaInfo.json_dict_to_object_dict(input_json_dict)
        output_obj_dict = OpMetaInfo.json_dict_to_object_dict(output_json_dict)
        op_meta_info = OpMetaInfo(qualified_name,
                                  has_monitor=True,
                                  header_dict=header_json_dict,
                                  input_dict=input_obj_dict,
                                  output_dict=output_obj_dict)

        # parse all nodes
        nodes = []
        for graph_node_json_dict in nodes_json_list:
            node = None
            for node_class in [OpNode, GraphNode]:
                node = node_class.from_json_dict(graph_node_json_dict, registry=registry)
                if node is not None:
                    break
            if node is not None:
                nodes.append(node)
            else:
                raise ValueError("unknown node type in graph '%s'" % qualified_name)

        graph = Graph(op_meta_info)
        graph.add_nodes(*nodes)

        for node_input in graph.input[:]:
            node_input.from_json_dict(input_json_dict)
        for node_output in graph.output[:]:
            node_output.from_json_dict(output_json_dict)

        graph.resolve_source_refs()
        return graph

    def to_json_dict(self):
        """
        Return a JSON-serializable dictionary representation of this object.

        :return: A JSON-serializable dictionary
        """
        # Developer note: keep variable naming consistent with Graph.from_json_dict() method

        # convert all inputs to JSON dicts
        input_json_dict = OrderedDict()
        for node_input in self._input[:]:
            node_input_json_dict = node_input.to_json_dict()
            if node_input.name in self.op_meta_info.output:
                node_input_json_dict.update(self.op_meta_info.input[node_input.name])
            input_json_dict[node_input.name] = node_input_json_dict

        # convert all outputs to JSON dicts
        output_json_dict = OrderedDict()
        for node_output in self._output[:]:
            node_output_json_dict = node_output.to_json_dict()
            if node_output.name in self.op_meta_info.output:
                node_output_json_dict.update(self.op_meta_info.output[node_output.name])
            output_json_dict[node_output.name] = node_output_json_dict

        # convert all nodes to JSON dicts
        nodes_json_list = []
        for node in self._nodes.values():
            nodes_json_list.append(node.to_json_dict())

        # convert 'data_type' Python types entries to JSON-strings
        input_json_dict = OpMetaInfo.object_dict_to_json_dict(input_json_dict)
        output_json_dict = OpMetaInfo.object_dict_to_json_dict(output_json_dict)

        graph_json_dict = OrderedDict()
        graph_json_dict['qualified_name'] = self.op_meta_info.qualified_name
        graph_json_dict['input'] = input_json_dict
        graph_json_dict['output'] = output_json_dict
        graph_json_dict['nodes'] = nodes_json_list

        return graph_json_dict

    def __str__(self):
        return self.id

    def __repr__(self):
        return "Graph(%s)" % repr(self.op_meta_info.qualified_name)


class NodeConnector:
    """Represents a named input or output of a :py:class:`Node`. """

    def __init__(self, node: Node, name: str, is_source: bool = False):
        assert node is not None
        assert name is not None
        assert name in node.op_meta_info.input or name in node.op_meta_info.output
        self._node = node
        self._name = name
        self._is_source = is_source
        self._source_ref = None
        self._source = None
        self._value = None

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
    def is_source(self) -> bool:
        return self._is_source

    @property
    def value(self):
        if self._source:
            return self._source.value
        else:
            return self._value

    @value.setter
    def value(self, new_value):
        self._value = new_value
        self._source = None

    @property
    def source(self) -> 'NodeConnector':
        return self._source

    @source.setter
    def source(self, new_source: 'NodeConnector'):
        if not new_source.is_source:
            raise ValueError("cannot connect '%s' with '%s' because the latter is not a valid source" % (
                self, new_source))
        self._source = new_source
        self._value = None

    def connect_source(self, value):
        if isinstance(value, NodeConnector):
            self.source = value
        else:
            self.value = value

    def resolve_source_ref(self):
        if self._source_ref:
            other_node_id, other_name = self._source_ref
            if other_node_id:
                other_node = self._node.root.find_node(other_node_id)
                if other_node:
                    node_connector = other_node.find_connector(other_name)
                    if node_connector:
                        self.connect_source(node_connector)
                        return
                    raise ValueError(
                        "cannot connect '%s' with '%s.%s' because node '%s' has no input/output named '%s'" % (
                            self, other_node_id, other_name, other_node_id, other_name))
                else:
                    raise ValueError("cannot connect '%s' with '%s.%s' because node '%s' does not exist" % (
                        self, other_node_id, other_name, other_node_id))
            else:
                other_node = self._node
                while other_node:
                    node_connector = other_node.find_connector(other_name)
                    if node_connector:
                        self.connect_source(node_connector)
                        return
                    other_node = other_node.parent
                raise ValueError(
                    "cannot connect '%s' with '%s' because '%s' does not exist" % (
                        self, other_name, other_name))

    def from_json_dict(self, json_dict):
        self._source_ref = None
        self._source = None
        self._value = None
        connector_source_def = json_dict.get(self.name, None)
        if not connector_source_def:
            connector_source_def = self.name
        elif not isinstance(connector_source_def, str):
            connector_json_dict = json_dict.get(self.name, None)
            if 'source' in connector_json_dict:
                if 'value' in connector_json_dict:
                    raise ValueError(
                        "error decoding '%s' because \"source\" and \"value\" are mutually exclusive" % self)
                connector_source_def = connector_json_dict['source']
            elif 'value' in connector_json_dict:
                # Care: constant may be converted to a real Python value here
                # Must add converter callback, or so.
                self.value = connector_json_dict['value']
                return
            else:
                raise ValueError(
                    "error decoding '%s' because either \"source\" or \"value\" must be provided" % self.node_id)

        parts = connector_source_def.rsplit('.', maxsplit=1)
        if len(parts) == 1 and parts[0]:
            node_id = None
            connector_name = parts[0]
        elif len(parts) == 2 and parts[0] and parts[1]:
            node_id = parts[0]
            connector_name = parts[1]
        else:
            raise ValueError(
                "error decoding '%s' because the \"source\" value format is neither <name> nor <node-id>.<name>" % self)
        self._source_ref = node_id, connector_name

    def to_json_dict(self):
        """
        Return a JSON-serializable dictionary representation of this object.

        :return: A JSON-serializable dictionary
        """
        json_dict = dict()
        if self._source:
            json_dict['source'] = '%s.%s' % (self._source.node.id, self._source.name)
        else:
            json_dict['value'] = self._value
        return json_dict

    def __str__(self):
        return "%s.%s" % (self._node.id, self._name)

    def __repr__(self):
        return "NodeConnector(%s, %s, is_source=%s)" % (repr(self.node_id), repr(self.name), repr(self.is_source))
