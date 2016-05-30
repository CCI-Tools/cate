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
from typing import List

from ect.core import Monitor
from .op import REGISTRY, OpMetaInfo, OpRegistration
from .util import Namespace


class Node(metaclass=ABCMeta):
    """
    Nodes can be used to construct networks or graphs of operations.
    Input and output of an operation are available as node attributes of type :py:class:`Connector`.

    :param op_meta_info: Meta-information about the operation, see :py:class:`OpMetaInfo`.
    :param output_cls: Class of the output to create.
    :param node_id: A node ID. If None, a unique name will be generated.
    """

    def __init__(self, op_meta_info: OpMetaInfo, output_cls, node_id=None):
        if not op_meta_info:
            raise ValueError('op_meta_info must be given')
        node_id = node_id if node_id is not None else type(self).__name__ + str(id(self))
        self._id = node_id
        self._op_meta_info = op_meta_info
        self._node_input_namespace = NodeInputNamespace(self)
        self._node_output_namespace = NodeOutputNamespace(self, output_cls=output_cls)

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
        super(OpNode, self).__init__(op_meta_info, NodeOutput, node_id=node_id)
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
            if 'output_of' in node_input_source_dict:
                json_output_of = node_input_source_dict['output_of']
                node_id, node_output_name = json_output_of.rsplit('.', maxsplit=1)
                node_input.set_source(NodeOutputRefInput(node_id, node_output_name))
            elif 'constant' in node_input_source_dict:
                pass
                # json_constant = node_input_source_dict['constant']
                # check (nf) - convert constant from its json_constant representation into a Python value
                # node_input.set_source(ConstantValueSource(json_constant))
            elif 'parameter' in node_input_source_dict:
                pass
                # We currently ignore the 'parameter' value, we only need to know if the source is bound to a parameter
                # json_parameter = node_input_source_dict['parameter']
                # node_input.set_source(ParameterValueSource())
        # node_output_dict = json_dict.get('output', None)
        return op_node

    def to_json_dict(self):
        """
        Return a JSON-serializable dictionary representation of this object.

        :return: A JSON-serializable dictionary
        """
        node_input_dict = OrderedDict()
        for node_input in self.input[:]:
            source = node_input.source
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
        op_meta_info = op_meta_info if op_meta_info is not None else OpMetaInfo(graph_id)
        super(Graph, self).__init__(op_meta_info, GraphOutput, node_id=graph_id)
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
        # Developer note: keep variable naming consistent with Graph.from_json_dict() method
        node_input_json_dict = OrderedDict()
        for node_input in self._node_input_namespace[:]:
            node_input_json_dict[node_input.name] = node_input.to_json_dict()

        node_output_json_dict = OrderedDict()
        for node_output in self._node_output_namespace[:]:
            node_output_json_dict[node_output.name] = node_output.to_json_dict()

        graph_nodes_list = []
        for node in self._nodes:
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

        graph_id = graph_json_dict.get('id', None)
        # node_input_json_dict = graph_json_dict.get('input', [])
        # node_output_json_dict = graph_json_dict.get('output', {})
        graph_nodes_json_list = graph_json_dict.get('nodes', {})

        nodes = []
        for graph_node_json_dict in graph_nodes_json_list:
            if 'op' in graph_node_json_dict:
                node = OpNode.from_json_dict(graph_node_json_dict, registry=registry)
            elif 'graph' in graph_node_json_dict:
                node = Graph.from_json_dict(graph_node_json_dict, registry=registry)
            else:
                raise ValueError("either the 'op' or 'graph' property must be given")
            nodes.append(node)

        # Now replace all ProxyValueSource by real NodeOutput
        node_dict = {node.id: node for node in nodes}
        for node in nodes:
            for node_input in node.input[:]:
                if isinstance(node_input.source, NodeOutputRefInput):
                    other_node_id = node_input.source.node_id
                    other_node_output_name = node_input.source.node_output_name
                    if other_node_id not in node_dict:
                        raise ValueError("unknown node '%s'" % other_node_id)
                    other_node = node_dict[other_node_id]
                    if other_node_output_name not in other_node.output:
                        raise ValueError("unknown output '%s' of node '%s'", (other_node_output_name, other_node_id))
                    other_node_output = other_node.output[other_node_output_name]
                    node_input.source.set_node_output(other_node_output)
                    node_input.join(other_node_output)

        graph = Graph(*nodes, graph_id=graph_id)
        return graph

    def __str__(self):
        return "Graph('%s')" % self.op_meta_info.qualified_name

    def __repr__(self):
        return "Graph('%s')" % self.op_meta_info.qualified_name

    def _update_on_node_added(self, node):
        graph_meta_info = self.op_meta_info
        node_meta_info = node.op_meta_info
        for input_name, input_value in node.input:
            assert input_value is not None
            if isinstance(input_value, ExternalInput):
                # Make sure graph meta_info is correct
                if input_name not in graph_meta_info.input:
                    graph_meta_info.input[input_name] = dict(node_meta_info.input[input_name])
                # Now the graph gets the ExternalInput and the node gets an InputRefInput
                self.input[input_name] = input_value
                node.input[input_name] = SourceRefInput(input_value)
        for node_output in node.output[:]:
            if not node_output.targets:
                name = node_output.name
                # Make sure graph meta_info is correct
                if name not in graph_meta_info.output:
                    graph_meta_info.output[name] = dict(node_meta_info.output[name])
                # Add output
                self.output[name] = node_output


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
    @abstractmethod
    def get_value(self):
        """Get the value of this input."""
        pass


class Target(Json, metaclass=ABCMeta):
    @abstractmethod
    def set_value(self, value):
        """Set the *value* of this output."""
        pass


class ExternalInput(Source, Target):
    """
    (1) Used as input to any node. Buffered.
    """

    def __init__(self):
        self._value = UNDEFINED

    def get_value(self):
        return self._value

    def set_value(self, value):
        self._value = value

    def to_json_dict(self):
        return dict(external=True)


class SourceRefInput(Source):
    """
    (2) Used both as input for nodes and as output for Graphs. Unbuffered.
    """

    def __init__(self, source: Source):
        self._source = source

    def get_value(self):
        return self._source.get_value()

    def to_json_dict(self):
        return dict(source=self._source.to_json_dict())


class ConstantInput(Source):
    """
    (4) A value source whose actual value is a constant value. Buffered.
    """

    def __init__(self, constant):
        self._constant = constant

    def get_value(self):
        return self._constant

    def to_json_dict(self):
        # Care: self._constant may not be JSON-serializable!
        # Must add converter callback, or so.
        return dict(constant=self._constant)


class NodeOutputRefInput(Source):
    """
    (5) Used both as input for nodes and as output for Graphs. Unbuffered.
    """

    def __init__(self, node_id: str, node_output_name: str):
        self._node_id = node_id
        self._node_output_name = node_output_name
        self._node_output = None

    def node_id(self) -> str:
        return self._node_id

    def node_output_name(self) -> str:
        return self._node_output_name

    def set_node_output(self, node_output: 'NodeOutput'):
        assert node_output.node.id == self._node_id
        assert node_output.name == self._node_output_name
        self._node_output = node_output

    def get_value(self):
        self._assert_resolved()
        return self._node_output.get_value()

    def to_json_dict(self):
        self._assert_resolved()
        return dict(output_of="%s.%s" % (self._node_id, self._node_output_name))

    def _assert_resolved(self):
        if self._node_output is None:
            raise ValueError("unresolved output '%s' of node '%s'" % (self._node_output_name, self._node_id))


class SourceHolder(metaclass=ABCMeta):
    @abstractmethod
    def set_source(self, source: Source):
        """Get the *source* of this source holder."""
        pass


class TargetTracker(metaclass=ABCMeta):
    @abstractmethod
    def add_target(self, target):
        pass

    @abstractmethod
    def remove_target(self, target):
        pass


class NodeInput(Source, SourceHolder):
    def __init__(self, node: Node, name: str, source: Source = None):
        self._node = node
        self._name = name
        if source is None:
            self._source = ExternalInput()
        else:
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

    def set_source(self, new_source: Source):
        old_source = self._source
        if isinstance(old_source, TargetTracker):
            old_source.remove_target(self)
        self._source = new_source
        if isinstance(new_source, TargetTracker):
            new_source.add_target(self)

    def get_value(self):
        return self._source.get_value()

    def to_json_dict(self):
        return self._source.to_json_dict()


class NodeOutput(Source, Target, TargetTracker):
    def __init__(self, node: Node, name: str):
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
    def targets(self) -> List:
        return list(self._targets)

    def add_target(self, target):
        if target not in self._targets:
            self._targets.append(target)

    def remove_target(self, target):
        if target in self._targets:
            self._targets.remove(target)

    def get_value(self):
        return self._value

    def set_value(self, value):
        self._value = value

    def to_json_dict(self):
        return {}


class GraphOutput(Source, SourceHolder):
    def __init__(self, graph: Graph, name: str, source: Source = None):
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

    def set_source(self, new_source: Source):
        old_source = self._source
        if isinstance(old_source, TargetTracker):
            old_source.remove_target(self)
        self._source = new_source
        if isinstance(new_source, TargetTracker):
            new_source.add_target(self)

    def get_value(self):
        return self._source.get_value()

    def to_json_dict(self):
        return self._source.to_json_dict()


class NodeInputNamespace(Namespace):
    def __init__(self, node: Node):
        self._node = node
        inputs = [(input_name, NodeInput(node, input_name)) for input_name, _ in node.op_meta_info.input]
        super(NodeInputNamespace, self).__init__(inputs)

    def __setattr__(self, name, value):
        if name == '_node':
            super(NodeInputNamespace, self).__setattr__(name, value)
            return
        node_input = self.__getattr__(name)
        if not isinstance(value, Source):
            value = ConstantInput(value)
        node_input.set_source(value)

    def __getattr__(self, name):
        try:
            return super(NodeInputNamespace, self).__getattr__(name)
        except AttributeError:
            raise AttributeError("'%s' is not an input of node '%s'" % (name, self._node.id))

    def __delattr__(self, input_name):
        raise NotImplementedError()


class NodeOutputNamespace(Namespace):
    def __init__(self, node: Node, output_cls):
        self._node = node
        outputs = [(output_name, output_cls(node, output_name)) for output_name, _ in node.op_meta_info.output]
        super(NodeOutputNamespace, self).__init__(outputs)

    def __setattr__(self, name, value):
        if name == '_node':
            super(NodeOutputNamespace, self).__setattr__(name, value)
            return
        node_output = self.__getattr__(name)
        if not isinstance(value, Source):
            raise AttributeError("invalid value for output '%s' of node '%s'" % (name, self._node.id))
        if not isinstance(node_output, SourceHolder):
            raise AttributeError("output '%s' of node '%s' cannot be set" % (name, self._node.id))
        node_output.set_source(value)

    def __getattr__(self, name):
        try:
            return super(NodeOutputNamespace, self).__getattr__(name)
        except AttributeError:
            raise AttributeError("'%s' is not an output of node '%s'" % (name, self._node.id))

    def __delattr__(self, name):
        raise NotImplementedError()
