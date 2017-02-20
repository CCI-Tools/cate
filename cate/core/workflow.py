# The MIT License (MIT)
# Copyright (c) 2017 by the Cate Development Team and contributors
#
# Permission is hereby granted, free of charge, to any person obtaining a copy of
# this software and associated documentation files (the "Software"), to deal in
# the Software without restriction, including without limitation the rights to
# use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies
# of the Software, and to permit persons to whom the Software is furnished to do
# so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

__author__ = "Norman Fomferra (Brockmann Consult GmbH)"

"""
Description
===========

Provides classes that are used to construct processing *workflows* (networks, directed acyclic graphs)
from processing *steps* including Python callables, Python expressions, external processes, and other workflows.

This module provides the following data types:

* A :py:class:`Node` has zero or more *inputs* and zero or more *outputs* and can be invoked
* A :py:class:`Workflow` is a ``Node`` that is composed of ``Step`` objects
* A :py:class:`Step` is a ``Node`` that is part of a ``Workflow`` and performs some kind of data processing.
* A :py:class:`OpStep` is a ``Step`` that invokes a Python operation (any callable).
* A :py:class:`ExprStep` is a ``Step`` that executes a Python expression string.
* A :py:class:`WorkflowStep` is a ``Step`` that executes a ``Workflow`` loaded from an external (JSON) resource.
* A :py:class:`NodePort` belongs to exactly one ``Node``. Node ports represent both the named inputs and
  outputs of node. A node port has a name, a property ``source``, and a property ``value``.
  If ``source`` is set, it must be another ``NodePort`` that provides the actual port's value.
  The value of the ``value`` property can be basically anything that has an external (JSON) representation.

Workflow input ports are usually unspecified, but ``value`` may be set.
Workflow output ports and a step's input ports are usually connected with output ports of other contained steps
or inputs of the workflow via the ``source`` attribute.
A step's output ports are usually unconnected because their ``value`` attribute is set by a step's concrete
implementation.

Step node inputs and workflow outputs are indicated in the input specification of a node's external JSON
representation:

* ``{"source": "NODE_ID.PORT_NAME" }``: the output (or input) named *PORT_NAME* of another node given by *NODE_ID*.
* ``{"source": ".PORT_NAME" }``: current step's output (or input) named *PORT_NAME* or of any of its parents.
* ``{"source": "NODE_ID" }``: the one and only output of a workflow or of one of its nodes given by *NODE_ID*.
* ``{"value": NUM|STR|LIST|DICT|null }``: a constant (JSON) value.

Workflows are callable by the CLI in the same way as single operations. The command line form for calling an
operation is currently:::

    cate run OP|WORKFLOW [ARGS]

Where *OP* is a registered operation and *WORKFLOW* is a JSON file containing a JSON workflow representation.

Technical Requirements
======================

**Combine processors and other operations to create operation chains or processing graphs**

:Description: Provide the means to connect multiple processing steps, which may be registered operations, operating
    system calls, remote service invocations.

:URD-Sources:
    * CCIT-UR-LM0001: processor management allowing easy selection of tools and functionalities.
    * CCIT-UR-LM0003: easy construction of graphs without any knowledge of a programming language (Graph Builder).
    * CCIT-UR-LM0004: selection of a number of predefined standard processing chains.
    * CCIT-UR-LM0005: means to configure a processor chain comprised of one processor only from the library to
      execute on data from the Common Data Model.

----

**Integration of external, ECV-specific programs**

:Description: Some processing step might only be solved by executing an external tool. Therefore,
    a special workflow step shall allow for invocation of external programs hereby mapping input values to
    program arguments, and program outputs to step outputs. It shall also be possible to monitor the state of the
    running sub-process.

:URD-Source:
    * CCIT-UR-LM0002: accommodating ECV-specific processors in cases where the processing is specific to an ECV.

----

**Programming language neutral representation**

:Description: Processing graphs must be representable in a programming language neutral representation such as
    XML, JSON, YAML, so they can be designed by non-programmers and can be easily serialised, e.g. for communication
    with a web service.

:URD-Source:
    * CCIT-UR-LM0003: easy construction of graphs without any knowledge of a programming language
    * CCIT-UR-CL0001: reading and executing script files written in XML or similar

----

Verification
============

The module's unit-tests are located in
`test/test_workflow.py <https://github.com/CCI-Tools/cate-core/blob/master/test/test_workflow.py>`_
and may be executed using ``$ py.test test/test_workflow.py --cov=cate/core/workflow.py`` for extra code
coverage information.

Components
==========
"""

from abc import ABCMeta, abstractmethod
from collections import OrderedDict
from io import IOBase
from itertools import chain
from typing import Sequence, Optional, Union, List, Dict

from cate.util import Namespace, UNDEFINED
from cate.util.monitor import Monitor
from .op import OP_REGISTRY, OpRegistration
from cate.util.opmetainf import OpMetaInfo
from .workflow_svg import Drawing as _Drawing
from .workflow_svg import Graph as _Graph
from .workflow_svg import Node as _Node


class Node(metaclass=ABCMeta):
    """
    Base class for all nodes including parent nodes (e.g. :py:class:`Workflow`) and child nodes (e.g. :py:class:`Step`).

    All nodes have inputs and outputs, and can be invoked to perform some operation.

    Inputs and outputs are exposed as attributes of the :py:attr:`input` and :py:attr:`output` properties and
    are both of type :py:class:`NodePort`.

    :param op_meta_info: Meta-information about the operation, see :py:class:`OpMetaInfo`.
    :param node_id: A node ID. If None, a unique name will be generated.
    """

    def __init__(self,
                 op_meta_info: OpMetaInfo,
                 node_id: str):
        if not op_meta_info:
            raise ValueError('op_meta_info must be given')
        if not node_id:
            raise ValueError('node_id must be given')
        self._op_meta_info = op_meta_info
        self._id = node_id
        self._input = self._new_input_namespace()
        self._output = self._new_output_namespace()

    @property
    def op_meta_info(self) -> OpMetaInfo:
        """The node's operation meta-information."""
        return self._op_meta_info

    @property
    def id(self) -> str:
        """The node's operation meta-information."""
        return self._id

    @property
    def input(self) -> Namespace:
        """The node's inputs."""
        return self._input

    @property
    def output(self) -> Namespace:
        """The node's outputs."""
        return self._output

    @property
    def root_node(self) -> 'Node':
        """The root_node node."""
        node = self
        while node.parent_node:
            node = node.parent_node
        return node

    @property
    def parent_node(self) -> Optional['Node']:
        """The node's parent node or ``None`` if this node has no parent."""
        return None

    def find_node(self, node_id) -> Optional['Node']:
        """Find a (child) node with the given *node_id*."""
        return None

    def requires(self, other_node: 'Node') -> bool:
        """
        Does this node require *other_node* for its computation?
        Is *other_node* a source of this node?

        :param other_node: The other node.
        :return: ``True`` if this node is a target of *other_node*
        """
        return self.distance_to(other_node) >= 0

    def distance_to(self, other_node: 'Node') -> int:
        """
        If *other_node* is a source of this node, then return the number of connections from this node to *node*.
        If it is a direct source return ``1``, if it is a source of the source of this node return ``2``, cate.
        If *other_node* is this node, return 0.
        If *other_node* is not a source of this node, return -1.

        :param other_node: The other node.
        :return: The distance to *other_node*
        """
        if self == other_node:
            return 0
        for port in self._input[:]:
            if port.source is not None and port.source.node is other_node:
                return 1
        for port in self._input[:]:
            if port.source is not None:
                distance = port.source.node.requires(other_node)
                if distance > 0:
                    return distance + 1
        return -1

    def collect_predecessors(self, predecessors: List['Node'], excludes: List['Node'] = None):
        """Collect this node (self) and preceding nodes in *predecessors*."""
        if excludes and self in excludes:
            return
        if self in predecessors:
            predecessors.remove(self)
        predecessors.insert(0, self)
        for port in self.input[:]:
            if port.source is not None:
                port.source.node.collect_predecessors(predecessors, excludes)

    def __call__(self, value_cache: dict = None, monitor=Monitor.NONE, **input_values):
        """
        Make this class instance's callable. The call is delegated to :py:meth:`call()`.

        :param value_cache: An optional value cache.
        :param monitor: An optional progress monitor.
        :param input_values: The input values.
        :return: The output value.
        """
        return self.call(value_cache=value_cache, monitor=monitor, input_values=input_values)

    def call(self, value_cache: dict = None, monitor=Monitor.NONE, input_values: dict = None):
        """
        Calls this workflow with given *input_values* and returns the result.

        The method does the following:
        1. Set default_value where input values are missing in *input_values*
        2. Validate the input_values using this workflows's meta-info
        3. Set this workflow's input port values
        4. Invoke this workflow with given *value_cache* and *monitor*
        5. Get this workflow's output port values. Named outputs will be returned as dictionary.

        :param value_cache: An optional value cache.
        :param monitor: An optional progress monitor.
        :param input_values: The input values.
        :return: The output values.
        """
        input_values = input_values or {}
        # 1. Set default_value where input values are missing in *input_values*
        self.op_meta_info.set_default_input_values(input_values)
        # 2. Validate the input_values using this workflows's meta-info
        self.op_meta_info.validate_input_values(input_values)
        # 3. Set this workflow's input port values
        self.set_input_values(input_values)
        # 4. Invoke this workflow with given *value_cache* and *monitor*
        self.invoke(value_cache=value_cache, monitor=monitor)
        # 5. Get this workflow's output port values. Named outputs will be returned as dictionary.
        return self.get_output_value()

    @abstractmethod
    def invoke(self, value_cache: dict = None, monitor: Monitor = Monitor.NONE):
        """
        Invoke this node's underlying operation with input values from
        :py:attr:`input`. Output values in :py:attr:`output` will
        be set from the underlying operation's return value(s).

        :param value_cache: An optional dictionary that serves as a cache for node invocation results.
               A node may put a result into the cache after invocation, or return a value from the cache, if it exists.
        :param monitor: An optional progress monitor.
        """

    def set_input_values(self, input_values):
        for node_input in self.input[:]:
            node_input.value = input_values[node_input.name]

    def get_output_value(self):
        if self.op_meta_info.has_named_outputs:
            return {output.name: output.value for output in self.output[:]}
        else:
            return self.output[OpMetaInfo.RETURN_OUTPUT_NAME].value

    @abstractmethod
    def to_json_dict(self):
        """
        Return a JSON-serializable dictionary representation of this object.

        :return: A JSON-serializable dictionary
        """

    def resolve_source_refs(self):
        """Resolve unresolved source references in inputs and outputs."""
        for port in chain(self._output[:], self._input[:]):
            port.resolve_source_ref()

    def remove_orphaned_sources(self, orphaned_node: 'Node'):
        # Set all input/output ports to None, whose source are still old_step.
        # This will also set each port's source to None.
        # TODO (forman, 20160929): Actually, we should remove ports that still refer to old_step.
        for port in chain(self._output[:], self._input[:]):
            if port.source is not None and port.source.node is orphaned_node:
                port.value = None

    def find_port(self, name) -> Optional['NodePort']:
        """
        Find port with given name. Output ports are searched first, then input ports.
        :param name: The port name
        :return: The port, or ``None`` if it couldn't be found.
        """
        if name in self._output:
            return self._output[name]
        if name in self._input:
            return self._input[name]
        return None

    def _body_string(self) -> Optional[str]:
        return None

    def _format_port_assignments(self, namespace: Namespace, is_input: bool):
        port_assignments = []
        for port in namespace[:]:
            if port.source:
                port_assignments.append('%s=%s' % (port.name, str(port.source)))
            elif port.has_value:
                port_assignments.append('%s=%s' % (port.name, repr(port.value)))
            elif is_input:
                default_value = self.op_meta_info.input[port.name].get('default_value', None)
                port_assignments.append('%s=%s' % (port.name, repr(default_value)))
            else:
                port_assignments.append('%s' % port.name)
        return ', '.join(port_assignments)

    def __str__(self):
        """String representation."""
        op_meta_info = self.op_meta_info
        body_string = self._body_string() or op_meta_info.qualified_name
        input_assignments = self._format_port_assignments(self.input, True)
        output_assignments = ''
        if op_meta_info.has_named_outputs:
            output_assignments = self._format_port_assignments(self.output, False)
            output_assignments = ' -> (%s)' % output_assignments
        return '%s = %s(%s)%s [%s]' % (self.id, body_string, input_assignments, output_assignments, type(self).__name__)

    @abstractmethod
    def __repr__(self):
        """String representation for developers."""

    def _new_input_namespace(self):
        return self._new_namespace(self.op_meta_info.input.keys())

    def _new_output_namespace(self):
        return self._new_namespace(self.op_meta_info.output.keys())

    def _new_namespace(self, names):
        return Namespace([(name, NodePort(self, name)) for name in names])


class Workflow(Node):
    """
    A workflow of (connected) steps.

    :param name_or_op_meta_info: Qualified operation name or meta-information object of type :py:class:`OpMetaInfo`.
    """

    def __init__(self, name_or_op_meta_info: Union[str, OpMetaInfo]):
        if isinstance(name_or_op_meta_info, str):
            op_meta_info = OpMetaInfo(name_or_op_meta_info)
        else:
            op_meta_info = name_or_op_meta_info
        super(Workflow, self).__init__(op_meta_info, op_meta_info.qualified_name)
        self._steps = OrderedDict()

    @property
    def steps(self) -> List['Step']:
        return list(self._steps.values())

    def find_steps_to_compute(self, step_id: str) -> List['Step']:
        """
        Compute the list of steps required to compute the output of the step with the given *step_id*.
        The order of the returned list is its execution order, with the step given by *step_id* is the last one.

        :param step_id: The step to be computed last and whose output value is requested.
        :return: a list of steps, which is never empty
        """
        if step_id not in self._steps:
            raise ValueError('step_id argument does not identify a step: %s' % step_id)

        step = self._steps[step_id]
        steps = []
        step.collect_predecessors(steps, [self])
        return steps

    def find_node(self, step_id: str) -> Optional['Step']:
        # is it the ID of one of the direct children?
        if step_id in self._steps:
            return self._steps[step_id]
        # is it the ID of one of the children of the children?
        for node in self._steps.values():
            other_node = node.find_node(step_id)
            if other_node:
                return other_node
        return None

    def add_steps(self, *steps: Sequence['Step'], can_exist: bool = False) -> None:
        for step in steps:
            self.add_step(step, can_exist=can_exist)

    def add_step(self, step: 'Step', can_exist: bool = False) -> Optional['Step']:
        old_step = self._steps.get(step.id)
        if old_step is not None and not can_exist:
            raise ValueError("step '%s' already exists" % step.id)

        is_new = old_step is not step
        if is_new:
            self._steps[step.id] = step

        step._parent_node = self

        if is_new and (old_step is not None):
            # If the step already existed before, we must resolve source references again
            self.resolve_source_refs()
            # After reassigning sources, remove ports whose source is still old_step.
            # noinspection PyTypeChecker
            self.remove_orphaned_sources(old_step)

        return old_step

    def remove_step(self, step: Union[str, 'Step'], must_exist: bool = False) -> Optional['Step']:
        step_id = step if isinstance(step, str) else step.id
        if step_id not in self._steps:
            if must_exist:
                raise ValueError("step '%s' not found" % step_id)
            return None
        old_step = self._steps.pop(step_id)
        step._parent_node = None

        if old_step is not None:
            # After removing old_step, remove ports whose source is still old_step.
            self.remove_orphaned_sources(old_step)

        return old_step

    def resolve_source_refs(self) -> None:
        """Resolve unresolved source references in inputs and outputs."""
        super(Workflow, self).resolve_source_refs()
        for step in self._steps.values():
            step.resolve_source_refs()

    def remove_orphaned_sources(self, removed_node: Node):
        """
        Remove all input/output ports, whose source is still referring to *removed_node*.
        :param removed_node: A removed node.
        """
        super(Workflow, self).remove_orphaned_sources(removed_node)
        for step in self._steps.values():
            step.remove_orphaned_sources(removed_node)

    def invoke(self, value_cache: dict = None, monitor=Monitor.NONE) -> None:
        """
        Invoke this workflow by invoking all all of its step nodes.

        :param value_cache: An optional dictionary that serves as a cache for node invocation results.
               A node may put a result into the cache after invocation, or return a value from the cache, if it exists.
        :param monitor: An optional progress monitor.
        """
        self.invoke_steps(self.steps, value_cache=value_cache, monitor=monitor)

    @classmethod
    def invoke_steps(cls,
                     steps: List['Step'],
                     value_cache: dict = None,
                     monitor_label: str = None,
                     monitor=Monitor.NONE):
        step_count = len(steps)
        if step_count == 1:
            steps[0].invoke(value_cache=value_cache, monitor=monitor)
        elif step_count > 1:
            monitor_label = monitor_label or "Executing {step_count} workflow step(s)"
            with monitor.starting(monitor_label.format(step_count=step_count), step_count):
                for step in steps:
                    step.invoke(value_cache=value_cache, monitor=monitor.child(work=1))

    @classmethod
    def load(cls, file_path_or_fp: Union[str, IOBase], registry=OP_REGISTRY) -> 'Workflow':
        """
        Load a workflow from a file or file pointer. The format is expected to be "Workflow JSON".

        :param file_path_or_fp: file path or file pointer
        :param registry: Operation registry
        :return: a workflow
        """
        import json
        if isinstance(file_path_or_fp, str):
            with open(file_path_or_fp) as fp:
                json_dict = json.load(fp)
        else:
            json_dict = json.load(file_path_or_fp)
        return Workflow.from_json_dict(json_dict, registry=registry)

    def store(self, file_path_or_fp: Union[str, IOBase]) -> None:
        """
        Store a workflow to a file or file pointer. The format is "Workflow JSON".

        :param file_path_or_fp: file path or file pointer
        """
        import json

        json_dict = self.to_json_dict()
        dump_kwargs = dict(indent='  ')
        if isinstance(file_path_or_fp, str):
            with open(file_path_or_fp, 'w') as fp:
                json.dump(json_dict, fp, **dump_kwargs)
        else:
            json.dump(json_dict, file_path_or_fp, **dump_kwargs)

    @classmethod
    def from_json_dict(cls, workflow_json_dict, registry=OP_REGISTRY) -> 'Workflow':
        # Developer note: keep variable naming consistent with Workflow.to_json_dict() method

        qualified_name = workflow_json_dict.get('qualified_name', None)
        if qualified_name is None:
            raise ValueError('missing mandatory property "qualified_name" in Workflow-JSON')
        header_json_dict = workflow_json_dict.get('header', {})
        input_json_dict = workflow_json_dict.get('input', {})
        output_json_dict = workflow_json_dict.get('output', {})
        steps_json_list = workflow_json_dict.get('steps', [])

        # convert 'data_type' entries to Python types in op_meta_info_input_json_dict & node_output_json_dict
        input_obj_dict = OpMetaInfo.json_dict_to_object_dict(input_json_dict)
        output_obj_dict = OpMetaInfo.json_dict_to_object_dict(output_json_dict)
        op_meta_info = OpMetaInfo(qualified_name,
                                  has_monitor=True,
                                  header_dict=header_json_dict,
                                  input_dict=input_obj_dict,
                                  output_dict=output_obj_dict)

        # parse all step nodes
        steps = []
        step_count = 0
        for step_json_dict in steps_json_list:
            step_count += 1
            node = None
            for node_class in [OpStep, WorkflowStep, ExprStep, NoOpStep, SubProcessStep]:
                node = node_class.from_json_dict(step_json_dict, registry=registry)
                if node is not None:
                    steps.append(node)
                    break
            if node is None:
                raise ValueError("unknown type for node #%s in workflow '%s'" % (step_count, qualified_name))

        workflow = Workflow(op_meta_info)
        workflow.add_steps(*steps)

        for node_input in workflow.input[:]:
            node_input.from_json_dict(input_json_dict)
        for node_output in workflow.output[:]:
            node_output.from_json_dict(output_json_dict)

        workflow.resolve_source_refs()
        return workflow

    def to_json_dict(self) -> dict:
        """
        Return a JSON-serializable dictionary representation of this object.

        :return: A JSON-serializable dictionary
        """
        # Developer note: keep variable naming consistent with Workflow.from_json_dict() method

        # convert all inputs to JSON dicts
        input_json_dict = OrderedDict()
        for node_input in self._input[:]:
            node_input_json_dict = node_input.to_json_dict()
            if node_input.name in self.op_meta_info.input:
                node_input_json_dict.update(self.op_meta_info.input[node_input.name])
            input_json_dict[node_input.name] = node_input_json_dict

        # convert all outputs to JSON dicts
        output_json_dict = OrderedDict()
        for node_output in self._output[:]:
            node_output_json_dict = node_output.to_json_dict()
            if node_output.name in self.op_meta_info.output:
                node_output_json_dict.update(self.op_meta_info.output[node_output.name])
            output_json_dict[node_output.name] = node_output_json_dict

        # convert all step nodes to JSON dicts
        steps_json_list = []
        for step in self._steps.values():
            steps_json_list.append(step.to_json_dict())

        # convert 'data_type' Python types entries to JSON-strings
        header_json_dict = self.op_meta_info.header
        input_json_dict = OpMetaInfo.object_dict_to_json_dict(input_json_dict)
        output_json_dict = OpMetaInfo.object_dict_to_json_dict(output_json_dict)

        workflow_json_dict = OrderedDict()
        workflow_json_dict['qualified_name'] = self.op_meta_info.qualified_name
        workflow_json_dict['header'] = header_json_dict
        workflow_json_dict['input'] = input_json_dict
        workflow_json_dict['output'] = output_json_dict
        workflow_json_dict['steps'] = steps_json_list

        return workflow_json_dict

    def __repr__(self) -> str:
        return "Workflow(%s)" % repr(self.op_meta_info.qualified_name)

    def _repr_svg_(self) -> str:
        """
        Get a SVG-representation for IPython notebooks.

        :return: An SVG-representation of this workflow.
        """
        graph = _convert_workflow_to_graph(self)
        return _Drawing(graph).to_svg()


class Step(Node):
    """
    A step is an inner node of a workflow.

    :param op_meta_info: Meta-information about the operation, see :py:class:`OpMetaInfo`.
    :param node_id: A node ID. If None, a unique name will be generated.
    """

    def __init__(self, op_meta_info: OpMetaInfo, node_id: str):
        super(Step, self).__init__(op_meta_info, node_id)
        self._parent_node = None

    @property
    def parent_node(self):
        """The node's ID."""
        return self._parent_node

    @classmethod
    def from_json_dict(cls, json_dict, registry=OP_REGISTRY) -> Optional['Step']:
        node = cls.new_step_from_json_dict(json_dict, registry=registry)
        if node is None:
            return None

        node_input_dict = json_dict.get('input', {})
        for name, properties in node_input_dict.items():
            if name not in node.input:
                # update op_meta_info
                node.op_meta_info.input[name] = node.op_meta_info.input.get(name, {})
                # then create a new port
                node.input[name] = NodePort(node, name)
            node_input = node.input[name]
            node_input.from_json_dict(node_input_dict)

        node_output_dict = json_dict.get('output', {})
        for name, properties in node_output_dict.items():
            if name not in node.output:
                # first update op_meta_info
                node.op_meta_info.output[name] = node.op_meta_info.output.get(name, {})
                # then create a new port
                node.output[name] = NodePort(node, name)
            node_output = node.output[name]
            node_output.from_json_dict(node_output_dict)

        return node

    @classmethod
    @abstractmethod
    def new_step_from_json_dict(cls, json_dict, registry=OP_REGISTRY) -> Optional['Step']:
        """Create a new step node instance from the given *json_dict*"""

    def to_json_dict(self):
        """
        Return a JSON-serializable dictionary representation of this object.

        :return: A JSON-serializable dictionary
        """

        node_dict = OrderedDict()
        node_dict['id'] = self.id

        self.enhance_json_dict(node_dict)

        node_dict['input'] = OrderedDict(
            [(node_input.name, node_input.to_json_dict()) for node_input in self.input[:]])
        node_dict['output'] = OrderedDict(
            [(node_output.name, node_output.to_json_dict()) for node_output in self.output[:]])

        return node_dict

    @abstractmethod
    def enhance_json_dict(self, node_dict: OrderedDict):
        """Enhance the given JSON-compatible *node_dict* by step specific elements."""


class WorkflowStep(Step):
    """
    A `WorkflowStep` is a step node that invokes an externally stored :py:class:`Workflow`.

    :param workflow: The referenced workflow.
    :param resource: A resource (e.g. file path, URL) from which the workflow was loaded.
    :param node_id: A node ID. If None, a unique ID will be generated.
    """

    def __init__(self, workflow, resource, node_id=None):
        if not workflow:
            raise ValueError('workflow must be given')
        if not resource:
            raise ValueError('resource must be given')
        node_id = node_id if node_id else 'workflow_step_' + hex(id(self))[2:]
        super(WorkflowStep, self).__init__(workflow.op_meta_info, node_id)
        self._workflow = workflow
        self._resource = resource
        # Connect the workflow's inputs with this node's input sources
        for workflow_input in workflow.input[:]:
            name = workflow_input.name
            assert name in self.input
            workflow_input.source = self.input[name]

    @property
    def workflow(self) -> 'Workflow':
        """The workflow."""
        return self._workflow

    @property
    def resource(self) -> str:
        """The workflow's resource path (file path, URL)."""
        return self._resource

    def invoke(self, value_cache: dict = None, monitor: Monitor = Monitor.NONE):
        """
        Invoke this node's underlying :py:attr:`workflow` with input values from
        :py:attr:`input`. Output values in :py:attr:`output` will
        be set from the underlying workflow's return value(s).

        :param value_cache: An optional value cache.
        :param monitor: An optional progress monitor.
        """

        if value_cache is not None and hasattr(value_cache, 'child'):
            value_cache = value_cache.child(self.id)

        # Invoke underlying workflow.
        self._workflow.invoke(value_cache=value_cache, monitor=monitor)

        # transfer workflow output values into this node's output values
        for workflow_output in self._workflow.output[:]:
            assert workflow_output.name in self.output
            node_output = self.output[workflow_output.name]
            node_output.value = workflow_output.value

    @classmethod
    def new_step_from_json_dict(cls, json_dict, registry=OP_REGISTRY):
        resource = json_dict.get('workflow', None)
        if resource is None:
            return None
        workflow = Workflow.load(resource, registry=registry)
        return WorkflowStep(workflow, resource, node_id=json_dict.get('id', None))

    def enhance_json_dict(self, node_dict: OrderedDict):
        node_dict['workflow'] = self._resource

    def __repr__(self):
        return "WorkflowStep(%s, '%s', node_id='%s')" % (repr(self._workflow), self.resource, self.id)


class OpStep(Step):
    """
    An `OpStep` is a step node that invokes a registered operation of type :py:class:`OpRegistration`.

    :param operation: A fully qualified operation name or operation object such as a class or callable.
    :param registry: An operation registry to be used to lookup the operation, if given by name.
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
        node_id = node_id if node_id else 'op_step_' + hex(id(self))[2:]
        op_meta_info = op_registration.op_meta_info
        super(OpStep, self).__init__(op_meta_info, node_id)
        self._op_registration = op_registration

    @property
    def op(self):
        """The operation registration. See :py:class:`cate.core.op.OpRegistration`"""
        return self._op_registration

    def invoke(self, value_cache: dict = None, monitor: Monitor = Monitor.NONE):
        """
        Invoke this node's underlying operation :py:attr:`op` with input values from
        :py:attr:`input`. Output values in :py:attr:`output` will
        be set from the underlying operation's return value(s).

        :param value_cache: An optional value cache.
        :param monitor: An optional progress monitor.
        """
        input_values = OrderedDict()
        for node_input in self.input[:]:
            if node_input.has_value:
                input_values[node_input.name] = node_input.value

        can_cache = value_cache is not None and self.op_meta_info.can_cache
        if can_cache and self.id in value_cache:
            return_value = value_cache[self.id]
        else:
            return_value = self._op_registration(monitor=monitor, **input_values)
            if can_cache:
                value_cache[self.id] = return_value

        if self.op_meta_info.has_named_outputs:
            for output_name, output_value in return_value.items():
                self.output[output_name].value = output_value
        else:
            self.output[OpMetaInfo.RETURN_OUTPUT_NAME].value = return_value

    def __call__(self, monitor=Monitor.NONE, **input_values):
        """
        Make this class instance's callable.

        The method directly calls the operation without setting this node's :py:attr:`input` values
        and consequently ignoring this step's :py:attr:`output` values.

        :param monitor: An optional progress monitor.
        :param input_values: The input value(s).
        :return: The output value(s).
        """
        if self.op_meta_info.has_monitor:
            input_values[OpMetaInfo.MONITOR_INPUT_NAME] = monitor
        return self._op_registration(**input_values)

    @classmethod
    def new_step_from_json_dict(cls, json_dict, registry=OP_REGISTRY):
        op_name = json_dict.get('op', None)
        if op_name is None:
            return None
        return cls(op_name, node_id=json_dict.get('id', None), registry=registry)

    def enhance_json_dict(self, node_dict: OrderedDict):
        node_dict['op'] = self.op_meta_info.qualified_name

    def __repr__(self):
        return "OpStep(%s, node_id='%s')" % (self.op_meta_info.qualified_name, self.id)


class ExprStep(Step):
    """
    An ``ExprStep`` is a step node that computes its output from a simple (Python) *expression* string.

    :param expression: A simple (Python) expression string.
    :param input_dict: input name to input properties mapping.
    :param output_dict: output name to output properties mapping.
    :param node_id: A node ID. If None, a unique ID will be generated.
    """

    def __init__(self, expression: str, input_dict=None, output_dict=None, node_id=None):
        if not expression:
            raise ValueError('expression must be given')
        node_id = node_id if node_id else 'expr_step_' + hex(id(self))[2:]
        op_meta_info = OpMetaInfo(node_id, input_dict=input_dict, output_dict=output_dict)
        if len(op_meta_info.output) == 0:
            op_meta_info.output[op_meta_info.RETURN_OUTPUT_NAME] = {}
        super(ExprStep, self).__init__(op_meta_info, node_id)
        self._expression = expression

    @property
    def expression(self) -> str:
        """The expression."""
        return self._expression

    def invoke(self, value_cache: dict = None, monitor: Monitor = Monitor.NONE):
        """
        Invoke this node's underlying operation :py:attr:`op` with input values from
        :py:attr:`input`. Output values in :py:attr:`output` will
        be set from the underlying operation's return value(s).

        :param value_cache: An optional value cache.
        :param monitor: An optional progress monitor.
        """
        input_values = OrderedDict()
        for node_input in self.input[:]:
            input_values[node_input.name] = node_input.value

        can_cache = value_cache is not None and self.op_meta_info.can_cache
        if can_cache and self.id in value_cache:
            return_value = value_cache[self.id]
        else:
            return_value = eval(self.expression, None, input_values)
            if can_cache:
                value_cache[self.id] = return_value

        if self.op_meta_info.has_named_outputs:
            for output_name, output_value in return_value.items():
                self.output[output_name].value = output_value
        else:
            self.output[OpMetaInfo.RETURN_OUTPUT_NAME].value = return_value

    @classmethod
    def new_step_from_json_dict(cls, json_dict, registry=OP_REGISTRY):
        expression = json_dict.get('expression', None)
        if expression is None:
            return None
        return cls(expression, node_id=json_dict.get('id', None))

    def enhance_json_dict(self, node_dict: OrderedDict):
        node_dict['expression'] = self.expression

    def _body_string(self):
        return '"%s"' % self.expression

    def __repr__(self):
        return "ExprNode('%s', node_id='%s')" % (self.expression, self.id)


class NoOpStep(Step):
    """
    A ``NoOpStep`` "performs" a no-op, which basically means, it does nothing.
    However, it might still be useful to define step that or duplicates or renames output values by connecting
    its own output ports with any of its own input ports. In other cases it might be useful to have a
    ``NoOpStep`` as a placeholder or blackbox for some other real operation that will be put into place at a later
    point in time.

    :param input_dict: input name to input properties mapping.
    :param output_dict: output name to output properties mapping.
    :param node_id: A node ID. If None, a unique ID will be generated.
    """

    def __init__(self, input_dict=None, output_dict=None, node_id=None):
        node_id = node_id if node_id else 'no_op_step_' + hex(id(self))[2:]
        op_meta_info = OpMetaInfo(node_id, input_dict=input_dict, output_dict=output_dict)
        if len(op_meta_info.output) == 0:
            op_meta_info.output[op_meta_info.RETURN_OUTPUT_NAME] = {}
        super(NoOpStep, self).__init__(op_meta_info, node_id)

    def invoke(self, value_cache: dict = None, monitor: Monitor = Monitor.NONE):
        """
        Invoke this node's underlying operation :py:attr:`op` with input values from
        :py:attr:`input`. Output values in :py:attr:`output` will
        be set from the underlying operation's return value(s).

        :param value_cache: An optional value cache.
        :param monitor: An optional progress monitor.
        """
        input_values = OrderedDict()
        for node_input in self.input[:]:
            input_values[node_input.name] = node_input.value

    @classmethod
    def new_step_from_json_dict(cls, json_dict, registry=OP_REGISTRY):
        no_op = json_dict.get('no_op', None)
        if no_op is None:
            return None
        return cls(node_id=json_dict.get('id', None))

    def enhance_json_dict(self, node_dict: OrderedDict):
        node_dict['no_op'] = True

    def _body_string(self):
        return 'noop'

    def __repr__(self):
        return "NoOpStep(node_id='%s')" % self.id


class SubProcessStep(Step):
    """
    A ``SubProcessStep`` is a step node that computes its output by a sub-process created from the
    given *sub_process_arguments*.

    :param sub_process_arguments: The sub process' arguments as list where the first entry is usually an executable and
           remaining entries are the executable's arguments.
    :param input_dict: input name to input properties mapping.
    :param output_dict: output name to output properties mapping.
    :param node_id: A node ID. If None, a unique ID will be generated.
    """

    def __init__(self,
                 sub_process_arguments: List[str],
                 environment_variables: Dict[str, str] = None,
                 working_directory: str = '',
                 input_dict=None,
                 output_dict=None,
                 node_id=None):
        if not sub_process_arguments:
            raise ValueError('sub_process_arguments must be given')
        node_id = node_id if node_id else 'sub_process_step_' + hex(id(self))[2:]
        op_meta_info = OpMetaInfo(node_id, input_dict=input_dict, output_dict=output_dict)
        if len(op_meta_info.output) == 0:
            op_meta_info.output[op_meta_info.RETURN_OUTPUT_NAME] = {}
        super(SubProcessStep, self).__init__(op_meta_info, node_id)
        self._sub_process_arguments = sub_process_arguments
        # noinspection PyArgumentList
        self._environment_variables = dict(**environment_variables) if environment_variables else {}
        self._working_directory = working_directory

    @property
    def sub_process_arguments(self) -> List[str]:
        """The sub process' arguments."""
        return self._sub_process_arguments

    def invoke(self, value_cache: dict = None, monitor: Monitor = Monitor.NONE):
        """
        Invoke this node's underlying operation :py:attr:`op` with input values from
        :py:attr:`input`. Output values in :py:attr:`output` will
        be set from the underlying operation's return value(s).

        :param value_cache: An optional value cache.
        :param monitor: An optional progress monitor.
        """
        input_values = OrderedDict()
        for node_input in self.input[:]:
            input_values[node_input.name] = node_input.value

        # TODO (forman, 20160625): SubProcessStep: add more options to transform given arguments list into a new one
        #                      from given input values
        # For example: 1) use input as new argument (replacement) --> DONE
        #              2) generate text file (e.g. parameter / config file) from template and from input values
        #                 use new filename as new argument
        # TODO (forman, 20160625): SubProcessStep: add option to generate dictionary of named output values
        # For example: 1) add parse pattern so that stdout of sub_process can be converted into numeric progress
        #              2) send all stdout lines to monitor as textual messages
        # TODO (forman, 20160625): SubProcessStep: use monitor here

        interpolated_arguments = []
        for arg in self._sub_process_arguments:
            for key, value in input_values.items():
                arg = arg.replace('{{%s}}' % key, str(value))
            interpolated_arguments.append(arg)

        import subprocess
        return_value = subprocess.run(interpolated_arguments, shell=True).returncode

        if self.op_meta_info.has_named_outputs:
            # will never reach this code, see to-do above
            for output_name, output_value in return_value.items():
                self.output[output_name].value = output_value
        else:
            self.output[OpMetaInfo.RETURN_OUTPUT_NAME].value = return_value

    @classmethod
    def new_step_from_json_dict(cls, json_dict, registry=OP_REGISTRY):
        sub_process_arguments = json_dict.get('sub_process_arguments', None)
        # working_directory = json_dict.get('working_directory', None)
        # environment_variables = json_dict.get('environment_variables', None)
        if sub_process_arguments is None:
            return None
        return cls(sub_process_arguments, node_id=json_dict.get('id', None))

    def enhance_json_dict(self, node_dict: OrderedDict):
        node_dict['sub_process_arguments'] = self._sub_process_arguments
        node_dict['working_directory'] = self._working_directory
        node_dict['environment_variables'] = self._environment_variables

    def _body_string(self):
        return '"%s"' % ' '.join(self.sub_process_arguments)

    def __repr__(self):
        return "SubProcessStep(%s, node_id='%s')" % (repr(self._sub_process_arguments), self.id)


class NodePort:
    """Represents a named input or output port of a :py:class:`Node`. """

    def __init__(self, node: Node, name: str):
        assert node is not None
        assert name is not None
        assert name in node.op_meta_info.input or name in node.op_meta_info.output
        self._node = node
        self._name = name
        self._source_ref = None
        self._source = None
        self._value = UNDEFINED

    @property
    def node(self) -> Node:
        return self._node

    @property
    def node_id(self) -> str:
        return self._node.id

    @property
    def name(self) -> str:
        return self._name

    @property
    def has_value(self):
        if self._source:
            return self._source.has_value
        elif self._value is UNDEFINED:
            return False
        else:
            return True

    @property
    def value(self):
        if self._source:
            return self._source.value
        elif self._value is UNDEFINED:
            return None
        else:
            return self._value

    @value.setter
    def value(self, new_value):
        self._value = new_value
        self._source = None
        self._source_ref = None

    @property
    def source(self) -> 'NodePort':
        return self._source

    @source.setter
    def source(self, new_source: 'NodePort'):
        if self is new_source:
            raise ValueError("cannot connect '%s' with itself" % self)
        self._source = new_source
        self._source_ref = (new_source.node_id, new_source.name) if new_source else None
        self._value = UNDEFINED

    def resolve_source_ref(self):
        """
        Resolve this node port's source reference, if any.

        If the source reference has the form *node-id.port-name* then *node-id* must be the ID of the
        workflow or any contained step and *port-name* must be a name either of one of its input or output ports.

        If the source reference has the form *.port-name* then *node-id* will refer to either the current step or any
        of its parent nodes that contains an input or output named *port-name*.

        If the source reference has the form *node-id* then *node-id* must be the ID of the
        workflow or any contained step which has exactly one output.

        If *node-id* refers to a workflow, then *port-name* is resolved first against the workflow's inputs
        followed by its outputs.
        If *node-id* refers to a workflow's step, then *port-name* is resolved first against the step's outputs
        followed by its inputs.

        :raise ValueError: if the source reference is invalid.
        """
        if self._source_ref:
            other_node_id, other_name = self._source_ref
            if other_node_id and other_name:
                root_node = self._node.root_node
                other_node = root_node if root_node.id == other_node_id else root_node.find_node(other_node_id)
                if other_node:
                    node_port = other_node.find_port(other_name)
                    if node_port:
                        self.source = node_port
                        return
                    raise ValueError(
                        "cannot connect '%s' with '%s.%s' because node '%s' has no input/output named '%s'" % (
                            self, other_node_id, other_name, other_node_id, other_name))
                else:
                    raise ValueError("cannot connect '%s' with '%s.%s' because node '%s' does not exist" % (
                        self, other_node_id, other_name, other_node_id))
            elif other_node_id:
                root_node = self._node.root_node
                other_node = root_node if root_node.id == other_node_id else root_node.find_node(other_node_id)
                if other_node:
                    if len(other_node.output) == 1:
                        node_port = other_node.output[0]
                        self.source = node_port
                        return
                    else:
                        raise ValueError(
                            "cannot connect '%s' with node '%s' because it has %s named outputs" % (
                                self, other_node_id, len(other_node.output)))
                else:
                    raise ValueError("cannot connect '%s' with output of node '%s' because node '%s' does not exist" % (
                        self, other_node_id, other_node_id))
            elif other_name:
                # look for 'other_name' first in this scope and then the parent scopes
                other_node = self._node
                while other_node:
                    node_port = other_node.find_port(other_name)
                    if node_port:
                        self.source = node_port
                        return
                    other_node = other_node.parent_node
                raise ValueError(
                    "cannot connect '%s' with '.%s' because '%s' does not exist in any scope" % (
                        self, other_name, other_name))

    def from_json_dict(self, json_dict):
        self._source_ref = None
        self._source = None
        self._value = None

        if json_dict is None:
            return

        port_json = json_dict.get(self.name, None)
        if port_json is None:
            return

        source_format_msg = "error decoding '%s' because the \"source\" value format is " \
                            "neither \"<node-id>.<name>\", \"<node-id>\", nor \".<name>\""

        if not isinstance(port_json, str):
            port_json_dict = port_json
            if 'source' in port_json_dict:
                if 'value' in port_json_dict:
                    raise ValueError(
                        "error decoding '%s' because \"source\" and \"value\" are mutually exclusive" % self)
                port_json = port_json_dict['source']
            elif 'value' in port_json_dict:
                # Care: constant may be converted to a real Python value here
                # Must add converter callback, or so.
                self.value = port_json_dict['value']
                return
            else:
                return

        parts = port_json.rsplit('.', maxsplit=1)
        if len(parts) == 1 and parts[0]:
            node_id = parts[0]
            port_name = None
        elif len(parts) == 2:
            if not parts[1]:
                raise ValueError(source_format_msg % self)
            node_id = parts[0] if parts[0] else None
            port_name = parts[1]
        else:
            raise ValueError(source_format_msg % self)
        self._source_ref = node_id, port_name

    def to_json_dict(self):
        """
        Return a JSON-serializable dictionary representation of this object.

        :return: A JSON-serializable dictionary
        """
        json_dict = dict()
        if self.source is not None:
            json_dict['source'] = '%s.%s' % (self._source.node.id, self._source.name)
        elif self.has_value:
            # TODO (forman, 20160927): do not serialize output values, they may not be JSON-serializable (hack!)
            write_value = self._name not in self._node.op_meta_info.output
            if write_value:
                json_dict['value'] = self._value
        return json_dict

    def __str__(self):
        if self.name == OpMetaInfo.RETURN_OUTPUT_NAME:
            return self._node.id
        else:
            return "%s.%s" % (self._node.id, self._name)

    def __repr__(self):
        return "NodePort(%s, %s)" % (repr(self.node_id), repr(self.name))


def _convert_workflow_to_graph(workflow: Workflow):
    graph_nodes = {step.id: _convert_step_to_graph_node(step) for step in workflow.steps}
    graph = _Graph(workflow.op_meta_info.qualified_name,
                   [name for name, _ in workflow.input],
                   [name for name, _ in workflow.output],
                   list(graph_nodes.values()))

    graph_nodes[workflow.id] = graph
    _wire_target_node_graph_nodes(workflow, graph_nodes)
    for step in workflow.steps:
        _wire_target_node_graph_nodes(step, graph_nodes)

    return graph


def _convert_step_to_graph_node(step: Step):
    return _Node(step.op_meta_info.qualified_name,
                 [name for name, _ in step.input],
                 [name for name, _ in step.output])


def _wire_target_node_graph_nodes(target_node, graph_nodes):
    for _, target_port in target_node.input:
        _wire_target_port_graph_nodes(target_port, graph_nodes)
    for _, target_port in target_node.output:
        _wire_target_port_graph_nodes(target_port, graph_nodes)


def _wire_target_port_graph_nodes(target_port, graph_nodes):
    if target_port.source is None:
        return
    target_node = target_port.node
    target_gnode = graph_nodes[target_node.id]
    source_port = target_port.source
    source_node = source_port.node
    source_gnode = graph_nodes[source_node.id]
    source_gnode.find_port(source_port.name).connect(target_gnode.find_port(target_port.name))


def _close_value(value):
    if hasattr(value, 'close'):
        # noinspection PyBroadException
        try:
            value.close()
        except:
            pass


class ValueCache(dict):
    """
    A dictionary that can be closed. If closed, all values that have a ``close`` attribute are closed as well.
    Values are also closed, if they are removed.
    """

    def __init__(self):
        super(ValueCache, self).__init__()

    def __del__(self):
        self._close_values()

    def __setitem__(self, key, value):
        old_value = self.get(key)
        super(ValueCache, self).__setitem__(key, value)
        if old_value is not value:
            _close_value(old_value)

    def __delitem__(self, key):
        old_value = self.get(key)
        super(ValueCache, self).__delitem__(key)
        if old_value is not None:
            _close_value(old_value)

    def child(self, key: str) -> 'ValueCache':
        child_key = key + '.__child__'
        if child_key not in self:
            self[child_key] = ValueCache()
        return self[child_key]

    def close(self):
        self._close_values()

    def _close_values(self):
        values = list(self.values())
        self.clear()
        for value in values:
            _close_value(value)
