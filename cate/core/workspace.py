# The MIT License (MIT)
# Copyright (c) 2016, 2017 by the ESA CCI Toolbox development team and contributors
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

"""
This module defines the ``Workspace`` class and the ``WorkspaceError`` exception type.
"""

import os
import shutil
import sys
from collections import OrderedDict
from threading import RLock
from typing import List, Any, Dict, Optional

import fiona
import pandas as pd
import xarray as xr

from .workflow import Workflow, OpStep, NodePort, ValueCache
from ..conf import conf
from ..conf.defaults import WORKSPACE_DATA_DIR_NAME, WORKSPACE_WORKFLOW_FILE_NAME, SCRATCH_WORKSPACES_PATH
from ..core.cdm import get_tiling_scheme
from ..core.op import OP_REGISTRY
from ..util.monitor import Monitor
from ..util.misc import object_to_qualified_name, to_json, new_indexed_name
from ..util.safe import safe_eval
from ..util.undefined import UNDEFINED
from ..util.im import get_chunk_size
from ..util.opmetainf import OpMetaInfo
from ..util.namespace import Namespace

__author__ = "Norman Fomferra (Brockmann Consult GmbH)"

#: An JSON-serializable operation argument is a one-element dictionary taking two possible forms:
#: 1. dict(value=Any):  a value which may be any constant Python object which must JSON-serializable
#: 2. dict(source=str): a reference to a step port name
OpArg = Dict[str, Any]

#: JSON-serializable, positional operation arguments
OpArgs = List[OpArg]

#: JSON-serializable, keyword operation arguments
OpKwArgs = Dict[str, OpArg]


def mk_op_arg(arg) -> OpArg:
    """
    Utility function which turns an argument into an operation argument.
    If *args* is a ``str`` and starts with "@" it is turned into a "source" argument,
    otherwise it is turned into a "value" argument.
    """
    return dict(source=arg[1:]) if isinstance(arg, str) and arg.startswith('@') else dict(value=arg)


def mk_op_args(*args) -> OpArgs:
    """
    Utility function which converts a list into positional operation arguments.
    """
    return [mk_op_arg(arg) for arg in args]


def mk_op_kwargs(**kwargs) -> OpKwArgs:
    """
    Utility function which converts a dictionary into operation keyword arguments.
    """
    return OrderedDict([(kw, mk_op_arg(arg)) for kw, arg in kwargs.items()])


class Workspace:
    """
    A Workspace uses a :py:class:`Workflow` to record user operations.
    By design, workspace workflows have no inputs and every step is an output.
    """

    def __init__(self, base_dir: str, workflow: Workflow, is_modified: bool = False):
        assert base_dir
        assert workflow
        self._base_dir = base_dir
        self._workflow = workflow
        self._is_scratch = (base_dir or '').startswith(SCRATCH_WORKSPACES_PATH)
        self._is_modified = is_modified
        self._is_closed = False
        self._resource_cache = ValueCache()
        self._user_data = dict()
        self._lock = RLock()

    def __del__(self):
        self.close()

    @property
    def base_dir(self) -> str:
        """The Workspace's workflow."""
        return self._base_dir

    @property
    def workflow(self) -> Workflow:
        """The Workspace's workflow."""
        self._assert_open()
        return self._workflow

    @property
    def resource_cache(self) -> ValueCache:
        """The Workspace's resource cache."""
        return self._resource_cache

    @property
    def is_scratch(self) -> bool:
        return self._is_scratch

    @property
    def is_closed(self) -> bool:
        return self._is_closed

    @property
    def is_modified(self) -> bool:
        return self._is_modified

    @property
    def workspace_dir(self) -> str:
        return self.get_workspace_dir(self.base_dir)

    @property
    def workflow_file(self) -> str:
        return self.get_workflow_file(self.base_dir)

    @property
    def user_data(self) -> dict:
        return self._user_data

    @classmethod
    def get_workspace_dir(cls, base_dir) -> str:
        return os.path.join(base_dir, WORKSPACE_DATA_DIR_NAME)

    @classmethod
    def get_workflow_file(cls, base_dir) -> str:
        return os.path.join(cls.get_workspace_dir(base_dir), WORKSPACE_WORKFLOW_FILE_NAME)

    @classmethod
    def new_workflow(cls, header: dict = None) -> Workflow:
        return Workflow(OpMetaInfo('workspace_workflow',
                                   has_monitor=True,
                                   header=header or {}))

    @classmethod
    def create(cls, base_dir: str, description: str = None) -> 'Workspace':
        return Workspace(base_dir, Workspace.new_workflow(dict(description=description or '')))

    @classmethod
    def open(cls, base_dir: str, monitor: Monitor = Monitor.NONE) -> 'Workspace':
        if not os.path.isdir(cls.get_workspace_dir(base_dir)):
            raise WorkspaceError('Not a valid workspace: %s' % base_dir)
        try:
            workflow_file = cls.get_workflow_file(base_dir)
            workflow = Workflow.load(workflow_file)
            workspace = Workspace(base_dir, workflow)

            # Read resources for persistent steps
            persistent_steps = [step for step in workflow.steps if step.persistent]
            if persistent_steps:
                with monitor.starting('Reading resources', len(persistent_steps)):
                    for step in persistent_steps:
                        workspace._read_resource_from_file(step.id)
                    monitor.progress(1)

            return workspace
        except (IOError, OSError) as e:
            raise WorkspaceError(str(e)) from e

    def close(self):
        if self._is_closed:
            return
        with self._lock:
            self._resource_cache.close()
            # Remove all resource files that are no longer required
            if os.path.isdir(self.workspace_dir):
                persistent_ids = {step.id for step in self.workflow.steps if step.persistent}
                for filename in os.listdir(self.workspace_dir):
                    res_file = os.path.join(self.workspace_dir, filename)
                    if os.path.isfile(res_file) and filename.endswith('.nc'):
                        res_name = filename[0: -3]
                        if res_name not in persistent_ids:
                            try:
                                os.remove(res_file)
                            except (OSError, IOError) as e:
                                print('error:', e)

    def save(self, monitor: Monitor = Monitor.NONE):
        self._assert_open()
        with self._lock:
            base_dir = self.base_dir
            try:
                if not os.path.isdir(base_dir):
                    os.mkdir(base_dir)
                workspace_dir = self.workspace_dir
                if not os.path.isdir(workspace_dir):
                    os.mkdir(workspace_dir)
                self.workflow.store(self.workflow_file)

                # Write resources for all persistent steps
                persistent_steps = [step for step in self.workflow.steps if step.persistent]
                if persistent_steps:
                    with monitor.starting('Writing resources', len(persistent_steps)):
                        for step in persistent_steps:
                            self._write_resource_to_file(step.id)
                            monitor.progress(1)

                self._is_modified = False
            except (IOError, OSError) as e:
                raise WorkspaceError(str(e)) from e

    def _write_resource_to_file(self, res_name):
        res_value = self._resource_cache.get(res_name)
        if res_value is not None:
            resource_file = os.path.join(self.workspace_dir, res_name + '.nc')
            try:
                res_value.to_netcdf(resource_file)
            except AttributeError:
                pass
            except Exception as e:
                print('error:', e)

    def _read_resource_from_file(self, res_name):
        res_file = os.path.join(self.workspace_dir, res_name + '.nc')
        if os.path.isfile(res_file):
            try:
                res_value = xr.open_dataset(res_file)
                self._resource_cache[res_name] = res_value
            except Exception as e:
                print('error:', e)

    # <<< Issue #270

    def set_resource_persistence(self, res_name: str, persistent: bool):
        with self._lock:
            self._assert_open()
            res_step = self.workflow.find_node(res_name)
            if res_step is None:
                raise WorkspaceError('Resource "%s" not found' % res_name)
            if res_step.persistent == persistent:
                return
            res_step.persistent = persistent

    @classmethod
    def from_json_dict(cls, json_dict):
        base_dir = json_dict.get('base_dir', None)
        workflow_json = json_dict.get('workflow', {})
        is_modified = json_dict.get('is_modified', False)
        workflow = Workflow.from_json_dict(workflow_json)
        return Workspace(base_dir, workflow, is_modified=is_modified)

    def to_json_dict(self):
        with self._lock:
            self._assert_open()
            return OrderedDict([('base_dir', self.base_dir),
                                ('is_scratch', self.is_scratch),
                                ('is_modified', self.is_modified),
                                ('is_saved', os.path.exists(self.workspace_dir)),
                                ('workflow', self.workflow.to_json_dict()),
                                ('resources', self._resources_to_json_list())
                                ])

    def _resources_to_json_list(self):
        resource_descriptors = []
        resource_cache = dict(self._resource_cache)
        for res_step in self.workflow.steps:
            res_name = res_step.id
            if res_name in resource_cache:
                res_id = self._resource_cache.get_id(res_name)
                res_update_count = self._resource_cache.get_update_count(res_name)
                resource = resource_cache.pop(res_name)
                resource_descriptor = self._get_resource_descriptor(res_id, res_update_count, res_name, resource)
                resource_descriptors.append(resource_descriptor)
        if len(resource_cache) > 0:
            # We should not get here as all resources should have an associated workflow step!
            for res_name, resource in resource_cache.items():
                res_id = self._resource_cache.get_id(res_name)
                res_update_count = self._resource_cache.get_update_count(res_name)
                resource_descriptor = self._get_resource_descriptor(res_id, res_update_count, res_name, resource)
                resource_descriptors.append(resource_descriptor)
        return resource_descriptors

    def _get_resource_descriptor(self, res_id: int, res_update_count: int, res_name: str, resource):
        variable_descriptors = []
        coords_descriptors = []
        data_type_name = object_to_qualified_name(type(resource))
        resource_json = dict(id=res_id, updateCount=res_update_count, name=res_name, dataType=data_type_name)
        if isinstance(resource, xr.Dataset):

            var_names = sorted(resource.data_vars.keys())
            for var_name in var_names:
                if not var_name.endswith('_bnds'):
                    variable = resource.data_vars[var_name]
                    variable_descriptors.append(self._get_xarray_variable_descriptor(variable))

            var_names = sorted(resource.coords.keys())
            for var_name in var_names:
                variable = resource.coords[var_name]
                coords_descriptors.append(self._get_xarray_variable_descriptor(variable, is_coord=True))

            # noinspection PyArgumentList
            resource_json.update(dimSizes=to_json(resource.dims),
                                 attributes=Workspace._attrs_to_json_dict(resource.attrs),
                                 variables=variable_descriptors,
                                 coordVariables=coords_descriptors)

        elif isinstance(resource, pd.DataFrame):

            var_names = list(resource.columns)
            for var_name in var_names:
                variable = resource[var_name]
                variable_descriptors.append(self._get_pandas_variable_descriptor(variable))
            # noinspection PyArgumentList,PyTypeChecker
            resource_json.update(variables=variable_descriptors)

        elif isinstance(resource, fiona.Collection):

            num_features = len(resource)
            properties = resource.schema.get('properties')
            if properties:
                for var_name, var_type in properties.items():
                    variable_descriptors.append({
                        'name': var_name,
                        'dataType': var_type,
                        'isFeatureAttribute': True,
                    })
            geometry = resource.schema.get('geometry')
            # noinspection PyArgumentList
            resource_json.update(variables=variable_descriptors,
                                 geometry=geometry,
                                 numFeatures=num_features)
        return resource_json

    @staticmethod
    def _attrs_to_json_dict(attrs: dict) -> Dict[str, Any]:
        attr_json_dict = {}
        for name, value in attrs.items():
            attr_json_dict[name] = to_json(value)
        return attr_json_dict

    @staticmethod
    def _get_pandas_variable_descriptor(variable: pd.Series):
        return {
            'name': variable.name,
            'dataType': object_to_qualified_name(variable.dtype),
            'numDims': variable.ndim,
            'shape': variable.shape,
        }

    # noinspection PyMethodMayBeStatic
    def _get_xarray_variable_descriptor(self, variable: xr.DataArray, is_coord=False):
        attrs = variable.attrs
        variable_info = {
            'name': variable.name,
            'dataType': object_to_qualified_name(variable.dtype),
            'numDims': len(variable.dims),
            'dimNames': variable.dims,
            'shape': variable.shape,
            'chunkSizes': get_chunk_size(variable),
            'attributes': Workspace._attrs_to_json_dict(attrs),
            'isCoord': is_coord
        }

        if not is_coord:
            tiling_scheme = get_tiling_scheme(variable)
            if tiling_scheme:
                variable_info['imageLayout'] = tiling_scheme.to_json()
                variable_info['isYFlipped'] = tiling_scheme.geo_extent.inv_y
        elif variable.ndim == 1:
            # Serialize data of coordinate variables.
            # To limit data transfer volume, we serialize data arrays only if they are 1D.
            # Note that the 'data' field is used to display coordinate labels in the GUI only.
            variable_info['data'] = to_json(variable.data)

        display_settings = conf.get_variable_display_settings(variable.name)
        if display_settings:
            mapping = dict(colorMapName='color_map',
                           displayMin='display_min',
                           displayMax='display_max')
            for var_prop_name, display_settings_name in mapping.items():
                if display_settings_name in display_settings:
                    variable_info[var_prop_name] = display_settings[display_settings_name]

        return variable_info

    def delete(self):
        with self._lock:
            self.close()
            try:
                shutil.rmtree(self.workspace_dir)
            except (IOError, OSError) as e:
                raise WorkspaceError(str(e)) from e

    def delete_resource(self, res_name: str):
        with self._lock:
            res_step = self.workflow.find_node(res_name)
            if res_step is None:
                raise WorkspaceError('Resource "%s" not found' % res_name)

            dependent_steps = []
            for step in self.workflow.steps:
                if step is not res_step and step.requires(res_step):
                    dependent_steps.append(step.id)

            if dependent_steps:
                raise WorkspaceError('Cannot delete resource "%s" because the following resource(s) '
                                     'depend on it: %s' % (res_name, ', '.join(dependent_steps)))

            self.workflow.remove_step(res_step)
            if res_name in self._resource_cache:
                del self._resource_cache[res_name]

    def rename_resource(self, res_name: str, new_res_name: str) -> None:
        Workspace._validate_res_name(new_res_name)
        with self._lock:
            res_step = self.workflow.find_node(res_name)
            if res_step is None:
                raise WorkspaceError('Resource "%s" not found' % res_name)
            res_step_new = self.workflow.find_node(new_res_name)
            if res_step_new is res_step:
                return
            if res_step_new is not None:
                raise WorkspaceError('Resource "%s" cannot be renamed to "%s", '
                                     'because "%s" is already in use.' % (res_name, new_res_name, new_res_name))

            res_step.set_id(new_res_name)

            if res_name in self._resource_cache:
                self._resource_cache.rename_key(res_name, new_res_name)

    def set_resource(self,
                     op_name: str,
                     op_kwargs: OpKwArgs,
                     res_name: Optional[str] = None,
                     overwrite: bool = False,
                     validate_args=False) -> str:
        """
        Set a resource named *res_name* to the result of an operation *op_name* using the given operation arguments
        *op_kwargs*.

        :param res_name: An optional resource name. If given and not empty, it must be unique within this workspace.
               If not provided, a workspace-unique resource name will be generated.
        :param op_name: The name of a registered operation.
        :param op_kwargs: The operation's keyword arguments. Each argument must be a dict having either a "source" or
               "value" key.
        :param overwrite:
        :param validate_args:
        :return: The resource name, either the one passed in or a generated one.
        """
        assert op_name
        assert op_kwargs is not None

        op = OP_REGISTRY.get_op(op_name)
        if not op:
            raise WorkspaceError('Unknown operation "%s"' % op_name)

        with self._lock:
            if not res_name:
                default_res_pattern = conf.get_default_res_pattern()
                res_pattern = op.op_meta_info.header.get('res_pattern', default_res_pattern)
                res_name = self._new_resource_name(res_pattern)
            Workspace._validate_res_name(res_name)

            new_step = OpStep(op, node_id=res_name)

            workflow = self.workflow

            # This namespace will allow us to wire the new resource with existing workflow steps
            # We only add step outputs, so we cannot reference another step's input neither.
            # This is not a problem because a workspace's workflow doesn't have any inputs
            # to be referenced anyway.
            namespace = dict()
            for step in workflow.steps:
                output_namespace = step.outputs
                namespace[step.id] = output_namespace

            does_exist = res_name in namespace
            if not overwrite and does_exist:
                raise WorkspaceError('A resource named "%s" already exists' % res_name)

            if does_exist:
                # Prevent resource from self-referencing
                namespace.pop(res_name, None)

            # Wire new op_step with outputs from existing steps
            for input_name, input_value in op_kwargs.items():
                if input_name not in new_step.inputs:
                    raise WorkspaceError('"%s" is not an input of operation "%s"' % (input_name, op_name))
                input_port = new_step.inputs[input_name]

                if 'source' in input_value:
                    source = input_value['source']
                    if source is not None:
                        source = safe_eval(source, namespace)
                    if isinstance(source, NodePort):
                        # source is an output NodePort of another step
                        input_port.source = source
                    elif isinstance(source, Namespace):
                        # source is output_namespace of another step
                        if OpMetaInfo.RETURN_OUTPUT_NAME not in source:
                            raise WorkspaceError('Illegal argument for input "%s" of operation "%s',
                                                 (input_name, op_name))
                        input_port.source = source[OpMetaInfo.RETURN_OUTPUT_NAME]
                elif 'value' in input_value:
                    # Constant value
                    input_port.value = input_value['value']
                else:
                    raise WorkspaceError('Illegal argument for input "%s" of operation "%s', (input_name, op_name))

            if validate_args:
                inputs = new_step.inputs
                input_values = {kw: inputs[kw].source or inputs[kw].value for kw, v in op_kwargs.items()}
                # Validate all values except those of type NodePort (= the sources)
                op.op_meta_info.validate_input_values(input_values, [NodePort])

            old_step = workflow.find_node(res_name)

            # Collect keys of invalidated cache entries, initialize with res_name
            ids_of_invalidated_steps = {res_name}
            if old_step is not None:
                # Collect all IDs of steps that depend on old_step, if any
                for step in workflow.steps:
                    requires = step.requires(old_step)
                    if requires:
                        ids_of_invalidated_steps.add(step.id)

            workflow = self._workflow
            # noinspection PyUnusedLocal
            workflow.add_step(new_step, can_exist=True)
            self._is_modified = True

            # Remove any cached resource values, whose steps became invalidated
            for key in ids_of_invalidated_steps:
                if key in self._resource_cache:
                    self._resource_cache[key] = UNDEFINED

        return res_name

    def run_op(self, op_name: str, op_kwargs: OpKwArgs, monitor=Monitor.NONE):
        assert op_name
        assert op_kwargs is not None

        op = OP_REGISTRY.get_op(op_name)
        if not op:
            raise WorkspaceError('Unknown operation "%s"' % op_name)

        with self._lock:
            unpacked_op_kwargs = {}
            for input_name, input_value in op_kwargs.items():
                if 'source' in input_value:
                    unpacked_op_kwargs[input_name] = safe_eval(input_value['source'], self.resource_cache)
                elif 'value' in input_value:
                    unpacked_op_kwargs[input_name] = input_value['value']

            with monitor.starting("Running operation '%s'" % op_name, 2):
                self.workflow.invoke(context=self._new_context(), monitor=monitor.child(work=1))
                op(monitor=monitor.child(work=1), **unpacked_op_kwargs)

    def execute_workflow(self, res_name: str = None, monitor: Monitor = Monitor.NONE):
        self._assert_open()

        with self._lock:
            if not res_name:
                steps = self.workflow.sorted_steps
            else:
                res_step = self.workflow.find_node(res_name)
                if res_step is None:
                    raise WorkspaceError('Resource "%s" not found' % res_name)
                steps = self.workflow.find_steps_to_compute(res_step.id)
            if len(steps):
                self.workflow.invoke_steps(steps, context=self._new_context(), monitor=monitor)
                return steps[-1].get_output_value()
            else:
                return None

    def _new_context(self):
        return dict(value_cache=self._resource_cache, workspace=self)

    def _assert_open(self):
        if self._is_closed:
            raise WorkspaceError('Workspace is already closed: ' + self._base_dir)

    def _new_resource_name(self, res_pattern):
        return new_indexed_name({step.id for step in self.workflow.steps}, res_pattern)

    @staticmethod
    def _validate_res_name(res_name: str):
        if not res_name.isidentifier():
            raise WorkspaceError(
                "Resource name '%s' is not valid. "
                "The name must only contain the uppercase and lowercase letters A through Z, the underscore _ and, "
                "except for the first character, the digits 0 through 9." % res_name)


class WorkspaceError(Exception):
    """
    Error raised by methods of the ``Workspace`` class.

    :param message: Error message
    """

    def __init__(self, message):
        super().__init__(message)

    @property
    def cause(self):
        return self.__cause__
