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

__author__ = "Norman Fomferra (Brockmann Consult GmbH)"

"""
This module defines the ``Workspace`` class and the ``WorkspaceError`` exception type.
"""

import os
import shutil
import sys
from collections import OrderedDict
from typing import List, Any, Dict

import fiona
import numpy as np
import pandas as pd
import xarray as xr

from ..conf.defaults import WORKSPACE_DATA_DIR_NAME, WORKSPACE_WORKFLOW_FILE_NAME, SCRATCH_WORKSPACES_PATH
from ..core.cdm import get_lon_dim_name, get_lat_dim_name
from ..core.op import OP_REGISTRY
from .workflow import Workflow, OpStep, NodePort, ValueCache
from ..util import Monitor, Namespace, object_to_qualified_name, to_json, safe_eval, UNDEFINED
from ..util.im import ImagePyramid, get_chunk_size
from ..util.opmetainf import OpMetaInfo

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
            raise WorkspaceError(e)

    def close(self):
        if self._is_closed:
            return
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
            raise WorkspaceError(e)

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

            resource_json.update(dimSizes=to_json(resource.dims),
                                 attributes=self._attrs_to_json_dict(resource.attrs),
                                 variables=variable_descriptors,
                                 coordVariables=coords_descriptors)

        elif isinstance(resource, pd.DataFrame):

            var_names = list(resource.columns)
            for var_name in var_names:
                variable = resource[var_name]
                variable_descriptors.append(self._get_pandas_variable_descriptor(variable))
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
            resource_json.update(variables=variable_descriptors,
                                 geometry=geometry,
                                 numFeatures=num_features)
        return resource_json

    def _attrs_to_json_dict(self, attrs: dict) -> Dict[str, Any]:
        attr_json_dict = {}
        for name, value in attrs.items():
            attr_json_dict[name] = to_json(value)
        return attr_json_dict

    def _get_pandas_variable_descriptor(self, variable: pd.Series):
        return {
            'name': variable.name,
            'dataType': object_to_qualified_name(variable.dtype),
            'numDims': variable.ndim,
            'shape': variable.shape,
        }

    def _get_xarray_variable_descriptor(self, variable: xr.DataArray, is_coord=False):
        attrs = variable.attrs
        variable_info = {
            'name': variable.name,
            'dataType': object_to_qualified_name(variable.dtype),
            'numDims': len(variable.dims),
            'dimNames': variable.dims,
            'shape': variable.shape,
            'chunkSizes': get_chunk_size(variable),
            # 'fill_value': self._get_float_attr(attrs, '_FillValue'),
            # 'valid_min': self._get_float_attr(attrs, 'valid_min'),
            # 'valid_max': self._get_float_attr(attrs, 'valid_max'),
            # 'add_offset': self._get_float_attr(attrs, 'add_offset'),
            # 'scale_factor': self._get_float_attr(attrs, 'scale_factor'),
            # 'standard_name': self._get_unicode_attr(attrs, 'standard_name'),
            # 'long_name': self._get_unicode_attr(attrs, 'long_name'),
            # 'units': self._get_unicode_attr(attrs, 'units', default_value='-'),
            # 'comment': self._get_unicode_attr(attrs, 'comment'),
            'attributes': self._attrs_to_json_dict(attrs),
            'isCoord': is_coord
        }

        if not is_coord:
            image_config = self._get_variable_image_config(variable)
            if image_config:
                variable_info['imageLayout'] = image_config
                variable_info['isYFlipped'] = Workspace._is_y_flipped(variable)
        elif variable.ndim == 1:
            # Serialize data of coordinate variables.
            # To limit data transfer volume, we serialize data arrays only if they are 1D.
            # Note that the 'data' field is used to display coordinate labels in the GUI only.
            variable_info['data'] = to_json(variable.data)

        return variable_info

    # noinspection PyMethodMayBeStatic
    def _get_variable_image_config(self, variable):
        lat_dim_name = get_lat_dim_name(variable)
        lon_dim_name = get_lon_dim_name(variable)
        if not lat_dim_name or not lon_dim_name:
            return None

        if lat_dim_name in variable.coords and lon_dim_name in variable.coords:
            lats = variable.coords[lat_dim_name]
            lons = variable.coords[lon_dim_name]

            if len(lons) >= 2:
                lon_delta = 0.5 * abs(lons[1] - lons[0])
                west = min(lons[0], lons[-1]) - lon_delta
                east = max(lons[0], lons[-1]) + lon_delta
            elif len(lons) == 1:
                west = east = lons[0]
            else:
                # Note, this is actually an error condition
                west = east = 0

            if len(lats) >= 2:
                lat_delta = 0.5 * abs(lats[1] - lats[0])
                south = min(lats[0], lats[-1]) - lat_delta
                north = max(lats[0], lats[-1]) + lat_delta
            elif len(lats) == 1:
                south = north = lats[0]
            else:
                # Note, this is actually an error condition
                south = north = 0

            south = 90 if south > 90 else (-90 if south < -90 else float(south))
            north = 90 if north > 90 else (-90 if north < -90 else float(north))
            west = 180 if west > 180 else (-180 if west < -180 else float(west))
            east = 180 if east > 180 else (-180 if east < -180 else float(east))
            if south == north or west == east:
                return None
        else:
            south = -90
            north = 90
            west = -180
            east = 180

        max_size, tile_size, num_level_zero_tiles, num_levels = ImagePyramid.compute_layout(array=variable)
        return {
            'sector': {
                'west': west,
                'south': south,
                'east': east,
                'north': north,
            },
            'numLevels': num_levels,
            'numLevelZeroTilesX': num_level_zero_tiles[0],
            'numLevelZeroTilesY': num_level_zero_tiles[1],
            'tileWidth': tile_size[0],
            'tileHeight': tile_size[1]
        }

    @staticmethod
    def _is_y_flipped(variable):
        lat_coords = variable.coords[get_lat_dim_name(variable)]
        return lat_coords.to_index().is_monotonic_increasing

    # noinspection PyMethodMayBeStatic
    def _get_unicode_attr(self, attr, key, default_value=''):
        if key in attr:
            value = attr.get(key)
            if type(value) == bytes or type(value) == np.bytes_:
                return value.decode('unicode_escape')
            elif type(value) != str:
                return str(value)
            else:
                return value
        return default_value

    # noinspection PyMethodMayBeStatic
    def _get_float_attr(self, attr, key, default_value=None):
        if key in attr:
            # noinspection PyBroadException
            try:
                return float(attr.get(key))
            except:
                pass
        return default_value

    def delete(self):
        self.close()
        try:
            shutil.rmtree(self.workspace_dir)
        except (IOError, OSError) as e:
            raise WorkspaceError(e)

    def delete_resource(self, res_name: str):
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

    def set_resource(self, res_name: str, op_name: str, op_kwargs: OpKwArgs, overwrite=False, validate_args=False):
        assert res_name
        assert op_name
        assert op_kwargs is not None

        op = OP_REGISTRY.get_op(op_name)
        if not op:
            raise WorkspaceError('Unknown operation "%s"' % op_name)

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
            raise WorkspaceError('Resource "%s" already exists' % res_name)

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
                        raise WorkspaceError('Illegal argument for input "%s" of operation "%s', (input_name, op_name))
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

    def run_op(self, op_name: str, op_kwargs: OpKwArgs, monitor=Monitor.NONE):
        assert op_name
        assert op_kwargs

        op = OP_REGISTRY.get_op(op_name)
        if not op:
            raise WorkspaceError('Unknown operation "%s"' % op_name)

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


class WorkspaceError(Exception):
    def __init__(self, cause, *args, **kwargs):
        if isinstance(cause, Exception):
            super(WorkspaceError, self).__init__(str(cause), *args, **kwargs)
            _, _, traceback = sys.exc_info()
            self.with_traceback(traceback)
        elif isinstance(cause, str):
            super(WorkspaceError, self).__init__(cause, *args, **kwargs)
        else:
            super(WorkspaceError, self).__init__(*args, **kwargs)
        self._cause = cause

    @property
    def cause(self):
        return self._cause
