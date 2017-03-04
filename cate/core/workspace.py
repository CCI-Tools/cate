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
from typing import List

import fiona
import numpy as np
import pandas as pd
import xarray as xr

from cate.conf.defaults import WORKSPACE_DATA_DIR_NAME, WORKSPACE_WORKFLOW_FILE_NAME, SCRATCH_WORKSPACES_PATH
from cate.core.cdm import get_lon_dim_name, get_lat_dim_name
from cate.core.op import OP_REGISTRY, parse_op_args
from cate.core.workflow import Workflow, OpStep, NodePort, ValueCache
from cate.util import Monitor, Namespace, object_to_qualified_name
from cate.util.im import ImagePyramid, get_chunk_size
from cate.util.opmetainf import OpMetaInfo


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

    @classmethod
    def get_workspace_dir(cls, base_dir) -> str:
        return os.path.join(base_dir, WORKSPACE_DATA_DIR_NAME)

    @classmethod
    def get_workflow_file(cls, base_dir) -> str:
        return os.path.join(cls.get_workspace_dir(base_dir), WORKSPACE_WORKFLOW_FILE_NAME)

    @classmethod
    def new_workflow(cls, header_dict: dict = None) -> Workflow:
        return Workflow(OpMetaInfo('workspace_workflow',
                                   has_monitor=True,
                                   header_dict=header_dict or {}))

    @classmethod
    def create(cls, base_dir: str, description: str = None) -> 'Workspace':
        return Workspace(base_dir, Workspace.new_workflow(dict(description=description or '')))

    @classmethod
    def open(cls, base_dir: str) -> 'Workspace':
        if not os.path.isdir(cls.get_workspace_dir(base_dir)):
            raise WorkspaceError('Not a valid workspace: %s' % base_dir)
        try:
            workflow_file = cls.get_workflow_file(base_dir)
            workflow = Workflow.load(workflow_file)
            return Workspace(base_dir, workflow)
        except (IOError, OSError) as e:
            raise WorkspaceError(e)

    def close(self):
        if self._is_closed:
            return
        self._resource_cache.close()

    def save(self):
        self._assert_open()
        base_dir = self.base_dir
        try:
            if not os.path.isdir(base_dir):
                os.mkdir(base_dir)
            workspace_dir = self.workspace_dir
            if not os.path.isdir(workspace_dir):
                os.mkdir(workspace_dir)
            self.workflow.store(self.workflow_file)
            self._is_modified = False
        except (IOError, OSError) as e:
            raise WorkspaceError(e)

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
                            ('resources', self._resources_to_json_dict())
                            ])

    def _resources_to_json_dict(self):
        resource_descriptors = []
        resource_cache = dict(self._resource_cache)
        for res_step in self.workflow.steps:
            res_name = res_step.id
            if res_name in resource_cache:
                resource = resource_cache.pop(res_name)
                resource_descriptor = self._get_resource_descriptor(res_name, resource)
                resource_descriptors.append(resource_descriptor)
        if len(resource_cache) > 0:
            # We should not get here as all resources should have an associated workflow step!
            for res_name, resource in resource_cache.items():
                resource_descriptor = self._get_resource_descriptor(res_name, resource)
                resource_descriptors.append(resource_descriptor)
        return resource_descriptors

    def _get_resource_descriptor(self, res_name: str, resource):
        variable_descriptors = []
        if isinstance(resource, xr.Dataset):
            var_names = sorted(resource.data_vars.keys())
            for var_name in var_names:
                variable = resource.data_vars[var_name]
                variable_descriptors.append(self._get_xarray_variable_descriptor(variable))
            return dict(name=res_name,
                        dataType=object_to_qualified_name(type(resource)),
                        variables=variable_descriptors)
        elif isinstance(resource, pd.DataFrame):
            var_names = list(resource.columns)
            for var_name in var_names:
                variable = resource[var_name]
                variable_descriptors.append(self._get_pandas_variable_descriptor(variable))
            return dict(name=res_name,
                        dataType=object_to_qualified_name(type(resource)),
                        variables=variable_descriptors)
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
            return dict(name=res_name,
                        dataType=object_to_qualified_name(type(resource)),
                        variables=variable_descriptors,
                        geometry=geometry,
                        numFeatures=num_features)

    def _get_pandas_variable_descriptor(self, variable: pd.Series):
        return {
            'name': variable.name,
            'dataType': object_to_qualified_name(variable.dtype),
            'ndim': variable.ndim,
            'shape': variable.shape,
            'dimensions': variable.dims,
        }

    def _get_xarray_variable_descriptor(self, variable: xr.DataArray):
        attrs = variable.attrs
        variable_info = {
            'name': variable.name,
            'dataType': object_to_qualified_name(variable.dtype),
            'ndim': len(variable.dims),
            'shape': variable.shape,
            'chunks': get_chunk_size(variable),
            'dimensions': variable.dims,
            'fill_value': self._get_float_attr(attrs, '_FillValue'),
            'valid_min': self._get_float_attr(attrs, 'valid_min'),
            'valid_max': self._get_float_attr(attrs, 'valid_max'),
            'add_offset': self._get_float_attr(attrs, 'add_offset'),
            'scale_factor': self._get_float_attr(attrs, 'scale_factor'),
            'standard_name': self._get_unicode_attr(attrs, 'standard_name'),
            'long_name': self._get_unicode_attr(attrs, 'long_name'),
            'units': self._get_unicode_attr(attrs, 'units', default_value='-'),
            'comment': self._get_unicode_attr(attrs, 'comment'),
        }
        image_config = self._get_variable_image_config(variable)
        if image_config:
            variable_info['imageLayout'] = image_config
            variable_info['y_flipped'] = Workspace._is_y_flipped(variable)
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

            lat_delta = 0.5 * abs(lats[1] - lats[0]) if len(lats) else 0
            lon_delta = 0.5 * abs(lons[1] - lons[0]) if len(lons) else 0

            south = min(lats[0], lats[-1]) - lat_delta
            north = max(lats[0], lats[-1]) + lat_delta
            west = min(lons[0], lons[-1]) - lon_delta
            east = max(lons[0], lons[-1]) + lon_delta

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
            # print(key, ' type:', str(type(value)), ' value:', str(value))
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
                dependent_steps.append(res_step.id)

        if dependent_steps:
            raise WorkspaceError('Cannot delete resource "%s" because the following resource '
                                 'depend on it: %s' % (res_step, ', '.join(dependent_steps)))

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
            self._resource_cache[new_res_name] = self._resource_cache.pop(res_name)

    def set_resource(self, res_name: str, op_name: str, op_args: List[str], overwrite=False, validate_args=False):
        assert res_name
        assert op_name
        assert op_args

        op = OP_REGISTRY.get_op(op_name)
        if not op:
            raise WorkspaceError('Unknown operation "%s"' % op_name)

        new_step = OpStep(op, node_id=res_name)

        workflow = self.workflow

        # This namespace will allow us to wire the new resource with existing workflow steps
        # We only add step outputs, so we cannot reference another step's input neither.
        # Note that workspace workflows never have any inputs to be referenced anyway.
        namespace = dict()
        for step in workflow.steps:
            output_namespace = step.output
            namespace[step.id] = output_namespace

        does_exist = res_name in namespace
        if not overwrite and does_exist:
            raise WorkspaceError('Resource "%s" already exists' % res_name)

        if does_exist:
            # Prevent resource from self-referencing
            namespace.pop(res_name, None)

        op_kwargs = self._parse_op_args(op, op_args, namespace, validate_args)

        return_output_name = OpMetaInfo.RETURN_OUTPUT_NAME

        # Wire new op_step with outputs from existing steps
        for input_name, input_value in op_kwargs.items():
            if input_name not in new_step.input:
                raise WorkspaceError('"%s" is not an input of operation "%s"' % (input_name, op_name))
            input_port = new_step.input[input_name]
            if isinstance(input_value, NodePort):
                # input_value is an output NodePort of another step
                input_port.source = input_value
            elif isinstance(input_value, Namespace):
                # input_value is output_namespace of another step
                if return_output_name not in input_value:
                    raise WorkspaceError('Illegal value for input "%s"' % input_name)
                input_port.source = input_value['return']
            else:
                # Neither a Namespace nor a NodePort, it must be a constant value
                input_port.value = input_value

        old_step = workflow.find_node(res_name)

        # Collect keys of invalidated cache entries, initialize with res_name
        ids_of_invalidated_steps = {res_name}
        if old_step is not None:
            # Collect all IDs of steps that depend on old_step, if any
            for step in workflow.steps:
                requires = step.requires(old_step)
                if requires:
                    ids_of_invalidated_steps.add(step.id)

        print(ids_of_invalidated_steps)

        workflow = self._workflow
        # noinspection PyUnusedLocal
        workflow.add_step(new_step, can_exist=True)
        self._is_modified = True

        # Remove any cached resource values, whose steps became invalidated
        for key in ids_of_invalidated_steps:
            if key in self._resource_cache:
                del self._resource_cache[key]

    def run_op(self, op_name: str, op_args: List[str], validate_args=False, monitor=Monitor.NONE):
        assert op_name
        assert op_args

        op = OP_REGISTRY.get_op(op_name)
        if not op:
            raise WorkspaceError('Unknown operation "%s"' % op_name)

        with monitor.starting("Running operation '%s'" % op_name, 2):
            self.workflow.invoke(self.resource_cache, monitor=monitor.child(work=1))
            op_kwargs = self._parse_op_args(op, op_args, self.resource_cache, validate_args)
            op(monitor=monitor.child(work=1), **op_kwargs)

    def execute_workflow(self, res_name: str = None, monitor: Monitor = Monitor.NONE):
        self._assert_open()
        if not res_name:
            steps = self.workflow.sorted_steps
        else:
            res_step = self.workflow.find_node(res_name)
            if res_step is None:
                raise WorkspaceError('Resource "%s" not found' % res_name)
            steps = self.workflow.find_steps_to_compute(res_step.id)
        Workflow.invoke_steps(steps, value_cache=self._resource_cache, monitor=monitor)
        return steps[-1].get_output_value()

    # noinspection PyMethodMayBeStatic
    def _parse_op_args(self, op, raw_op_args, namespace: dict, validate_args: bool):
        try:
            # some arguments may now be of type 'Namespace' or 'NodePort', which are outputs of other workflow steps
            op_args, op_kwargs = parse_op_args(raw_op_args, namespace=namespace)
        except ValueError as e:
            raise WorkspaceError(e)
        if op_args:
            raise WorkspaceError("Positional arguments are not yet supported")
        if validate_args:
            # validate the op_kwargs using the operation's meta-info
            namespace_types = set(type(value) for value in namespace.values())
            op.op_meta_info.validate_input_values(op_kwargs, except_types=namespace_types)
        return op_kwargs

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
