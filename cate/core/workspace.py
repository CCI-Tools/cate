# The MIT License (MIT)
# Copyright (c) 2016-2023 by the ESA CCI Toolbox team and contributors
#
# Permission is hereby granted, free of charge, to any person obtaining a
# copy of this software and associated documentation files (the "Software"),
# to deal in the Software without restriction, including without limitation
# the rights to use, copy, modify, merge, publish, distribute, sublicense,
# and/or sell copies of the Software, and to permit persons to whom the
# Software is furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
# FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
# DEALINGS IN THE SOFTWARE.

"""
This module defines the ``Workspace`` class.
"""

import logging
import os
import shutil
from collections import OrderedDict
from threading import RLock
from typing import List, Any, Dict, Optional

import fiona
import pandas as pd
import xarray as xr

from cate.conf import conf
from cate.conf.defaults import DEFAULT_SCRATCH_WORKSPACES_PATH
from cate.conf.defaults import WORKSPACE_DATA_DIR_NAME
from cate.conf.defaults import WORKSPACE_WORKFLOW_FILE_NAME
from cate.util.im import get_chunk_size
from cate.util.misc import new_indexed_name
from cate.util.misc import object_to_qualified_name
from cate.util.misc import to_json
from cate.util.misc import to_scalar
from cate.util.monitor import Monitor
from cate.util.namespace import Namespace
from cate.util.opmetainf import OpMetaInfo
from cate.util.safe import safe_eval
from cate.util.undefined import UNDEFINED
from .cdm import get_tiling_scheme
from .op import OP_REGISTRY
from .types import GeoDataFrame
from .types import ValidationError
from .workflow import NodePort
from .workflow import OpStep
from .workflow import ValueCache
from .workflow import Workflow

_LOG = logging.getLogger('cate')

_RESOURCE_PERSISTENCE_FORMATS = dict(
    netcdf4=('nc', xr.open_dataset, 'to_netcdf'),
    zarr=('zarr', xr.open_zarr, 'to_zarr')
)

#: An JSON-serializable operation argument is a one-element dictionary
#  taking two possible forms:
#: 1. dict(value=Any):  a value which may be any constant Python object
#     which must JSON-serializable
#: 2. dict(source=str): a reference to a step port name
OpArg = Dict[str, Any]

#: JSON-serializable, positional operation arguments
OpArgs = List[OpArg]

#: JSON-serializable, keyword operation arguments
OpKwArgs = Dict[str, OpArg]


def mk_op_arg(arg) -> OpArg:
    """
    Utility function which turns an argument into an operation argument.
    If *args* is a ``str`` and starts with "@" it is turned into a
    "source" argument, otherwise it is turned into a "value" argument.
    """
    return dict(source=arg[1:]) \
        if (isinstance(arg, str)
            and arg.startswith('@')) \
        else dict(value=arg)


def mk_op_args(*args) -> OpArgs:
    """
    Utility function which converts a list into positional
    operation arguments.
    """
    return [mk_op_arg(arg) for arg in args]


def mk_op_kwargs(**kwargs) -> OpKwArgs:
    """
    Utility function which converts a dictionary into
    operation keyword arguments.
    """
    return OrderedDict([(kw, mk_op_arg(arg)) for kw, arg in kwargs.items()])


class Workspace:
    """
    A Workspace uses a :py:class:`Workflow` to record user operations.
    By design, workspace workflows have no inputs and every step is an output.
    """

    _base_dir_to_id: Dict[str, int] = {}
    _last_id = 0
    _id_lock = RLock()

    def __init__(self,
                 base_dir: str,
                 workflow: Workflow,
                 is_modified: bool = False,
                 preferred_id: Optional[int] = None):
        assert base_dir
        assert workflow
        self._id = self.get_id_from_base_dir(base_dir,
                                             preferred_id=preferred_id)
        self._base_dir = base_dir
        self._workflow = workflow
        self._is_scratch = (base_dir or '').startswith(
            DEFAULT_SCRATCH_WORKSPACES_PATH
        )
        self._is_modified = is_modified
        self._is_closed = False
        self._resource_cache = ValueCache()
        self._user_data = dict()
        self._lock = RLock()

    def __del__(self):
        self.close()

    @classmethod
    def get_id_from_base_dir(cls,
                             base_dir: str,
                             preferred_id: Optional[int] = None) -> int:
        with cls._id_lock:
            id = cls._get_id_from_base_dir(base_dir, preferred_id)
            _LOG.info(f'{base_dir!r} --> {id}')
            return id

    @classmethod
    def get_base_dir_from_id(cls, id: int) -> str:
        with cls._id_lock:
            base_dir = cls._get_base_dir_from_id(id)
            _LOG.info(f'{id} --> {base_dir!r}')
            return base_dir

    @classmethod
    def _get_id_from_base_dir(cls,
                              base_dir: str,
                              preferred_id: Optional[int]) -> int:
        base_dir_to_id = cls._base_dir_to_id
        if isinstance(preferred_id, int):
            # Try reusing base_dir --> id
            if base_dir not in base_dir_to_id \
                    and preferred_id not in set(base_dir_to_id.values()):
                base_dir_to_id[base_dir] = preferred_id
                return preferred_id
        if base_dir in base_dir_to_id:
            id = base_dir_to_id[base_dir]
        else:
            id = cls._last_id + 1
            cls._last_id = id
            base_dir_to_id[base_dir] = id
        return id

    @classmethod
    def _get_base_dir_from_id(cls, id: int) -> str:
        for base_dir, _id in cls._base_dir_to_id.items():
            if id == _id:
                return base_dir
        raise ValueError(f'No base directory'
                         f' found for workspace identifier #{id}')

    @property
    def id(self) -> int:
        """The Workspace' identifier."""
        return self._id

    @property
    def base_dir(self) -> str:
        """The Workspace' container directory."""
        return self._base_dir

    @property
    def workflow(self) -> Workflow:
        """The Workspace' workflow."""
        self._assert_open()
        return self._workflow

    @property
    def resource_cache(self) -> ValueCache:
        """The Workspace' resource cache."""
        return self._resource_cache

    @property
    def is_scratch(self) -> bool:
        return self._is_scratch

    @is_scratch.setter
    def is_scratch(self, value: bool):
        self._is_scratch = value

    @property
    def is_closed(self) -> bool:
        return self._is_closed

    @property
    def is_modified(self) -> bool:
        return self._is_modified

    @property
    def workspace_data_dir(self) -> str:
        return self.get_workspace_data_dir(self.base_dir)

    @property
    def workflow_file(self) -> str:
        return self.get_workflow_file(self.base_dir)

    @property
    def user_data(self) -> dict:
        return self._user_data

    @classmethod
    def get_workspace_data_dir(cls, base_dir) -> str:
        return os.path.join(base_dir, WORKSPACE_DATA_DIR_NAME)

    @classmethod
    def get_workflow_file(cls, base_dir) -> str:
        return os.path.join(cls.get_workspace_data_dir(base_dir),
                            WORKSPACE_WORKFLOW_FILE_NAME)

    @classmethod
    def new_workflow(cls, header: dict = None) -> Workflow:
        return Workflow(OpMetaInfo('workspace_workflow',
                                   has_monitor=True,
                                   header=header or {}))

    @classmethod
    def create(cls, base_dir: str, description: str = None) -> 'Workspace':
        return Workspace(base_dir,
                         Workspace.new_workflow(
                             dict(description=description or '')
                         ))

    @classmethod
    def open(cls,
             base_dir: str,
             monitor: Monitor = Monitor.NONE) -> 'Workspace':
        if not os.path.isdir(cls.get_workspace_data_dir(base_dir)):
            raise ValidationError('Not a valid workspace: %s' % base_dir)
        workflow_file = cls.get_workflow_file(base_dir)
        workflow = Workflow.load(workflow_file)
        workspace = Workspace(base_dir, workflow)

        # Read resources for persistent steps
        persistent_steps = [step for step in workflow.steps
                            if step.persistent]
        if persistent_steps:
            with monitor.starting('Reading resources', len(persistent_steps)):
                for step in persistent_steps:
                    workspace._read_resource_from_file(step.id)
                monitor.progress(1)

        return workspace

    def close(self):
        with self._lock:
            if self._is_closed:
                return
            self._resource_cache.close()
            # Remove all resource files that are no longer required
            if os.path.isdir(self.workspace_data_dir):
                persistent_ids = {step.id for step in self.workflow.steps
                                  if step.persistent}
                for filename in os.listdir(self.workspace_data_dir):
                    res_file = os.path.join(self.workspace_data_dir, filename)
                    if os.path.isfile(res_file) and filename.endswith('.nc'):
                        res_name = filename[0: -3]
                        if res_name not in persistent_ids:
                            try:
                                os.remove(res_file)
                            except OSError:
                                _LOG.exception('closing workspace failed')

    def save(self, monitor: Monitor = Monitor.NONE):
        with self._lock:
            self._assert_open()
            base_dir = self.base_dir
            if not os.path.isdir(base_dir):
                os.makedirs(base_dir)
            workspace_data_dir = self.workspace_data_dir
            if not os.path.isdir(workspace_data_dir):
                os.mkdir(workspace_data_dir)
            self.workflow.store(self.workflow_file)

            # Write resources for all persistent steps
            persistent_steps = [step for step in self.workflow.steps
                                if step.persistent]
            if persistent_steps:
                with monitor.starting('Writing resources',
                                      len(persistent_steps)):
                    for step in persistent_steps:
                        self._write_resource_to_file(step.id)
                        monitor.progress(1)

            self._is_modified = False

    def _write_resource_to_file(self, res_name):
        res_value = self._resource_cache.get(res_name)
        if isinstance(res_value, xr.Dataset):
            format_props = _RESOURCE_PERSISTENCE_FORMATS.get(
                conf.get_dataset_persistence_format()
            )
            if format_props:
                ext, _, write_attr = format_props
                if hasattr(res_value, write_attr):
                    write_method = getattr(res_value, write_attr)
                    # noinspection PyBroadException
                    try:
                        resource_file = os.path.join(self.workspace_data_dir,
                                                     res_name + '.' + ext)
                        write_method(resource_file)
                    except Exception:
                        _LOG.exception(
                            'writing resource "%s" to file failed' % res_name
                        )

    def _read_resource_from_file(self, res_name):
        for ext, open_dataset, _ in _RESOURCE_PERSISTENCE_FORMATS.values():
            res_file = os.path.join(self.workspace_data_dir,
                                    res_name + '.' + ext)
            if os.path.exists(res_file):
                # noinspection PyBroadException
                try:
                    res_value = open_dataset(res_file)
                    self._resource_cache[res_name] = res_value
                except Exception:
                    _LOG.exception(
                        'reading resource "%s" from file failed' % res_name
                    )

    def set_resource_persistence(self, res_name: str, persistent: bool):
        with self._lock:
            self._assert_open()
            res_step = self.workflow.find_node(res_name)
            if res_step is None:
                raise ValidationError('Resource "%s" not found' % res_name)
            if res_step.persistent == persistent:
                return
            res_step.persistent = persistent

    @classmethod
    def from_json_dict(cls, json_dict):
        preferred_id = json_dict.get('id', None)
        base_dir = json_dict.get('base_dir', None)
        workflow_json = json_dict.get('workflow', {})
        is_modified = json_dict.get('is_modified', False)
        workflow = Workflow.from_json_dict(workflow_json)
        return Workspace(base_dir,
                         workflow,
                         is_modified=is_modified,
                         preferred_id=preferred_id)

    def to_json_dict(self):
        with self._lock:
            self._assert_open()
            return OrderedDict([
                ('id', self.id),
                ('base_dir', self.base_dir),
                ('is_scratch', self.is_scratch),
                ('is_modified', self.is_modified),
                ('is_saved', os.path.exists(self.workspace_data_dir)),
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
                res_update_count = self._resource_cache.get_update_count(
                    res_name)
                resource = resource_cache.pop(res_name)
                resource_descriptor = self._get_resource_descriptor(
                    res_id,
                    res_update_count,
                    res_name,
                    resource
                )
                resource_descriptors.append(resource_descriptor)
        if len(resource_cache) > 0:
            # We should not get here as all resources should have
            # an associated workflow step!
            for res_name, resource in resource_cache.items():
                res_id = self._resource_cache.get_id(res_name)
                res_update_count = self._resource_cache.get_update_count(
                    res_name)
                resource_descriptor = self._get_resource_descriptor(
                    res_id,
                    res_update_count,
                    res_name,
                    resource
                )
                resource_descriptors.append(resource_descriptor)
        return resource_descriptors

    @classmethod
    def _get_resource_descriptor(cls,
                                 res_id: int,
                                 res_update_count: int,
                                 res_name: str,
                                 resource: Any):
        data_type_name = object_to_qualified_name(type(resource))
        resource_json = dict(id=res_id,
                             updateCount=res_update_count,
                             name=res_name,
                             dataType=data_type_name)
        if isinstance(resource, xr.Dataset):
            cls._update_resource_json_from_dataset(
                resource_json, resource
            )
        elif isinstance(resource, GeoDataFrame):
            cls._update_resource_json_from_feature_collection(
                resource_json, resource.features
            )
        elif isinstance(resource, pd.DataFrame):
            cls._update_resource_json_from_data_frame(
                resource_json, resource
            )
        elif isinstance(resource, fiona.Collection):
            cls._update_resource_json_from_feature_collection(
                resource_json, resource
            )
        return resource_json

    @classmethod
    def _update_resource_json_from_dataset(cls, resource_json, dataset):
        coords_descriptors = []
        variable_descriptors = []
        var_names = sorted(dataset.data_vars.keys())
        for var_name in var_names:
            if not var_name.endswith('_bnds'):
                variable = dataset.data_vars[var_name]
                variable_descriptors.append(
                    cls._get_xarray_variable_descriptor(variable)
                )
        var_names = sorted(dataset.coords.keys())
        for var_name in var_names:
            variable = dataset.coords[var_name]
            coords_descriptors.append(
                cls._get_xarray_variable_descriptor(variable, is_coord=True)
            )
        # noinspection PyArgumentList
        resource_json.update(
            dimSizes=to_json(dataset.dims),
            attributes=Workspace._attrs_to_json_dict(dataset.attrs),
            variables=variable_descriptors,
            coordVariables=coords_descriptors
        )

    @classmethod
    def _update_resource_json_from_data_frame(cls,
                                              resource_json,
                                              data_frame: pd.DataFrame):
        variable_descriptors = []
        var_names = list(data_frame.columns)
        for var_name in var_names:
            variable = data_frame[var_name]
            variable_descriptors.append(
                cls._get_pandas_variable_descriptor(variable)
            )
        # noinspection PyArgumentList,PyTypeChecker

        if len(data_frame.shape) == 2:
            num_rows, num_columns = data_frame.shape
        else:
            num_rows = len(data_frame)
            num_columns = 0

        attributes = {
            'num_rows': num_rows,
            'num_columns': num_columns,
        }

        if hasattr(data_frame, 'crs') and data_frame.crs is not None:
            attributes['crs'] = str(data_frame.crs)

        if hasattr(data_frame, 'geom_type'):
            geom_type = data_frame.geom_type
            if isinstance(geom_type, pd.Series) and geom_type.size > 0:
                attributes['geom_type'] = str(geom_type.iloc[0])

        resource_json.update(variables=variable_descriptors,
                             attributes=attributes)

    @classmethod
    def _update_resource_json_from_feature_collection(
            cls,
            resource_json,
            features: fiona.Collection
    ):
        variable_descriptors = []
        num_features = len(features)
        num_properties = 0
        schema_properties = features.schema.get('properties')
        if schema_properties and isinstance(schema_properties, dict):
            num_properties = len(schema_properties)
            for var_name, var_type in schema_properties.items():
                variable_descriptors.append({
                    'name': var_name,
                    'dataType': var_type,
                    'isFeatureAttribute': True,
                })

        if num_features == 1 and len(variable_descriptors) >= 1:
            # For single rows we can provide feature values directly,
            # given they are scalars
            feature = list(features)[0]
            if isinstance(feature, dict) and 'properties' in feature:
                feature_properties = feature['properties']
                if isinstance(feature_properties, dict):
                    for var_name, var_value in feature_properties.items():
                        scalar_value = _to_json_scalar_value(var_value)
                        if scalar_value is not UNDEFINED:
                            variable_descriptors[0]['value'] = scalar_value

        geom_type = features.schema.get('geometry')

        resource_json.update(variables=variable_descriptors,
                             geometry=geom_type,
                             numFeatures=num_features,
                             attributes={
                                 'num_rows': num_features,
                                 'num_columns': num_properties,
                                 'geom_type': str(geom_type or '?'),
                                 'crs': str(features.crs),
                                 'crs_wkt': str(features.crs_wkt),
                                 'driver': str(features.driver),
                             })

    @classmethod
    def _attrs_to_json_dict(cls, attrs: dict) -> Dict[str, Any]:
        attr_json_dict = {}
        for name, value in attrs.items():
            attr_json_dict[name] = to_json(value)
        return attr_json_dict

    @classmethod
    def _get_pandas_variable_descriptor(cls, variable: pd.Series):
        variable_info = {
            'name': variable.name,
            'dataType': object_to_qualified_name(variable.dtype),
            'numDims': variable.ndim,
            'shape': variable.shape,
            'isFeatureAttribute': True,
        }
        if variable.size == 1:
            scalar_value = _to_json_scalar_value(variable.values)
            if scalar_value is not UNDEFINED:
                variable_info['value'] = scalar_value
        return variable_info

    @classmethod
    def _get_xarray_variable_descriptor(cls,
                                        variable: xr.DataArray,
                                        is_coord=False):
        attrs = variable.attrs
        variable_info = {
            'name': variable.name,
            'dataType': object_to_qualified_name(variable.dtype),
            'numDims': len(variable.dims),
            'dimNames': variable.dims,
            'shape': variable.shape,
            'chunkSizes': get_chunk_size(variable),
            'attributes': Workspace._attrs_to_json_dict(attrs),
            'isCoord': is_coord,
            'isDefault': conf.is_default_variable(variable.name),
        }

        if not is_coord:
            tiling_scheme = get_tiling_scheme(variable)
            if tiling_scheme:
                variable_info['imageLayout'] = tiling_scheme.to_json()
                variable_info['isYFlipped'] = tiling_scheme.geo_extent.inv_y
        elif variable.ndim == 1:
            # Serialize data of coordinate variables.
            # To limit data transfer volume, we serialize data arrays
            # only if they are 1D.
            # Note that the 'data' field is used to display coordinate
            # labels in the GUI only.
            variable_info['data'] = to_json(variable.data)

        if variable.size == 1:
            scalar_value = _to_json_scalar_value(variable.values)
            if scalar_value is not UNDEFINED:
                variable_info['value'] = scalar_value

        display_settings = conf.get_variable_display_settings(variable.name)
        if display_settings:
            mapping = dict(colorMapName='color_map',
                           displayMin='display_min',
                           displayMax='display_max')
            for var_prop_name, display_settings_name in mapping.items():
                if display_settings_name in display_settings:
                    variable_info[var_prop_name] \
                        = display_settings[display_settings_name]

        return variable_info

    def delete(self):
        with self._lock:
            self.close()
            shutil.rmtree(self.workspace_data_dir)

    def delete_resource(self, res_name: str):
        with self._lock:
            res_step = self.workflow.find_node(res_name)
            if res_step is None:
                raise ValidationError('Resource "%s" not found' % res_name)

            dependent_steps = []
            for step in self.workflow.steps:
                if step is not res_step and step.requires(res_step):
                    dependent_steps.append(step.id)

            if dependent_steps:
                raise ValidationError(
                    'Cannot delete resource "%s" because the following'
                    ' resource(s) depend on it: %s'
                    % (res_name, ', '.join(dependent_steps))
                )

            self.workflow.remove_step(res_step)
            if res_name in self._resource_cache:
                del self._resource_cache[res_name]

    def rename_resource(self, res_name: str, new_res_name: str) -> None:
        Workspace._validate_res_name(new_res_name)
        with self._lock:
            res_step = self.workflow.find_node(res_name)
            if res_step is None:
                raise ValidationError('Resource "%s" not found' % res_name)
            res_step_new = self.workflow.find_node(new_res_name)
            if res_step_new is res_step:
                return
            if res_step_new is not None:
                raise ValidationError(
                    'Resource "%s" cannot be renamed to "%s", '
                    'because "%s" is already in use.'
                    % (res_name, new_res_name, new_res_name)
                )

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
        Set a resource named *res_name* to the result of an operation
        *op_name* using the given operation arguments *op_kwargs*.

        :param res_name: An optional resource name.
            If given and not empty, it must be unique within this workspace.
            If not provided, a workspace-unique resource name
            will be generated.
        :param op_name: The name of a registered operation.
        :param op_kwargs: The operation's keyword arguments.
            Each argument must be a dict having either a "source" or
            "value" key.
        :param overwrite:
        :param validate_args:
        :return: The resource name,
            either the one passed in or a generated one.
        """
        assert op_name
        assert op_kwargs is not None

        op = OP_REGISTRY.get_op(op_name)
        if not op:
            raise ValidationError('Unknown operation "%s"' % op_name)

        with self._lock:
            if not res_name:
                default_res_pattern = conf.get_default_res_pattern()
                res_pattern = op.op_meta_info.header.get('res_pattern',
                                                         default_res_pattern)
                res_name = self._new_resource_name(res_pattern)
            Workspace._validate_res_name(res_name)

            new_step = OpStep(op, node_id=res_name)

            workflow = self.workflow

            # This namespace will allow us to wire the new resource with
            # existing workflow steps.
            # We only add step outputs, so we cannot reference another
            # step's input neither.
            # This is not a problem because a workspace's workflow
            # doesn't have any inputs to be referenced anyway.
            namespace = dict()
            for step in workflow.steps:
                output_namespace = step.outputs
                namespace[step.id] = output_namespace

            does_exist = res_name in namespace
            if not overwrite and does_exist:
                raise ValidationError(
                    'A resource named "%s" already exists' % res_name
                )

            if does_exist:
                # Prevent resource from self-referencing
                namespace.pop(res_name, None)

            # Wire new op_step with outputs from existing steps
            for input_name, input_value in op_kwargs.items():
                if input_name not in new_step.inputs:
                    raise ValidationError(
                        '"%s" is not an input of operation "%s"'
                        % (input_name, op_name)
                    )
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
                            raise ValidationError(
                                'Illegal argument for input "%s"'
                                ' of operation "%s'
                                % (input_name, op_name)
                            )
                        input_port.source = source[
                            OpMetaInfo.RETURN_OUTPUT_NAME
                        ]
                elif 'value' in input_value:
                    # Constant value
                    input_port.value = input_value['value']
                else:
                    raise ValidationError(
                        'Illegal argument for input "%s" of operation "%s'
                        % (input_name, op_name)
                    )

            if validate_args:
                inputs = new_step.inputs
                input_values = {kw: (inputs[kw].source or inputs[kw].value)
                                for kw, v in op_kwargs.items()}
                # Validate all values except those of type
                # NodePort (= the sources)
                op.op_meta_info.validate_input_values(input_values,
                                                      [NodePort])

            old_step = workflow.find_node(res_name)

            # Collect keys of invalidated cache entries,
            # initialize with res_name
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

            # Remove any cached resource values,
            # whose steps became invalidated
            for key in ids_of_invalidated_steps:
                if key in self._resource_cache:
                    self._resource_cache[key] = UNDEFINED

        return res_name

    def run_op(self, op_name: str, op_kwargs: OpKwArgs, monitor=Monitor.NONE):
        assert op_name
        assert op_kwargs is not None

        unpacked_op_kwargs = {}
        returns = False

        with self._lock:
            op = OP_REGISTRY.get_op(op_name)
            if not op:
                raise ValidationError('Unknown operation "%s"' % op_name)

            for input_name, input_value in op_kwargs.items():
                if 'should_return' == input_name and 'value' in input_value:
                    returns = input_value['value']
                elif 'source' in input_value:
                    unpacked_op_kwargs[input_name] = safe_eval(
                        input_value['source'],
                        self.resource_cache
                    )
                elif 'value' in input_value:
                    unpacked_op_kwargs[input_name] = input_value['value']

        # Allow executing self.workflow.invoke() out of the locked
        # context so we can run tasks in parallel
        with monitor.starting("Running operation '%s'" % op_name, 2):
            self.workflow.invoke(context=self._new_context(),
                                 monitor=monitor.child(work=1))
            return_value = op(monitor=monitor.child(work=1),
                              **unpacked_op_kwargs)
            if returns:
                return return_value

    def execute_workflow(self,
                         res_name: str = None,
                         monitor: Monitor = Monitor.NONE):
        self._assert_open()

        steps = None

        with self._lock:
            if not res_name:
                steps = self.workflow.sorted_steps
            else:
                res_step = self.workflow.find_node(res_name)
                if res_step is None:
                    raise ValidationError(
                        'Resource "%s" not found' % res_name
                    )
                steps = self.workflow.find_steps_to_compute(res_step.id)

        # Allow executing self.workflow.invoke_steps() out of
        # the locked context, so we can run tasks in parallel
        if steps and len(steps):
            self.workflow.invoke_steps(steps,
                                       context=self._new_context(),
                                       monitor=monitor)
            return steps[-1].get_output_value()
        else:
            return None

    def _new_context(self):
        return dict(value_cache=self._resource_cache, workspace=self)

    def _assert_open(self):
        if self._is_closed:
            raise ValidationError(
                'Workspace is already closed: ' + self._base_dir
            )

    def _new_resource_name(self, res_pattern):
        return new_indexed_name({step.id for step in self.workflow.steps},
                                res_pattern)

    @staticmethod
    def _validate_res_name(res_name: str):
        if not res_name.isidentifier():
            raise ValidationError(
                "Resource name '%s' is not valid."
                " The name must only contain the uppercase"
                " and lowercase letters A through Z, the underscore _ and,"
                " except for the first character,"
                " the digits 0 through 9." % res_name)


def _to_json_scalar_value(value, nchars=1000):
    return to_scalar(value, ndigits=3, nchars=nchars, stringify=True)
