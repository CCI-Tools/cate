import json
import os
import sys
from abc import ABCMeta, abstractmethod
from collections import OrderedDict
from typing import List

from ect.core.op import OpMetaInfo, parse_op_args
from ect.core.util import Namespace
from ect.core.workflow import Workflow, OpStep, NodePort

WORKSPACE_DATA_DIR_NAME = '.ect-workspace'
WORKSPACE_WORKFLOW_FILE_NAME = 'workflow.json'


# TODO (forman, 20160908): implement file lock for workspaces in access


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


class Workspace:
    """
    A Workspace uses a :py:class:`Workspace` to record user operations.
    By design, workspace workflows have no inputs and every step is an output.
    """

    def __init__(self, base_dir: str, workflow: Workflow):
        assert base_dir
        assert workflow
        self._base_dir = base_dir
        self._workflow = workflow

    @property
    def base_dir(self) -> str:
        """The Workspace's workflow."""
        return self._base_dir

    @property
    def workflow(self) -> Workflow:
        """The Workspace's workflow."""
        return self._workflow

    @classmethod
    def workspace_dir(cls, base_dir) -> str:
        return os.path.join(base_dir, WORKSPACE_DATA_DIR_NAME)

    @classmethod
    def workflow_file(cls, base_dir) -> str:
        return os.path.join(cls.workspace_dir(base_dir), WORKSPACE_WORKFLOW_FILE_NAME)

    @classmethod
    def create(cls, base_dir: str, description: str = None) -> 'Workspace':
        try:
            if not os.path.isdir(base_dir):
                os.mkdir(base_dir)

            workspace_dir = cls.workspace_dir(base_dir)
            workflow_file = cls.workflow_file(base_dir)
            if not os.path.isdir(workspace_dir):
                os.mkdir(workspace_dir)
            elif os.path.isfile(workflow_file):
                raise WorkspaceError('workspace exists: %s' % base_dir)

            workflow = Workflow(OpMetaInfo('workspace_workflow',
                                           has_monitor=True,
                                           header_dict=dict(description=description or '')))
            workflow.store(workflow_file)
            return Workspace(base_dir, workflow)
        except (IOError, OSError, FileExistsError) as e:
            raise WorkspaceError(e)

    @classmethod
    def load(cls, base_dir: str) -> 'Workspace':
        try:
            workflow_file = cls.workflow_file(base_dir)
            workflow = Workflow.load(workflow_file)
            return Workspace(base_dir, workflow)
        except (IOError, OSError) as e:
            raise WorkspaceError(e)

    def store(self):
        workflow_file = self.workflow_file(self.base_dir)
        self.workflow.store(workflow_file)

    @classmethod
    def from_json_dict(cls, json_dict):
        base_dir = json_dict.get('base_dir', None)
        workflow_json = json_dict.get('workflow', None)
        workflow = Workflow.from_json_dict(workflow_json)
        return Workspace(base_dir, workflow)

    def to_json_dict(self):
        return OrderedDict([('base_dir', self.base_dir),
                            ('workflow', self.workflow.to_json_dict())])

    def add_resource(self, res_name: str, op_name: str, op_args: List[str]):
        assert res_name
        assert op_name
        assert op_args

        from ect.core.op import OP_REGISTRY
        op = OP_REGISTRY.get_op(op_name)
        if not op:
            raise WorkspaceError("unknown operation '%s'" % op_name)

        op_step = OpStep(op, res_name)

        workflow = self.workflow

        namespace = dict()
        for step in workflow.steps:
            output_namespace = step.output
            namespace[step.id] = output_namespace

        if res_name in namespace:
            raise WorkspaceError("resource '%s' already exists" % res_name)

        raw_op_args = op_args
        try:
            op_args, op_kwargs = parse_op_args(raw_op_args, namespace=namespace)
        except ValueError as e:
            raise WorkspaceError(e)

        if op_args:
            raise WorkspaceError("positional arguments are not yet supported")

        return_output_name = OpMetaInfo.RETURN_OUTPUT_NAME

        for input_name, input_value in op_kwargs.items():
            if input_name not in op_step.input:
                raise WorkspaceError("'%s' is not an input of operation '%s'" % (input_name, op_name))
            input_port = op_step.input[input_name]
            if isinstance(input_value, NodePort):
                output_port = input_value
                input_port.source = output_port
            elif isinstance(input_value, Namespace):
                output_namespace = input_value
                if return_output_name not in output_namespace:
                    raise WorkspaceError("illegal value for input '%s'" % input_name)
                output_port = output_namespace[return_output_name]
                input_port.source = output_port
            else:
                input_port.value = input_value

        self._workflow.add_steps(op_step)
        if op_step.op_meta_info.has_named_outputs:
            # TODO (forman, 20160908): Support named operation outputs. Must create multiple workflow outputs here.
            raise WorkspaceError("operation '%s' has named outputs which are not (yet) supported" % op_name)
        self._workflow.op_meta_info.output[res_name] = op_step.op_meta_info.output[return_output_name]
        output_port = NodePort(self._workflow, res_name)
        output_port.source = op_step.output[return_output_name]
        self._workflow.output[res_name] = output_port


class WorkspaceManager(metaclass=ABCMeta):
    @abstractmethod
    def init_workspace(self, base_dir: str, description: str = None):
        pass

    @abstractmethod
    def get_workspace(self, base_dir: str):
        pass


class FSWorkspaceManager(WorkspaceManager):
    def __init__(self):
        self._workspace_cache = dict()

    @classmethod
    def abs_dir(cls, dir_path):
        if not dir_path:
            dir_path = os.curdir
        return os.path.abspath(dir_path)

    def init_workspace(self, base_dir: str = None, description: str = None) -> Workspace:
        base_dir = self.abs_dir(base_dir)
        workspace = self._workspace_cache.get(base_dir, None)
        if workspace:
            raise WorkspaceError('workspace exists: %s' % base_dir)
        workspace = Workspace.create(base_dir, description=description)
        self._workspace_cache[base_dir] = workspace
        return workspace

    def get_workspace(self, base_dir: str = None) -> Workspace:
        base_dir = self.abs_dir(base_dir)
        workspace = self._workspace_cache.get(base_dir, None)
        if workspace:
            return workspace
        workspace = Workspace.load(base_dir)
        self._workspace_cache[base_dir] = workspace
        return workspace


import urllib.parse
import urllib.request


def encode_path(path_pattern: str, path_args: dict = None, query_args: dict = None):
    path = path_pattern
    if path_args:
        quoted_pattern_args = dict(path_args)
        for name, value in path_args.items():
            quoted_pattern_args[name] = urllib.parse.quote(str(value)) if value is not None else ''
        path = path_pattern.format(**quoted_pattern_args)
    query_string = ''
    if query_args:
        query_string = '?' + urllib.parse.urlencode(query_args)
    return path + query_string


class WebAPIWorkspaceManager(WorkspaceManager):
    def __init__(self, port=8888, address='localhost', timeout=120):
        self.base_url = 'http://%s:%s' % (address, port)
        self.timeout = timeout

    def _url(self, path_pattern: str, path_args: dict = None, query_args: dict = None):
        return self.base_url + encode_path(path_pattern, path_args=path_args, query_args=query_args)

    def _fetch_json(self, url, error_type):
        with urllib.request.urlopen(url, timeout=self.timeout) as response:
            json_text = response.read()
        json_dict = json.loads(json_text.decode('utf-8'))
        if 'error' in json_dict:
            raise error_type(json_dict.get('message', json_dict['error']))
        return json_dict

    def init_workspace(self, base_dir: str, description: str = None) -> Workspace:
        url = self._url('/ws/init', query_args=dict(base_dir=base_dir, description=description or ''))
        json_dict = self._fetch_json(url, WorkspaceError)
        return Workspace.from_json_dict(json_dict)

    def get_workspace(self, base_dir: str) -> Workspace:
        url = self._url('/ws/get/{base_dir}', path_args=dict(base_dir=base_dir))
        json_dict = self._fetch_json(url, WorkspaceError)
        return Workspace.from_json_dict(json_dict)
