import os
import sys
from abc import ABCMeta, abstractmethod

from ect.core.op import OpMetaInfo
from ect.core.workflow import Workflow, OpStep, NodePort

WORKSPACE_DATA_DIR_NAME = '.ect-workspace'
WORKSPACE_WORKFLOW_FILE_NAME = 'workflow.json'


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


class WorkspaceManager(metaclass=ABCMeta):
    @abstractmethod
    def init_workspace(self, base_dir: str):
        pass


class FileSystemWorkspaceManager(WorkspaceManager):
    def init_workspace(self, base_dir: str = None):
        try:
            if not base_dir:
                base_dir = os.path.abspath(os.curdir)
            base_dir = os.path.abspath(base_dir)
            if not os.path.isdir(base_dir):
                os.mkdir(base_dir)
            workspace_dir = os.path.join(base_dir, WORKSPACE_DATA_DIR_NAME)
            workflow_file = os.path.join(workspace_dir, WORKSPACE_WORKFLOW_FILE_NAME)
            if not os.path.isdir(workspace_dir):
                os.mkdir(workspace_dir)
            elif os.path.isfile(workflow_file):
                raise WorkspaceError('workspace exists: %s' % base_dir)
            workflow = self._new_workflow()
            workflow.store(workflow_file)
        except (IOError, OSError, FileExistsError) as e:
            raise WorkspaceError(e)

    @classmethod
    def _new_workflow(cls):
        workflow = Workflow(OpMetaInfo('workspace_workflow',
                                       has_monitor=True,
                                       header_dict=dict(description='A workflow used to record operations '
                                                                    'performed in an ECT workspace. '
                                                                    'By design, workspace '
                                                                    'workflows have no inputs '
                                                                    'and every step is an output.')))
        return workflow


class Workspace:
    def __init__(self, path):
        self._path = path
        self._workflow = Workflow('workspace-wf')

    @property
    def workflow(self) -> Workflow:
        """The Workspace's workflow."""
        return self._workflow

    def add_resource(self, name, op, **kwargs):
        step = OpStep(op, name)
        for k, v in kwargs.items():
            if k in step.input:
                port = step.input[k]
                # print(k,kwargs[k])
                node = self._workflow.find_node(v)
                if node:
                    port.source = node.output['return']
                else:
                    port.value = v
            else:
                raise ValueError('unknown parameter "%s"' % k)
        self._workflow.add_steps(step)
        self._workflow.op_meta_info.output[name] = step.op_meta_info.output['return']
        output_port = NodePort(self._workflow, name)
        output_port.source = step.output['return']
        self._workflow.output[name] = output_port
