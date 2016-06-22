"""
ESA CCI Toolbox API.
"""

from .version import __version__

from .core.io import open_dataset, query_data_sources
from .core.io import DataStore, DataSource
from .core.io import FileSetDataStore, FileSetDataSource, FileSetInfo
from .core.cdm import DatasetCollection, Dataset
from .core.op import op, op_input, op_output, op_return, OpMetaInfo, OpRegistration
from .core.workflow import Workflow, Step, Node, OpStep, WorkflowStep, ExprStep
from .core.monitor import Monitor, ConsoleMonitor
