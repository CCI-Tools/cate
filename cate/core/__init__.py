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

"""
Cate's core API.
"""

# noinspection PyUnresolvedReferences
from .ds import DataStore, DataSource, open_dataset, query_data_sources
# noinspection PyUnresolvedReferences
from .op import op, op_input, op_output, op_return, OpMetaInfo, OpRegistration
# noinspection PyUnresolvedReferences
from .workflow import Workflow, Step, Node, OpStep, NoOpStep, SubProcessStep, ExprStep, WorkflowStep, NodePort
# noinspection PyUnresolvedReferences
from ..util.monitor import Monitor, ChildMonitor, ConsoleMonitor

# Run plugin registration by importing the plugin module
# noinspection PyUnresolvedReferences
from .plugin import cate_init as _

del _

__all__ = """
    DataStore, DataSource, open_dataset, query_data_sources
    op, op_input, op_output, op_return, OpMetaInfo, OpRegistration
    Workflow, Step, Node, OpStep, NoOpStep, SubProcessStep, ExprStep, WorkflowStep, NodePort
    Monitor, ChildMonitor, ConsoleMonitor
""".split()
