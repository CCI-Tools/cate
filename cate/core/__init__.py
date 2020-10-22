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
Cate's core API.
"""

import os.path
import sys

# See https://github.com/CCI-Tools/cate/issues/397
extra_path = os.path.join(sys.prefix, 'site-packages')
if os.path.isdir(extra_path) and extra_path not in sys.path:
    sys.path.append(extra_path)

# noinspection PyUnresolvedReferences
from .common import initialize_proxy, configure_user_agent
initialize_proxy()
# See https://github.com/CCI-Tools/cate/issues/510
configure_user_agent()

# noinspection PyUnresolvedReferences
from .ds import DataStore, open_dataset, find_data_store, DATA_STORE_REGISTRY

# noinspection PyUnresolvedReferences
from .op import op, op_input, op_output, op_return, Operation, OP_REGISTRY, \
    new_expression_op, new_subprocess_op

# noinspection PyUnresolvedReferences
from .workflow import Workflow, Step, Node, OpStep, NoOpStep, SubProcessStep, ExpressionStep, WorkflowStep, NodePort, \
    new_workflow_op

# noinspection PyUnresolvedReferences
from ..util.monitor import Monitor, ChildMonitor, ConsoleMonitor

# noinspection PyUnresolvedReferences
from ..util.opmetainf import OpMetaInfo

# Run plugin registration by importing the plugin module
# noinspection PyUnresolvedReferences
from .plugin import cate_init as _

del _
