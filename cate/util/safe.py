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

import sys
from typing import Dict, Any, Callable

__author__ = "Norman Fomferra (Brockmann Consult GmbH)"


def _get_safe_globals_accessor() -> Callable[[], dict]:
    safe_builtin_names = [
        "abs",
        "all",
        "any",
        "ascii",
        "bin",
        "bool",
        "bytearray",
        "bytes",
        "callable",
        "chr",
        "complex",
        "dict",
        "divmod",
        "enumerate",
        "filter",
        "float",
        "format",
        "frozenset",
        "getattr",
        "hasattr",
        "hash",
        "hex",
        "int",
        "isinstance",
        "issubclass",
        "iter",
        "len",
        "list",
        "map",
        "max",
        "min",
        "next",
        "object",
        "oct",
        "ord",
        "pow",
        "range",
        "repr",
        "reversed",
        "round",
        "set",
        "slice",
        "sorted",
        "str",
        "sum",
        "tuple",
        "type",
        "zip",
    ]

    safe_module_names = [
        'cmath',
        'math',
    ]

    builtins = sys.modules['builtins']

    safe_globals = {}
    for name in safe_builtin_names:
        safe_globals[name] = builtins.__dict__[name]

    for name in safe_module_names:
        safe_globals[name] = __import__(name)

    safe_globals['__builtins__'] = None
    safe_globals['builtins'] = None

    def _get_safe_globals_closure():
        """Return a global environment for safe expression evaluation."""
        return dict(safe_globals)

    return _get_safe_globals_closure


get_safe_globals = _get_safe_globals_accessor()


def safe_eval(expression: str, local_namespace: Dict[str, Any] = None):
    """
    The **expression** argument is parsed and evaluated as a Python expression 
    using the **local_namespace** mapping as local namespace. 
    The **expression** has no access to the current environment and only limited access to the standard builtins, i.e.
    only functions considered safe are allowed, e.g. *abs*, *min*, *max*, etc.
    
    Syntax errors are reported as exceptions.
        
    :param expression: A Python expression.
    :param local_namespace: The local namespace in which **expression** is evaluated.
    :return: The result of the evaluated expression.
    """
    return eval(expression, get_safe_globals(), local_namespace or {})
