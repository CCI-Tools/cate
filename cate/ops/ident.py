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
This module provides type-save identity operations.

They can be used to rename a value of a given type.

All operations in this module are tagged with the ``"utility"`` tag.
"""

from cate.core.op import op


@op(tags=['utility'])
def ident_bool(value: bool) -> bool:
    """
    Return the given boolean value.

    :param value: A boolean value.
    """
    return value


@op(tags=['utility'])
def ident_int(value: int) -> int:
    """
    Return the given integer value.

    :param value: An integer value.
    """
    return value


@op(tags=['utility'])
def ident_float(value: float) -> float:
    """
    Return the given floating point value.

    :param value: A floating point value.
    """
    return value


@op(tags=['utility'])
def ident_str(value: str) -> str:
    """
    Return the given string value.

    :param value: A string value.
    """
    return value
