"""
Tests for anomaly operations
"""

from unittest import TestCase

import numpy as np
import xarray as xr
import datetime as datetime

from cate.ops import anomaly
from cate.core.op import OP_REGISTRY
from cate.util.misc import object_to_qualified_name


def assert_dataset_equal(expected, actual):
    # this method is functionally equivalent to
    # `assert expected == actual`, but it checks each aspect
    # of equality separately for easier debugging
    assert expected.equals(actual), (expected, actual)


class Test
