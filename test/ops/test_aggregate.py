"""
Tests for aggregation operations
"""

from unittest import TestCase

from cate.ops import long_term_average, temporal_aggregation 


def assert_dataset_equal(expected, actual):
    # this method is functionally equivalent to
    # `assert expected == actual`, but it checks each aspect
    # of equality separately for easier debugging
    assert expected.equals(actual), (expected, actual)


class TestLTA(TestCase):
    """
    Test long term averaging
    """
    def test_nominal(self):
        """
        Test nominal execution
        """
        pass

    def test_registered(self):
        """
        Test registered operation execution
        """
        pass


class TestTemporalAggregation(TestCase):
    """
    Test temporal aggregation
    """
    def test_nominal(self):
        """
        Test nominal exeuction
        """
        pass

    def test_registered(self):
        """
        Test registered operation execution
        """
        pass
