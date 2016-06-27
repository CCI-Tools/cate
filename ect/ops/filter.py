"""
Description
===========

Filter a dataset based on ECV variable names

Components
==========
"""

from ect.core.cdm_xarray import XArrayDatasetAdapter
from ect.core.op import op_input, op_output
from ect.core.cdm import Dataset

@op_input('variable_names', description='List of regex patterns that identify the variables to keep')
@op_input('regex', description='If True, variable names are expected to contain regex_ patterns')
@op_input('copy', decription='If True, the returned dataset will likely contain data copies of the original data')
@op_output('return', description='A filtered dataset')
def filter_dataset(ds:Dataset, var_names:list=None, regex:bool=False, copy:bool=False):
    """
    Filter the dataset, by leaving only desired variables.

    Whether the ``filter`` method returns a view or a copy of the underlying data depends on the concrete format and
    nature of the data.

    .. _regex: https://docs.python.org/3.5/library/re.html

    :param variable_names: List of regex patterns that identify the variables to keep.
    :param regex: If ``True``, *variable_names* is expected to contain regex_ patterns.
    :param copy: If ``True``, the returned dataset will likely contain data copies of the original dataset
    :return: a new, filtered dataset of type :py:class:`Dataset`
    """
    return ds.filter(var_names, regex, copy)
