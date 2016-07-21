import xarray as xr

from ect.core.op import op

# TODO (mz, 201607211): parma 'copy' unused

@op()
def filter(dataset: xr.Dataset, variable_names: list = None, regex = False, copy: bool = False) -> xr.Dataset:
    """
    Filter the dataset, by leaving only desired variables.

    Whether the ``filter`` method returns a view or a copy of the underlying data depends on the concrete format and
    nature of the data.

    .. _regex: https://docs.python.org/3.5/library/re.html

    :param dataset: The dataset.
    :param variable_names: List of regex patterns that identify the variables to keep.
    :param regex: If ``True``, *variable_names* is expected to contain regex_ patterns.
    :param copy: If ``True``, the returned dataset will likely contain data copies of the original dataset
    :return: a new, filtered dataset of type :py:class:`xr.Dataset`
    """
    if not variable_names:
        return dataset

    dropped_var_names = list(dataset.data_vars.keys())

    if not regex:
        for var_name in variable_names:
            if var_name in dropped_var_names:
                dropped_var_names.remove(var_name)
    else:
        import re
        for var_name_pattern in variable_names:
            prog = re.compile(var_name_pattern)
            for dropped_var_name in list(dropped_var_names):
                if prog.match(dropped_var_name):
                    dropped_var_names.remove(dropped_var_name)

    return dataset.drop(dropped_var_names)
