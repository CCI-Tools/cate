"""
Description
===========

Correlation operations

Components
==========
"""
import math
import xarray as xr

from ect.core.op import op_input


@op_input('ds_y', description="The 'dependent' Time series dataset")
@op_input('ds_x', description="The 'variable' Time series dataset")
@op_input('path', description="File path where to save the correlation parameters")
def pearson_correlation(ds_y:xr.Dataset, ds_x:xr.Dataset, path:str=None):
    """
    Do product moment Pearson's correlation analysis.
    This assumes that the input datasets are 'timeseries' datasets,
    meaning, they contain a single 1D variable that can be obtained
    by running different time-series operations, either by selecting
    a single point or doing spatial mean of the whole dataset.

    :param ds_y: The 'dependent' time series dataset
    :param ds_x: The 'variable' time series dataset
    """
    xr_y = ds_y
    xr_x = ds_x

    # We Expect to have a single data variable in the dataset
    if len(xr_y.data_vars) != 1 or len(xr_x.data_vars) != 1:
        raise TypeError('Dataset should have a single data variable')

    array_y = None
    array_x = None

    for key in xr_y.data_vars.keys():
        array_y = xr_y[key]

    for key in xr_x.data_vars.keys():
        array_x = xr_x[key]

    y_mean = array_y.mean().data
    x_mean = array_x.mean().data

    a = 0.
    b = 0.
    c = 0.

    for i in range(0,len(array_y.data)):
        a = a+((array_x[i]-x_mean)*(array_y[i]-y_mean))
        b = b+(array_x[i]-pow(x_mean, 2))
        c = c+(array_y[i]-pow(y_mean, 2))

    corr_coef = a/(math.sqrt(b*c))
    test = corr_coef*math.sqrt((len(array_y.data)-2)/(1-pow(corr_coef, 2)))

    # Save the result if file path is given
    if path:
        with open(path, "w") as text_file:
            print("Correlation coefficient: {}".format(corr_coef.values), file=text_file)
            print("Test value: {}".format(test.values), file=text_file)

    return {'correlation_coefficient':corr_coef, 'test_value':test}
