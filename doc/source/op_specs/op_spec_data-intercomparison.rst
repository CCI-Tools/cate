Processor
=========
*Define the processor and point to the applicable algorithm for implementation of this processor, by following this convention:*

--------------------------

:Processor name: Data Intercomparison
:Algorithm name: *XXX*
:Algorithm reference: *XXX*
:Description: This processor serves for bivariate statistical calculations. Two timeseries (point data or spatial mean of areal data) as well as two sets of areal data (one point in time or temporal mean of several time steps) can be analyzed.
:Applicable use cases: UC9

--------------------------

Options
========================

*Describe options regarding the use of the processor.*

-----------------------------------------------------------

:name: selection of timeseries or areal data
:description: If two timeseries are compared, point data or the spatial mean of areal data is employed. For two sets of areal data one point in time or the temporal mean of several time steps is analyzed.

---------------------------------

:name: selection of pixel-by-pixel time series analysis 
:description: for two timeseries are compared, data from all points is analyzed separately; a map displaying the results per pixel is created.

---------------------------------

:name: selection of time step-by-time step areal data analysis 
:description: for two sets of areal data each point in time is analzed separately; a time series plot showing the results is created.

---------------------------------

:name: contingency table
:description: creation of a contingency table showing the frequency distribution of variable1 and variable2, derivation of related quantitites
:items: table, absolute marginal probabilities, relative marginal probabilities, conditional relative probabilities, test on independency

---------------------------------

:name: location parameters
:description: calculation of one single value for data description
:items: arithmetic mean center, weighted arithmetic mean center, median center 

---------------------------------

:name: dispersion parameters
:description: calculation of a measure for the dispersion of the data inside the sample 
:items: standard distance

---------------------------------

:name: correlation analysis 
:description: calculation of measures for the strength and direction of the connection between variable1 and variable2. 
:items: standardized contingency coefficient, rank correlation coefficient (Spearman), product-moment correlation coefficient (Pearson), ...

---------------------------------

:name: regression analysis 
:description: calculation of coefficients of a function which serves as a approximation of the connection between variable1 and variable2; one of both variables is assumed to be regressor resp. predictor (independent variable), the other as regressand (dependent variable)
:items: linear regression, determination coefficient, non-linear regression

---------------------------------

:name: plot
:description: plots results
:items: scatter plot (for correlation analysis), scatter plot with regression function (for regression analysis), time series plot of time step-by-time step results (correlation)

---------------------------------

:name: table
:description: displays a table
:items: contingency table, ...

---------------------------------

:name: map
:description: mapping of results
:items: map of pixel-by-pixel results (correlation)

---------------------------------


Input data
==========
*Describe all input data (except for parameters) here, following this convention:*

--------------------------

:name: longitude (lon, x)
:type: floating point number
:range: [-180.; +180.] respectively [0.; 360.]
:dimensionality: vector
:description: grid information on longitudes

--------------------------

:name: latitude (lat, y)
:type: floating point number
:range: [-90.; +90.]
:dimensionality: vector
:description: grid information on latitudes

--------------------------

:name: height (z)
:type: floating point number
:range: [-infinity; +infinity]
:dimensionality: vector
:description: grid information on height/depth

-----------------------------

:name: variable1
:type: floating point number
:range: [-infinity; +infinity]
:dimensionality: cube or 4D
:description: values of a certain variable

-----------------------------

:name: variable2
:type: floating point number
:range: [-infinity; +infinity]
:dimensionality: cube or 4D
:description: values of a certain variable

-----------------------------

:name: time (steps)
:type: double?
:range: [0; +infinity]
:dimensionality: vector
:description: days/months since ...

-----------------------------


Output data
===========
*Description of anticipated output data.*

--------------------------

:name: contingency table
:type: integer
:range: [0; +infinity]
:dimensionality: matrix 
:description: contingency table between variable1 and variable2 showing their frequency distribution

---------------------------------

:name: absolute marginal probabilities
:type: integer
:range: [0; +infinity]
:dimensionality: vector (one for columns, on for rows)
:description: sum of individual probabilities in one row or column, all marginal totals sum up to n

---------------------------------

:name: relative marginal probabilities
:type: floating point number
:range: [0.; +1.]
:dimensionality: vector (one for columns, on for rows)
:description: sum of individual relative probabilities in on row or columnm all relative marginal probabilities sum up to 1

---------------------------------

:name: conditional relative probabilities
:type: floating point number
:range: [0.; +1.]
:dimensionality: scalar (for one scenario)
:description: probability of variable1 or variable2 attaining a certain value under the condition of variable2 or variable 1 attaining a certain value

---------------------------------

:name: independency
:type: boole
:range: {0, 1}
:dimensionality: scalar (for one scenario)
:description: a boolean value indicating if two feature characteristics are dependent

---------------------------------

:name: arithmetic mean center
:type: floating point number
:range: [-infinity; +infinity]
:dimensionality: scalar
:description: *barycenter* of a bivariate distribution (arithmetic mean of variable1 and variable2)

---------------------------------

:name: weighted arithmetic mean center
:type: floating point number
:range: [-infinity; +infinity]
:dimensionality: scalar
:description: like arithmetic mean center, but weighted 

---------------------------------

:name: median center
:type: floating point number
:range: [-infinity; +infinity]
:dimensionality: scalar
:description: iteratively approximated median center of a bivariate distribution from medians of variable1 and variable2

---------------------------------

:name: standard distance
:type: floating point number
:range: [-infinity; +infinity]
:dimensionality: scalar
:description: measure of spatial point distribution and relative location of points (equivalent to univariate standard deviation)

---------------------------------

:name: standardized contingency coefficient 
:type: floating point number
:range: [0.; +1.]
:dimensionality: scalar
:description: for correlation analysis of nominally scaled data

---------------------------------

:name: rank correlation coefficient (Spearman)
:type: floating point number
:range: [-1.; +1.]
:dimensionality: scalar
:description: for correlation analysis of ordinally scaled data

---------------------------------

:name: product-moment correlation coefficient (Pearson)
:type: floating point number
:range: [-1.; +1.]
:dimensionality: scalar
:description: for correlation analysis for metrically scaled data

---------------------------------

:name: regression coefficient (slope) of linear regression
:type: floating point number
:range: [-infinity; +infinity]
:dimensionality: scalar
:description: slope of a linear function describes best the connection between variable1 and variable2

---------------------------------

:name: regression constant (y-intercept) of linear regression
:type: floating point number
:range: [-infinity; +infinity]
:dimensionality: scalar
:description: regression constant (y-intercept) of a linear function which describes the connection between variable1 and variable2

---------------------------------

:name: determination coefficient
:type: floating point number
:range: [0.; +1.]
:dimensionality: scalar
:description: evaluation of regression 

---------------------------------

:name: constants and coefficients of nonlinear regression 
:type: floating point number
:range: [-infinity; +infinity]
:dimensionality: vector 
:description: coefficients of a non-linear function which describes the connection between varaible1 and variable2

---------------------------------

:name: plot
:description: displays a plot (see Options_)

---------------------------------

:name: table
:description: displays a table (see Options_)

---------------------------------


Parameters
==========
*Define applicable parameters here. A parameter differs from an input in that it has a default value. Parameters are often used to control certain aspects of the algorithm behavior.*

--------------------------

:name: date
:type: *double?*
:valid values: [1; +infinity]
:default value: - 
:description: for comparisons of areal datasets one point in time (or a temporal mean value) is used

--------------------------

:name: start date
:type: *double?*
:valid values: [1; +infinity]
:default value: first time step defined by input data 
:description: first step of time period to be employed

--------------------------

:name: end date
:type: *double?*
:valid values: [1; +infinity]
:default value: last time step defined by input data 
:description: last step of time period to be employed

--------------------------

:name: lon, x (longitudinal position)
:type: floating point number
:valid values: [-180.; +180.] resp. [0.; 360.]
:default value: -
:description: longitudinal coordinate of point of interest for comparisons of timeseries

--------------------------

:name: lat, y (latitudinal position)
:type: floating point number
:valid values: [-90.; +90.]
:default value: -
:description: latitudinal coordinate of point of interest for comparisons of timeseries

---------------------------------

:name: lon1, x1 (longitudinal position)
:type: floating point number
:valid values: [-180.; +180.] respectively [0.; 360.]
:default value: minimum longitude of input data
:description: longitudinal coordinate limiting rectangular area of interest

--------------------------

:name: lon2, x2 (longitudinal position)
:type: floating point number
:valid values: [-180.; +180.] resp. [0.; 360.]
:default value: maximum longitude of input data 
:description: longitudinal coordinate limiting rectangular area of interest

--------------------------

:name: lat1, y1 (latitudinal position)
:type: floating point number
:valid values: [-90.; +90.]
:default value: minimum latitude of input data 
:description: latitudinal coordinate limiting rectangular area of interest

--------------------------

:name: lat2, y2 (latitudinal position)
:type: floating point number
:valid values: [-90.; +90.]
:default value: maximum latitude of input data 
:description: latitudinal coordinate limiting rectangular area of interest

-----------------------------

*more coordinates necessary for non-rectangular areas and 3D data*

-----------------------------

:name: weight1
:type: floating point number
:valid values: [-infinity; +infinity]
:default value: -
:description: weight of variable1 for weighted arithmetic mean center

-----------------------------

:name: weight2
:type: floating point number
:valid values: [-infinity; +infinity]
:default value: -
:description: weight of variable2 for weighted arithmetic mean center

-----------------------------

*more coordinates necessary for non-rectangular areas and 3D data*

-----------------------------

*for plot settings, the procedure is forwarded to the Visualisation processor*

-----------------------------

Computational complexity
==============================

*Describe how the algorithm memory requirement and processing time scale with input size. Most algorithms should be linear or in n*log(n) time, where n is the number of elements of the input.*

--------------------------

:time: *Time complexity*
:memory: *Memory complexity*

--------------------------

Convergence
===========
*If the algorithm is iterative, define the criteria for the algorithm to stop processing and return a value. Describe the behavior of the algorithm if the convergence criteria are never reached.*

Known error conditions
======================
*If there are combinations of input data that can lead to the algorithm failing, describe here what they are and how the algorithm should respond to this. For example, by logging a message*

Example
=======
*If there is a code example (Matlab, Python, etc) available, provide it here.*

::

  for a in [5,4,3,2,1]:  # this is program code, shown as-is
    print a
  print "it's..."
  # a literal block continues until the indentation ends
