Operation
=========
*Define the Operation and point to the applicable algorithm for implementation of this Operation, by following this convention:*

--------------------------

:Operation name: Product-Moment Correlation (Pearson) 
:Algorithm name: *XXX*
:Algorithm reference: *XXX*
:Description: This Operation performs a correlation analysis for metrically scaled data. 
:Applicable use cases: :doc:`UC9 <../../use_cases/UC09>`
--------------------------

Options
========================

*Describe options regarding the use of the Operation.*

-----------------------------------------------------------

:name: temporal correlation
:description: performs a correlation analysis regarding temporally variable values
:items: one location, pixel-by-pixel, spatial mean

-------------------------------------

:name: spatial correlation
:description: performs a correlation analysis regarding spatially variable values
:items: one point in time, time-by-time, temporal mean

-----------------------------------

:name: scatter-plot
:description: displays a scatter-plot showing corresponding variable values (not for time-by-time and pixel-by-pixel analysis)

---------------------------------

:name: time series plot
:description: plots results for spatial time-by-time correlation

---------------------------------

:name: map
:description: produces and displays a map showing pixel-by-pixel correlations

---------------------------------

:name: table
:description: produces a table  listing pixel-by-pixel respectively time-by-time correlation coefficients

---------------------------------

:name: t test
:description: performs a t test to assess the significance level of the results

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

:name: product-moment correlation coefficient (Pearson)
:type: floating point number
:range: [-1.; +1.]
:dimensionality: scalar
:description: for correlation analysis for metrically scaled data

---------------------------------

:name: signficance level
:type: floating point number
:range: [0; +infinity]
:dimensionality: vector 
:description: significance level of correlation

---------------------------------

:name: scatter plot
:description: displays a plot (see Options_)

---------------------------------

:name: time series plot
:description: displays a time series plot (see Options_)

---------------------------------

:name: map
:description: displays a map (see Options_)

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

*for plot settings, the procedure is forwarded to the Visualisation Operation*

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
