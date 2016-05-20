Processor
=========
*Define the processor and point to the applicable algorithm for implementation of this processor, by following this convention:*

--------------------------

:Processor name: Visualisation
:Algorithm name: *XXX*
:Algorithm reference: *XXX*
:Description: This processor serves the visualisation of data.
:Applicable use cases: UC9

--------------------------

Options
========================

*Describe options regarding the use of the processor.*

--------------------------

:name: plot anomalies
:description: plots anomalies insted of absolute values
:settings: spatial and temporal boundaries for reference value

---------------------------------

:name: table
:description: displays a table
:items: contingency table, ...

---------------------------------

:name: time series plot (one dataset)
:description: plots time series (point data or spatial mean of areal data) 
:settings: legend, annotations, colours, symbols

---------------------------------

:name: time series plot (multiple datasets)
:description: plots time series (point data or spatial mean of areal data) of multiple datasets on the same axes
:settings: legend, annotations, colours, symbols

---------------------------------

:name: histogram
:description: plots a histogram (point data or spatial mean of areal data)
:settings: classification, legend, annotations, colours, symbols 

---------------------------------

:name: chart
:description: plots a chart 
:settings: type of chart, classification, legend, annotations, colours, symbols 

---------------------------------

:name: Hovmoller diagram 
:description: plots a Hovmoller diagram (time-latitude, time-longitude, time-height)
:settings: legend, annotations, colours, dimensions (latitude, longitude, height)

---------------------------------

:name: Hovmoller diagram (multiple datasets)
:description: plots multiple Hovmoller diagrams side by side (time-latitude, time-longitude, time-height)
:settings: legend, annotations, colours, dimensions (latitude, longitude, height)

---------------------------------

:name: map
:description: plots a map (data of one time step or temporal mean) 
:settings: legend, land contours, north arrow, grid, ...

---------------------------------

:name: map (muultiple datasets)
:description: plots multiple maps (data of one time step or temporal mean) side by side 
:settings: legend, land contours, north arrow, grid, ...

---------------------------------

:name: animated map
:description: plots an animated map (data of different time steps) 
:settings: legend, land contours, north arrow, grid, ...

---------------------------------

:name: animated map (multiple datasets)
:description: plotds multiple animated maps (data of different time steps) side by side
:settings: legend, land contours, north arrow, grid, ...

---------------------------------

equals

---------------------------------

:name: animate time series (one dataset) 
:description: display animated time series.

---------------------------------

:name: animate time series (multiple datasets) 
:description:  display multiple animated time series side by side

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

:name: variable(s)
:type: floating point number
:range: [-infinity; +infinity]
:dimensionality: cube or 4D
:description: values of (a) certain variable(s)

-----------------------------

:name: time (steps)
:type: *double?*
:range: [0; +infinity]
:dimensionality: vector
:description: days/months since ...

-----------------------------


Output data
===========
*Description of anticipated output data.*

--------------------------------

:name: plot
:type: plot
:description: displays a plot (see Options_)

---------------------------------

:name: map
:type: map
:description: displays a map (see Options_)

---------------------------------


:name: table
:type: table
:description: displays a table (see Options_)

---------------------------------


Parameters
==========
*Define applicable parameters here. A parameter differs from an input in that it has a default value. Parameters are often used to control certain aspects of the algorithm behavior.*

--------------------------

:name: start date
:type: *double?*
:valid values: *[1; infinity]*
:default value: first time step defined by input data 
:description: first step of time period to be employed

--------------------------

:name: end date
:type: *double?*
:valid values: *[1; infinity]*
:default value: last time step defined by input data 
:description: last step of time period to be employed

--------------------------

:name: lon, x (longitudinal position)
:type: floating point number
:valid values: [-180.; +180.] resp. [0.; 360.]
:default value: -
:description: longitudinal coordinate of point of interest

--------------------------

:name: lat, y (latitudinal position)
:type: floating point number
:valid values: [-90.; +90.]
:default value: -
:description: latitudinal coordinate of point of interest

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

:name: x-axis annotation/label
:type: character
:valid values: all
:default value: probability, time, name of variable, ... (depends on type of plot)
:description: label for x-axis

-----------------------------

:name: y-axis annotation/label
:type: character
:valid values: all
:default value: name of variable (depends on type of plot)
:description: label for y-axis

-----------------------------

:name: heading annotation/label
:type: character
:valid values: all
:default value: name of variable (depends on type of plot)
:description: text for image heading

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

    for a in [5,4,3,2,1]:   # this is program code, shown as-is
        print a
    print "it's..."
    # a literal block continues until the indentation ends
