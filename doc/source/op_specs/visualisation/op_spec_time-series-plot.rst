================
Time Series Plot
================


Operation
=========
*Define the Operation and point to the applicable algorithm for implementation of this Operation, by following this convention:*

--------------------------

:Operation name: Time Series Plot
:Algorithm name: *XXX*
:Algorithm reference: `Wikpedia entry on time series (visualization) <https://en.wikipedia.org/wiki/Time_series#Visualization>`_ 
:Description: This operation produces and displays one or multiple time series plots based on point data or the spatial mean of areal data.
:Applicable use cases: :doc:`UC9 <../use_cases/UC09>`

--------------------------

Options
=======

*Describe options regarding the use of the Operation.*

--------------------------

:name: plot anomalies
:description: plots anomalies instead of absolute values
:settings: reference period (or region) for anomaly calculation

---------------------------------

:name: multiple datasets
:description: plots multiple time series on the same axes

---------------------------------

:name: multiple datasets, lagged
:description: plots multiple time series on the same axes in a lagged manner

---------------------------------

:name: plot settings
:description: settings for the plot 
:settings: legend, colours, symbols

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

:name: time series plot
:type: plot
:description: displays a time series plot (see Options_)

---------------------------------


Parameters
==========
*Define applicable parameters here. A parameter differs from an input in that it has a default value. Parameters are often used to control certain aspects of the algorithm behavior.*

--------------------------

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
========================

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

