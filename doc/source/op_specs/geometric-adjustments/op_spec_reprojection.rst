============
Reprojection
============


Operation
=========
*Define the Operation and point to the applicable algorithm for implementation of this Operation, by following this convention:*

--------------------------

:Operation name: Reprojection
:Algorithm name: *XXX*
:Algorithm reference: *XXX*
:Description: This Operation converts spatial data from one coordinate system to another.
:Applicable use cases: :doc:`UC9 <../use_cases/UC09>`

--------------------------

Options
=======

*Describe options regarding the use of the Operation.*

--------------------------

:name: coordinate system
:description: requested coordinate system
:items: UTM, geographic, regular lat-lon, polar centred, sinusoidal, tripolar, ...

--------------------------

:name: ellipsoid
:description: requested ellipsoid
:items: WGS84, GRS80, Bessel, Clarke

---------------------------------

:name: reprojection method
:description: method for reprojecting the data
:items: nearest neighbor (primarily for thematic maps), bilinear, cubic convolution 

---------------------------------

:name: propagation of uncertainties
:description: analysis of propagation of uncertainties during geometric adjustment

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

:name: variable
:type: floating point number
:range: [-infinity; +infinity]
:dimensionality: cube or 4D
:description: values of a certain variable

-----------------------------


Output data
===========
*Description of anticipated output data.*

--------------------------

:name: adjusted longitude (lon', x')
:type: floating point number
:range: [-180.; +180.] respectively [0.; 360.]
:dimensionality: vector
:description: new grid information on longitudes

--------------------------

:name: adjusted latitude (lat', y')
:type: floating point number
:range: [-90.; +90.]
:dimensionality: vector
:description: new grid information on latitudes

--------------------------

:name: adjusted height (z')
:type: floating point number
:range: [-infinity; +infinity]
:dimensionality: vector
:description: new grid information on height/depth

-----------------------------

:name: adjusted variable
:type: floating point number
:range: [-infinity; +infinity]
:dimensionality: cube or 4D
:description: new values of a certain variable

-----------------------------

Parameters
==========

*Define applicable parameters here. A parameter differs from an input in that it has a default value. Parameters are often used to control certain aspects of the algorithm behavior.*

--------------------------

:name: nx
:type: integer
:valid values: [1; infinity]
:default value: number of longitudes in dataset
:description: original number of longitudes

--------------------------

:name: ny
:type: integer
:valid values: [1; infinity]
:default value: number of latitudes in dataset
:description: original number of latitudes

--------------------------

:name: nz
:type: integer
:valid values: [1; infinity]
:default value: number of altitude levels in dataset
:description: original number of altitude levels

--------------------------

:name: nx'
:type: integer
:valid values: [1; infinity]
:default value: -
:description: adjusted number of longitudes

--------------------------

:name: ny'
:type: integer
:valid values: [1; infinity]
:default value: -
:description: adjusted number of latitudes

--------------------------

:name: nz'
:type: integer
:valid values: [1; infinity]
:default value: -
:description: adjusted number of altitude levels 

--------------------------

:name: size of sliding window
:type: integer
:valid values: [1; infinity], odd numbers 
:default value: 3
:description: side length of the sliding window used for interpolation (e.g. 3x3, 9x9)

--------------------------

:name: original coordinate system
:description: definition of original coordiate system

--------------------------

:name: adjusted coordinate system
:description: definition of requested coordiate system

--------------------------



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

