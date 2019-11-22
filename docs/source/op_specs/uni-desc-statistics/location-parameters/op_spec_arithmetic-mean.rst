==============
Aritmetic Mean
==============

Operation
=========

.. *Define the Operation and point to the applicable algorithm for implementation of this Operation, by following this convention:*

--------------------------

:Operation name: Arithmetic Mean
:Algorithm name: *XXX*
:Algorithm reference: *XXX*
:Description: This operation serves for the calculation of arithmetic means.
:Utilised in: :doc:`../../uc_workflows/uc09_workflow`,  :doc:`../../uc_workflows/uc06_workflow`

--------------------------

Options
=======

.. *Describe options regarding the use of the Operation.*

--------------------------

:name: temporal
:description: for the calculation of temporal means

--------------------------

:name: spatial 
:description: for the calculation of spatial means

--------------------------

:name: spatio-temporal 
:description: for the calculation of spatiotemporal means

--------------------------

:name: weighting 
:description: for the calculation of weighted means
:settings: weighting factors

--------------------------

Input data
==========

.. *Describe all input data (except for parameters) here, following this convention:*

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

-------------------------------------------------------

:name: variable(s)
:type: floating point number
:range: [-infinity; +infinity]
:dimensionality: cube or 4D
:description: values of (a) certain variable(s)

-----------------------------

:name: time (steps)
:type: integer or double
:range: [0; +infinity]
:dimensionality: vector
:description: days/months since ...

-----------------------------

:name: weighting factors
:type: floating point
:range: [0; +infinity]
:dimensionality: vector or array 
:description: weighting factors, same dimensions as input data 

--------------------------


Output data
===========

.. *Description of anticipated output data.*

---------------------------------

:name: arithmetic mean
:type: floating point
:description: arithmetic mean of the input data (details see Options_)

---------------------------------


.. Parameters
.. ==========

.. *Define applicable parameters here. A parameter differs from an input in that it has a default value. Parameters are often used to control certain aspects of the algorithm behavior.*

.. --------------------------


.. Computational complexity
.. ==============================

.. *Describe how the algorithm memory requirement and processing time scale with input size. Most algorithms should be linear or in n*log(n) time, where n is the number of elements of the input.*

.. --------------------------

.. :time: *Time complexity*
.. :memory: *Memory complexity*

.. --------------------------

.. Convergence
.. ===========

.. *If the algorithm is iterative, define the criteria for the algorithm to stop processing and return a value. Describe the behavior of the algorithm if the convergence criteria are never reached.*

.. Known error conditions
.. ======================

.. *If there are combinations of input data that can lead to the algorithm failing, describe here what they are and how the algorithm should respond to this. For example, by logging a message*

.. Example
.. =======

.. *If there is a code example (Matlab, Python, etc) available, provide it here.*

.. ::

..     for a in [5,4,3,2,1]:   # this is program code, shown as-is
..         print a
..     print "it's..."
..     # a literal block continues until the indentation ends
