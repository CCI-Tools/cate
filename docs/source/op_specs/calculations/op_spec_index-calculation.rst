=================
Index Calculation
=================

Operation
=========

.. *Define the Operation and point to the applicable algorithm for implementation of this Operation, by following this convention:*

--------------------------

:Operation name: Index Calculation
.. :Algorithm name: *XXX*
.. :Algorithm reference: *XXX*
:Description: This Operation serves for calculation of (pre-defined) indices involving spatial and temporal averaging, anomalies, standardization, filtering etc. 
:Utilised in: :doc:`../uc_workflows/uc06_workflow` 

--------------------------

Options
=======

.. *Describe options regarding the use of the Operation.*

--------------------------

:name: Niño3.4 index
:description: five month running mean of anomalies of monthly means of SST in Niño3.4 region (120°W-170°W, 5°S- 5°N), see `UCAR webpage on El Niño indices <http://www.cgd.ucar.edu/cas/catalog/climind/Nino_3_3.4_indices.html>`_
:settings: time series calculation, Boolean El Niño (threshold +0.4 deg C), Boolean La Niña (threshold -0.4 deg C)

--------------------------

.. :name: 
.. :description:
.. :settings: 

.. --------------------------

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


Output data
===========

.. *Description of anticipated output data.*


---------------------------------

:name: index timeseries (values or Boolean)
:type: floating point or Boolean
:description: timeseries of values or Boolean results of index calculation

---------------------------------


.. Parameters
.. ==========

.. *Define applicable parameters here. A parameter differs from an input in that it has a default value. Parameters are often used to control certain aspects of the algorithm behavior.*

.. --------------------------

.. :name: lon1, x1 (longitudinal position)
.. :type: floating point number
.. :valid values: [-180.; +180.] respectively [0.; 360.]
.. :default value: minimum longitude of input data
.. :description: longitudinal coordinate limiting rectangular area of interest

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
