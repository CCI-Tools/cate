=================
Long-term average
=================

Operation
=========

.. *Define the Operation and point to the applicable algorithm for implementation of this Operation, by following this convention:*

--------------------------

:Operation name: Long-term average
.. :Algorithm name: *XXX*
.. :Algorithm reference: *XXX*
:Description: This Operation serves for the calculation of long-term averages as reference.
:Utilised in: :doc:`../../uc_workflows/uc06_workflow`

--------------------------

Options
=======

.. *Describe options regarding the use of the Operation.*

--------------------------

:name: preserve saisonality 
:description: calculate long-term mean for every timestep inside a year (month, day, ...)
:settings: reference period

--------------------------

:name: one value
:description: calculate one long-term mean without preserving saisonality
:settings: reference period

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


Output data
===========

.. *Description of anticipated output data.*


---------------------------------

:name: long-term average
:type: floating point number
:dimensionality: one value or vector
:description: input data transformed to long-term average

---------------------------------


Parameters
==========

.. *Define applicable parameters here. A parameter differs from an input in that it has a default value. Parameters are often used to control certain aspects of the algorithm behavior.*

--------------------------

:name: time1, tim1 
:type: integer or double
:valid values: [0; +infinity]
:default value: start point of input period
:description: starting point of reference period

--------------------------

:name: time2, tim2 
:type: integer or double
:valid values: [0; +infinity]
:default value: terminal point of input period
:description: terminal point of reference period

--------------------------


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

Example
=======

.. *If there is a code example (Matlab, Python, etc) available, provide it here.*

.. code-block:: fortranfixed

	# ny number of years
	# variable: var(year, month)

	##############################
	# with saisonality
	do month=1,12
		longtermmean(month)=mean(var(year, month), year=1,ny)
	enddo

	#anomaly
	var(year, month)=var(year,month)-longtermmean(month)

	##############################
	# without seasonality 

	longtermmean=mean(var)

	#anomaly
	var(year, month)=var(year,month)-longtermmean
