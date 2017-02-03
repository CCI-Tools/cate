==============
Detection of Outliers
==============

Operation
=========

.. *Define the Operation and point to the applicable algorithm for implementation of this Operation, by following this convention:*

--------------------------

:Operation name: Detection of Outliers
:Algorithm name: *XXX*
:Algorithm reference: *XXX*
:Description: This Operation enables the detection of outliers within a sample.
.. :Utilised in: :doc:`../uc_workflows/uc02_workflow`  .. uc02_workflow needs to be defined!

--------------------------

Options
========================

.. *Describe options regarding the use of the Operation.*

--------------------------

:name: percentile-approach
:description: identify valid values inside the range of two limiting percentiles
:settings: ** to be defined later **

--------------------------

:name: threshold-approach
:description: identify valid values within a given range determined by two threshold values
:settings: ** to be defined later **

--------------------------

Input data
==========

.. *Describe all input data (except for parameters) here, following this convention:*

.. --------------------------

.. :name: longitude (lon, x)
.. :type: floating point number
.. :range: [-180.; +180.] respectively [0.; 360.]
.. :dimensionality: vector
.. :description: grid information on longitudes

.. --------------------------

.. :name: latitude (lat, y)
.. :type: floating point number
.. :range: [-90.; +90.]
.. :dimensionality: vector
.. :description: grid information on latitudes

.. --------------------------

.. :name: height (z)
.. :type: floating point number
.. :range: [-infinity; +infinity]
.. :dimensionality: vector
.. :description: grid information on height/depth

-----------------------------

:name: time (steps)
:type: integer or double
:range: [0; +infinity]
:dimensionality: vector
:description: days/months since ...

-----------------------------

:name: variable(s)
:type: floating point number
:range: [-infinity; +infinity]
:dimensionality: vector
:description: values of (a) certain variable(s)

-----------------------------

Output data
===========

.. *Description of anticipated output data.*


---------------------------------

:name: cleaned sample
:type: floating point number
:range: [-infinity; +infinity]
:dimensionality: vector 
:description: clean input after outliers have been removed

---------------------------------


Parameters
==========

.. *Define applicable parameters here. A parameter differs from an input in that it has a default value. Parameters are often used to control certain aspects of the algorithm behavior.*

--------------------------

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

::

	'''The following program is an example for the 'Detection of Outliers'. The suggested method is a detection of outliers
	 based on percentiles or threshold-limitation.

	 Step 1:
	A random dataset with a length of 95 floats within the span of 15 and 25 is generated randomly. Five outlier values are
	added by hand.

	Step 2:
	Prompt:: Decide between the two approaches/methods.

	Step 3:
	Prompt:: Set limitations either a percentage [%] or a value embracing the distribution.

	Step 4:
	Prompt:: Flag or drop the outliers. If falgged: column_stack a new column with 0/1. '1' flags an outlier.

	Step 5:
	Implemt of an 'R-like' which()-statement.

	Step 6: Exclude or flag the values.

	Return-Object: 'new_sampl' based on the prior decisions.

	#Comment: This method of detecting outliers is just one of many! UC2 is a perfect example of a 'Detection o Outliers'
	via two threshold-values giving a rigid limition for the span of values allowed. When the data is assumed to be tempera-
	tures in Celius measured during the summer. I.e. the User could save drop/flag all values lower 15 and greater 25,
	since the temperature in the given period is considered to vary in that range.

	02.02.2017 Stephan Herzog
	'''

	#import modules
	import numpy as np

	## - TEST DATA - ##
	#Generate 95 random values within 15 and 25; pass it to 'vec1'
	sampl = np.random.uniform(low=15.0,high=25.0,size=95)
	sampl = np.append(sampl,[-3.141,42,1337,-273.15,21122012])
	np.random.shuffle(sampl)


	######BEGIN: VOR DEM PROMPT DIE ABFRAGE EINBAUEN OB PERCENTIL_METHODE ODER SCHWELLWERT!!!!
	logical_prompt = raw_input("Please decide between the methods for a detection of outliers: Press (1) for a percentile-"
							   "approach; Press (2) for a threshold-approach.")

	## - Calc. of percentiles - ##
	if (logical_prompt == '1') :
		prompt1lower = raw_input("Please enter the lower limit for the percentile: ")   ##Suggestion: 2.5
		prompt2upper = raw_input("Please enter the upper limit for the percentile: ")   ##Suggestion: 97.5

		p_lower = np.percentile(sampl, float(prompt1lower))     ##key aspect
		p_upper = np.percentile(sampl, float(prompt2upper))     ##key aspect

	## - Prompt for threshold - ##
	if (logical_prompt == '2') :
		p_lower = raw_input("Please enter the lower limit for the threshold: ")
		p_upper = raw_input("Please enter the upper limit for the threshold: ")

		p_lower = float(p_lower)
		p_upper = float(p_upper)

	## - Prompt for flag or drop - ##
	logical = raw_input("Should the outliers be flagged? (Y/N)")

	## - Identfiy values within limits - ##
	which = lambda lst:list(np.where(lst)[0])       ##key aspect

	lst = map(lambda x:(x<p_lower or x>p_upper), sampl)

	print(which(lst))
	## - Flag or Drop Outliers - ##
	if ( logical == 'Y') :
		flag = np.repeat(0,len(sampl))
		flag[which(lst)] = 1
		new_sampl = np.column_stack((sampl,flag))
		print(new_sampl.shape)
		print(new_sampl[which(lst),:])
	else:
		new_sampl = np.delete(sampl,which(lst))
		print(new_sampl.shape)

	## - Write to Output - ## e.g. .csv or other
