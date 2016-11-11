============
EOF Analysis
============

Operation
=========

.. *Define the Operation and point to the applicable algorithm for implementation of this Operation, by following this convention:*

--------------------------

:Operation name: EOF Analysis
.. :Algorithm name: *XXX*
:Algorithm reference: `wikipedia entry on Principal Component Analysis <https://en.wikipedia.org/wiki/Principal_component_analysis>`, `Blog entry on step by step PCA implementation in Python <http://sebastianraschka.com/Articles/2014_pca_step_by_step.html>`, 
:Description: This Operations serves for the application of Empricial Orthogonal Function (EOF) Analysis, also known as Principal Component Analysis (PCA), for data analysis regarding spatial patterns/modes. EOF Analysis implies the removal of redundancy. 
:Utilised in: :doc:`../uc_workflows/uc06_workflow` 

--------------------------

Options
========================

.. *Describe options regarding the use of the Operation.*

--------------------------

:name: rotated
:description: decide if EOF analysis should be rotatated
:settings: no rotation, varimax, ...

--------------------------

:name: matrix
:description: decide to use correlation or covariance matrix
:settings: correlation matrix or covariance matrix

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

:name: principal components (PCs)
:type: floating point number
:range: [-infinity.; +infinity]
:dimensionality: vector
:description: temporal evolution of variance belonging to spatial pattern, number of

---------------------------------

:name: empirical orthogonal functions (EOFs)
:type: floating point number
:range: [-infinity.; +infinity]
:dimensionality: array
:description: also named eigenvectors; tendency and strength of dominant spatial pattern of variance. All eigenvectors are orthogonal to one another. 

---------------------------------

:name: eigenvalues
:type: floating point number
:range: [0; 1] for correlation matrix, [0; +infinity] for covariance matrix
:dimensionality: scalar
:description: ith eigenvalue constitutes measure for the portion of variance explained by the ith PC/EOF

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

.. Example
.. =======

.. *If there is a code example (Matlab, Python, etc) available, provide it here.*

.. ::

..     for a in [5,4,3,2,1]:   # this is program code, shown as-is
..         print a
..     print "it's..."
..     # a literal block continues until the indentation ends
