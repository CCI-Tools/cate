==================
Spatial Subsetting
==================

Operation
=========
.. *Define the Operation and point to the applicable algorithm for implementation of this Operation, by following this convention:*

--------------------------

:Operation name: Spatial Subsetting
.. :Algorithm name: *XXX*
.. :Algorithm reference: *XXX* 
:Description: This Operation provides the functionality to select data of a region of interest. All data outside will be discarded.
:Utilised in: :doc:`../uc_workflows/uc09_workflow`

--------------------------

Options
========================

.. *Describe options regarding the use of the Operation.*

--------------------------

:name: polygon subsetting
:description: spatial subsetting of data inside a polygon

--------------------------

:name: polygon subsetting by selection from a list of main regions
:description: analysis of propagation of uncertainties during geometric adjustment

---------------------------------

:name: point subsetting
:description: selection of a spatial point to retrieve all temporal in formation of that point

---------------------------------

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

-----------------------------

:name: time (time, t)
:type: integer or double
:range: [0; +infinity]
:dimensionality: vector
:description: days/months since ...

-----------------------------

:name: variable
:type: floating point number
:range: [-infinity; +infinity]
:dimensionality: cube or 4D
:description: values of a certain variable

-----------------------------


Output data
===========
.. *Description of anticipated output data.*

--------------------------

:name: subset of variable 
:type: floating point number
:range: [-infinity; +infinity]
:dimensionality: vector or cube or 4D
:description: values of a certain variable for the chosen area of interest 

--------------------------

Parameters
==========

.. *Define applicable parameters here. A parameter differs from an input in that it has a default value. Parameters are often used to control certain aspects of the algorithm behavior.*

--------------------------

:name: lon, x (longitudinal position)
:type: floating point number
:valid values: [-180.; +180.] resp. [0.; 360.]
.. :default value: -
:description: longitudinal coordinate of point of interest

--------------------------

:name: lat, y (latitudinal position)
:type: floating point number
:valid values: [-90.; +90.]
.. :default value: -
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

  #     Fortran example code for spatial subsetting/sub-setting
  c     Spatial Subsetting
  c-----Example region: n3.4
        x1=190.
        x2=240.
        y1=-5.
        y2=5.

        data_new=0.
      
        do t=1,nt
          do y=1,ny
            do x=1,nx
              if(lat(y).lt.y1.or.lat(y).gt.y2)then
                continue
              elseif(lon(x).lt.x1.or.lon(x).gt.x2)then
                continue
              else
                data_new(x,y,t)=data_old(x,y,t)
              endif
            enddo !x
          enddo !y
        enddo !t
  c-----------------------------------------------------------
