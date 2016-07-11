==================
Temporal Filtering
==================

Operation
=========
.. *Define the Operation and point to the applicable algorithm for implementation of this Operation, by following this convention:*

--------------------------

:Operation name: Temporal Filtering
:Algorithm name: *XXX*
:Algorithm reference: *XXX* 
:Description: This Operation allows the selection of data within a time range. Only the data that falls within the selected time period is kept, if any.
:Utilised in: :doc:`../uc_workflows/uc09_workflow`

--------------------------

Options
========================

.. *Describe options regarding the use of the Operation.*

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
:dimensionality: cube or 4D
:description: values of a certain variable for the chosen time period 

--------------------------

Parameters
==========

.. *Define applicable parameters here. A parameter differs from an input in that it has a default value. Parameters are often used to control certain aspects of the algorithm behavior.*

--------------------------

:name: start date, t1
:type: *double?*
:valid values: [1; +infinity]
:default value: first time step defined by input data 
:description: first step of time period to be employed

--------------------------

:name: end date, t2
:type: *double?*
:valid values: [1; +infinity]
:default value: last time step defined by input data 
:description: last step of time period to be employed

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

  #     Fortran example code for temporal filtering/sub-setting
  c     Temporal Filtering
  c-----e.g. time as days since 1800-01-01 -> time(1)=1800-01-01 (t integer, not double)
        t1=1999-01-01
        t2=2015-12-31
  
        data_new=0.
        
        do t=1,nt
          do y=1,ny
            do x=1,nx
              if(time(t).ge.t1.and.time(t).le.t2)then
                data_new(x,y,t)=data_old(x,y,t)
              endif
            enddo !x
          enddo !y
        enddo !t
  c-----------------------------------------------------------
