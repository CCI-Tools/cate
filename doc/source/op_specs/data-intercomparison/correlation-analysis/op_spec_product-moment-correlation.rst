====================================
Product-Moment Correlation (Pearson)
====================================


Operation
=========
.. *Define the Operation and point to the applicable algorithm for implementation of this Operation, by following this convention:*

--------------------------

:Operation name: Product-Moment Correlation (Pearson) 
.. :Algorithm name: *XXX*
:Algorithm reference: `Wikipedia entry on Pearson product-moment correlation coefficient <https://en.wikipedia.org/wiki/Pearson_product-moment_correlation_coefficient>`_
:Description: This Operation performs a correlation analysis for metrically scaled data (assumption: normal distribution).
:Utilised in: :doc:`../../uc_workflows/uc09_workflow`

--------------------------

Options
=======

.. *Describe options regarding the use of the Operation.*

-----------------------------------------------------------

:name: temporal correlation
:description: performs a correlation analysis regarding temporally variable values
:items: one grid cell, cell-by-cell, spatial mean

-------------------------------------

:name: spatial correlation
:description: performs a correlation analysis regarding spatially variable values
:items: one point in time, time-by-time, temporal mean

-----------------------------------

:name: scatter-plot
:description: displays a scatter-plot showing corresponding variable values (not for time-by-time and pixel-by-pixel analysis)

---------------------------------

:name: time series plot
:description: plots results for spatial time-by-time correlation

---------------------------------

:name: map
:description: produces and displays a map showing cell-by-cell correlations

---------------------------------

:name: table
:description: produces a table  listing pixel-by-pixel respectively time-by-time correlation coefficients

---------------------------------

:name: t test
:description: performs a t test to assess the significance level of the results

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

:name: variable1
:type: floating point number
:range: [-infinity; +infinity]
:dimensionality: cube or 4D
:description: values of a certain geophysical quantity

-----------------------------

:name: variable2
:type: floating point number
:range: [-infinity; +infinity]
:dimensionality: cube or 4D
:description: values of a certain geophysical quantity

-----------------------------

:name: time (time, t)
:type: integer or double
:range: [0; +infinity]
:dimensionality: vector
:description: days/months since ...

-----------------------------


Output data
===========

.. *Description of anticipated output data.*

--------------------------

:name: product-moment correlation coefficient (Pearson)
:type: floating point number
:range: [-1.; +1.]
:dimensionality: scalar
:description: for correlation analysis for metrically scaled data

---------------------------------

:name: signficance
:type: boolean
:range: {0,1}
:dimensionality:  scalar
:description: significant or non-significant


*alternatively*


:name: level of signficance
:type: floating point number
:range: [0; +infinity]
:dimensionality: scalar
:description: significance level of correlation

---------------------------------

:name: scatter plot
:description: displays a plot (see Options_)

---------------------------------

:name: time series plot
:description: displays a time series plot (see Options_)

---------------------------------

:name: map
:description: displays a map (see Options_)

---------------------------------

:name: table
:description: displays a table (see Options_)

---------------------------------


Parameters
==========

.. *Define applicable parameters here. A parameter differs from an input in that it has a default value. Parameters are often used to control certain aspects of the algorithm behavior.*

--------------------------

:name: level of significance
:type: floating point number
:valid values: [0; 1]
:default value: 0.95
:description: level of significance for t test, determines t value to be compared with test value

--------------------------

*for plot settings, the procedure is forwarded to the Visualisation Operation*

-----------------------------

.. Computational complexity
.. ========================

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

  # Fortran subroutine for product moment correlation analysis (includes mean value function)

  c-----subroutine "correlation"
  c.....calculation of 
  c.....a) product-moment corellation coefficient "cc" between x(t) and y(t), t=[1,nt]
  c.....b) test-value "test" for t-test
        subroutine s_correlation(nt,x,y,cc,test) !Zeit   
        implicit none   
        integer nt,t
        real x(nt),dummy,dummy2,dummy3,y(nt),cc,test,f_mw
  
        dummy=0.
        dummy2=0.
        dummy3=0.
        do t=1,nt
          dummy=dummy+((x(t)-f_mw(n,x))*(y(t)-f_mw(n,y)))
          dummy2=dummy2+((x(t)-f_mw(n,x))**2)
          dummy3=dummy3+((y(t)-f_mw(n,y))**2)
        enddo !ja
        cc=(dummy)/sqrt(dummy2*dummy3)
        test=cc*sqrt((n-2)/(1-(cc**2)))
      
        return
        end

  c-----function "mean value"
  c.....calculation of mean value f_mw(nt,x) of vairable x with a sample size nt 
        real function f_mw(nt,x)
        implicit none
        integer nt,t
        real x(nt)

         f_mw=0.
        do t=1,nt
          f_mw=f_mw+x(t)
        enddo
        f_mw=f_mw/float(nt)

        return
        end

::