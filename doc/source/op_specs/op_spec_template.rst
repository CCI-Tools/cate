
Processor
=========
Define the processor and point to the applicable algorithm for implementation of this processor, by following this convention:

--------------------------

:Processor name: Some name
:Algorithm name: Some name
:Algorithm reference: an URL
:Description: Free text description
:Applicable use cases: Which use cases from URD this processor is used for.

--------------------------

Input data
==========
Describe all input data (except for parameters) here, following this convention:

--------------------------

:name: Name
:type: integer,floating point number, etc
:range: Valid values, often described by min/max
:dimensionality: Expected number of dimensions (scalar, vector, matrix, cube, 4D, etc)
:description: Free text description of the input data

-----------------------------

:name: Name
:type: integer,floating point number, etc
:range: Valid values, often described by min/max
:dimensionality: Expected number of dimensions (scalar, vector, matrix, cube, 4D, etc)
:description: Free text description of the input data

--------------------------

Output data
===========
Description of anticipated output data.

--------------------------

:name: Name
:type: integer,floating point number, etc
:range: Valid values, often described by min/max
:dimensionality: Expected number of dimensions (scalar, vector, matrix, cube, 4D, etc)
:description: Free text description of the output data

--------------------------

Parameters
==========
Define applicable parameters here. A parameter differs from an input in that it has a default value. Parameters are often used to control certain aspects of the algorithm behavior.

--------------------------

:name:
:valid values:
:default value:
:description:

--------------------------

:name:
:valid values:
:default value:
:description:

--------------------------

Computational complexity
========================
Describe how the algorithm memory requirement and processing time scale with input size. Most algorithms should be linear or in n*log(n) time, where n is the number of elements of the input.

--------------------------

:time: Time complexity
:memory: Memory complexity

--------------------------

Convergence
===========
If the algorithm is iterative, define the criteria for the algorithm to stop processing and return a value. Describe the behavior of the algorithm if the convergence criteria are never reached.

Known error conditions
======================
If there are combinations of input data that can lead to the algorithm failing, describe here what they are and how the algorithm should respond to this. For example, by logging a message

Example
=======
If there is a code example (Matlab, Python, etc) available, provide it here.

::

    for a in [5,4,3,2,1]:   # this is program code, shown as-is
        print a
    print "it's..."
    # a literal block continues until the indentation ends
