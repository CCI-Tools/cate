========================
Operation Specifications
========================


In this section, the non-trivial operations and data processors used in the ESA CCI Toolbox are specified.
The term *operation* used here first refers to CCI Toolbox functions that will become part of the API as usual Python
functions. Secondly and at  the same time it refers to instances of such functions that are stored along
with additional meta-data in the CCI Toolbox *operation registry* as described in :ref:`op`. The latter will be used to
allow invocation of functions from the CCI Toolbox' command-line interface (CLI) and desktop application (GUI).

The intended readership of this chapter are software end users wishing to understand the details of the algorithms
and methods used in the CCI Toolbox.

Filtering and Selections
========================

.. toctree::
   :maxdepth: 1
   :numbered:

   op_specs/filtering_selections/op_spec_category_filtering_selections
   op_specs/filtering_selections/op_spec_spatial-filtering
   op_specs/filtering_selections/op_spec_temporal-filtering

Visualisation
=============

.. toctree::
   :maxdepth: 1
   :numbered:

   op_specs/visualisation/op_spec_category_visualisation
   op_specs/visualisation/op_spec_time-series-plot
   op_specs/visualisation/op_spec_animated-map

Geometric Adjustments
=====================

.. toctree::
   :maxdepth: 1
   :numbered:

   op_specs/geometric-adjustments/op_spec_category_geometric-adjustment
   op_specs/geometric-adjustments/op_spec_coregistration

Data Inter-Comparison
=====================

.. toctree::
   :maxdepth: 1
   :numbered:

   op_specs/data-intercomparison/op_spec_category_data-intercomparison
   op_specs/data-intercomparison/correlation-analysis/op_spec_subcategory_correlation-analysis
   op_specs/data-intercomparison/correlation-analysis/op_spec_product-moment-correlation

