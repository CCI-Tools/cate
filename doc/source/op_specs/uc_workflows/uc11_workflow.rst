Use Case #11 Workflow
=====================

This sections describes an exemplary workflow performed to accomplish the climate problem given by
Use Case #11 :ref:`uc_11`.

#.	The user selects several CCI ECV data products from a checklist (geophysical quantities e.g. sea ice extent, glacier calving front location, corrected sea surface height, sea surface temperature, cloud optical thickness, aerosol type ).
#.	The user selects an ECV data product on a climate data server in Japan. 
#.	The user selects the Operation :doc:`Geospatial Gap Filling <../geometric-adjustments/geospatial-gapfilling>` from the Operation Category :doc:`Geometric Adjustment <../geometric-adjustments/op_spec_category_geometric-adjustment>`.
#.	The user selects the particular options (Method: nearest neighbour, apply to multiple data products).
#.	The user executes the Operation.
#.	The Toolbox performs a geospatial gap filling of the datasets.
#.	The user selects the Operation :doc:`Reprojection <../geometric-adjustments/op_spec_reprojection>` from the Operation Category :doc:`Geometric Adjustment <../geometric-adjustments/op_spec_category_geometric-adjustment>`.
#.	The user selects the particular options (polar stereographic grid, apply to multiple data products).
#.	The user executes the Operation.
#.	The Toolbox performs a reprojection of the data products onto the chosen polar stereographic grid.
#.	The user selects the Operation :doc:`Map <../visualisation/op_spec_map>` from the Operation Category :doc:`Visualisation <../visualisation/op_spec_category_visualisation>`.
#.	The user selects options (multiple data products).
#.	The user executes the Operation.
#.	The Toolbox displays maps (including legend, land contours etc.) of the reprojected ECVs side by side and provides a slide-bar which allows the user to dynamically fade-out the interpolated grid cells. 
