Use Case #9 Relationships between Aerosol and Cloud ECV
=======================================================

:User Characteristics: User Level 1 High-level expert user (User Community 2 earth system science community)

:Problem Definition: A climate scientist wishes to analyse potential correlations between Aerosol and Cloud ECVs. 

:Exemplary Workflow: 

#.	The user selects CCI data products from a checklist (geophysical quantities: aerosol optical depth or absorbing aerosol index, cloud cover). 
#.	The user selects the operation :doc:`Co-Registration <../geometric-adjustments/op_spec_coregistration>` from the operation category :doc:`Geometric Adjustment <../geometric-adjustments/op_spec_category_geometric-adjustments>`.
#.	The user selects the particular options (use of grid1 or grid2, interpolation method, propagation of uncertainties analysis).
#.	The user clicks a button to execute.
#.	The Toolbox performs a co-registration of one dataset onto the coordinate system of the other. 
#.	The user selects the operation :doc:`Spatial Subsetting <../subsetting_selections/op_spec_spatial-subsetting>` from the operation category :doc:`Subsetting and Selections <../subsetting_selections/op_spec_category_subsetting_selections>`.
#.	The user selects options (geospatial point of interest on a rotatable globe (GUI) or specification of coordinates).
#.	The user clicks a button to execute.
#.	The Toolbox creates a spatial subset of the data. 
#.	The user selects the operation :doc:`Temporal Subsetting <../subsetting_selections/op_spec_temporal-subsetting>` from the operation category :doc:`Subsetting and Selections <../subsetting_selections/op_spec_category_subsetting_selections>`.
#.	The user selects start and end years of a time range.
#.	The user clicks a button to execute.
#	The Toolbox creates a temporal subset of the data. 
#.	The user selects the operation :doc:`Time Series Plot <../visualisation/op_spec_time-series-plot>` from the operation category :doc:`Visualisation <../visualisation/op_spec_category_visualisation>`.
#.	The user selects options (multiple data products).
#.	The user clicks a button to execute.
#.	The Toolbox plots time series on the same axes. 
#.	The user selects the operation :doc:`Product-Moment Correlation (Pearson) <../data-intercomparison/correlation-analysis/op_spec_product-moment-correlation>` (operation category :doc:`Data Intercomparison <../data-intercomparison/op_spec_category_data-intercomparison>`, operation subcategory :doc:`Correlation Analysis <../data-intercomparison/correlation-analysis/op_spec_subcategory_correlation-analysis>`).
#.	The user selects options (scatter-plot).
#.	The user clicks a button to execute.
#.	The Toolbox plots a scatter-plot and correlation statistics on the screen. 
#.	The user choses a “Save Image” option which saves the plot as a PNG file.
#.	The user selects the operation :doc:`Spatial Subsetting <../subsetting_selections/op_spec_spatial-subsetting>` from the operation category :doc:`Subsetting and Selections <../subsetting_selections/op_spec_category_subsetting_selections>`.
#.	The user selects options (polygon on the rotatable globe (GUI) or specification of coordinates).
#.	The user clicks a button to execute.
#.	The Toolbox creates a spatial subset of the data. 
#.	The user selects the operation :doc:`Animated Map <../visualisation/op_spec_animated-map>` from the operation category :doc:`Visualisation <../visualisation/op_spec_category_visualisation>`.
#.	The user selects options (multiple data products).
#.	The user clicks a button to execute.
#.	The Toolbox displays side-by-side animations.
#.	The user clicks a button to save the animated maps as GIF files.
#.	The user selects the operation :doc:`Product-Moment Correlation (Pearson) <../data-intercomparison/correlation-analysis/op_spec_product-moment-correlation>` (operation category :doc:`Data Intercomparison <../data-intercomparison/op_spec_category_data-intercomparison>`, operation subcategory :doc:`Correlation Analysis <../data-intercomparison/correlation-analysis/op_spec_subcategory_correlation-analysis>`).
#.	The user selects options (grid cell-by-grid cell, map).
#.	The user clicks a button to execute.
#.	The Toolbox performs a grid cell-by-grid cell correlation between the two twodimensional time series, and generates a correlation map displayed on the screen. 
#.	The user clicks buttons to save the map (PNG) as well as the correlation statistics (ASCII).


Operations UC9 
==============

- :doc:`Filtering and Selections <../subsetting_selections/op_spec_category_subsetting_selections>`

	- :doc:`Spatial Filtering <../subsetting_selections/op_spec_spatial-subsetting>`
	- :doc:`Temporal Filtering <../subsetting_selections/op_spec_temporal-subsetting>`
	
	
- :doc:`Geometric Adjustment <../geometric-adjustments/op_spec_category_geometric-adjustments>`

	- :doc:`Co-Registration <../geometric-adjustments/op_spec_coregistration>`
	
- :doc:`Visualisation <../visualisation/op_spec_category_visualisation>`

	- :doc:`Time Series Plot <../visualisation/op_spec_time-series-plot>`
	- :doc:`Animated Map <../visualisation/op_spec_animated-map>`

	
- :doc:`Data Intercomparison <../data-intercomparison/op_spec_category_data-intercomparison>`
		
	- :doc:`Correlation Analysis <../data-intercomparison/correlation-analysis/op_spec_subcategory_correlation-analysis>`
	
		- :doc:`Product-Moment Correlation (Pearson) <../data-intercomparison/correlation-analysis/op_spec_product-moment-correlation>`


*not implemented as operations*

- *Load Data*
- *Save Image*
- *(Save Plot)*
