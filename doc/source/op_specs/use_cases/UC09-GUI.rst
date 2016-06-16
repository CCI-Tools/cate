Use Case #9 Relationships between Aerosol and Cloud ECV
=======================================================

:User Characteristics: User Level 1 High-level expert user (User Community 2 earth system science community)

:Problem Definition: A climate scientist wishes to analyse potential correlations between Aerosol and Cloud ECVs. 

:Exemplary Workflow: 

#.	The user selects CCI data products from a checklist. 
#.	The user selects the operation :doc:`Co-Registration <../geometric-adjustments/op_spec_coregistration>` from the operation category :doc:`Geometric Adjustment <../geometric-adjustments/op_spec_category_geometric-adjustment>`.
#.	The user selects the particular options (use of grid1 or grid2, interpolation method, propagation of uncertainties analysis).
#.	The user clicks a button to execute.
#.	The Toolbox performs a co-registration of one dataset onto the coordinate system of the other. 
#.	The user selects the operation :doc:`Spatial Filtering <../filtering_selections/op_spec_spatial-filtering>` from the operation category :doc:`Filtering and Selections <../filtering_selections/op_spec_category_filtering_selections>`.
#.	The user selects options (geospatial point of interest on a rotatable globe (GUI) or specification of coordinates).
#.	The user clicks a button to execute.
#.	The Toolbox creates a spatial subset of the data. 
#.	The user selects the operation :doc:`Temporal Filtering <../filtering_selections/op_spec_temporal-filtering>` from the operation category :doc:`Filtering and Selections <../filtering_selections/op_spec_category_filtering_selections>`.
#.	The user selects start and end years of a time range.
#.	The user clicks a button to execute.
#	The Toolbox creates a temporal subset of the data. 
#.	The user selects the operation :doc:`Time Series Plot <../visualisation/op_spec_time-series-plot>` from the operation category :doc:`Visualisation <../visualisation/op_spec_category_visualisation>`.
#.	The user selects options (multiple datasets).
#.	The user clicks a button to execute.
#.	The Toolbox plots time series on the same axes. 
#.	The user selects the operation :doc:`Product-Moment Correlation (Pearson) <../data-intercomparison/correlation-analysis/op_spec_product-moment-correlation>` (operation category :doc:`Data Intercomparison <../data-intercomparison/op_spec_category_data-intercomparison>`, operation subcategory :doc:`Correlation Analysis <../data-intercomparison/correlation-analysis/op_spec_subcategory_correlation-analysis>`).
#.	The user selects options (scatter-plot).
#.	The user clicks a button to execute.
#.	The Toolbox plots a scatter-plot and correlation statistics on the screen. 
#.	The user choses a “Save Image” option which saves the plot as a PNG file.
#.	The user selects the operation :doc:`Spatial Filtering <../filtering_selections/op_spec_spatial-filtering>` from the operation category :doc:`Filtering and Selections <../filtering_selections/op_spec_category_filtering_selections>`.
#.	The user selects options (polygon on the rotatable globe (GUI) or specification of coordinates).
#.	The user clicks a button to execute.
#.	The Toolbox creates a spatial subset of the data. 
#.	The user selects the operation :doc:`Animated Map <../visualisation/op_spec_animated-map>` from the operation category :doc:`Visualisation <../visualisation/op_spec_category_visualisation>`.
#.	The user selects options (multiple datasets).
#.	The user clicks a button to execute.
#.	The Toolbox displays side-by-side animations.
#.	The user clicks a button to save the animated maps as GIF files.
#.	The user selects the operation :doc:`Product-Moment Correlation (Pearson) <../data-intercomparison/correlation-analysis/op_spec_product-moment-correlation>` (operation category :doc:`Data Intercomparison <../data-intercomparison/op_spec_category_data-intercomparison>`, operation subcategory :doc:`Correlation Analysis <../data-intercomparison/correlation-analysis/op_spec_subcategory_correlation-analysis>`).
#.	The user selects options (map).
#.	The user clicks a button to execute.
#.	The Toolbox performs a pixel-by-pixel correlation between the two twodimensional time series, and generates a correlation map displayed on the screen. 
#.	The user clicks buttons to save the map (PNG) as well as the correlation statistics (ASCII).


Operations UC9 
==============

- :doc:`Filtering and Selections <../filtering_selections/op_spec_category_filtering_selections>`

	- :doc:`Spatial Filtering <../filtering_selections/op_spec_spatial-filtering>`
	- :doc:`Temporal Filtering <../filtering_selections/op_spec_temporal-filtering>`
	
	
- :doc:`Geometric Adjustment <../geometric-adjustments/op_spec_category_geometric-adjustment>`

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
