Use Case #6 Workflow
====================

This sections describes an exemplary workflow performed to accomplish the climate problem given by
Use Case #6 :ref:`uc_06`.
Version 1: Niño3.4 Index

#.	The user selects CCI ECV data products from a checklist (geophysical quantities: sea surface temperature, surface soil moisture volumetric absolutes).
#.	The user selects the Operation :doc:`Co-Registration <../geometric-adjustments/op_spec_coregistration>` from the Operation Category :doc:`Geometric Adjustments <../geometric-adjustments/op_spec_category_geometric-adjustments>`.
#.	The user selects the particular options (Co-Registration method: spline, propagation of uncertainties analysis).
#.	The user executes the Operation.
#.	The Toolbox performs a co-registration of one dataset onto the coordinate system of the other. 
#.	The user selects the Operation :doc:`Temporal Subsetting <../subsetting_selections/op_spec_temporal-subsetting>` from the Operation Category :doc:`Subsetting and Selections <../subsetting_selections/op_spec_category_subsetting_selections>`.
#.	The user selects start and end years of a time period.
#.	The user executes the Operation.
#.	The Toolbox creates a temporal subset of the data. 
#.	The user selects the Operation :doc:`Long-term Average <../uni-desc-statistics/comparison/op_spec_long-term_average>` (Operation Category :doc:`Univariate Descriptive Statistics <../uni-desc-statistics/op_spec_category_uni-desc-statistics>`, Operation Subcategory :doc:`Comparison <../uni-desc-statistics/comparison/op_spec_subcategory_comparison>`).
#.	The user selects options (preserve saisonality, reference period)
#.	The user executes the Operation.
#.	The Toolbox generates a new dataset comprising a one-year time series of the long-term average of the data.
#.	The user selects the Operation :doc:`Index Calculation <../calculations/op_spec_index-calculation>` (Operation Category :doc:`Calculations <../calculations/op_spec_category_calculations>`) or a user-supplied plugin/API.
#.	The user selects the requested ENSO index from a list of pre-defined indices regarding the region selected.
#.	The user executes the Operation.
#.	The Toolbox/plug-in generates an index time series.
#.	The user selects the Operation :doc:`Arithmetics <../calculations/op_spec_arithmetics>` (Operation Category :doc:`Calculations <../calculations/op_spec_category_calculations>`).
#.	The user enters the calculation specification (log transformation of SM data).
#.	The user executes the Operation.
#.	The Toolbox generates a new dataset.
#.	The user selects the Operation :doc:`Long-term Average <../uni-desc-statistics/comparison/op_spec_long-term_average>` (Operation Category :doc:`Univariate Descriptive Statistics <../uni-desc-statistics/op_spec_category_uni-desc-statistics>`, Operation Subcategory :doc:`Comparison <../uni-desc-statistics/comparison/op_spec_subcategory_comparison>`).
#.	The user selects options (preserve saisonality, reference period)
#.	The user executes the Operation.
#.	The Toolbox generates a new dataset comprising a one-year time series of the long-term average of the data.
#.	The user selects the Operation :doc:`Anomalies <../uni-desc-statistics/comparison/op_spec_anomalies>` (Operation Category :doc:`Univariate Descriptive Statistics <../uni-desc-statistics/op_spec_category_uni-desc-statistics>`, Operation Subcategory :doc:`Comparison <../uni-desc-statistics/comparison/op_spec_subcategory_comparison>`).
#.	The user enters the calculation specification (absolute anomaly of log transformed SM data with respect to mean of reference period).
#.	The user executes the Operation.
#.	The Toolbox generates a new dataset.
#.	The user selects the Operation :doc:`Product-Moment Correlation (Pearson) <../data-intercomparison/correlation-analysis/op_spec_product-moment-correlation>` (Operation Category :doc:`Data Intercomparison <../data-intercomparison/op_spec_category_data-intercomparison>`, Operation Subcategory :doc:`Correlation Analysis <../data-intercomparison/correlation-analysis/op_spec_subcategory_correlation-analysis>`).
#.	The user selects options (map, table, 30 days lag time).
#.	The user executes the Operation. 
#.	The Toolbox generates a map showing the correlation between the ENSO index and soil moisture as well as a table comprising the location-specific correlation coefficients including correlation flags.
#.	The user selects the Operation :doc:`Spatial Subsetting <../subsetting_selections/op_spec_spatial-subsetting>` from the Operation Category :doc:`Subsetting and Selections <../subsetting_selections/op_spec_category_subsetting_selections>`.
#.	The user selects options (selecting of a point location in SE Asia).
#.	The user executes the Operation.
#.	The Toolbox creates a spatial subset containing the point data.
#.	The user selects the Operation :doc:`Product-Moment Correlation (Pearson) <../data-intercomparison/correlation-analysis/op_spec_product-moment-correlation>` (Operation Category :doc:`Data Intercomparison <../data-intercomparison/op_spec_category_data-intercomparison>`, Operation Subcategory :doc:`Correlation Analysis <../data-intercomparison/correlation-analysis/op_spec_subcategory_correlation-analysis>`).
#.	The user selects options (time series plot, 30 days lag time).
#.	The user executes the Operation. 
#.	The Toolbox generates a time series plot and correlation statistics.
#.	The user saves images and underlying data on a local disk.


Version 2: Multivariate ENSO Index

#.	The user selects CCI (geophysical quantities: sea surface temperature, cloud cover) and non-CCI (geophysical quantities: sea level pressure, zonal surface wind components, meridional surface wind components, surface air temperature) ECV data products from a checklist.
#.	The user selects the Operation :doc:`Co-Registration <../geometric-adjustments/op_spec_coregistration>` from the Operation Category :doc:`Geometric Adjustments <../geometric-adjustments/op_spec_category_geometric-adjustments>`.
#.	The user selects the particular options (Co-Registration method: spline, propagation of uncertainties analysis).
#.	The user executes the Operation.
#.	The Toolbox performs a co-registration of one dataset onto the coordinate system of the other.
#.	The user selects the Operation :doc:`Spatial Subsetting <../subsetting_selections/op_spec_spatial-subsetting>` from the Operation Category :doc:`Subsetting and Selections <../subsetting_selections/op_spec_category_subsetting_selections>`.
#.	The user selects options (drawing of a polygon).
#.	The user executes the Operation.
#.	The Toolbox creates a spatial subset containing data of the selected region.
#.	The user selects the Operation :doc:`Temporal Subsetting <../subsetting_selections/op_spec_temporal-subsetting>` from the Operation Category :doc:`Subsetting and Selections <../subsetting_selections/op_spec_category_subsetting_selections>`.
#.	The user selects start and end years of a time period.
#.	The user executes the Operation.
#.	The Toolbox creates a temporal subset of the data. 
#.	The user selects the Operation :doc:`Seasonal Values <../calculations/op_spec_seasonal-values>` (Operation Category :doc:`Calculations <../calculations/op_spec_category_calculations>`).
#.	The user selects options (bi-monthly seasons, sliding).
#.	The user executes the Operation.
#.	The Toolbox creates 12 new time series per geophysical quantitity comprising bi-monthly values.
#.	The user selects the Operation :doc:`Long-term Average <../uni-desc-statistics/comparison/op_spec_long-term_average>` (Operation Category :doc:`Univariate Descriptive Statistics <../uni-desc-statistics/op_spec_category_uni-desc-statistics>`, Operation Subcategory :doc:`Comparison <../uni-desc-statistics/comparison/op_spec_subcategory_comparison>`).
#.	The user selects options (preserve saisonality, reference period)
#.	The user executes the Operation.
#.	The Toolbox generates a new dataset comprising a one-year time series of the long-term average of the data.
#.	The user selects the Operation :doc:`EOF Analysis <../complex-computations/op_spec_eof>` from the Operation Category :doc:`Complex Computations <../complex-computations/op_spec_category_complex-computations>`.
#.	The user selects options (combined EOF analysis, correlation matrix, …; apply to multiple data products).
#.	The user executes the Operation.
#.	The Toolbox performs a combined EOF analysis for each of the 12 bi-monthly seasons.
#.	The user selects the Operation :doc:`Arithmetics <../calculations/op_spec_arithmetics>` (Operation Category :doc:`Calculations <../calculations/op_spec_category_calculations>`).
#.	The user enters calculating specifications for combining the 12 separate time series (JF, FM, MA, …) of the first principal component to one consecutive dataset.
#.	The user executes the Operation.
#.	The Toolbox calculates a new time series.
#.	The user selects the Operation :doc:`Arithmetics <../calculations/op_spec_arithmetics>` (Operation Category :doc:`Calculations <../calculations/op_spec_category_calculations>`).
#.	The user enters the calculation specification (log transformation of SM data).
#.	The user executes the Operation.
#.	The Toolbox generates a new dataset.
#.	The user selects the Operation :doc:`Long-term Average <../uni-desc-statistics/comparison/op_spec_long-term_average>` (Operation Category :doc:`Univariate Descriptive Statistics <../uni-desc-statistics/op_spec_category_uni-desc-statistics>`, Operation Subcategory :doc:`Comparison <../uni-desc-statistics/comparison/op_spec_subcategory_comparison>`).
#.	The user selects options (preserve saisonality, reference period)
#.	The user executes the Operation.
#.	The Toolbox generates a new dataset comprising a one-year time series of the long-term average of the data.
#.	The user selects the Operation :doc:`Anomalies <../uni-desc-statistics/comparison/op_spec_anomalies>` (Operation Category :doc:`Univariate Descriptive Statistics <../uni-desc-statistics/op_spec_category_uni-desc-statistics>`, Operation Subcategory :doc:`Comparison <../uni-desc-statistics/comparison/op_spec_subcategory_comparison>`).
#.	The user enters the calculation specification (absolute anomaly of log transformed SM data with respect to mean of reference period).
#.	The user executes the Operation.
#.	The Toolbox generates a new dataset.
#.	The user selects the Operation :doc:`Product-Moment Correlation (Pearson) <../data-intercomparison/correlation-analysis/op_spec_product-moment-correlation>` (Operation Category :doc:`Data Intercomparison <../data-intercomparison/op_spec_category_data-intercomparison>`, Operation Subcategory :doc:`Correlation Analysis <../data-intercomparison/correlation-analysis/op_spec_subcategory_correlation-analysis>`).
#.	The user selects options (map, table, 30 days lag time).
#.	The user executes the Operation. 
#.	The Toolbox generates a map showing the correlation between the ENSO index and soil moisture as well as a table comprising the location-specific correlation coefficients including correlation flags.
#.	The user selects the Operation :doc:`Spatial Subsetting <../subsetting_selections/op_spec_spatial-subsetting>` from the Operation Category :doc:`Subsetting and Selections <../subsetting_selections/op_spec_category_subsetting_selections>`.
#.	The user selects options (selecting of a point location in SE Asia).
#.	The user executes the Operation.
#.	The Toolbox creates a spatial subset containing the point data.
#.	The user selects the Operation :doc:`Product-Moment Correlation (Pearson) <../data-intercomparison/correlation-analysis/op_spec_product-moment-correlation>` (Operation Category :doc:`Data Intercomparison <../data-intercomparison/op_spec_category_data-intercomparison>`, Operation Subcategory :doc:`Correlation Analysis <../data-intercomparison/correlation-analysis/op_spec_subcategory_correlation-analysis>`).
#.	The user selects options (time series plot, 30 days lag time).
#.	The user executes the Operation. 
#.	The Toolbox generates a time series plot and correlation statistics.
#.	The user saves images and underlying data on a local disk. In doing this, the user selects TIFF and CSV as file formats.


Additional features:

#.	The user selects CCI ECV data products from a checklist (geophysical quantities e.g. burned area, cloud cover, phytoplankton chlorophyll-A concentration, sea ice concentration).
#.	The user selects the Operation :doc:`Co-Registration <../geometric-adjustments/op_spec_coregistration>` from the Operation Category :doc:`Geometric Adjustments <../geometric-adjustments/op_spec_category_geometric-adjustments>`.
#.	The user selects the particular options (Co-Registration method: spline, propagation of uncertainties analysis, apply to multiple data products).
#.	The user executes the Operation.
#.	The Toolbox performs a co-registration of one dataset onto the coordinate system of the other.
#.	The user selects the Operation :doc:`Spatial Subsetting <../subsetting_selections/op_spec_spatial-subsetting>` from the Operation Category :doc:`Subsetting and Selections <../subsetting_selections/op_spec_category_subsetting_selections>`.
#.	The user selects options (drawing of a polygon).
#.	The user executes the Operation.
#.	The Toolbox creates a spatial subset containing data of the selected region.
#.	The user selects the Operation :doc:`Temporal Subsetting <../subsetting_selections/op_spec_temporal-subsetting>` from the Operation Category :doc:`Subsetting and Selections <../subsetting_selections/op_spec_category_subsetting_selections>`.
#.	The user selects start and end years of a time period.
#.	The user executes the Operation.
#.	The Toolbox creates a temporal subset of the data. 
#.	The user selects the Operation :doc:`Arithmetic Mean <../uni-desc-statistics/location-parameters/op_spec_arithmetic-mean>` (Operation Category :doc:`Univariate Descriptive Statistics <../uni-desc-statistics/op_spec_category_uni-desc-statistics>`, Operation Subcategory :doc:`Location Parameters <../uni-desc-statistics/location-parameters/op_spec_subcategory_location-parameters>`).
#.	The user selects options (temporal mean, propagation of uncertainties, apply to multiple data products).
#.	The user executes the Operation.
#.	The Toolbox calculates the temporal mean for every grid cell in the selected area. 
#.	The user selects the Operation :doc:`Long-term Average <../uni-desc-statistics/comparison/op_spec_long-term_average>` (Operation Category :doc:`Univariate Descriptive Statistics <../uni-desc-statistics/op_spec_category_uni-desc-statistics>`, Operation Subcategory :doc:`Comparison <../uni-desc-statistics/comparison/op_spec_subcategory_comparison>`).
#.	The user selects options (preserve saisonality, reference period)
#.	The user executes the Operation.
#.	The Toolbox generates a new dataset comprising a one-year time series of the long-term average of the data.
#.	The user selects the Operation :doc:`Map <../visualisation/op_spec_map>` from the Operation Category :doc:`Visualisation <../visualisation/op_spec_category_visualisation>`.
#.	The user selects options (multiple data products).
#.	The user executes the Operation.
#.	The Toolbox displays side-by-side maps showing mean values of the used geophysical quantities.
#.	The user selects the Operation :doc:`Anomalies <../uni-desc-statistics/comparison/op_spec_anomalies>` (Operation Category :doc:`Univariate Descriptive Statistics <../uni-desc-statistics/op_spec_category_uni-desc-statistics>`, Operation Subcategory :doc:`Comparison <../uni-desc-statistics/comparison/op_spec_subcategory_comparison>`).
#.	The user selects options (reference period, apply to multiple data products).
#.	The user executes the Operation.
#.	The Toolbox calculates anomalies for every grid cell in the selected area.
#.	The user selects the Operation :doc:`Animated Map <../visualisation/op_spec_animated-map>` from the Operation Category :doc:`Visualisation <../visualisation/op_spec_category_visualisation>`.
#.	The user selects options (multiple data products).
#.	The user executes the Operation.
#.	The Toolbox displays maps showing animations of evolving anomalies of the used geophysical quantities side-by-side.
#.	The user selects the Operation :doc:`Spatial Subsetting <../subsetting_selections/op_spec_spatial-subsetting>` from the Operation Category :doc:`Subsetting and Selections <../subsetting_selections/op_spec_category_subsetting_selections>`.
#.	The user selects options (drawing of a polygon).
#.	The user executes the Operation.
#.	The Toolbox creates a spatial subset containing data of the selected region.
#.	The user selects the Operation :doc:`Arithmetic Mean <../uni-desc-statistics/location-parameters/op_spec_arithmetic-mean>` (Operation Category :doc:`Univariate Descriptive Statistics <../uni-desc-statistics/op_spec_category_uni-desc-statistics>`, Operation Subcategory :doc:`Location Parameters <../uni-desc-statistics/location-parameters/op_spec_subcategory_location-parameters>`).
#.	The user selects options (spatial mean, propagation of uncertainties, apply to multiple data products).
#.	The user executes the Operation.
#.	The Toolbox generates new time series consisting of regional mean values. 
#.	The user selects the Operation :doc:`Long-term Average <../uni-desc-statistics/comparison/op_spec_long-term_average>` (Operation Category :doc:`Univariate Descriptive Statistics <../uni-desc-statistics/op_spec_category_uni-desc-statistics>`, Operation Subcategory :doc:`Comparison <../uni-desc-statistics/comparison/op_spec_subcategory_comparison>`).
#.	The user selects options (preserve saisonality, reference period)
#.	The user executes the Operation.
#.	The Toolbox generates a new dataset comprising a one-year time series of the long-term average of the data.
#.	The user selects the Operation :doc:`Product-Moment Correlation (Pearson) <../data-intercomparison/correlation-analysis/op_spec_product-moment-correlation>` (Operation Category :doc:`Data Intercomparison <../data-intercomparison/op_spec_category_data-intercomparison>`, Operation Subcategory :doc:`Correlation Analysis <../data-intercomparison/correlation-analysis/op_spec_subcategory_correlation-analysis>`).
#.	The user selects options (scatter plot, apply to multiple data products).
#.	The user executes the Operation.
#.	The Toolbox displays a scatter plots and correlation statistics on the screen. 
#.	The user saves images and underlying data on a local disk. 
