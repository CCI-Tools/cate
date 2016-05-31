Use Case #9 Relationships between Aerosol and Cloud ECV
=======================================================

:User Characteristics: User Level 1 High-level expert user (User Community 2 earth system science community)

:Problem Definition: A climate scientist wishes to analyse potential correlations between Aerosol and Cloud ECVs. 

:Exemplary Workflow: 

a)	The user selects CCI data products from a checklist. 
b)	The user selects the operation :doc:`Co-Registraion <../geometric-adjustment/ob_spec_coregistration>` from the operation category :doc:`Geometric Adjustment <geometric-adjustment/ob_spec_category_geometric-adjustments>`.
c)	The user selects the particular options (use of grid1 or grid2, interpolation method, propagation of uncertainties analysis).
d)	The user clicks a button to execute.
e)	The Toolbox performs a co-registration of one dataset onto the coordinate system of the other. 
f)	The user selects a geospatial point of interest on a rotatable globe. 
g)	The user selects start and end years of a time range. 
h)	The user selects the operation :doc:`Time Series Plot <../visualisation/ob_spec_time-series-plot>` from the operation category :doc:`Visualisation <visualisation/ob_spec_category_visualisation>`. 
i)	The user selects options (multiple datasets).
j)	The user clicks a button to execute.
k)	The Toolbox plots time series on the same axes. 
l)	The user selects the operation :doc:`Product-Moment Correlation (Pearson) <../data-intercomparison/correlation-analysis/ob_spec_product-moment-correlation>` (operation category :doc:`Data Intercomparison <../data-intercomparison/ob_spec_category_data-omtercomparison>`, operation subcategory :doc:`Correlation Analysis <../data-intercomparison/correlation-analysis/ob_spec_subcategory_correlation-analysis>`).
m)	The user selects options (scatter-plot).
n)	The user clicks a button to execute.
o)	The Toolbox plots a scatter-plot and correlation statistics on the screen. 
p)	The user clicks on a “Save Image” option which saves the plot as a PNG file
q)	The user re-specifies the geospatial area of interest as a polygon on the rotatable globe.
r)	The user selects the operation :doc:`Animated Map <../visualisation/ob_spec_animated-map>` from the operation category :doc:`Visualisation <../visualisation/ob_spec_category_visualisation>`. 
s)	The user selects options (multiple datasets).
t)	The user clicks a button to execute.
u)	The Toolbox displays side-by-side animations.
v)	The user selects the operation :doc:`Product-Moment Correlation (Pearson) <../data-intercomparison/correlation-analysis/ob_spec_product-moment-correlation>` (operation category :doc:`Data Intercomparison <../data-intercomparison/ob_spec_category_data-omtercomparison>`, operation subcategory :doc:`Correlation Analysis <../data-intercomparison/correlation-analysis/ob_spec_subcategory_correlation-analysis>`).
w)	The user selects options (map).
x)	The user clicks a button to execute.
y)	The Toolbox performs a pixel-by-pixel correlation between the two twodimensional time series, and generates a correlation map displayed on the screen. 
z)	The user clicks a button to save the output.


Operations UC9 
==============

- :doc:`Geometric Adjustment <../geometric-adjustment/ob_spec_category_geometric-adjustments>`

	- :doc:`Co-Registration <../geometric-adjustment/ob_spec_coreregistration>`
	
- :doc:`Visualisation <../visualisation/ob_spec_category_visualisation>`

	- :doc:`Time Series Plot <../visualisation/ob_spec_time-series-plot>`
	- :doc:`Animated Map <../visualisation/ob_spec_animated-map>`

	
- :doc:`Data Intercomparison <../data-intercomparison/ob_spec_category_data-intercomparison>`
		
	- :doc:`Correlation Analysis <../data-intercomparison/correlation-analysis/ob_spec_subcategory_correlation-analysis>`
	
		- :doc:`Product-Moment Correlation Coefficient (Pearson) <../data-intercomparison/correlation-analysis/ob_spec_product-moment-correlation>`


*not implemented as operations*

- *Parameter Settings* (selection of time span and AOI -> as default to all Operations)
- *Save Image*
- *(Save Plot)*