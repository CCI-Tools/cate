Operations 
==========

**Preprocessing**

- Geometric Adjustments

	- Co-Registration
	- Reprojection
	- Resampling
	- Geospatial Gap Filling
	- Matchup Dataset Generation 

- Non-Geometric Adjustments

	- Temporal Aggregation
	- Temporal Gap Filling
	- Temporal Concatenation

- Subsetting and Selections
	
	- Spatial Subsetting
	- Spatial Resolution *(either Geometric Adjustment or selection from different available datasets)*
	- Selection of a season of interest *(-> Calculations)*	
	- Temporal Subsetting
	- Temporal Resolution *(either Non-Geometric Adjustment or selection from different available datasets)*	
	- Pixel Class Extraction
	- Pixel Extraction by Values
	- Masks (land, sea, lakes -> cp. Clipping)

**Statistics and Calculations**

- Calculations
	
	- Seasonal Values
	- Arithmetics
	- Index Calculation

- Univariate Descriptive Statistics

	- Probabilities	
	
		- Probabilities *(absolute, relative, percental)*
		- Commulated Probabilities
		- Classification *(probabilities, histogram, pie chart, ...)*

	- Location Parameters
	
		- Arithmetic Mean *(weighting option)*
		- Percentiles and Median
		- Modus
		- Geometric Mean 

	- Dispersion Parameters
		
		- Range *(includes Minumum and Maximum)*
		- Variance and Standard Deviation
	
	- Shape Parameters
		
		- Skewness
		- Kurtosis 
	
	- Temporal Comparison
	
		- Relative Values
		- Anomalies
		- Standardization
		- Cumulative Changes
		- Hovmöller Analysis
	
	- Filtering 
	
		- Detection of Outliers
		- Filtering *(High Pass, Low Pass, Band Pass)*
		- Removal of Seasonal Cycles
		
	
- Data Intercomparison *(= Bivariate Statistics)*

	- Contingency Table

		- Contingency Table
		- Marginal Probabilities
		- Conditional Relative Probabilities
		- Test on Independency
	
	- Location Parameters
	
		- Arithmetic Mean Center
		- Median Center

	- Dispersion Parameters
		
		- Standard Distance 
		
	- Correlation Analysis
	
		- Standardized Contingency Coefficient
		- Rank Correlation Coefficient (Spearman)
		- Product-Moment Correlation Coefficient (Pearson)

	- Regression Analysis

		- Linear Regression Analysis
		- Determination Coefficient
		- Non-linear Regression Analysis 

		
- Statistical Inference

	- Test Statistics
	
		- T Test
		- Chi Square Test
		- ...

	- Analysis of Variance
	- Test on Distributions 
	- Probability Density Functions

- Time Series Analysis	

	- Homogeneity 
	- Cyclic Features
	- Autocorrelation
	- Trend Analysis
	- Harmonic Analysis (=Fourier Analysis)
	- Spectral Analysis of Variance 
		
- Array Processing and Statistics

	- all other operations on multi-dimensional basis

- Ensemble Statistics	

	- Multi Dataset Mean
	- Uncertainties and Spreads

- Complex Computations

	- EOF Analysis
	- Factor Analysis
	- Cluster Analyis

- Band Arithmetics and Statistics + GIS Tools

	- Band Arithmetics
	- Band Statistics
	- Area Estimation
	- Extract by Attributes
	- Mean Position
	- Layer Operations *(-> Band Arithmetics and Statistics)*
	- Data Merging of Different ECVs *(-> Band Arithmetics)*
	- Clipping 

- Evaluation and Quality Control	 *(propagation of uncertainties included in operations)*
	
	- Visual Consistency Checks (Histogramm as option)
	- Model Calibration and Evaluation 
	- Data Validation

**Visualisation Module**

- Visualisation

	- Table 
	- Time Series Plot 
	- Plot
	- Map 
	- Animated Map
	
**not clear**
	
- Ice Sheets Analysis *-> part of BA + GIS?*


**not specified as operations**

- *Load File*
- *Save Image*
- *(Save Plot)*