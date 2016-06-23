.. _Intergovernmental Panel on Climate Change: http://www.ipcc.ch/
.. _CCI Climate Modelling User Group: http://www.esa-cmug-cci.org/
.. _WCRP Grand Science Challenges: http://www.wcrp-climate.org/grand-challenges
.. _International Geosphere-Biosphere Programme: http://www.igbp.net/
.. _Coupled Model Intercomparison Project: http://cmip-pcmdi.llnl.gov/
.. _Coupled Carbon Cycle Climate Intercomparison Project: http://www.wcrp-climate.org/modelling-wgcm-mip-catalogue/modelling-wgcm-mips/230-modelling-wgcm-c4mip

=========
Use Cases
=========

Use cases provide application scenarios and requirements along which it will be demonstrated
how the CCI Toolbox will be implemented and operated.

Use cases are defined for various user types and their climate questions come from diverse various application areas,
see :numref:`user_types`.

.. _user_types:

.. table:: User Types
   
   ==== ============================================ =====================================================================
   Nr   User Type                                    Description of application area
   ==== ============================================ =====================================================================
   1    International climate research community     Contributing to `Intergovernmental Panel on Climate Change`_ (IPCC)
                                                     scientific assessments, including climate model development,
                                                     verification and data-assimilation, and scientists performing
                                                     research on climate change monitoring, detection, attribution and
                                                     mitigation. This includes (but is not limited to) the
                                                     `CCI Climate Modelling User Group`_ (CMUG) and the Climate
                                                     Research Groups (CRG) within each CCI ECV project.

   2    Earth system science community               Working at a higher level than individual climate indicators,
                                                     interested in Earth processes, interactions and feedbacks
                                                     involving a fusion of theory, observations and models to which
                                                     ECVs can play a role. This community includes, but is not exclusive
                                                     to, those interested in `WCRP Grand Science Challenges`_, climate
                                                     system integrative approaches, major science themes, global change
                                                     and socio-economic impact of climate change. Example potential
                                                     users include the `International Geosphere-Biosphere Programme`_
                                                     (IGBP), dynamic global vegetation modellers, the
                                                     `Coupled Model Intercomparison Project`_ (CMIP), and the
                                                     `Coupled Carbon Cycle Climate Intercomparison Project`_ (C4MIP).

   3    Climate service developers and providers	 For use in the development and provision of climate services.
                                                     The provision of climate services is outside the scope of the CCI
                                                     programme, nevertheless ESA aims to proactively support
                                                     parties involved in the development and provision of such services.

   4    Earth system reanalysis community            For use in reanalysis model development, verification and
                                                     data-assimilation.

   5    International bodies                         Responsible for climate change policy making and coordination of
                                                     climate change measurement, mitigation and adaptation efforts,
                                                     including UNFCCC, CEOS, IPCC, and COP participants.

   6    Undergraduate and postgraduate students      Academic interest in climate change. Sustained and dedicated
                                                     actions to generate and disseminate a substantial volume of
                                                     effective communication and educational materials on the specific
                                                     subject of Earth Observation and Climate Change to a wider
                                                     audience are required by the Agency. The CCI Toolbox shall
                                                     support this endeavour.

   7    Knowledgeable public                         Access and interaction to the latest scientific data on
                                                     climate change.
   ==== ============================================ =====================================================================

Each use case is introduced by a problem definition, which addresses a typical climate problem.
This is followed by the required CCI Toolbox features and a sequence of single steps,
how a user is expecting to use these features in the CCI Toolbox.

.. _uc_01:

IPCC Support
============

:User Types:
    * International climate research community
    * International bodies

:Problem Definition: In its Summary for Policy Makers, the fifth IPCC Assessment Report [RD-2] shows
    four ECVs of the marine environment as indicators of a changing climate. This figure depicting the “(a)
    extent of Northern Hemisphere March-April (spring) average snow cover; (b) extent of Arctic July-August-September
    (summer) average sea ice; (c) change in global mean upper ocean (0–700 m) heat content aligned to 2006−2010,
    and relative to the mean of all datasets for1970; (d) global mean sea level relative to the 1900–1905 mean of the
    longest running dataset, and with all datasets aligned to have the same value in 1993, the first year of satellite
    altimetry data” in the form of annual values with available uncertainties expressed as shadings, could also
    constitute a CCI Toolbox product. For a second figure, change in sea ice extent and ocean heat content are
    calculated on a regional basis and contrasted with land surface temperature anomalies. Additionally, global
    averages of land surface, land and ocean surface temperature as well as ocean heat content changes are presented.
    All observational time series are compared with model output. This could have been a CCI Toolbox operation, too.

:Required Toolbox Features Step 1:

    * Access to and ingestion of multi ESA CCI ECVs (Sea Ice, Ocean Colour, SST and Sea Level)
    * Access to and ingestion of other ECV sources (ESA GlobSnow, historic non-ESA data)
    * Tools to perform QC on input data (at least visual checking, consistency with historic time series)
    * Resampling and aggregation to a common spatio-temporal grid including propagation of uncertainties
    * Extraction of snow cover from LC
    * Comparison of sea ice coverage from Sea Ice, OC and SST (this may require own processors)
    * User programmed model to derive upper ocean heat content from SST
    * Aggregation to global averages including uncertainty propagation
    * Line plots as output, showing means and uncertainties

:Additionally Required Toolbox Features Step 2:

    * Access to and ingestion of further ESA data (LST from GlobTemperature) and model output
    * Band math or user programmed tool to combine SST and land surface temperature
    * Spatial filtering to perform the analysis on a regional scale (e.g. using shape files)
    * Ensemble statistics to show model ensemble mean and uncertainties in comparison to results based
      on (satellite) observations

.. _uc_02:

School Seminar Climate and Weather
==================================

:User Types:
    * Knowledgeable public

:Problem Definition: As a school project, measurements of air temperature, precipitation and wind speed from the
    school-run weather station shall be compared to long-term climate data in the form of ESA’s CCI Cloud and
    Soil moisture climatological means. Finally, it shall be assessed if the measurements are within the climate
    means for the particular location.

:Required Toolbox Features:
    * Access to and ingestion of ESA CCI Cloud and Soil Moisture data
    * Access to and ingestion of user supplied data; if required programming of an interface to a measurement device
    * Extraction of cloud and soil moisture time series data corresponding to the location of the school
    * Calculating the climatological means from the time series including propagation of uncertainties
    * Filtering of the measurement data from the meteorological station: e.g. detection of outlier or gap filling
      (implemented in the toolbox or programmed by the students)
    * Generation of a line plot showing the CCI and the meteorological station data.
    * Optional: comparison of the climatology at the school location with those from other locations on earth:
      selection of other locations and comparing the climatologies in one graph (i.e. without meteorological station
      data from the other location)

:Notes: This could also be a user visiting the website of a meteorological station and the website has included a
    widget that accesses the toolbox to perform the steps described.

.. _uc_03:

Glaciers and Sea Level Rise
===========================

:User Types:
    * International climate research community
    * Earth system science community
    * Earth system reanalysis community

:Problem Definition: A scientist wants to know: “What is the contribution of all glaciers to global sea level
    rise over a given time period in the future?”.

:Required Toolbox Features:
    * Access to and ingestion of ESA CCI Glacier, Ice Sheet and Sea Level data
    * Access to and ingestion of all relevant in-situ measurements from the past  (via WGMS)
    * Access to and ingestion of a globally complete dataset of glacier outlines complete with a
      time-stamp (vector data)
    * Access to and ingestion of topographic information for each glacier from a DEM
    * Spatial and temporal aggregation, re-gridding and possibly gap filling in order to make the data fields
      compatible with the model grid for model calibration and validation
    * Hypsometry calculation with a user-supplied plug-in (i.e. extending the toolbox, CLI, API, GIS tools)
    * Spatial resampling and converting back and forth between different coordinate systems, projections and
      ellipsoids to match all data spatially (co-registration)
    * Running of a prediction model (user-supplied plug-in or use of CLI, API), output creation (maps, graphs, tables)
      and comparison with validation data

.. _uc_04:

Extreme Weather Climate Service
===============================

:User Types:
    * Climate service developers and providers

:Problem Definition: In March 2012, the article “US heatwave may have been made more likely by global warming” by
    Andrew Freedman, senior science writer for Climate Central, was published in *The Guardian*. He wrote
    about extreme events, using the example of the increased occurrence of heat waves in March in relation
    to Greenhouse Gases. The article included a map of temperature anomalies over North America compared to
    the 2000–2001 reference period to illustrate this. Furthermore, several statements which require analysis of
    large data sets and time series were made. The CCI Data and CCI Toolbox could have supported this analysis.

:Required Toolbox Features:
    * Access to and ingestion of ESA CCI GHG data
    * Access to and ingestion of ESA GlobTemperature data
    * Geometric adjustments
    * Spatial subsetting
    * Computation of statistical quantities (mean of all month/season of a reference time series and percentiles)
    * Computation of anomalies
    * Map generation and with a simple colour coding to present a clear message

.. _uc_05:

School Seminar Glacier
======================

:User Types:
    * Undergraduate and postgraduate students

:Problem Definition: A student (at school) wants to know for a seminar paper: “What is the largest glacier in the
    world and how has this glacier changed in the past compared to other glacierized regions?”.

:Required Toolbox Features:
    * Access to and ingestion of the Randolph Glacier Inventory (RGI; database with contributions of CCI Glaciers) via
      GLIMS homepage
    * Sorting for size
    * Selection, extraction and saving to disk of the data for the largest glacier
    * Access to and ingestion of glacier fluctuation data, e.g. from World Glacier Monitoring Service (WGMS)
    * Filtering of fluctuation data according to a selection based on reference data (here the RGI data)
    * Extraction of a summary of global glacier fluctuations from WGMS data base
    * Data comparison (statistical values, deviations, graphs, maps, animations) and export

.. _uc_06:

Teleconnection Explorer
=======================

:User Types:
    * Undergraduate and postgraduate students

:Problem Definition: As part of a project on climatic teleconnection, a student investigates how El Niño-Southern
    Oscillation (ENSO) relates to monsoon rainfall. A result could be a plot showing the sliding correlation between
    Indian Summer Monsoon Rainfall (ISMR) and Niño3.4 SST anomalies [RD-4]. A more sophisticated version of this
    task would be to calculate the Multivariate ENSO Index (MEI, [RD-5],[RD-6]). Additionally, also the comparison
    of the ENSO index with other CCI datasets (e.g. Cloud, Fire) would be interesting.

:Required Toolbox Features:
    * Access to and ingestion of ESA CCI SST and Soil Moisture data
    * Geometric adjustments
    * Spatial (manually by drawing a polygon of the particular area) and temporal filtering and subsetting for
      both data sets
    * Calculation of anomalies and statistical quantities
    * Visual presentation of statistical results and time series
    * ENSO index calculation from SST data (built-in function, user-supplied plug-in or CLI, API)
    * Calculation of the absolute anomaly on the log transformed soil moisture data (this should be a standard
      function/processor provided by the toolbox)
    * Calculation of the correlation between the two time series with a lag of 30 days
    * Generation of a correlation map and export of the correlation data (format options) regarding the date range
      chosen
    * Generation of a time series plot of the correlation by the selection of a location in South East Asia on
      the correlation map
    * Saving of the image and the underlying data (format options)

In case of choosing the MEI instead of a solely SST-based index:

    * Access to and ingestion of additional datasets for MEI (sea-level pressure (P), zonal (U) and meridional (V)
      components of the surface wind, sea surface temperature (S), surface air temperature (A), and total cloudiness
      fraction of the sky (C))
    * Geometric adjustments
    * Index calculation including EOF analysis (incorporated by built-in function, user-supplied plug-in or CLI, API)

:Additional Features:
    * Access to and ingestion of additional ESA CCI data sets
    * Geometric adjustments
    * Spatial and temporal filtering
    * Calculation of statistic quantities and correlations
    * Generation of maps and plots
    * Export of the data

.. _uc_07:

Regional Cryosphere Climate Service
===================================

:User Types:
    * Climate service developers and providers

:Problem Definition: The Federal Office of Environment (FOEN) in Switzerland wants to provide an internet-based
    platform to disseminate latest information on the cryosphere and its changes in Switzerland. Such information could
    be, for example, the number of days with snow or other parameters like the glacier extent, mean cloud coverage in
    a specific region or start of the melting season. Before the technical work with the toolbox can be performed a
    user survey would be required to obtain detailed requirements for such a climate service.

:Required Toolbox Features:
    * Access to and ingestion of RGI Glacier and WGMS fluctuation data sets
    * Access to and ingestion of meteorological and snow cover data (from MeteoSchweiz and Institute for Snow and
      Avalanche Research (SLF))
    * Geometric adjustments and spatial intersection
    * Access to and ingestion of ESA CCI Glacier (+ Land Cover, Clouds) data sets
    * Access to and ingestion of latest meteorological data
    * Geometric adjustments
    * Extraction of area and time period
    * Generation of graphs (e.g. cumulative glacier length changes): descriptive statistical analysis (at least mean
      values, variances, anomalies) with user-controlled display and format options, annotations (need to load and
      display information on limitation and data sources)
    * Decision on any other data that should be made available (e.g. more permanently, quick links)

:Note: The general decision on layout, data sets etc. will be taken by the FOEN outside the CCI Toolbox but
    will be input to the selection options.

.. _uc_08:

World Glacier Monitoring Service
================================

:User Types:
    * International bodies

:Problem Definition: A service of the World Glacier Monitoring Service (WGMS) based on ESA CCI products,
    combined with other environmental parameters as well as meta data on glaciers, could be the provision of a
    database of glacier volume changes derived from remote sensing data (e.g. DEM differencing and altimetry sensors)

:Required Toolbox Features:
    * Access to and ingestion of RGI Glacier and WGMS fluctuation data sets
    * Access to and ingestion of ESA CCI Glacier data
    * Access to and ingestion of altimetry data and glacier meta data
    * Geometric adjustments
    * Subsetting and filtering of data according to user defined criteria
    * Data quality and consistency checks
    * Search for information about persons responsible for meta data according to a list of criteria, procurement of
      meta data
    * Adjustment of formats and metadata until they fit into the database (reference keys)
    * Additional: Selection of locations, time-periods, Calculation of means, anomalies, variances
    * Quality checks and data upload to the database

.. _uc_09:

Relationships between Aerosol and Cloud ECV
===========================================

:User Types:
    * Earth system science community

:Problem Definition: A climate scientist wishes to analyse potential correlations between Aerosol and Cloud ECVs.

:Required Toolbox Features:
    * Access to and ingestion of ESA CCI Aerosol and Cloud data (Aerosol Optical Depth and Cloud Fraction)
    * Geometric adjustments
    * Spatial (point, polygon) and temporal subsetting
    * Visualisation of both times series at the same time: e.g. time series plot, time series animation
    * Correlation analysis, scatter-plot of correlation statistics, saving of image and correlation statistics on disk
      (format options)


:Exemplary Workflow: :doc:`op_specs/uc_workflows/uc09_workflow`


.. _uc_10:

Scientific Investigation of NAO Signature
=========================================

:User Types:
    * Earth system science community

:Problem Definition: A climate scientist wishes to investigate the signature of the North Atlantic Oscillation (NAO)
    in multiple ECVs using a processor built by another climate scientist and contributed to the toolbox.

:Required Toolbox Features:
    * Access to and ingestion of ESA CCI ECV data products
    * Access to and ingestion of external data (NAO time series)
    * Geometric adjustments
    * Spatial and temporal subsetting
    * Use of externally developed plug-in to apply R [RD-7]: removal of seasonal cycles, lag-correlation analysis
      between each ECV and the NAO index
    * Generation of time-series plot for each ECV
    * Export statistics output to local disk

.. _uc_11:

School Project on Arctic Climate Change
=======================================

:User Types:
    * Undergraduate and postgraduate students

:Problem Definition: As part of a project on Arctic climate change, an undergraduate student wishes to look at
    different ECVs on a polar stereographic projection.

:Required Toolbox Features:
    * Access to and ingestion of CCI ECV data products
    * Access to and ingestion of ECV data products from external server
    * Remapping to fit data onto user-chosen projection
    * Spatial and temporal subsetting
    * Gap-filling (user-chosen strategy)
    * Generation of scalable maps

.. _uc_12:

Marine Environmental Monitoring
===============================

:User Types:
    * Climate service developers and providers
    * Knowledgeable public

:Problem Definition: The eReef project examines the living conditions of the Great Barrier Reef via
    two subprojects. On the one hand, the aim of the Marine Water Quality Dashboard is to estimate water
    quality indicators from ocean colour data to deduce brightness and therefore the vitality of
    coral-competing seagrass and algae. ReefTemp Next Generation, on the other hand, seeks to assess
    the risk of bleaching due to overly warm water by calculating heat stress indices. This could also
    be a task for the CCI Toolbox environment.

:Required Toolbox Features:
    * Access to and ingestion of ESA CCI SST and Ocean Colour data
    * Access to and ingestion of data concerning water constituents, plant growth, brightness, competitor relationships,
      coral vulnerability to heat stress
    * Geometric adjustments
    * Temporal and spatial subsetting
    * Implementation of a water optical property model (plug-in, CLI, API)
    * Calculation of anomalies, extremes (+ trend analysis, correlations)
    * Index calculation (plug-in, CLI, API)
    * Visualisation, graphs, data export

.. _uc_13:

Drought Occurrence Monitoring in Eastern Africa
===============================================

:User Types:
    * Climate service developers and providers
    * International bodies
    * Knowledgeable public

:Problem Definition: Due to time-lagged teleconnections, weather conditions in Eastern Africa are highly influenced
    by climate modes of variability in remote regions. Therefore, climate indices such as for ENSO, MJO or QBO as well
    as the NDVI can be used to estimate the drought probability in Africa. Long time series from satellite observations
    act as a basis for the construction of statistical forecasting models, which are then run by latest meteorological
    data.

:Required Toolbox Features:
    * Access to and ingestion of ESA CCI SST, Clouds, Land Cover data
    * Access to and ingestion of non-CCI observational (e.g. NST, PRE, OLR, SLP)  and latest meteorological data
    * Geometric adjustments
    * Spatial and temporal subsetting (for each variable)
    * NDVI and climate index calculation (ENSO, MJO, QBO indices), includes descriptive statistics
    * Estimation of predictor (SST, SST gradients, OLR, cloud properties, climate indices) – predicant (NST and PRE E
      Africa) relationship by time-lagged (linear) regression model (plug-in, CLI, API)
    * Run model by means of latest meteorological data
    * Visualisation and export of results (graphs, maps, animations, tables)

.. _uc_14:

Drought Impact Monitoring and Assessment in China
=================================================

:User Types:
    * Climate service developers and providers
    * International bodies

:Problem Definition: (Solely basic idea taken from WMO (2015))
    Drought occurrence and severity in terms of fire, vegetation state and soil moisture shall be estimated by the
    use of temperature and rainfall (+ humidity and evapo-transpiration) data to prepare countermeasures.
    This is achieved by the construction of an empirical statistical model using satellite-derived time series
    which is afterwards run by actual meteorological data.

:Required Toolbox Features:
    * Access to and ingestion of ESA CCI Soil Moisture, Fire, Land Cover data
    * Access to and ingestion of non-CCI NST and PRE observation and latest meteorological data
    * Geometric adjustments
    * Spatial and temporal subsetting (for each variable)
    * (Descriptive statistic analysis)
    * Estimation of predictor (NST, PRE) – predicant (soil moisture, vegetation state, fire occurrence) and PRE
      E Africa) relationship by time-lagged (linear) regression model (plug-in, CLI, API)
    * Run model by means of latest meteorological data
    * Visualisation and export of results (graphs, maps, animations, tables)

.. _uc_15:

Renewable Energy Resource Assessment with regard to Topography
==============================================================

:User Types:
    * Climate service developers and providers
    * International bodies

:Problem Definition: The long-term potential for renewable energy generation is to be estimated by considering the
    effect of cloud features, aerosols, ozone and water vapour on solar irradiance as well as topographical data.

:Required Toolbox Features:
    * Access to and ingestion of ESA CCI Ozone, Clouds, Aerosols, Land Cover and Glaciers (albedo, snow/ice coverage)
    * Access to and ingestion of non-CCI data (water vapour, pressure, precipitation, surface radiation budget),
      irradiance observations
    * External topographical data: preprocessed data regarding roof area, tilt, orientation from DEM
    * Geometric adjustments
    * Spatial and temporal subsetting
    * Implementation of fast radiative transfer calculations (plug-in, CLI, API) to deduce solar irradiance
    * Extraction of areas with high potential regarding solar irradiance (set appropriate boundary values)
    * Extraction of areas with suitable tilt and orientation
    * Visualisation of suitable areas in a map
    * Estimation of Solar Power potential from pixel count
    * Export of Results

.. _uc_16:

Monitoring Tropical Deforestation
=================================

:User Types:
    * Climate service developers and providers
    * International bodies

:Problem Definition: Maps of forest cover, change and deforestation shall be produced depicting forest status and
    trends. Additionally, vector data regarding infrastructure (e.g. road works) could be obtained from local
    authorities and compared with forest evolution.

:Required Toolbox Features:
    * Access to and ingestion of ESA CCI Land Cover data
    * Access to and ingestion of additional data regarding biomass production, carbon emission, leaf areas,
      forest health
    * Additional: access  to and ingestion of vector data regarding infrastructure
    * Spatial and temporal adjustments and subsetting
    * Extraction of forest class
    * Estimation of forest area for multiple time-steps
    * Additional: layer operations comprising infrastructure and forest data (vector and raster)
    * Visualisation of forest area changes (animated?), relation to infrastructure
    * Data export

.. _uc_17:

Stratospheric Ozone Monitoring and Assessment
=============================================

:User Types:
    * Climate service developers and providers
    * International bodies

:Problem Definition: As UV exposure is a highly relevant health factor, the state of the ozone layer shall
    be monitored as well as its influence parameters.

:Required Toolbox Features:
    * Access to and ingestion of ESA CCI Ozone, GHG(, Aerosol) data
    * Access to and ingestion of surface-based measurements of ozone-depleting substances and other emissions,
        data regarding ozone-UV exposure relationships
    * Geometric adjustments
    * Spatial (horizontal and vertical) and temporal subsetting
    * Assessment of total ozone values as well as vertical profiles
    * Estimation of UV exposure by the use of ozone-UV exposure relationship data
    * Correlation analysis between ozone values and concentrations of various GHGs and ozone-depleting substances
    * Trend analysis of stratospheric ozone concentrations
    * Visualisation (maps, graphs) and export of the results

.. _uc_18:

Examination of ENSO and its Impacts based on ESA CCI Data
=========================================================

:User Types:
    * Undergraduate and postgraduate students

:Problem Definition: As a project work, a student’s task is to conduct an examination of ENSO solely by the use
    of ESA CCI data. For this, the first principal component of the combined EOF analysis of cloud cover, sea level
    and sea surface temperature in the (central/eastern) equatorial Pacific shall be intercompared with ocean colour
    (eastern equatorial Pacific), fire disturbance and soil moisture (landmasses adjacent to the eastern and western
    tropical Pacific).

:Required Toolbox Features:
    * Access to and ingestion of ESA CCI Cloud, Sea Level and SST data
    * Selection of required products/variables
    * Temporal/spatial selections or aggregations in case of differing temporal or spatial data set resolutions
    * Temporal and spatial filtering regarding time period and particular areas of interest, spatial mean values
      for ocean colour, fire, soil moisture (particular regional boundaries need to be assessed)
    * Test for normal distribution (using plug-in/API)
    * EOF analysis:
          * Removal of seasonal cycle and linear/quadratic trends to clarify ENSO signal
          * Conduction of EOF analysis involving array processing and statistics by means of a plug-in/API
          * Visual examination of EOF map and eigenvalues, to clarify if ENSO typical patterns are present and explained
            variance is sufficiently high
    * Correlation statistics (different lags) between time series of first principal component and ocean colour,
      fire disturbance E, fire disturbance W, soil moisture E, soil moisture W including t test for the assessment
      of significance
    * Plotting of all computed time series in one coordinate system
    * Option to manually select point location on globe to compare data with PC1
    * Storage of plots, time series data, correlation statistics on local disk

.. _uc_19:

GHG Emissions over Europe
=========================

:User Types:
    * Knowledgeable public

:Problem Definition: A person wants to know how greenhouse gas emissions over Europe evolved during the last years.

:Required Toolbox Features:
    * Access to and ingestion of ESA CCI GHG data
    * Selection of required products/variables
    * Temporal and spatial subsetting
    * Generation of maps/animations depicting the evolution of GHG emissions

.. _uc_20:

Examination of North Eastern Atlantic SST Projections
=====================================================

:User Types:
    * Climate research community

:Problem Definition: A climate scientist uses CCI data to validate the output of several CMIP5 models concerning SST
    in the north eastern Atlantic Ocean.
    Afterwards he picks the best model runs to perform a trend analysis regarding the future evolution
    using the ensemble mean and uncertainties as well as probability density functions. Applying an Analysis of
    Variance, he examines the different results of the models.

:Required Toolbox Features:
    * Access to and ingestion of ESA CCI SST data
    * Access to and ingestion of CMIP5 model data
    * Filtering regarding variable
    * Geometric adjustments
    * Spatial and temporal subsetting
    * Quality assessment of model data by means of satellite-observed SST data using plug-in/API (user-determined
      validation method), discarding of models undercutting certain values
    * Application of best models for trend analysis (removal of seasonal cycles)
    * Calculation of SST anomaly/increase values for several time steps compared with reference data (ensemble mean
      and spread/uncertainties), construct probability density functions, examination of differing results by ANOVA
    * Visualisation
    * Data export

.. _uc_21:

Investigation of Relationships between Ice Sheet ECV Parameters
===============================================================

:User Types:
    * Earth system science community

:Problem Definition: A scientist wants to gain insight into the relationship between the Ice Sheets CCI ECV
    parameters. At first, Surface Elevation Change (SEC), Ice Velocity (IV), and Gravitational Mass Balance (GMB)
    are compared. Afterwards, a basin-wise comparison of Surface Elevation Change averages and Gravimetry Mass
    Balance averages is conducted. And finally, vector and grid data are compared by co-plotting of IV and
    Calving Front Location (CFL) data. Additionally, it would be interesting to examine the relationships between
    sea ice, SST around Greenland, glacier melt respectively cloud cover and SEC/IV.

:Required Toolbox Features:
    * Access to and ingestion of ECV parameter data (SEC, IV, GMB)
    * Re-gridding of all data to the SEC grid
    * Display the data as different layers
    * Calculation of the IV vector magnitude (per pixel) and display as a new layer
    * Temporal interpolation of the SEC data to the GMB data times
    * Calculation of the correlation coefficient (per pixel) between the SEC data and the GMB data for a given GMB
      measurement time, display as a new layer
    * Access to and ingestion of a polygon shapefile corresponding to one of the GMB basins
    * Filtering of the SEC values and the GMB values; discarding of the ones outside the GMB basin polygon
    * Calculation of the average of the GMB and SEC values inside the basin polygon for each point in the time series
    * Plotting of the averaged values in a time series plot, comparison with the provided GMB total basin values
    * Access to and ingestion of the CFL time series; each element in the time series is a set of (lon/lat) line
      segments
    * Plotting of the CFL line segments on top of the IV magnitude for different years

:Optional:
    * Access to and ingestion of ECV parameter data (sea ice, SST, glacier melt, cloud cover)
    * Re-gridding of all data to the SEC grid
    * Temporal and spatial subsetting
    * Calculation of correlation coefficients
    * Visualisation and export


