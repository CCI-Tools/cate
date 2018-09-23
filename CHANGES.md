## Version 2.0.0.dev19 (in development)

* Must click twice to expand point to polygon [#709](https://github.com/CCI-Tools/cate/issues/709)
* Added operation `write_geo_data_frame()` [#758](https://github.com/CCI-Tools/cate/issues/758)
* Numbers displayed with too many digits [#754](https://github.com/CCI-Tools/cate/issues/754)
* Improved error handling in operation `pearson_correlation_scalar()``, addresses [#746](https://github.com/CCI-Tools/cate/issues/746)
* Fixed error in `plot_xxx()` operations `"'NoneType' object is not iterable"` [#749](https://github.com/CCI-Tools/cate/issues/749)
* Fixed problem with `coregister()` operation on data subsets [#747](https://github.com/CCI-Tools/cate/issues/747)
* Fixed operations `data_frame_aggregate()` and  `data_frame_subset()` to let users select variables (columns) of selected data frame.
* Added information about resources of type `DataFrame` and `GeoDataFrame` in the details section of the **WORSPACE** panel.
* Updated default colour mappings and default variables for more **Sea Level CCI** products

## Version 2.0.0.dev18

* Added new operation `data_frame_aggregate()` [#707](https://github.com/CCI-Tools/cate/issues/707)
* Values of scalar variables are now always shown in **VARIABLES** panel in Cate Desktop [#702](https://github.com/CCI-Tools/cate/issues/702)
  Unfortunately, this feature did not find its way into 2.0.0.dev17, instead only its related bug [#743](https://github.com/CCI-Tools/cate/issues/743).
* Fixed "KeyError: 0" for one one-record data frames [#743](https://github.com/CCI-Tools/cate/issues/743)
* Removed option to load GML files in Cate Desktop solving [#734](https://github.com/CCI-Tools/cate/issues/734)

## Version 2.0.0.dev17

* Added information about resources of type `GeoDataFrame` (Shapefiles, GeoJSON) in the details section of the **WORSPACE** panel in Cate Desktop [#705](https://github.com/CCI-Tools/cate/issues/705)
* Added new operation `merge()` [#740](https://github.com/CCI-Tools/cate/issues/740)
* Added new operation `data_frame_subset()` [#708](https://github.com/CCI-Tools/cate/issues/708)
* Fixed display of CCI Sea Level MSLAMPH data [#722](https://github.com/CCI-Tools/cate/issues/722)
* Improve indexers to first do a validation with respect to the available dimensions and the selected remaining_dims [#730](https://github.com/CCI-Tools/cate/issues/730)
* Improve plotting capabilities to allow multi-variable plotting and format specification [#704](https://github.com/CCI-Tools/cate/issues/704)
* Fixed code signing issue during the installer build on MacOS and Windows [#726](https://github.com/CCI-Tools/cate/issues/726)
* Fixed Cate Desktop failed to start (in Ubuntu 18) due to missing .so [#729](https://github.com/CCI-Tools/cate/issues/729)
* Fixed Cate Desktop failed to start (in Windows) due to unable to find dll files [#725](https://github.com/CCI-Tools/cate/issues/725)
* User-defined Setup fails with existing Miniconda/Anaconda installation [#728](https://github.com/CCI-Tools/cate/issues/728)

## Version 2.0.0.dev16

* Added new operation `data_frame_find_closest()` [#706](https://github.com/CCI-Tools/cate/issues/706)
* Added new operations `compute_dataset()` and `compute_data_frame()` [#703](https://github.com/CCI-Tools/cate/issues/703).
* Fixed division by zero error in RGB tile generation if data min and max were equal 
* Allow displaying and working with CCI Sea Level MSLAMPH data.
  Addresses [#531](https://github.com/CCI-Tools/cate/issues/531).
* Improved chunking when opening local netCDF datasets, improved memory footprint of
  ``subset_spatial`` operation when masking is enabled
  Addresses [#701](https://github.com/CCI-Tools/cate/issues/701)
* Fix a bug where correlation would fail with differing time dimensions
  Addresses [#700](https://github.com/CCI-Tools/cate/issues/700)
* Fix a bug where coregistration would fail in some cases when the grouped by dimension
  is not squeezed out automatically.
  Addresses [#684](https://github.com/CCI-Tools/cate/issues/684)
* Fix a bug where coregistration would fail with some datasets on very fine grids due to
  floating point calculation errors
  Addresses [#714](https://github.com/CCI-Tools/cate/issues/714)
* Fix a bug with a wrong spatial subset appearing when saving/opening a workspace
  Addresses [#693](https://github.com/CCI-Tools/cate/issues/693)
* Fix window certificate error [#696](https://github.com/CCI-Tools/cate/issues/696)

## Version 2.0.0.dev15

* Fixed operation progress monitor which was broken due to an update of the Dask library
* Added dataset detection funcionality for new or removed DS [#227](https://github.com/CCI-Tools/cate/issues/227) 

## Version 2.0.0.dev14

* ESA sea-level data not correctly displayed [#661](https://github.com/CCI-Tools/cate/issues/661)
* Added colour mapping defaults for CCI Sea Level data.
* Extended max. table size to 10000 elements (workaround).
* User Guide Improvements and Updates [#409](https://github.com/CCI-Tools/cate/issues/409):
  * Replaced references to Cate 1.0 to Cate 2.0 and updated most of figures
  * Rewrote section about setup including installation and configuration
  * Added a new section about the new **STYLES** panel 
  * Updated section about **PLACES** panel to include information on how to generate a polyline, polygon, and box
  * Updated section about **LAYERS** panel to describe its new elements

## Version 2.0.0.dev13

*Skipped.*

## Version 2.0.0.dev12

* No longer hide any ODP datasets in GUI [#669](https://github.com/CCI-Tools/cate/issues/669)
* Added experimental support for [Zarr](http://zarr.readthedocs.io/en/stable/) data I/O format [#659](https://github.com/CCI-Tools/cate/issues/659)
* The operation  `long_term_average` now works with daily, monthly and seasonal datasets [#471](https://github.com/CCI-Tools/cate/issues/471)
* Fixed problem in `cate-webapi-start` occurring on Linux when using address `localhost` (related to [#627](https://github.com/CCI-Tools/cate/issues/627))
* Updated `anomaly_external` to retain global attributes and do more input validation [#666](https://github.com/CCI-Tools/cate/issues/666)

## Version 2.0.0.dev11

* Lacking cancelable progress monitor when opening large datasets [#640](https://github.com/CCI-Tools/cate/issues/640)
* Wrong chunk size does not allow to import some dataset [#631](https://github.com/CCI-Tools/cate/issues/631)
* Local dataset not recognised [#557](https://github.com/CCI-Tools/cate/issues/557)
* Allow exporting any data as CSV [#637](https://github.com/CCI-Tools/cate/issues/637)
* Using `localhost` instead of explicit IP to maybe target [#627](https://github.com/CCI-Tools/cate/issues/627) 
* The `read_netcdf()` operation uses Dask chunking so we can expand all variables by a 'time' dimension 
  without loading all data into memory. 

## Version 2.0.0.dev10

* Support datasets with 0,360 degree longitude ranges [#620](https://github.com/CCI-Tools/cate/issues/620)
* Temporal aggregation operation can now aggregate to pre-defined seasons, as well as custom resolutions [#472](https://github.com/CCI-Tools/cate/issues/472)
* We now use "MB" units instead of "MiB" (part of [#325](https://github.com/CCI-Tools/cate/issues/325))
* Fixed a bug with animation generation [#585](https://github.com/CCI-Tools/cate/issues/585)
* Upgrade to using newer xarray version after an upstream bugfix [#579](https://github.com/CCI-Tools/cate/issues/579)
* Fixed a bug of unable to do pixel values extraction if one of the workflow has an error [#616](https://github.com/CCI-Tools/cate/issues/616)
* Add the capability to create Hovmoeller plots [#503](https://github.com/CCI-Tools/cate/issues/503)
* Add a reduce operation that lets the user create arbitrary data reductions [#618](https://github.com/CCI-Tools/cate/issues/618)

## Version 2.0.0.dev9

* Representative default variables [#590](https://github.com/CCI-Tools/cate/issues/590).
* Tasks are no longer executed in parallel [#606](https://github.com/CCI-Tools/cate/issues/606).
* WebAPI service problem in CLI [#600](https://github.com/CCI-Tools/cate/issues/600)
* Improve error messages and handling [#393](https://github.com/CCI-Tools/cate/issues/393),
  introduced new error type `cate.core.types.ValidationError` for special treatment in the GUI.
* Make Cate HTTP User-Agent distinguishable [#510](https://github.com/CCI-Tools/cate/issues/510).
* Fixed broken WebAPI invocation from CLI.
* Use only one variable (http_proxy) for proxy URL in conf.py. The value of this variable is then returned when
  get_config() is called. [#544](https://github.com/CCI-Tools/cate/issues/544)

## Version 2.0.0.dev8

* Removed the `cate-webapi` command-line tool and replaced it by two others:
  * `cate-webapi-start` to start the Cate WebAPI service.
  * `cate-webapi-stop` to start the Cate WebAPI service. This script executes
    fast, as it will will not longer import any of the many packages Cate depends on. 
* Cate Desktop hangs when restarted after quit while running a task
  [#578](https://github.com/CCI-Tools/cate/issues/578)
* SST temporal aggregation error
  [#548](https://github.com/CCI-Tools/cate/issues/548)
* Scrambled time axis error
  [#538](https://github.com/CCI-Tools/cate/issues/538)

* Check local datasource name if it doesn't contain restricted/unsupported characters
  [#568](https://github.com/CCI-Tools/cate/issues/568)

## Version 2.0.0.dev7

* Cate Desktop hangs after upgrading WebAPI to 2.0.0.dev6
  [#569](https://github.com/CCI-Tools/cate/issues/569), using Tornado 5 webserver now.

## Version 2.0.0.dev6

* Activate script missing after "cate-cli" package installation
  [#569](https://github.com/CCI-Tools/cate/issues/569)
* Keep configuration of data stores path 
  [#439](https://github.com/CCI-Tools/cate/issues/439)

## Version 2.0.0.dev5

* Select long rectangles with ``subset_spatial()``
  [#541](https://github.com/CCI-Tools/cate/issues/541)
* Improve performance of ``subset_spatial()``, especially when masking complex polygons
  [#508](https://github.com/CCI-Tools/cate/issues/508)
* Select all pixels that are crossed by the given polygon in ``subset_spatial()``
  [#560](https://github.com/CCI-Tools/cate/issues/560)
* Enable ``subset_spatial()`` to work with all valid polygons, including sub-pixel ones.
  [#507](https://github.com/CCI-Tools/cate/issues/507)
* By default ``plot_map()`` and ``animate_map()`` now produce colormesh (pixel) plots.
  [#559](https://github.com/CCI-Tools/cate/issues/507)
* Fix issues with progress writing.

* Raise a more helpful error when Cate runs out of memory trying to save a plot.

## Version 2.0.0.dev4 

* Perform progress writing from the correct thread

## Version 2.0.0.dev3

* Operation to perform arbitrary dataset math
  [#556](https://github.com/CCI-Tools/cate/issues/556)
* New parameter `interval` for `animate_map()`


## Version 2.0.0.dev2

### Fixes

* CF valid_range not respected in data visualisation
  [#537](https://github.com/CCI-Tools/cate/issues/537)

## Version 2.0.0.dev1

### Improvements and new Features

* Added `data_frame_min(df)` and `data_frame_max(df)` operations to select features by min/max
  [#492](https://github.com/CCI-Tools/cate/issues/492)
* Added `data_frame_query(df, expr)` operation to query features
  [#486](https://github.com/CCI-Tools/cate/issues/486).
  If the data frame `df` contains a geometry column (a `GeoDataFrame` object),
  then the query expression `expr` can also contain geometric relationship tests,
  for example the expression
  `"population > 100000 and @within('-20, 40, 20, 80')"`
  could be used on a data frame to query for larger cities in Europe.
* Removed operation `read_geo_data_collection`. The preferred operation to read
  feature attribute tables ("data frames") with geometries from ESRI Shapefiles and GeoJSON files is
  `read_geo_data_frame`.
* CLI now launches a lot faster, e.g. try now `cate -h`
  [#58](https://github.com/CCI-Tools/cate/issues/58)
* Cate can now produce animated figures
  [#86](https://github.com/CCI-Tools/cate/issues/86)

### Fixes

* Be tolerant of "invalid" geometries passed to operations expecting
  polygon WKT values
  [#506](https://github.com/CCI-Tools/cate/issues/506)
* Cate wont work if installed on drive other than home drive
  [#466](https://github.com/CCI-Tools/cate/issues/466)
* Region constraint'-option for AEROSOL dataset returns 'code 20' error
  [#462](https://github.com/CCI-Tools/cate/issues/462)
* Address problems of a user working with Cloud and Aerosol
  [#478](https://github.com/CCI-Tools/cate/issues/478)
* Most projections not working in plot operations
  [#524](https://github.com/CCI-Tools/cate/issues/524)
* Resolve an index operation documentation issue
  [#496](https://github.com/CCI-Tools/cate/issues/496)
* Resolve a bug with wrong file open mode
  [#497](https://github.com/CCI-Tools/cate/issues/497)

## Version 1.0 (10.10.2017)

### Improvements and new Features

* List only data sources tested by the champion users
  [#435](https://github.com/CCI-Tools/cate/issues/435)
* Global temporal attributes are adjusted automatically when opening new datasets
* Global temporal attributes are adjusted automatically when converting from data frames
* Normalization and subsetting operation implementation logic is refactored out to util so that it can be re-used throughout Cate

### Fixes

* Get rid of Python user warnings
  [#446](https://github.com/CCI-Tools/cate/issues/446)
* Missing static background map
  [#453](https://github.com/CCI-Tools/cate/issues/453)
* Fixed displaying broken/incomplete/canceled data sources on local data sources list
  [#375](https://github.com/CCI-Tools/cate/issues/375)
* Generated resource names not always unique
  [#391](https://github.com/CCI-Tools/cate/issues/391)
* Multiple concurrent attempts to load the ODP index now always return the same result
  [#386](https://github.com/CCI-Tools/cate/issues/386)
* Use global temporal attributes to determine temporal resolution in aggregation operations
  [#340](https://github.com/CCI-Tools/cate/issues/340)
* Only allow valid python identifiers as resource names
  [#436](https://github.com/CCI-Tools/cate/issues/436)
* OS X installation error
  [#438](https://github.com/CCI-Tools/cate/issues/438)

## Version 0.9.0

### Improvements and new Features

* Added check if copying/downloading DS failed without any progress/complete files
  if so, remove empty DS
  [#375](https://github.com/CCI-Tools/cate/issues/375)
* Min/max computation should be monitored
  [#384](https://github.com/CCI-Tools/cate/issues/384)
* Added API to annotate deprecated operations and operation input/outputs.
  Refer to `op`, `op_input`, `op_output` decorators in `cate.op`.
  [#381](https://github.com/CCI-Tools/cate/issues/381)
* Configure default color maps
  [#372](https://github.com/CCI-Tools/cate/issues/372)
* Hide problematic ODP data sources
  [#368](https://github.com/CCI-Tools/cate/issues/368)
* Coregistration operation now works on n-dimensional datasets
  [#36](https://github.com/CCI-Tools/cate/issues/36)
* Add use case 2 script [#327](https://github.com/CCI-Tools/cate/issues/327)
  and [#146](https://github.com/CCI-Tools/cate/issues/146)
* long_term_average, temporal_aggregation, detect_outliers, spatial_subset and plot now work with both - datasets and dataframes.
* Unified backend of CLI and GUI on WebSocket [#120](https://github.com/CCI-Tools/cate/issues/120)
  As the GUI uses WebSocket, this remove a bit of duplicated code.
* The `pearson_correlation` operation has been split into two operations:
  * `pearson_correlation_simple` that produces a single pair of a correlation
    coefficient and a probability value for the given timeseries.
  * `pearson_correlation_map` produces a map of correlation coefficients and p-values
    and outputs this as a dataset that can be worked with further.
  Performance of pearson correlation has been radically improved. In addition, the operations can now
  accept both, a dataset and a dataframe and a map can be created also by
  performing correlation of a single timeseries against all spatial points in the
  other dataset.
* A uniform way of handling spatiotemporal global attributes has been introduced
* External executables such as the *CCI Land Cover User Tool*, the *CCI SST Regridding Tool*, or
  the *MPI Climate Data Operators* can now be registered as operations.
* In summary, workflows can now have steps of the following types:
  - A step that invokes a registered Cate operation, which is the default
    ```json
    {
         "op": <qualified op name>
    }
    ```
  - A step that invokes an external executable
    ```json
    {
         "command": <command pattern>,
         "cwd": <current working directory>
         "env": <dict of environment variables>
    }
    ```
  - A step that invokes another (JSON) workflow
    ```json
    {
         "workflow": <workflow JSON path>
    }
    ```
  - A step that executes a Python expression
    ```json
    {
         "expression": <Python expression>
    }
    ```
* Searching data sources from the CLI using "cate ds list -n" now matches against id and title
* Added `plot_scatter` and `plot_contour` operations ([#278](https://github.com/CCI-Tools/cate/issues/278)).
* Most `plot_` operations now have a new `title` parameter.
* A function annotated by one of the operator decorators (`@op`, `@op_input`, `@op_return`, `@op_output`)
  will be turned into an *operation registration* which is also callable.
  Calling the *operation registration* will validate all inputs and then pass arguments and
  keyword-arguments to the actual, original function.
* New `FileLike` type.
* Changed the JSON object representation of a (xarray/NetCDF-CF) variable to include all variable
  attributes. This changes the the response of various REST/WebSocket calls that return a workspace.


### Fixes

* Fixed reading datasource temporal coverage from config file (obsolete format)
  [#373](https://github.com/CCI-Tools/cate/issues/373)
* Merged (removed duplicated) meta information in datasource config file
  [#301](https://github.com/CCI-Tools/cate/issues/301)
* Land Cover CCI display must use dedicated color map
  [#364](https://github.com/CCI-Tools/cate/issues/364)
* Land Cover CCI data display wrongly positioned (temp. hack!)
  [#361](https://github.com/CCI-Tools/cate/issues/361)
* Make alpha blending work for all color maps
  [#360](https://github.com/CCI-Tools/cate/issues/360)
* CLI monitor not working
  [#353](https://github.com/CCI-Tools/cate/issues/353)
* GUI-Preferences for data store files do not overwrite conf.py
  [#350](https://github.com/CCI-Tools/cate/issues/350)
* Filter 't0' in the `make_local` step of **SOILMOISTURE** data sources to make the data usable
  [#326](https://github.com/CCI-Tools/cate/issues/326)
* Updated information about temporal, spatial coverage and variables of copied from ODP data sources (constraint-aware)
  [#315](https://github.com/CCI-Tools/cate/issues/315)
* Verify operations against the operation development checklist to ensure some
  quality baseline.
  [#291](https://github.com/CCI-Tools/cate/issues/291)
* Use only tags from a predefined set (maybe module name && list in developers' guide)
  [#280](https://github.com/CCI-Tools/cate/issues/280)
* Added option to use open_dataset in workflow with automatic copying remote data source and reusing/re-opening previusly copied data (constraint-aware)
  [#287](https://github.com/CCI-Tools/cate/issues/287)
* Generate unique default ID for local copies of remote data sources (constraint-aware)
  [#277](https://github.com/CCI-Tools/cate/issues/277)
* Coregistration works with n-dimensional datasets
  [#36](https://github.com/CCI-Tools/cate/issues/36)
  [#348](https://github.com/CCI-Tools/cate/issues/348)
* Date and time columns in CSV data are converted into datetime objects
* Fix use case 6 script
* Fix #320 (wrong file dialog for enso_nino34 operation in GUI)
* Fix temporal coverage for ODP datasets that are listed as a single dataset in the CSW and as multiple in the ESGF
* Fixed [#309](https://github.com/CCI-Tools/cate/issues/309)
* Ensure that our tile size matches the expected tile size: resize and fill in background value.
* Take tile size from dask, this should yield to better performance
* Fixed [#299](https://github.com/CCI-Tools/cate/issues/299)
    * renamed property `cate.core.ds.DataSource.name` to `id` 
    * renamed property `cate.core.ds.DataStore.name` to `id` 
    * renamed and changed signature of function `cate.core.ds.DataStore.query_data_sources(..., name=None)` 
      to `find_data_sources(..., id=None, query_expr=None)`
    * changed signature of method `cate.core.ds.DataStore.query(name, ...)` to `query(id=None, query_expr=None, ...)`
    * renamed and changed signature of method `cate.core.ds.DataSource.matches_filter(name)` to `matches(id=None, query_expr=None)`
    * added `title` property to `cate.core.ds.DataStore` and `cate.core.ds.DataSource`
    * made use of the new `id` and `title` properties of both `DataStore` and `DataSource` in their 
      JSON representations.
* Fixed [#294](https://github.com/CCI-Tools/cate/issues/294)
* Fixed [#286](https://github.com/CCI-Tools/cate/issues/286)
* Fixed [#285](https://github.com/CCI-Tools/cate/issues/285)
* Fixed [#283](https://github.com/CCI-Tools/cate/issues/283)
* Fixed [#281](https://github.com/CCI-Tools/cate/issues/281)
* Fixed [#270](https://github.com/CCI-Tools/cate/issues/270)
* Fixed [#273](https://github.com/CCI-Tools/cate/issues/273)
* Fixed [#262](https://github.com/CCI-Tools/cate/issues/262)
* Fixed [#201](https://github.com/CCI-Tools/cate/issues/201)
* Fixed [#223](https://github.com/CCI-Tools/cate/issues/223)
* Fixed [#267](https://github.com/CCI-Tools/cate/issues/267)
* Fixed a problem with getting the variable statistics for variables with more that 3 dimensions
* Switch CSW to same URL as the ODP
* JSON-RPC protocol changed slightly: method `__cancelJob__` has been renamed to `__cancel__`
  and its only parameter `jobId` became `id`.
* Fixed packaging location of file `countries.geojson` so that Cate Desktop can display it
* Fixed [#259](https://github.com/CCI-Tools/cate/issues/259)
* Fixed problem when the `lon` or `lat` coordinate variables were empty.
  See comments in [#276](https://github.com/CCI-Tools/cate/issues/276).


## Version 0.8.0

### Improvements and new Features

* Various documentation updates.
* `cate.webapi.websocket` now understands the operations
  `clean_workspace(base_dir)` and `delete_workspace_resource(basedir, res_name)`.

### Fixes

* Fixed wrong error message that was raised, when attempting to delete a resource on which other steps
  still depend on.
* Fixed [#263](https://github.com/CCI-Tools/cate/issues/263)
* Fixed [#257](https://github.com/CCI-Tools/cate/issues/257)

## Version 0.7.0

Initial version for testing.
