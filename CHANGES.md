## Version 2.0.0.dev5 (in dev) 

* Fix issues with progress writing.

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
