## Changes in version 0.9.0.dev7 (unreleased)

### Improvements and new Features

* Added API to annotate deprecated operations and operation input/outputs.
  Refer to `op`, `op_input`, `op_output` decorators in `cate.op`.
  [#381](https://github.com/CCI-Tools/cate-core/issues/381)
* Configure default color maps
  [#372](https://github.com/CCI-Tools/cate-core/issues/372)
* Hide problematic ODP data sources
  [#368](https://github.com/CCI-Tools/cate-core/issues/368)

### Issues Fixed/Resolved

* Fixed reading datasource temporal coverage from config file (obsolete format)
  [#373](https://github.com/CCI-Tools/cate-core/issues/373)
* Merged (removed duplicated) meta information in datasource config file
  [#301](https://github.com/CCI-Tools/cate-core/issues/301)
* Land Cover CCI display must use dedicated color map
  [#364](https://github.com/CCI-Tools/cate-core/issues/364)
* Land Cover CCI data display wrongly positioned (temp. hack!)
  [#361](https://github.com/CCI-Tools/cate-core/issues/361)
* Make alpha blending work for all color maps
  [#360](https://github.com/CCI-Tools/cate-core/issues/360)
* CLI monitor not working
  [#353](https://github.com/CCI-Tools/cate-core/issues/353)
* GUI-Preferences for data store files do not overwrite conf.py
  [#350](https://github.com/CCI-Tools/cate-core/issues/350)
* Filter 't0' in the `make_local` step of **SOILMOISTURE** data sources to make the data usable
  [#326](https://github.com/CCI-Tools/cate-core/issues/326)
* Updated information about temporal, spatial coverage and variables of copied from ODP data sources (constraint-aware)
  [#315](https://github.com/CCI-Tools/cate-core/issues/315)
* Verify operations against the operation development checklist to ensure some
  quality baseline.
  [#291](https://github.com/CCI-Tools/cate-core/issues/291)
* Use only tags from a predefined set (maybe module name && list in developers' guide)
  [#280](https://github.com/CCI-Tools/cate-core/issues/280)
* Added option to use open_dataset in workflow with automatic copying remote data source and reusing/re-opening previusly copied data (constraint-aware)
  [#287](https://github.com/CCI-Tools/cate-core/issues/287)
* Generate unique default ID for local copies of remote data sources (constraint-aware)
  [#277](https://github.com/CCI-Tools/cate-core/issues/277)

## Changes in version 0.9.0.dev6

### Improvements and new Features

* Add use case 2 script [#327](https://github.com/CCI-Tools/cate-core/issues/327)
  and [#146](https://github.com/CCI-Tools/cate-core/issues/146)
* long_term_average, temporal_aggregation, detect_outliers, spatial_subset and plot now work with both - datasets and dataframes.

### Issues Fixed/Resolved

* Date and time columns in CSV data are converted into datetime objects
* Fix use case 6 script
* Fix #320 (wrong file dialog for enso_nino34 operation in GUI)
* Fix temporal coverage for ODP datasets that are listed as a single dataset in the CSW and as multiple in the ESGF

## Changes in version 0.9.0.dev5

### Improvements and new Features

* Unified backend of CLI and GUI on WebSocket [#120](https://github.com/CCI-Tools/cate-core/issues/120)
  As the GUI uses WebSocket, this remove a bit of duplicated code.

### Issues Fixed/Resolved

* Fixed [#309](https://github.com/CCI-Tools/cate-core/issues/309)
* Ensure that our tile size matches the expected tile size: resize and fill in background value.
* Take tile size from dask, this should yield to better performance

## Changes in version 0.9.0.dev4

### Operation Improvements

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


### Issues Fixed/Resolved

* Fixed [#299](https://github.com/CCI-Tools/cate-core/issues/299)
    * renamed property `cate.core.ds.DataSource.name` to `id` 
    * renamed property `cate.core.ds.DataStore.name` to `id` 
    * renamed and changed signature of function `cate.core.ds.DataStore.query_data_sources(..., name=None)` 
      to `find_data_sources(..., id=None, query_expr=None)`
    * changed signature of method `cate.core.ds.DataStore.query(name, ...)` to `query(id=None, query_expr=None, ...)`
    * renamed and changed signature of method `cate.core.ds.DataSource.matches_filter(name)` to `matches(id=None, query_expr=None)`
    * added `title` property to `cate.core.ds.DataStore` and `cate.core.ds.DataSource`
    * made use of the new `id` and `title` properties of both `DataStore` and `DataSource` in their 
      JSON representations.
* Fixed [#294](https://github.com/CCI-Tools/cate-core/issues/294)
* Fixed [#286](https://github.com/CCI-Tools/cate-core/issues/286)
* Fixed [#285](https://github.com/CCI-Tools/cate-core/issues/285)
* Fixed [#283](https://github.com/CCI-Tools/cate-core/issues/283)
* Fixed [#281](https://github.com/CCI-Tools/cate-core/issues/281)
* Fixed [#270](https://github.com/CCI-Tools/cate-core/issues/270)
* Fixed [#273](https://github.com/CCI-Tools/cate-core/issues/273)
* Fixed [#262](https://github.com/CCI-Tools/cate-core/issues/262)
* Fixed [#201](https://github.com/CCI-Tools/cate-core/issues/201)
* Fixed [#223](https://github.com/CCI-Tools/cate-core/issues/223)
* Fixed [#267](https://github.com/CCI-Tools/cate-core/issues/267)
* Fixed a problem with getting the variable statistics for variables with more that 3 dimensions
* Switch CSW to same URL as the ODP
* JSON-RPC protocol changed slightly: method `__cancelJob__` has been renamed to `__cancel__`
  and its only parameter `jobId` became `id`.

## Changes in version 0.9.0.dev3

* Fixed packaging location of file `countries.geojson` so that Cate Desktop can display it

## Changes in version 0.9.0.dev2

### Improvements and new Features

* Added `plot_scatter` and `plot_contour` operations ([#278](https://github.com/CCI-Tools/cate-core/issues/278)).
* Most `plot_` operations now have a new `title` parameter.

### Fixes

* Fixed [#259](https://github.com/CCI-Tools/cate-core/issues/259)
* Fixed problem when the `lon` or `lat` coordinate variables were empty.
  See comments in [#276](https://github.com/CCI-Tools/cate-core/issues/276).

## Changes in version 0.9.0.dev1

* A function annotated by one of the operator decorators (`@op`, `@op_input`, `@op_return`, `@op_output`) 
  will be turned into an *operation registration* which is also callable.
  Calling the *operation registration* will validate all inputs and then pass arguments and 
  keyword-arguments to the actual, original function.
* New `FileLike` type.    
* Changed the JSON object representation of a (xarray/NetCDF-CF) variable to include all variable 
  attributes. This changes the the response of various REST/WebSocket calls that return a workspace.
  
## Changes in version 0.8.0rc7.dev1

* Fixed wrong error message that was raised, when attempting to delete a resource on which other steps 
  still depend on.
* Various documentation updates.

## Changes in version 0.8.0rc6

* `cate.webapi.websocket` now understands the operations 
  `clean_workspace(base_dir)` and `delete_workspace_resource(basedir, res_name)`.
* Fixed [#263](https://github.com/CCI-Tools/cate-core/issues/263)
* Fixed [#257](https://github.com/CCI-Tools/cate-core/issues/257)
