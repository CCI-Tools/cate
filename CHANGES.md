## Changes in version 0.9.0.dev4

pearson_correlation has been split into two operations.
pearson_correlation_simple that produces a single pair of a correlation
coefficient and a probability value for the given timeseries.
pearson_correlation_map produces a map of correlation coefficients and p_values
and outputs this as a dataset that can be worked with further.

Performance of pearson correlation has been radically improved, the operations can now
accept both, a dataset and a dataframe and a map can be created also by
performing correlation of a single timeseries against all spatial points in the
other dataset.

* Fixed [#285](https://github.com/CCI-Tools/cate-core/issues/285)
* Fixed [#283](https://github.com/CCI-Tools/cate-core/issues/283)
* Fixed [#281](https://github.com/CCI-Tools/cate-core/issues/281)
* Fixed [#270](https://github.com/CCI-Tools/cate-core/issues/270)
* Fixed [#273](https://github.com/CCI-Tools/cate-core/issues/273)
* Fixed [#262](https://github.com/CCI-Tools/cate-core/issues/262)
* Fixed [#201](https://github.com/CCI-Tools/cate-core/issues/201)

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
