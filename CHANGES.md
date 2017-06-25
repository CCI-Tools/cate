## Changes in version 0.8.0rc7.dev1

* Fixed wrong error message that was raised, when attempting to delete a resource on which other steps 
  still depend on.
* Various documentation updates.

## Changes in version 0.8.0rc6

* `cate.webapi.websocket` now understands the operations 
  `clean_workspace(base_dir)` and `delete_workspace_resource(basedir, res_name)`.
* Fixed https://github.com/CCI-Tools/cate-core/issues/263
* Fixed https://github.com/CCI-Tools/cate-core/issues/257