.. _Matplotlib Color Maps Reference: https://matplotlib.org/examples/color/colormaps_reference.html


=============
Configuration
=============


Configuration file
------------------

Cate's configuration settings are read from ``.cate/conf.py`` located in the current user's home directory.

Given here is an overview of the possible configuration settings:

:``data_stores_path``:
    Directory where Cate stores information about data stores and also saves local data files synchronized with their
    remote versions. Use the tilde '~' (also on Windows) within the path to point to your home directory.
    This directory can become rather populated once after a while and it is advisable to place it where there exists
    a high transfer rate and sufficient capacity. Ideally, you would let it point to a dedicated solid state disc (SSD).
    The default value for ``data_stores_path`` is the ``~/.cate/data_stores`` directory.

:``dataset_persistence_format``:
    Names the data format to be used when persisting datasets in the workspace.
    Possible values are 'netcdf4' or 'zarr' (much faster, but still experimental).

:``use_workspace_imagery_cache``:
    If set to ``True``, Cate will maintain a per-workspace
    cache for imagery generated from dataset variables. Such cache can accelerate
    image display, however at the cost of disk space.

:``default_res_pattern``:
    Default prefix for names generated for new workspace resources originating from opening data sources
    or executing workflow steps.
    This prefix is used only if no specific prefix is defined for a given operation.

:``included_ds_ids``:
    If ``included_ds_ids`` is a list, its entries are expected to be wildcard patterns for the identifiers of data
    sources to be included. By default, or if ``included_ds_ids`` is None, all data sources are included.

:``excluded_ds_ids``:
    If ``excluded_ds_ids`` is a list, its entries are expected to be wildcard patterns for the identifiers of data
    sources to be excluded. By default, or if ``excluded_ds_ids`` is None, no data sources are excluded.
    If both ``included_ds_ids`` and ``excluded_ds_ids`` are lists, we first include data sources using
    ``included_ds_ids`` then remove entries that match any result from applying ``excluded_data_sources``.

:``default_variables``:
     Configure names of variables that will be initially selected once a new
     dataset resource is opened in the GUI. Its value must be a set
     (``{...}``) of variable names.

:``variable_display_settings``:
    Configure / overwrite default variable display settings as used in various plot_<type>() operations
    and in the Cate Desktop GUI.
    Each entry maps a variable name to a dictionary with the following entries:
    * ``color_map``   - name of a color map taken from from `Matplotlib Color Maps Reference`_
    * ``display_min`` - minimum variable value that corresponds to the lower end of the color map
    * ``display_max`` - maximum variable value that corresponds to the upper end of the color map

    For example:::

        variable_display_settings = {
            'my_var': dict(color_map='viridis', display_min=0.1, display_max=0.8),
        }

:``default_color_map``:
    Default color map to be used for any variable not configured in 'variable_display_settings'
    'default_color_map' must be the name of a color map taken from from `Matplotlib Color Maps Reference`_.
    If not specified, the ultimate default is ``'inferno'``.


Environment variables
---------------------

:``CATE_ESA_CCI_ODP_DATA_STORE_PATH``:
    Overrides the location of the ESA CCI ODP data store directory whose parent directory would otherwise be given
    by the ``data_stores_path`` configuration setting.


:``CATE_LOCAL_DATA_STORE_PATH``:
    Overrides the location of the local data store directory whose parent directory would otherwise be given
    by the ``data_stores_path`` configuration setting.


:``HTTP_PROXY``, ``http_proxy``, ``HTTPS_PROXY``, ``https_proxy``, ``SOCKS_PROXY``, ``socks_proxy``:
    Recognized proxy server hosts.

:``NO_PROXY``, ``no_proxy``:
    Comma-separated lists of hosts that should bypass the proxy server.

