.. _Matplotlib Color Maps Reference: https://matplotlib.org/examples/color/colormaps_reference.html


=============
Configuration
=============


Cate's configuration file is called ``conf.py`` and is located in the ``~/.cate/1.0.0`` directory, where ``~`` is
the current user's home directory.

Given here is an overview of the possible configuration parameters:

:``data_stores_path``:
    Directory where Cate stores information about data stores and also saves local data files synchronized with their
    remote versions. Use the tilde '~' (also on Windows) within the path to point to your home directory.
    This directory can become rather populated once after a while and it is advisable to place it where there exists
    a high transfer rate and sufficient capacity. Ideally, you would let it point to a dedicated solid state disc (SSD).
    The default value for ``data_stores_path`` is the ``~/.cate/data_stores`` directory.

:``use_workspace_imagery_cache``:
    If set to ``True``, Cate will maintain a per-workspace
    cache for imagery generated from dataset variables. Such cache can accelerate
    image display, however at the cost of disk space.

:``included_data_sources``:
    If ``included_data_sources`` is a list, its entries are expected to be wildcard patterns for the identifiers of data
    sources to be included. By default, or if 'included_data_sources' is None, all data sources are included.

:``excluded_data_sources``:
    If ``excluded_data_sources`` is a list, its entries are expected to be wildcard patterns for the identifiers of data
    sources to be excluded. By default, or if 'excluded_data_sources' is None, no data sources are excluded.
    If both ``included_data_sources`` and ``excluded_data_sources`` are lists, we first include data sources using
    ``included_data_sources`` then remove entries that match any result from applying ``excluded_data_sources``.

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
