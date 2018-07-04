################################################################################
# This is a Cate configuration file.                                           #
#                                                                              #
# If this file is "~/.cate/conf.py", it is the active Cate configuration.      #
#                                                                              #
# If this file is named "conf.py.template" you can rename it and move          #
# it "~/.cate/conf.py" to make it the active Cate configuration file.          #
#                                                                              #
# As this is a regular Python script, you may use any Python code to compute   #
# the settings provided here.                                                  #
#                                                                              #
# Please find the configuration template for a given Cate VERSION at           #
#   https://github.com/CCI-Tools/cate/blob/vVERSION/cate/conf/template.py      #
# For example:                                                                 #
#   https://github.com/CCI-Tools/cate/blob/v2.0.0.dev4/cate/conf/template.py   #
################################################################################


# 'data_stores_path' is denotes a directory where Cate stores information about data stores and also saves
# local data files synchronized with their remote versions.
# Use the tilde '~' (also on Windows) within the path to point to your home directory.
#
# data_stores_path = '~/.cate/data_stores'

# 'dataset_persistence_format' names the data format to be used when persisting datasets in the workspace.
# Possible values are 'netcdf4' or 'zarr'.
# dataset_persistence_format = 'netcdf4'

# If 'use_workspace_imagery_cache' is True, Cate will maintain a per-workspace
# cache for imagery generated from dataset variables. Such cache can accelerate
# image display, however at the cost of disk space.
#
# use_workspace_imagery_cache = False

# Default prefix for names generated for new workspace resources originating from opening data sources
# or executing workflow steps.
# This prefix is used only if no specific prefix is defined for a given operation.
# default_res_pattern = 'res_{index}'

# User defined HTTP proxy settings, will replace one stored in System environment variable 'http_proxy'
# Accepted proxy details formats:
#   'http://user:password@host:port'
#   'https://user:password@host:port'
#   'http://host:port'
#   'https://host:port'
# http_proxy =

# Include/exclude data sources (currently effective in Cate Desktop GUI only, not used by API, CLI).
#
# If 'included_data_sources' is a list, its entries are expected to be wildcard patterns for the identifiers of data
# sources to be included. By default, or if 'included_data_sources' is None, all data sources are included.
# If 'excluded_data_sources' is a list, its entries are expected to be wildcard patterns for the identifiers of data
# sources to be excluded. By default, or if 'excluded_data_sources' is None, no data sources are excluded.
# If both 'included_data_sources' and 'excluded_data_sources' are lists, we first include data sources using
# 'included_data_sources' then remove entries that match any result from applying 'excluded_data_sources'.
#
# We put wildcards here that match all data sources that are known to work in GUI
# included_ds_ids = []

# We put wildcards here that match all data sources that are known NOT to work in GUI
excluded_ds_ids = [
    # Exclude datasets that usually take too long to download or cannot be easily aggregated
    # e.g.
    # 'esacci.*.day.*',
    # 'esacci.*.satellite-orbit-frequency.*',
    # 'esacci.LC.*',
]


# Configure names of variables that will be initially selected once a new
# dataset resource is opened in the GUI.
# default_variables = {
#     'cfc',             # Cloud CCI
#     'lccs_class',      # Land Cover CCI
#     'analysed_sst',    # Sea Surface Temperature CCI
# }


# Configure / overwrite default variable display settings as used in various plot_<type>() operations
# and in the Cate Desktop GUI.
# Each entry maps a variable name to a dictionary with the following entries:
#    color_map   - name of a color map taken from from https://matplotlib.org/examples/color/colormaps_reference.html
#    display_min - minimum variable value that corresponds to the lower end of the color map
#    display_max - maximum variable value that corresponds to the upper end of the color map
#
# variable_display_settings = {
#     'my_var': dict(color_map='viridis', display_min=0.1, display_max=0.8),
# }


# Default color map to be used for any variable not configured in 'variable_display_settings'
# 'default_color_map' must be the name of a color map taken from from
# https://matplotlib.org/examples/color/colormaps_reference.html
# default_color_map = 'jet'
default_color_map = 'inferno'
