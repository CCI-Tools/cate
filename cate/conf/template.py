################################################################################
# This is a Cate configuration file.                                           #
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
included_data_sources = [
    'esacci.AEROSOL.day.L3.AAI.multi-sensor.multi-platform.ms_uvai.1-5-7.r1',
    'esacci.AEROSOL.day.L3C.AER_PRODUCTS.AATSR.Envisat.ORAC.03-02.r1',
    'esacci.AEROSOL.day.L3C.AER_PRODUCTS.AATSR.Envisat.SU.4-21.r1',
    'esacci.AEROSOL.day.L3C.AOD.MERIS.Envisat.MERIS_ENVISAT.2-2.r1',
    'esacci.AEROSOL.mon.L3C.AER_PRODUCTS.AATSR.Envisat.ORAC.03-02.r1',
    'esacci.AEROSOL.mon.L3C.AER_PRODUCTS.AATSR.Envisat.SU.4-21.r1',
    'esacci.CLOUD.mon.L3C.CLD_PRODUCTS.AVHRR.multi-platform.AVHRR-AM.2-0.r1',
    'esacci.CLOUD.mon.L3C.CLD_PRODUCTS.AVHRR.multi-platform.AVHRR-PM.2-0.r1',
    'esacci.CLOUD.mon.L3C.CLD_PRODUCTS.MODIS.Aqua.MODIS_AQUA.2-0.r1',
    'esacci.CLOUD.mon.L3C.CLD_PRODUCTS.MODIS.Terra.MODIS_TERRA.2-0.r1',
    'esacci.CLOUD.mon.L3C.CLD_PRODUCTS.multi-sensor.Envisat.MERIS-AATSR.2-0.r1',
    'esacci.CLOUD.mon.L3C.CLD_PRODUCTS.multi-sensor.multi-platform.ATSR2-AATSR.2-0.r1',
    'esacci.FIRE.day.L4.BA.multi-sensor.multi-platform.MERIS.v4-1.r1',
    'esacci.OC.5-days.L3S.CHLOR_A.multi-sensor.multi-platform.MERGED.2-0.r1',
    'esacci.OC.5-days.L3S.IOP.multi-sensor.multi-platform.MERGED.2-0.r1',
    'esacci.OC.5-days.L3S.K_490.multi-sensor.multi-platform.MERGED.2-0.r1',
    'esacci.OC.8-days.L3S.CHLOR_A.multi-sensor.multi-platform.MERGED.2-0.r1',
    'esacci.OC.8-days.L3S.IOP.multi-sensor.multi-platform.MERGED.2-0.r1',
    'esacci.OC.8-days.L3S.K_490.multi-sensor.multi-platform.MERGED.2-0.r1',
    'esacci.OC.day.L3S.CHLOR_A.multi-sensor.multi-platform.MERGED.2-0.r1',
    'esacci.OC.day.L3S.IOP.multi-sensor.multi-platform.MERGED.2-0.1997-r1',
    'esacci.OC.day.L3S.IOP.multi-sensor.multi-platform.MERGED.2-0.1998-r1',
    'esacci.OC.day.L3S.IOP.multi-sensor.multi-platform.MERGED.2-0.1999-r1',
    'esacci.OC.day.L3S.IOP.multi-sensor.multi-platform.MERGED.2-0.2000-r1',
    'esacci.OC.day.L3S.IOP.multi-sensor.multi-platform.MERGED.2-0.2001-r1',
    'esacci.OC.day.L3S.IOP.multi-sensor.multi-platform.MERGED.2-0.2002-r1',
    'esacci.OC.day.L3S.IOP.multi-sensor.multi-platform.MERGED.2-0.2003-r1',
    'esacci.OC.day.L3S.IOP.multi-sensor.multi-platform.MERGED.2-0.2004-r1',
    'esacci.OC.day.L3S.IOP.multi-sensor.multi-platform.MERGED.2-0.2005-r1',
    'esacci.OC.day.L3S.IOP.multi-sensor.multi-platform.MERGED.2-0.2006-r1',
    'esacci.OC.day.L3S.IOP.multi-sensor.multi-platform.MERGED.2-0.2007-r1',
    'esacci.OC.day.L3S.IOP.multi-sensor.multi-platform.MERGED.2-0.2008-r1',
    'esacci.OC.day.L3S.IOP.multi-sensor.multi-platform.MERGED.2-0.2009-r1',
    'esacci.OC.day.L3S.IOP.multi-sensor.multi-platform.MERGED.2-0.2010-r1',
    'esacci.OC.day.L3S.IOP.multi-sensor.multi-platform.MERGED.2-0.2011-r1',
    'esacci.OC.day.L3S.IOP.multi-sensor.multi-platform.MERGED.2-0.2012-r1',
    'esacci.OC.day.L3S.IOP.multi-sensor.multi-platform.MERGED.2-0.2013-r1',
    'esacci.OC.day.L3S.K_490.multi-sensor.multi-platform.MERGED.2-0.r1',
    'esacci.OC.mon.L3S.CHLOR_A.multi-sensor.multi-platform.MERGED.2-0.r1',
    'esacci.OC.mon.L3S.IOP.multi-sensor.multi-platform.MERGED.2-0.r1',
    'esacci.OC.mon.L3S.K_490.multi-sensor.multi-platform.MERGED.2-0.r1',
    'esacci.OZONE.mon.L3.NP.multi-sensor.multi-platform.MERGED.fv0002.r1',
    'esacci.SOILMOISTURE.day.L3S.SSMS.multi-sensor.multi-platform.ACTIVE.03-2.r1',
    'esacci.SOILMOISTURE.day.L3S.SSMV.multi-sensor.multi-platform.COMBINED.03-2.r1',
    'esacci.SOILMOISTURE.day.L3S.SSMV.multi-sensor.multi-platform.PASSIVE.03-2.r1',
    'esacci.SST.day.L4.SSTdepth.multi-sensor.multi-platform.OSTIA.1-1.r1'
]
# We put wildcards here that match all data sources that are known NOT to work in GUI
excluded_data_sources = [
    # Exclude datasets that usually take too long to download or cannot be easily aggregated
    # e.g.
    # 'esacci.*.day.*',
    # 'esacci.*.satellite-orbit-frequency.*',
    # Exclude Land Cover CCI, see issues #361, #364, #371
    'esacci.LC.*',
]


# Configure any default variables of a dataset that will be initially selected and displayed first.
# 'default_display_variables' is a list comprising variable name sets. Each set may represent
# multiple similar datasets.
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
