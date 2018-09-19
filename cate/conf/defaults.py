# The MIT License (MIT)
# Copyright (c) 2016, 2017 by the ESA CCI Toolbox development team and contributors
#
# Permission is hereby granted, free of charge, to any person obtaining a copy of
# this software and associated documentation files (the "Software"), to deal in
# the Software without restriction, including without limitation the rights to
# use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies
# of the Software, and to permit persons to whom the Software is furnished to do
# so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

__author__ = "Norman Fomferra (Brockmann Consult GmbH)"

import os.path

from ..version import __version__

DEFAULT_DATA_DIR_NAME = '.cate'
DEFAULT_DATA_PATH = os.path.join(os.path.expanduser('~'), DEFAULT_DATA_DIR_NAME)
DEFAULT_VERSION_DATA_PATH = os.path.join(DEFAULT_DATA_PATH, __version__)

GLOBAL_CONF_FILE = os.path.join(DEFAULT_DATA_PATH, 'conf.py')
VERSION_CONF_FILE = os.path.join(DEFAULT_VERSION_DATA_PATH, 'conf.py')
LOCAL_CONF_FILE = 'cate-conf.py'
LOCATION_FILE = 'cate.location'

SCRATCH_WORKSPACES_DIR_NAME = 'scratch'
SCRATCH_WORKSPACES_PATH = os.path.join(DEFAULT_DATA_PATH, SCRATCH_WORKSPACES_DIR_NAME)

WORKSPACE_CACHE_DIR_NAME = '.cate-cache'
WORKSPACE_DATA_DIR_NAME = '.cate-workspace'
WORKSPACE_WORKFLOW_FILE_NAME = 'workflow.json'

DEFAULT_RES_PATTERN = 'res_{index}'

NETCDF_COMPRESSION_LEVEL = 9

_ONE_MIB = 1024 * 1024
_ONE_GIB = 1024 * _ONE_MIB

#: The data format to be used when persisting datasets in the workspace.
DATASET_PERSISTENCE_FORMAT = 'netcdf4'

#: Use a per-workspace file imagery cache, see REST "/res/tile/" API
WEBAPI_USE_WORKSPACE_IMAGERY_CACHE = False

# The number of bytes in a workspace's image file cache
WEBAPI_WORKSPACE_FILE_TILE_CACHE_CAPACITY = 1 * _ONE_GIB

# The number of bytes in a workspace's image in-memory cache
WEBAPI_WORKSPACE_MEM_TILE_CACHE_CAPACITY = 256 * _ONE_MIB

#: where the information about a running WebAPI service is stored
WEBAPI_INFO_FILE = os.path.join(DEFAULT_VERSION_DATA_PATH, 'webapi.json')

#: where a running WebAPI service logs to
WEBAPI_LOG_FILE_PREFIX = os.path.join(DEFAULT_VERSION_DATA_PATH, 'webapi.log')

#: allow a 100 ms period between two progress messages sent to the client
WEBAPI_PROGRESS_DEFER_PERIOD = 0.5

#: allow two minutes timeout for any synchronous workspace I/O
WEBAPI_WORKSPACE_TIMEOUT = 2 * 60.0

#: allow one hour timeout for any synchronous workflow resource processing
WEBAPI_RESOURCE_TIMEOUT = 60 * 60.0

#: allow one hour extra timeout for matplotlib to block the WebAPI service's main thread by showing a Qt window
WEBAPI_PLOT_TIMEOUT = 60 * 60.0

#: By default, WebAPI service will auto-exit after 2 hours of inactivity, if WebAPI auto-exit enabled
WEBAPI_ON_INACTIVITY_AUTO_STOP_AFTER = 120 * 60.0

#: By default, WebAPI service will auto-exit after 5 seconds if all workspaces are closed, if WebAPI auto-exit enabled
WEBAPI_ON_ALL_CLOSED_AUTO_STOP_AFTER = 5.0


DEFAULT_VARIABLES = {
    'AAOD550_mean',             # esacci.AEROSOL.*.L3C.AER_PRODUCTS.*
    'absorbing_aerosol_index',  # esacci.AEROSOL.*.L3.AAI.*
    'AOD550',                   # esacci.AEROSOL.*.L3C.AOD.*
    'cfc',                      # esacci.CLOUD.*.L3C.CLD_PRODUCTS.*
    'burned_area',              # esacci.FIRE.*.L4.BA.*
    'lccs_class',               # esacci.LC.L4.LCCS.*
    'atot_490',                 # esacci.OC.*.L3S.IOP.*, esacci.OC.*.L3S.OC_PRODUCTS.*
    'chlor_a',                  # esacci.OC.*.L3S.CHLOR_A.*, esacci.OC.*.L3S.OC_PRODUCTS.*
    'kd_490',                   # esacci.OC.*.L3S.K_490.*
    'O3_du_tot',                # esacci.OZONE.*.L3.NP.*
    'atmosphere_mole_content_of_ozone',  # esacci.OZONE.*.L3S.TC.*
    'Rrs_490',                  # esacci.OC.*.L3S.RRS.*
    'local_msl_trend',          # esacci.SEALEVEL.*.IND.MSL.*
    'sm',                       # esacci.SOILMOISTURE.*.L3S.SSMS.*
    'sea_surface_temperature',  # esacci.SST.*.L3U.SSTskin.*
    'analysed_sst',             # esacci.SST.*.L4.SSTdepth.*
}


VARIABLE_DISPLAY_SETTINGS = {
    # LC CCI
    'lccs_class': dict(color_map='land_cover_cci'),

    # OC CCI
    'kd_490': dict(color_map="bwr", display_min=0.0, display_max=0.5),
    'kd_490_bias': dict(display_min=-0.02, display_max=0.07),
    'kd_490_rmsd': dict(display_min=0.0, display_max=0.25),
    'total_nobs_sum': dict(display_min=1, display_max=500),
    'MERIS_nobs_sum': dict(display_min=1, display_max=500),
    'MODISA_nobs_sum': dict(display_min=1, display_max=500),
    'SeaWiFS_nobs_sum': dict(display_min=1, display_max=500),

    # Cloud CCI
    'cfc': dict(color_map="bone", display_min=0, display_max=1),

    # SST CCI
    'analysed_sst': dict(color_map="jet", display_min=270., display_max=310.),
    'analysis_error': dict(display_min=0., display_max=3.),
    'mask': dict(display_min=0, display_max=9),
    'sea_ice_fraction': dict(display_min=0., display_max=1.),
    'sea_ice_fraction_error': dict(display_min=0., display_max=0.2),

    # Aerosol CCI
    'absorbing_aerosol_index': dict(color_map="bwr", display_min=-2, display_max=2),
    'solar_zenith_angle': dict(color_map="bwr", display_min=35, display_max=80),
    'number_of_observations': dict(color_map="gray", display_min=0, display_max=150),

    # OZONE CCI
    'O3_du': dict(display_min=3, display_max=20),
    'O3_du_tot': dict(display_min=220, display_max=480),
    'O3_ndens': dict(display_min=1.5e11, display_max=1e12),
    'O3_vmr': dict(display_min=0.006, display_max=0.045),
    'O3e_du': dict(display_min=0, display_max=2),
    'O3e_du_tot': dict(display_min=0, display_max=2),
    'O3e_ndens': dict(display_min=9e9, display_max=1e11),
    'O3e_vmr': dict(display_min=0, display_max=0.005),
    'surface_pressure': dict(display_min=700, display_max=1010),

    # Fire CCI
    'burned_area': dict(color_map="hot", display_min=0, display_max=300000000),

    # Sea Level
    'ampl': dict(color_map="YlOrRd", display_min=0., display_max=0.12),
    'phase': dict(color_map="hsv", display_min=0., display_max=360.),
    'sla': dict(color_map="bwr", display_min=-0.2, display_max=0.2),
    'local_msl_trend': dict(color_map="coolwarm", display_min=-12., display_max=12.),
    'local_msl_trend_error': dict(color_map="afmhot", display_min=0., display_max=5.),
}

DEFAULT_COLOR_MAP = 'inferno'
