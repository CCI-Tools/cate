import os.path

from cate.core.conf import DEFAULT_DATA_PATH

SCRATCH_WORKSPACES_DIR_NAME = 'scratch-workspaces'
SCRATCH_WORKSPACES_PATH = os.path.join(DEFAULT_DATA_PATH, SCRATCH_WORKSPACES_DIR_NAME)

WORKSPACE_DATA_DIR_NAME = '.cate-workspace'
WORKSPACE_WORKFLOW_FILE_NAME = 'workflow.json'

# {{cate-config}}
# allow a 100 ms period between two progress messages sent to the client
WEBAPI_PROGRESS_DEFER_PERIOD = 0.5

# {{cate-config}}
# allow two minutes timeout for any synchronous workspace I/O
WEBAPI_WORKSPACE_TIMEOUT = 2 * 60.0

# {{cate-config}}
# allow one hour timeout for any synchronous workflow resource processing
WEBAPI_RESOURCE_TIMEOUT = 60 * 60.0

# {{cate-config}}
# allow one hour extra timeout for matplotlib to block the WebAPI service's main thread by showing a Qt window
WEBAPI_PLOT_TIMEOUT = 60 * 60.0

# {{cate-config}}
# By default, WebAPI service will auto-exit after 2 hours of inactivity (if caller='cate', the CLI)
WEBAPI_ON_INACTIVITY_AUTO_EXIT_AFTER = 120 * 60.0

# {{cate-config}}
# By default, WebAPI service will auto-exit after 5 seconds if all workspaces are closed (if caller='cate', the CLI)
WEBAPI_ON_ALL_CLOSED_AUTO_EXIT_AFTER = 5.0

# {{cate-config}}
WEBAPI_LOG_FILE = os.path.join(DEFAULT_DATA_PATH, 'webapi.log')
