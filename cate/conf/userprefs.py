import json
import logging
import os

from cate.conf import USER_PREFERENCES_FILE, DEFAULT_DATA_PATH

_DEFAULT_USER_PREFS = {
    "reopenLastWorkspace": True,
    "lastWorkspacePath": "",
    "autoUpdateSoftware": True,
    "autoShowNewFigures": True,
    "offlineMode": False,
    "showSelectedVariableLayer": True,
    "savedLayers": {},
    "selectedDataStoreId": "",
    "selectedDataSourceId": None,
    "dataSourceFilterExpr": "",
    "selectedOperationName": None,
    "operationFilterTags": [],
    "operationFilterExpr": "",
    "dataSourceListHeight": 200,
    "showDataSourceDetails": True,
    "resourceListHeight": 100,
    "showResourceDetails": True,
    "workflowStepListHeight": 100,
    "showWorkflowStepDetails": True,
    "operationListHeight": 200,
    "showOperationDetails": True,
    "variableListHeight": 200,
    "showVariableDetails": True,
    "layerListHeight": 160,
    "showLayerDetails": True,
    "panelContainerUndockedMode": False,
    "leftPanelContainerLayout": {
        "horPos": 300,
        "verPos": 400
    },
    "rightPanelContainerLayout": {
        "horPos": 300,
        "verPos": 400
    },
    "selectedLeftTopPanelId": "dataSources",
    "selectedLeftBottomPanelId": "operations",
    "selectedRightTopPanelId": "workspace",
    "selectedRightBottomPanelId": "variables",
    "placemarkCollection": {
        "type": "FeatureCollection",
        "features": []
    },
    "selectedPlacemarkId": None,
    "placemarkListHeight": 160,
    "showPlacemarkDetails": True,
    "defaultPlacemarkStyle": {
        "markerSize": "small",
        "markerColor": "#ff0000",
        "markerSymbol": "",
        "fill": "#0000ff",
        "fillOpacity": 0.5,
        "stroke": "#ffff00",
        "strokeOpacity": 0.5,
        "strokeWidth": 1
    },
    "workspacePanelMode": "steps",
    "showDataStoreDescription": False,
    "showDataStoreNotices": True,
    "showDataSourceIDs": True,
    "showLayerTextOverlay": True,
    "debugWorldView": False,
    "styleContext": "entity",
    "backendConfig": {
        "dataStoresPath": "~/.cate/data_stores",
        "useWorkspaceImageryCache": False,
        "resourceNamePattern": "res_{index}",
        "proxyUrl": None
    }
}

_LOG = logging.getLogger('cate')


def _write_user_prefs_file(user_prefs_file: str, user_prefs: dict):
    try:
        with open(user_prefs_file, 'w') as fp:
            json.dump(user_prefs, fp)
    except Exception as error:
        _LOG.warning('failed writing %s: %s' % (user_prefs_file, str(error)))


def _read_user_prefs(user_prefs_file: str) -> dict:
    user_prefs = dict()
    if user_prefs_file and os.path.isfile(user_prefs_file):
        with open(user_prefs_file, 'r') as pfile:
            user_prefs = json.load(pfile)

    return user_prefs


def set_user_prefs(prefs: dict, user_prefs_file: str = None):
    if not user_prefs_file:
        user_prefs_file = os.path.join(DEFAULT_DATA_PATH, USER_PREFERENCES_FILE)

    if not os.path.isfile(user_prefs_file):
        _write_user_prefs_file(user_prefs_file, _DEFAULT_USER_PREFS)
    else:
        _prefs = get_user_prefs(user_prefs_file)
        _prefs.update(prefs)
        _write_user_prefs_file(user_prefs_file, _prefs)


def get_user_prefs(user_prefs_file: str = None) -> dict:
    if not user_prefs_file:
        user_prefs_file = os.path.join(DEFAULT_DATA_PATH, USER_PREFERENCES_FILE)

    if not os.path.isfile(user_prefs_file):
        _write_user_prefs_file(user_prefs_file, _DEFAULT_USER_PREFS)

    return _read_user_prefs(user_prefs_file)
