import os
import tempfile
import unittest

from cate.conf.userprefs import get_user_prefs, set_user_prefs

_TEST_USER_PREFS = {
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


class MyTestCase(unittest.TestCase):
    def test_get_user_prefs(self):
        tdir = tempfile.gettempdir()
        tfile = os.path.join(tdir, 'test_get_prefs.txt')
        prefs = get_user_prefs(tfile)

        self.assertDictEqual(_TEST_USER_PREFS, prefs)

        n_prefs = _TEST_USER_PREFS.copy()
        n_prefs['autoUpdateSoftware'] = True

        prefs = set_user_prefs(n_prefs, tfile)

        self.assertDictEqual(n_prefs, prefs)

    def test_set_user_prefs(self):
        self.maxDiff = None
        tdir = tempfile.gettempdir()
        tfile = os.path.join(tdir, 'test_set_prefs.txt')
        prefs = set_user_prefs(_TEST_USER_PREFS, tfile)

        self.assertDictEqual(_TEST_USER_PREFS, prefs)

        n_prefs = _TEST_USER_PREFS.copy()
        n_prefs['autoUpdateSoftware'] = True

        prefs = set_user_prefs(n_prefs)

        self.assertDictEqual(n_prefs, prefs)

        n_prefs = _TEST_USER_PREFS.copy()
        n_prefs['autoUpdateSoftware'] = True

        prefs = set_user_prefs({'autoUpdateSoftware': True})

        self.assertDictEqual(n_prefs, prefs)


if __name__ == '__main__':
    unittest.main()
