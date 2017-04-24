call activate cate
set CATE_DISABLE_WEB_TESTS=1
set CATE_DISABLE_PLOT_TESTS=1
set CATE_DISABLE_GEOPANDAS_TESTS=1
py.test test
rem py.test test\core\test_workspace.py
rem py.test test/cli/test_cli.py
