@echo off
rem This script demonstrates
rem - how to extract a time series of data
rem - how to write a netCDF file
rem - how to bring up (matplotlib) plots in interactive mode

rem activate cate

rem delete existing workspace (if any).
cate ws del --yes
rem Note that most "cate ws" commands will enter cate's interactive workspace mode.
rem In this mode, a background process is keeping the resources used by workspaces alive.

rem Create a new in-memory workspace.
rem No workspace files will created until the workspace is saved.
cate ws new

rem Read a netCDF file containing precipitation & temperature with (time,lat,lon) dimensions
rem and assign the resulting dataset to resource "pt_ds".
cate res read pt_ds ..\test\data\precip_and_temp.nc
rem The "cate res read" command adds a new operation step to the workspace's workflow:
rem    pt_ds = read_object(file='..\test\data\precip_and_temp.nc', format=None)
rem Note that the command
rem    cate res read NAME FILE [FORMAT]
rem is a short form for the "cate res set" command:
rem    cate res set NAME read_object file=FILE [format=FORMAT]

rem Extract a time series and remember the result in resource "ts".
cate res set ts tseries_point ds=@pt_ds point=0,0
rem The "cate res set" command adds a new operation step to the workspace's workflow:
rem    ts = tseries_point(ds=pt_ds, point=(0, 0))
rem Please note the "@" prefix in "ds=@ds". This indicates that the value of
rem the resource named "pt_ds" is assigned to the parameter "ds" required by the
rem tseries_point() operation. Without the "@", the pt_ds would have been interpreted
rem as the character string "pt_ds".

rem Write a netCDF file.
cate res write ts ts.nc
rem The "cate res write" command does NOT add a new step to the workflow.
rem To do so, you would have used "cate res set" with the write_object() operation:
rem    cate res set out write_object obj=@ts file=ts.nc
rem which would add the step:
rem    out = write_object(obj=ts, file='ts.nc')
rem Note that the command
rem    cate res write DS FILE [FORMAT]
rem is a short form for the "cate ws run" command
rem    cate ws run write_object obj=@DS file=FILE [format=FORMAT]

rem Plot a variable of a dataset.
cate res plot ts
rem The "cate res plot" command does NOT add a new step to the workflow.
rem To do so, you would have used "cate res set" with the plot() operation:
rem    cate res set plt plot ds=@ts file='ts_plot.png'
rem Note that the command
rem    cate res plot DS -o FILE
rem is a short form for the "cate ws run" command
rem    cate ws run plot ds=@DS file=FILE

rem Plot individual variables
cate res plot ts -v temperature
cate res plot ts -v precipitation

rem Print current workspace status
cate ws status

rem We use "cate ws exit" to exit interactive workspace mode and terminate
rem a cate back-end process that is keeping the resource's state.
cate ws exit --yes
rem You could use "cate ws close" to close current workspace,
rem but stay in interactive workspace mode. In this case, a Cate
rem back-end process remains active in the background for a certain
rem amount of time.

