@echo off

rem Download Data. Select variables, region and time right away to save bandwith
cate ds copy esacci.SST.day.L4.SSTdepth.multi-sensor.multi-platform.OSTIA.1-1.r1 --name "SST_polar_2007" --time "2007-01-01,2007-01-15" --vars "analysed_sst, sea_ice_fraction" --region " -180, 60, 180, 90"
cate ds copy esacci.CLOUD.mon.L3C.CLD_PRODUCTS.AVHRR.multi-platform.AVHRR-AM.2-0.r1 --name "CLOUDS_polar_2007" --time "2007-01-01,2007-01-31" --vars "cfc, cee" --region " -180, 60, 180, 90"
cate ds copy esacci.AEROSOL.mon.L3.AAI.multi-sensor.multi-platform.ms_uvai.1-5-7.r1 --name "AEROSOL_polar_2007" --time "2007-01-01,2007-03-31" --region " -180, 60, 180, 90"

rmdir /S /Q uc11
mkdir uc11
cd uc11

rem Start interactive session by initialising an empty workspace
rem This will write a hidden directory .\.cate-workspace
cate ws init

rem Open the datasets
cate res open sst local.SST_polar_2007
cate res open clouds local.CLOUDS_polar_2007
cate res open aerosol local.AEROSOL_polar_2007

rem Create scalable map plots of the datasets
cate res set plot1 plot_map ds=@sst var="sea_ice_fraction" region="-180,60,180,90" projection="NorthPolarStereo" contour_plot=True file="sst_polar.svg"
cate res set plot2 plot_map ds=@clouds var="cfc" region="-180,60,180,90" projection="NorthPolarStereo" file="clouds_polar.svg"
cate res set plot3 plot_map ds=@aerosol region="-180,60,180,90" var="absorbing_aerosol_index" projection="NorthPolarStereo" file="aerosol_polar.svg"

rem Save the workspace
cate ws save
rem Close the workspace
cate ws close
rem Exit interactive session. Don't ask, answer is always "yes".
cate ws exit --yes

cd ..