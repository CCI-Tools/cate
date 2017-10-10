@echo off

rem Download soil moisture data
cate ds copy esacci.SOILMOISTURE.day.L3S.SSMV.multi-sensor.multi-platform.COMBINED.03-2.r1 --name SOIL_2007 --time "2007-01-01,2007-12-31" --region "72,8,85,17" --vars "sm,sm_uncertainty"

rem Download sea surface temperature data
cate ds copy esacci.SST.day.L4.SSTdepth.multi-sensor.multi-platform.OSTIA.1-1.r1 --name SST_2006_2007 --time "2006-01-01,2007-12-31" --region " -175,-10,-115,10" --vars "analysed_sst,analysis_error"

rmdir /S /Q uc06
mkdir uc06
cd uc06

rem Start interactive session by initialising an empty workspace
cate ws init

rem Open the datasets and assign to resource names
cate res open soil local.SOIL_2007
rem Doesn't work see Issue #257
cate res open sst local.SST_2006_2007

rem Perform temporal aggregation
cate res set soil_mon temporal_aggregation ds=@soil
cate res set sst_mon temporal_aggregation ds=@sst

rem Perform Long term averaging
cate res set sst_lta long_term_average ds=@sst_mon

rem Save the long term average dataset in the current directory
cate res write sst_lta sst_lta.nc

rem Perform ENSO index calculation
cate res set enso enso_nino34 ds=@sst_mon var="analysed_sst" file="sst_lta.nc"

rem Select a point of soil moisture in south of India
cate res set soil_mon_point tseries_point ds=@soil_mon point="78,12" var="sm"

rem Subset the datasets with a one month lag
cate res set soil_jannov subset_temporal ds=@soil_mon_point time_range="2007-01-01,2007-11-01"
cate res set enso_decoct subset_temporal ds=@enso time_range="2006-12-01,2007-10-01"

rem Perform correlation calculation
cate res set corr pearson_correlation_scalar ds_x=@enso_decoct ds_y=@soil_jannov var_x="ENSO N3.4 Index" var_y="sm"
cate res print corr

rem Save and close the workspace
cate ws save
cate ws close
rem Exit the interactive session
cate ws exit --yes

cd ..
