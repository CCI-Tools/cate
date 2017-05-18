call activate cate

rem Download some CCI Cloud data
cate ds copy esacci.CLOUD.mon.L3C.CLD_PRODUCTS.MODIS.Terra.MODIS_TERRA.1-0.r1 --name CLOUD_2007 --time 2007,2008
rem Download some CCI Ozone data
cate ds copy esacci.OZONE.mon.L3.NP.multi-sensor.multi-platform.MERGED.fv0002.r1 --name OZONE_2007 --time 2007,2008

rmdir /S /Q uc09
mkdir uc09
cd uc09

rem Start interactive session by initialising an empty workspace
rem This will write a hidden directory .\.cate-workspace
cate ws init

rem Open the Cloud and Ozone datasets and assign it to resources named "cloud" and "ozone"
cate res open cloud local.CLOUD_2007
cate res open ozone local.OZONE_2007

rem Create subsets of the "cloud" and "ozone" resources and assign it
rem to new resources named "cloud_sub" and "ozone_sub"
cate res set cloud_sub subset_spatial ds=@cloud region=0,30,10,40
cate res set ozone_sub subset_spatial ds=@ozone region=0,30,10,40

rem Coregister "cloud_sub" with "ozone_sub" and call the result "cloud_res"
cate res set cloud_res coregister ds_master=@ozone_sub ds_slave=@cloud_sub

rem Save the workspace
cate ws save
rem Close the workspace
cate ws close
rem Exit interactive session. Don't ask, answer is always "yes".
cate ws exit --yes

cd ..
