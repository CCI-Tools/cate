@echo off

set DS1=local.2010.AEROSOL.mon.L3.AAI.multi-sensor.multi-platform.ms_uvai.1-5-7.r1
set DS2=local.2010.CLOUD.mon.L3C.CLD_PRODUCTS.multi-sensor.multi-platform.ATSR2-AATSR.2-0.r1

cate run ^
   -o ds1=%DS1% ^
   -o ds2=%DS2% ^
   -w uc09-wf-corr.nc ^
   .\uc09-wf.json ^
   ds_x=ds1 ^
   ds_y=ds2 ^
   var_x=absorbing_aerosol_index ^
   var_y=cfc ^
   region=-20,20,30,70