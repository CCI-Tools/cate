call activate cate
cate ds make_local esacci.CLOUD.mon.L3C.CLD_PRODUCTS.MODIS.Terra.MODIS_TERRA.1-0.r1 CLOUD_2007 2007
cate ds make_local esacci.OZONE.mon.L3.NP.multi-sensor.multi-platform.MERGED.fv0002.r1 OZONE_2007 2007
mkdir uc09
cd uc09
cate ws init
cate res open cloud local.CLOUD_2007
cate res open ozone local.OZONE_2007
cate res set cloud_sub subset_spatial ds=cloud region=0,30,10,40
cate res set ozone_sub subset_spatial ds=ozone region=0,30,10,40
cate res set cloud_res coregister ds_master=ozone_sub ds_slave=cloud_sub
cate ws save
cate ws close
cd ..
