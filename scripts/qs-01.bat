rem Quick-start, variant 1: copy remote datasets to local first

call activate cate

cate ds copy -n OZONE_07 -t 2007-01-01,2007-12-30 esacci.OZONE.mon.L3.NP.multi-sensor.multi-platform.MERGED.fv0002.r1

cate ws new
cate res open oz07 local.OZONE_07
cate ws run plot_map ds=@oz07 var=O3_du_tot file=qs-01-O3_du_tot.png
cate ws exit -y

cate ds copy -n CLOUD_07 -t 2007-01-01,2007-12-30 esacci.CLOUD.mon.L3C.CLD_PRODUCTS.multi-sensor.multi-platform.ATSR2-AATSR.2-0.r1

cate ws new
cate res open cl07 local.CLOUD_07
cate ws run plot_map ds=@cl07 var=cfc file=qs-01-cfc.png
cate ws exit -y

