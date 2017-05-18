rem Quick-start, variant 2: directly access remote datasets (via OPeNDAP)

call activate cate

cate ws new
cate res open oz07 esacci.OZONE.mon.L3.NP.multi-sensor.multi-platform.MERGED.fv0002.r1 2007-01-01 2007-12-30
cate ws run plot_map ds=@oz07 var=O3_du_tot file=qs-02-O3_du_tot.png
cate ws exit -y

cate ws new
cate res open cl07 esacci.CLOUD.mon.L3C.CLD_PRODUCTS.AVHRR.NOAA-17.AVHRR_NOAA.1-0.r1 2007-01-01 2007-12-30
cate ws run plot_map ds=@cl07 var=cc_total file=qs-02-cc_total.png
cate ws exit -y

