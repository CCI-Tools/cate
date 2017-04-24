call activate cate

cate ws new
cate res open cl07 esacci.CLOUD.mon.L3C.CLD_PRODUCTS.AVHRR.NOAA-17.AVHRR_NOAA.1-0.r1 2007-01-01 2007-12-30
cate res open oz07 esacci.OZONE.mon.L3.NP.multi-sensor.multi-platform.MERGED.fv0002.r1 2007-01-01 2007-12-30
cate res set cc_tot select_var ds=@cl07 var=cc_total
cate res set oz_tot select_var ds=@oz07 var=O3_du_tot
cate ws run plot_map ds=@cc_tot var=cc_total file=fig1.png
cate ws run plot_map ds=@oz_tot var=O3_du_tot file=fig2.png
cate ws exit