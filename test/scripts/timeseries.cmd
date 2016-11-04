cate ws del --yes
cate ws new
cate res read X test/ui/precip_and_temp.nc
cate res set Y tseries_point ds=X lat=0 lon=0
cate --traceback res plot Y -v temperature
cate ws save
cate ws close


