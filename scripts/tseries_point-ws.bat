rem activate cate
cate ws del --yes
cate ws new
cate res read ds ..\test\data\precip_and_temp.nc
cate --traceback res set ts tseries_point ds=@ds point=0,0
cate res write ts ts.nc
cate res plot ts -v precipitation
cate res plot ts
cate ws status
cate ws close
