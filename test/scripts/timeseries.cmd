ect ws del --yes
ect ws new
ect res read X test/ui/precip_and_temp.nc
ect res set Y tseries_point ds=X lat=0 lon=0
ect --traceback res plot Y -v temperature
ect ws save
ect ws close


