#!/usr/bin/env bash

# Download some CCI Cloud data
cate ds copy esacci.CLOUD.mon.L3C.CLD_PRODUCTS.multi-sensor.multi-platform.ATSR2-AATSR.2-0.r1 --name CLOUD_2007 --time '2007-01-01,2007-03-31'
# Download some CCI Ozone data
cate ds copy esacci.OZONE.mon.L3.NP.multi-sensor.multi-platform.MERGED.fv0002.r1 --name OZONE_2007 --time '2007-01-01,2007-03-31'

rm -rf uc09/
mkdir uc09
cd uc09

# Start interactive session by initialising an empty workspace
# This will write a hidden directory .\.cate-workspace
cate ws init

# Open the Cloud and Ozone datasets and assign it to resources named "cloud" and "ozone"
cate res open cloud local.CLOUD_2007
cate res open ozone local.OZONE_2007

# Select the desired ECVs from the full datasets
cate res set ozone_tot select_var ds=@ozone var='O3_du_tot'
cate res set cloud_cfc select_var ds=@cloud var='cfc'

# Coregister "ozone_tot" with "cloud_cfc" and call the result "ozone_coreg"
cate res set ozone_coreg coregister ds_master=@cloud_cfc ds_slave=@ozone_tot

# Create subsets of the "cloud_cfc" and "ozone_coreg" resources and assign it
# to new resources named "cloud_sub" and "ozone_sub"
cate res set cloud_sub subset_spatial ds=@cloud_cfc region=0,30,10,40
cate res set ozone_sub subset_spatial ds=@ozone_coreg region=0,30,10,40

# Produce a correlation map
cate res set corr pearson_correlation ds_x=@ozone_sub ds_y=@cloud_sub var_x=O3_du_tot var_y=cfc
cate res write corr corr.nc
cate res print corr

# Extract mean timeseries and produce a scalar correlation
cate res set cloud_point tseries_point ds=@cloud_sub point=50,50
cate res set ozone_point tseries_point ds=@ozone_sub point=50,50
cate res set corr_scalar pearson_correlation_scalar ds_x=@ozone_point ds_y=@cloud_point var_x=O3_du_tot var_y=cfc
cate res write corr_scalar corr_scalar.txt
cate res print corr_scalar

# Save the workspace
cate ws save
# Close the workspace
cate ws close
# Exit interactive session. Don't ask, answer is always "yes".
cate ws exit --yes

cd ..
