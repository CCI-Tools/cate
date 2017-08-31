#!/usr/bin/env bash

#1. load data:
cate ds copy esacci.SOILMOISTURE.day.L3S.SSMV.multi-sensor.multi-platform.COMBINED.03-2.r1 --name SOILM_2006_2009_region_7_48_10_52 --time '2006-01-01,2009-12-31' --region '7,48,10,52' --vars 'sm'
cate ds copy esacci.CLOUD.mon.L3C.CLD_PRODUCTS.multi-sensor.multi-platform.ATSR2-AATSR.2-0.r1 --name CLOUD_2006_2009_region_7_48_10_52 --time '2006-01-01,2009-12-31' --region '7,48,10,52' --vars 'cfc'

rm -rf uc02/
mkdir uc02
cd uc02

#2. open workspace:
cate ws new

#3. open resource:
cate res open cloud local.CLOUD_2006_2009_region_7_48_10_52 
cate res open soilm local.SOILM_2006_2009_region_7_48_10_52

#3.b. lta:
cate res set cloud_lta long_term_average ds=@cloud var="cfc"
cate res set soilm_mon temporal_aggregation ds=@soilm
cate res set soilm_lta long_term_average ds=@soilm_mon var="sm"

#4. Select timeseries:
cate res set cloud_point tseries_point ds=@cloud_lta point=8.78,50.1 var="cfc"
cate res set soilm_point tseries_point ds=@soilm_lta point=8.78,50.1 var="sm"

#5. load stationdata (pointdata) 'read_csv'
cate res set station read_csv file="../testdata/produkt_klima_Tageswerte_20050715_20151231_07341_vers002.txt" delimiter=";" index_col="time"

#6. filter the stationdata (detection of outliers)
cate res set station_outl detect_outliers ds=@station var="precipitation" threshold_low=0.00 threshold_high=100.00 quantiles=False

#6.b. lta on station-data:
cate res set station_mon temporal_aggregation ds=@station_outl
cate res set station_lta long_term_average ds=@station_mon var="precipitation"
 
#7. Plot the timeseries of precipitation 
cate res set res_8 plot ds=@station_outl var="precipitation" file="plot1.jpg"
cate res set res_9 plot ds=@station_lta var="precipitation" file="plot2.jpg"

#8. Plot the timeseries of the ODP datasets
cate res set res_10 plot ds=@cloud_point var="cfc" file="plot3.jpg"
cate res set res_11 plot ds=@soilm_point var="sm" file="plot4.jpg"

cate ws save
cate ws close
cate ws exit -y

cd ..
