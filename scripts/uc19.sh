#!/usr/bin/env bash

# Download Ozone data. Select variables, region and time right away to save bandwith
cate ds copy esacci.OZONE.mon.L3.NP.multi-sensor.multi-platform.MERGED.fv0002.r1 --name "ozone-europe.mon.2007.2008" --time 2007-01-01,2008-12-31 --vars "O3_du_tot, O3_ndens" --region "POLYGON((-12.05078125 73.54664369613808,33.65234375 73.54664369613808,33.65234375 35.65604583948963,-12.05078125 35.65604583948963,-12.05078125 73.54664369613808))"

rm -rf uc19/
mkdir uc19
cd uc19

# Start interactive session by initialising an empty workspace
# This will write a hidden directory .\.cate-workspace
cate ws init

# Open the Ozone dataset and assign it to resource named "ozone"
cate res open ozone local.ozone-europe.mon.2007.2008

# Create and save an animation over time for O3_du_tot
cate res set anim1 animate_map ds=@ozone var="O3_du_tot" file="ozone-europe.html" true_range=True region="POLYGON((-12.05078125 73.54664369613808,33.65234375 73.54664369613808,33.65234375 35.65604583948963,-12.05078125 35.65604583948963,-12.05078125 73.54664369613808))"

# Create and save an animation over air_pressure for April 2007
cate res set anim2 animate_map ds=@ozone var="O3_ndens" file="ozone-europe-pressure.html" animate_dim="air_pressure" indexers="{'time':'2007-04-01'}" true_range=True region="POLYGON((-12.05078125 73.54664369613808,33.65234375 73.54664369613808,33.65234375 35.65604583948963,-12.05078125 35.65604583948963,-12.05078125 73.54664369613808))"

# @Norman, tried 'cate run animate_map ds=@ozone ...', but wouldn't work, complained about unresolved references ds=ozone

# Save the workspace
cate ws save
# Close the workspace
cate ws close
# Exit interactive session. Don't ask, answer is always "yes".
cate ws exit --yes

cd ..