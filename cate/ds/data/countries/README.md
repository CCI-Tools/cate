Data files are taken from [Download vector maps](https://geojson-maps.ash.ms/).

Original data is from [Natural Earth](https://www.naturalearthdata.com/downloads/). 
However, I (Norman) tried converting the Shapefiles into GeoJSON using fiona, 
but the 10m and 110m results will make Cesium 1.71 crash with a DeveloperError.
50m worked but produces ugly artifacts for the Russia polygon with Cesium.
