(load_product-intro)=
# Using this package

Before continuing with the notebooks of the "Data Products"-section, it is important
to have a basic understanding of how to use the `sdc-tools` package. This section will 
introduce you to the `load_product`-function. This function is the recommended main 
entry point for working with `sdc-tools`. It is a wrapper around various other, 
product-specific functions and its goal is to provide a unified and easy-to-use 
interface for loading data from the SDC.

A lot happens in the background and certain parameters are set to default 
values so that the function can be used with minimal effort. Most importantly,
all data products are loaded with the coordinate reference system (CRS) 
[EPSG:4326](https://epsg.io/4326) and pixel spacing is set to 0.0002°, which corresponds
to approximately 20 x 20 m at the equator.

The following basic example shows how to load Sentinel-2 L2A data for the year 
2020 of an area of interest, which has been saved locally as a vector file:

```{code-block} python
from sdc.load import load_product

s2_data = load_product(product="s2_l2a", 
                       vec="/path/to/my_area_of_interest.geojson", 
                       time_range=("2020-01-01", "2021-01-01))
```

The basic usage is to specify the following parameters:

- `product`: The name of the data product to load. The following strings are 
supported at the moment:
    - _"s1_rtc"_: Sentinel-1 Radiometric Terrain Corrected (RTC)
    - _"s1_surfmi"_: Sentinel-1 Surface Moisture Index (SurfMI)
    - _"s1_coh"_: Sentinel-1 Coherence (VV-pol, ascending)
    - _"s2_l2a"_: Sentinel-2 Level 2A (L2A)
    - _"sanlc"_: South African National Land Cover (SANLC)
    - _"mswep"_: Multi-Source Weighted-Ensemble Precipitation (MSWEP) daily
    - _"cop_dem"_: Copernicus Digital Elevation Model GLO-30
- `vec`: Filter the returned data spatially by either providing the name of a 
SALDi site in the format _"siteXX"_, where XX is the site number (e.g. 
_"site06"_), or a path to a vector file (any format [`GeoPandas`](https://geopandas.org/en/stable/index.html) 
can handle, e.g. GeoJSON, GeoPackage or ESRI Shapefile) that defines an area of 
interest as a subset of a SALDi site. Providing a vector file outside the 
spatial extent of the SALDi sites will result in an empty dataset. Please note, 
that the bounding box of the provided geometry will be used to load the 
data (see {ref}`clip_to_vec` for how to clip to the exact geometry).
- `time_range`: Filter the returned data temporally by providing a tuple of 
strings in the format _("YY-MM-dd", "YY-MM-dd")_, or _None_ to return all 
available data. If you want to use a different date format, you can also provide
the parameter `time_pattern` with a string that specifies the format of the
provided time strings.

The following additional parameters are product-specific, as indicated by their 
prefix (e.g. _s2_ for Sentinel-2 L2A):

- `s2_apply_mask`: Apply a quality and cloud mask to the Sentinel-2 L2A product by using 
its Scene Classification Layer (SCL) band. The default value is _True_.
- `sanlc_year`: Select a specific year of the SANLC product by providing an
integer in the format _YYYY_. The default value is _None_, which will return the
product for all available years: 2018 & 2020.

```{warning}
While it is possible to load data for an entire SALDi site by providing the site 
name (e.g. _"site06"_), please be aware that this will result in a large dataset 
and will very likely result in performance issues if your workflow is not 
optimized.

It is therefore recommended to load only a subset by providing a vector file 
defining an area of interest (e.g., using https://geojson.io/). Develop your 
workflow on a small subset of the data before scaling up!
```
