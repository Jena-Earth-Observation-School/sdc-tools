# Data Access

The following pages provide an overview of the datasets that are currently 
available in the SDC, how to access them, and furthermore, any additional 
information that might be useful for working with them.

```{tableofcontents}
```

## Using the `load_product`-function

This function is the recommended main entry point for loading data from the SDC. 
It is a wrapper around various other, product-specific functions and its goal is 
to provide a unified and easy-to-use interface for loading data from the SDC.

A lot happens in the background and certain parameters are set to default 
values, so that the function can be used with minimal effort. Most importantly,
all data products are loaded with the coordinate reference system (CRS) 
[EPSG:4326](https://epsg.io/4326) and resolution set to 0.0002°, which
corresponds to approximately 20 x 20 m at the equator.

```{note}
In the future, it will be possible to specify additional parameters to customize the data
loading process. This is currently being tracked in 
[this issue](https://github.com/Jena-Earth-Observation-School/sdc-tools/issues/7).
```

The following basic example shows how to load Sentinel-2 L2A data for the year 
2020 of an area of interest, which has been saved locally as a vector file:

```{code-block} python
from sdc.load import load_product

s2_data = load_product(product="s2_l2a", 
                       vec="/path/to/my_area_of_interest.geojson", 
                       time_range=("2020-01-01", "2021-01-01),
                       s2_apply_mask=True)
```

The basic usage is to specify the following parameters:

- `product`: The name of the data product to load. The following strings are 
supported at the moment:
    - `"s1_rtc"`: Sentinel-1 Radiometric Terrain Corrected (RTC)
    - `"s2_l2a"`: Sentinel-2 Level 2A (L2A)
    - `"sanlc"`: South African National Land Cover (SANLC) 2020
    - `"mswep"`: Multi-Source Weighted-Ensemble Precipitation (MSWEP) daily
- `vec`: Filter the returned data spatially by either providing the name of a 
SALDi site in the format `"siteXX"`, where XX is the site number (e.g. 
`"site06"`), or a path to a vector file (any format [fiona](https://github.com/Toblerity/Fiona) 
can handle, e.g. `.geojson`, `.shp`, `.gpkg`) that defines an area of interest 
as a subset of a SALDi site. Providing a vector file outside the spatial extent 
of the SALDi sites will result in an empty dataset. Please note, that always the
bounding box of the provided geometry will be used to load the data.
- `time_range`: Filter the returned data temporally by providing a tuple of 
strings in the format `("YY-MM-dd", "YY-MM-dd")`, or `None` to return all 
available data.
- `s2_apply_mask`: Apply a quality mask to the Sentinel-2 L2A product by using 
its `SCL`-band. The default value is `True`. As the name already suggests, this 
is only relevant for Sentinel-2 L2A data.

```{warning}
While it is possible to load data for an entire SALDi site by providing the site 
name (e.g. `"site06"`), please be aware that this will result in a large dataset 
and will very likely result in performance issues if your workflow is not 
optimized.

It is therefore recommended to load only a subset by providing a vector file 
defining an area of interest (e.g., using https://geojson.io/). Develop your 
workflow on a small subset of the data before scaling up.
```

(xarray-dask-intro)=
## Xarray, Dask and lazy loading

The `load_product`-function returns an `xarray.Dataset` object, which is a 
powerful data structure for working with multi-dimensional data. [Xarray](https://xarray.dev/) 
is a Python library that _"[...] introduces labels in the form of dimensions, 
coordinates and attributes on top of raw NumPy-like arrays, which allows for a 
more intuitive, more concise, and less error-prone developer experience."_. 
See the following resources for more information:
- [Overview: Why Xarray?](https://docs.xarray.dev/en/latest/getting-started-guide/why-xarray.html)
- Pretty much everything in the [Xarray documentation](https://docs.xarray.dev/en/latest/index.html) 😉
- [Tutorial: Xarray in 45 minutes](https://tutorial.xarray.dev/overview/xarray-in-45-min.html)

Xarray closely integrates with the [Dask](https://dask.org/) library, which is a 
_"[...] flexible library for parallel computing in Python."_ and allows for 
datasets to be loaded lazily, meaning that the data is not loaded into memory 
until it is actually needed. This is especially useful when working with large 
datasets that might not fit into the available memory. These datasets are split 
into smaller chunks that can then be processed in parallel. Most of this is 
happening in the background so you don't have to worry too much about it. 
However, it is important to be aware of it, as it affects the way you need to 
work with the data. For example, you need to be careful when applying certain 
operations, such as calling [`.values`](https://docs.xarray.dev/en/latest/generated/xarray.DataArray.values.html#xarray.DataArray.values), as they might trigger the entire 
dataset to be loaded into memory and can result in performance issues if the 
data has not been [aggregated](https://docs.xarray.dev/en/latest/api.html#aggregation)or [indexed](https://docs.xarray.dev/en/latest/user-guide/indexing.html) beforehand. 
Furthermore, you might reach a point where you need to use advanced techniques 
to optimize your workflow, such as re-orienting the chunks or [persisting](https://docs.dask.org/en/latest/best-practices.html#persist-when-you-can) 
intermediate results in memory.

The following resources provide more information:
- [User Guide: Using Dask with xarray](https://docs.xarray.dev/en/latest/user-guide/dask.html#using-dask-with-xarray)
- [Tutorial: Parallel computing with Dask](https://tutorial.xarray.dev/intermediate/xarray_and_dask.html#parallel-computing-with-dask)

```{note}
By default, the `load_product`-function returns an `xarray.Dataset` object that 
has been loaded lazily. Please make sure to familiarize yourself with the 
resources mentioned above to get the most out of working with the SDC! But also 
don't worry too much as the default settings should be fine for most use cases 
and you will also get more experienced over time.
```