
(override-defaults)=
# ...use other loading parameters with `load_product`?

```{warning}
The following section is only relevant if you deliberately want to override the
default loading parameters and if you know what you are doing. If you are not 
sure, please skip this section and just be happy with the default values 🙂 or 
get in contact with me to discuss your use case.
```

All data products except for the MSWEP product are loaded internally using the
[`odc.stac.load`](https://odc-stac.readthedocs.io/en/latest/_api/odc.stac.load.html#odc-stac-load)
-function. As mentioned above, some loading parameters are set to default values 
to make this package beginner-friendly and easier to use. To be more precise, 
the following defaults are used:
- `crs='EPSG:4326'`
- `resolution=0.0002`
- `resampling='bilinear'`
- `chunks={'time': -1, 'latitude': 'auto', 'longitude': 'auto'}`

The default values for `crs` and `resolution`, for example, are the native CRS 
and resolution of the Sentinel-1 RTC and the Sentinel-2 L2A products (most bands 
of the latter at least). The `resampling`-parameter is only relevant if a data 
product needs to be reprojected (note that this is overriden by default for the 
SANLC product to use `resampling='nearest'`, which is a better choice for 
categorical data). The `chunks`-parameter is important for loading the data 
lazily using Dask (see {ref}`xarray-dask-intro` for more information). The 
default values have been chosen to work well for time series analysis 
(alignment of chunks along the time dimension) and to be memory efficient 
(automatically choose the chunk size along the spatial dimensions based on 
Dask's default).

If you want to override these defaults or add additional parameters that 
influence the loading process, you can do so by providing the 
`override_defaults`-parameter to the [`load_product`](load_product-intro)
-function. This parameter should be a dictionary with keys corresponding to 
parameter names of the [`odc.stac.load`](https://odc-stac.readthedocs.io/en/latest/_api/odc.stac.load.html#odc-stac-load)
-function and values corresponding to the desired values. It is also possible to 
partially override the defaults while keeping the rest unchanged. The following 
is a simple example of how to override only the default `resolution`-parameter 
when loading the Sentinel-1 RTC product:

```{code-block} python
from sdc.load import load_product

override_defaults = {"resolution": 0.0001}
s1_data = load_product(product="s1_rtc", 
                       vec="/path/to/my_area_of_interest.geojson", 
                       time_range=("2020-01-01", "2021-01-01),
                       override_defaults=override_defaults)
```

```{note}
The above example might be a bit misleading, especially for beginners, as we 
can't magically increase the spatial resolution of Earth Observation data by
simply changing a parameter called `resolution`. What we do here instead is
changing the pixel spacing of the loaded data. Both terms are often used
interchangeably, but they are not the same. Please keep this in mind! In the 
example we need to use the term `resolution` as this is the name of the
corresponding parameter of the `odc.stac.load`-function.
```
