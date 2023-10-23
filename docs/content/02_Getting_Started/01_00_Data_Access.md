# Data Access

The following pages provide an overview of the datasets that are currently available in the SDC, how to access them, 
and furthermore, any additional information that might be useful for working with them.

```{tableofcontents}
```

## Using the `load_product`-function

This function is the recommended main entry point for loading data from the SDC. It is a wrapper around various other, 
product-specific functions and its goal is to provide a unified and easy-to-use interface for loading data from the SDC.

A lot happens in the background and certain parameters are set to default values, so that the function can be used with
minimal effort. However, it is also possible to specify additional parameters to customize the data loading process. See
the {ref}`advanced-loading` section for more information.

The following basic example shows how to load Sentinel-2 L2A data for SALDi site 06 for the year 2020:

```{code-block} python
from sdc.load import load_product

s2_data = load_product(product="s2_l2a", vec="site06", time_range=("2020-01-01", "2021-01-01))
```

The basic usage is to specify the following parameters:

- `product`: The name of the data product to load. The following strings are supported at the moment:
    - `"s1_rtc"`: Sentinel-1 RTC
    - `"s2_l2a"`: Sentinel-2 L2A
- `vec`: Filter the returned data spatially by either providing the name of a SALDi site in the format `"siteXX"`, where XX is the site number (see example above), or a vector file path if the area of interest is a subset of a SALDi site. Providing a vector file outside the spatial extent of the SALDi sites will result in an empty dataset.
- `time_range`: Filter the returned data temporally by providing a tuple of strings in the format `("YY-MM-dd", "YY-MM-dd")`, or `None` to return all available data.

(advanced-loading)=
## Advanced loading options

_Coming soon..._

![https://media.giphy.com/media/fky7SCCsAqGZy/giphy.gif](https://media.giphy.com/media/fky7SCCsAqGZy/giphy.gif)

