import dask
import xarray as xr

dask.config.set({'array.chunk-size': '256MiB'})
xr.set_options(keep_attrs=True)
