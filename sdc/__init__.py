import xarray as xr
import sdc.utils as utils

utils.overwrite_default_dask_chunk_size()
xr.set_options(keep_attrs=True)
