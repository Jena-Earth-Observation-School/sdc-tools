import dask
import xarray as xr
from ._cluster import start_cluster


dask.config.set({"array.rechunk.method": "p2p",
                 "optimization.fuse.active": False,
                 "array.chunk-size": "256MiB"})
xr.set_options(keep_attrs=True)

dask_client, dask_cluster = start_cluster()
