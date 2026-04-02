import dask
import xarray as xr
from draco import start_slurm_cluster

dask.config.set({"array.rechunk.method": "p2p",
                 "optimization.fuse.active": False,
                 "array.chunk-size": "256MiB"})
xr.set_options(keep_attrs=True)

dask_client, dask_cluster = start_slurm_cluster(processes=3, 
                                                cores=12,
                                                memory='18 GiB',
                                                walltime='01:00:00',
                                                reservation='SALDI')
