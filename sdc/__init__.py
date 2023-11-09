import xarray as xr
from sdc.cluster import start_slurm_cluster

xr.set_options(keep_attrs=True)

dask_client, cluster = start_slurm_cluster()
