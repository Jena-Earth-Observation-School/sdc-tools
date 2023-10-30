import math
from dask_jobqueue import SLURMCluster
from distributed import Client


def start_slurm_cluster(cores: int = 10,
                        processes: int = 1,
                        memory: str = "20 GiB",
                        walltime: str = "00:30:00") -> (Client, SLURMCluster):
    """
    Start a dask_jobqueue.SLURMCluster and a distributed.Client. The cluster will
    automatically scale up and down as needed.
    
    Parameters
    ----------
    cores : int, optional
        Total number of cores per job. Default is 10.
    processes : int, optional
        Number of (Python) processes per job. Default is 1, which is a good default for
        numpy-based workloads.
    memory : str, optional
        Total amount of memory per job. Default is "20 GiB".
    walltime : str, optional
        The walltime for the job in the format HH:MM:SS. Default is "00:30:00".
    
    Returns
    -------
    dask_client : Client
        The dask distributed Client object.
    cluster : SLURMCluster
        The dask_jobqueue SLURMCluster object.
    
    Examples
    --------
    >>> from sdc.cluster import start_slurm_cluster
    >>> dask_client, cluster = start_slurm_cluster()
    
    Notes
    -----
    The default values seem low, however, it is important to keep in mind that these
    values are defined per job and that the cluster will automatically be scaled up and
    down as needed. So, with a maximum of 10 jobs running at the same time, you will
    have 10 * 4 = 40 cores and 10 * 10GB = 100GB of memory available.
    """
    queue = 'short' if int(walltime[:2]) <= 3 else 'normal'
    max_jobs = math.ceil(40/cores)
    
    cluster = SLURMCluster(queue=queue,
                           cores=cores,
                           processes=processes,
                           memory=memory,
                           walltime=walltime,
                           worker_extra_args=["--lifetime", '25m' if
                                              queue == 'short' else '1h'])
    
    dask_client = Client(cluster)
    cluster.adapt(maximum=max_jobs)
    
    return dask_client, cluster
