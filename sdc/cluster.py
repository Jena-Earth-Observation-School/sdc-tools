import os
from dask_jobqueue import SLURMCluster
from distributed import Client


def start_slurm_cluster(cores: int = 10,
                        processes: int = 1,
                        memory: str = '20 GiB',
                        walltime: str = '00:30:00') -> (Client, SLURMCluster):
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
        Total amount of memory per job. Default is '20 GiB'.
    walltime : str, optional
        The walltime for the job in the format HH:MM:SS. Default is '00:30:00'.
    
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
    down as needed.
    """
    local_directory = os.path.join('/', 'scratch', os.getenv('USER'))
    
    cluster = SLURMCluster(queue='short',
                           cores=cores,
                           processes=processes,
                           memory=memory,
                           walltime=walltime,
                           interface='ib0',
                           job_script_prologue=['mkdir -p /scratch/$USER'],
                           worker_extra_args=['--lifetime', '25m'],
                           local_directory=local_directory)
    
    dask_client = Client(cluster)
    cluster.adapt(minimum_jobs=1, maximum_jobs=4,
                  # https://github.com/dask/dask-jobqueue/issues/498#issuecomment-1233716189
                  worker_key=lambda state: state.address.split(':')[0],
                  interval='10s')
    
    return dask_client, cluster
