import os
import datetime
import subprocess as sp
from dask_jobqueue import SLURMCluster
from distributed import Client


def start_slurm_cluster(cores: int = 10,
                        processes: int = 1,
                        memory: str = '20 GiB',
                        walltime: str = '00:30:00',
                        log_directory: str = None,
                        scheduler_options: dict = None) -> (Client, SLURMCluster):
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
    log_directory : str, optional
        The directory to write the log files to. Default is None, which writes the log
        files to ~/.sdc_logs/<date>.
    scheduler_options : dict, optional
        Additional scheduler options. Default is None, which sets the dashboard address
        to a free port based on the user id.
    
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
    
    if log_directory is None:
        if os.path.exists(os.getenv('HOME')):
            now = datetime.datetime.now().strftime('%Y-%m-%dT%H:%M')
            log_directory = os.path.join(os.getenv('HOME'), '.sdc_logs', now)
    
    if scheduler_options is None:
        port = _dashboard_port()
        scheduler_options = {'dashboard_address': f':{port}'}
    
    cluster = SLURMCluster(queue='short',
                           cores=cores,
                           processes=processes,
                           memory=memory,
                           walltime=walltime,
                           interface='ib0',
                           job_script_prologue=['mkdir -p /scratch/$USER'],
                           worker_extra_args=['--lifetime', '25m'],
                           local_directory=local_directory,
                           log_directory=log_directory,
                           scheduler_options=scheduler_options)
    
    dask_client = Client(cluster)
    cluster.adapt(minimum_jobs=1, maximum_jobs=4,
                  # https://github.com/dask/dask-jobqueue/issues/498#issuecomment-1233716189
                  worker_key=lambda state: state.address.split(':')[0],
                  interval='10s')
    
    return dask_client, cluster


def _dashboard_port(port: int = 8787):
    """Finding a free port for the dask dashboard based on the user id.
    """
    uid = sp.check_output('id -u', shell=True).decode('utf-8').replace('\n','')
    for i in uid:
        port += int(i)
    sp_check = 'lsof -i -P -n | grep LISTEN'
    while port in [int(x.split(':')[1].split(' (')[0]) for x in
                   sp.check_output(sp_check, shell=True).decode('utf-8').split('\n')
                   if x != '']:
        port += 1
    return port
