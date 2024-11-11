import os
import datetime
import time
import subprocess as sp
from dask_jobqueue import SLURMCluster
from distributed import Client
from distributed.utils import TimeoutError

from typing import Optional


def start_slurm_cluster(cores: int = 16,
                        processes: int = 2,
                        memory: str = '16 GiB',
                        walltime: str = '00:45:00',
                        log_directory: Optional[str] = None,
                        wait_timeout: int = 300
                        ) -> tuple[Client, SLURMCluster]:
    """
    Start a dask_jobqueue.SLURMCluster and a distributed.Client. The cluster will
    automatically scale up and down as needed.
    
    Parameters
    ----------
    cores : int, optional
        Total number of cores per job. Default is 16.
    processes : int, optional
        Number of processes per job. Default is 2.
    memory : str, optional
        Total amount of memory per job. Default is '16 GiB'.
    walltime : str, optional
        The walltime for the job in the format HH:MM:SS. Default is '00:45:00'.
    log_directory : str, optional
        The directory to write the log files to. Default is None, which writes the log
        files to ~/.sdc_logs/<YYYY-mm-ddTHH:MM>.
    wait_timeout : int, optional
        Timeout in seconds to wait for the cluster to start. Default is 300 seconds
        (5 minutes).
    
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
    """
    user_name = os.getenv('USER')
    home_directory = os.getenv('HOME')
    if any(x is None for x in [user_name, home_directory]):
        raise RuntimeError("Cannot determine user name or home directory")
    
    if log_directory is None:
        if os.path.exists(home_directory):
            now = datetime.datetime.now().strftime('%Y-%m-%dT%H:%M')
            log_directory = os.path.join(home_directory, '.sdc_logs', now)
    
    port = _dashboard_port()
    scheduler_options = {'dashboard_address': f':{port}'}
    
    kwargs = {'queue': 'short',
              'cores': cores,
              'processes': processes,
              'memory': memory,
              'walltime': walltime,
              'interface': 'ib0',
              'job_script_prologue': ['mkdir -p /scratch/$USER'],
              'worker_extra_args': ['--lifetime', '40m',
                                    '--lifetime-stagger', '4m'],
              'local_directory': os.path.join('/', 'scratch', user_name),
              'log_directory': log_directory,
              'scheduler_options': scheduler_options}
    
    dask_client, cluster = _create_cluster(**kwargs)
    
    start_time = time.time()
    time.sleep(10)
    print("[INFO] Trying to allocate requested resources on the cluster (timeout after 5 minutes)...")
    queue_switched = False
    
    while not is_cluster_ready(dask_client):
        if time.time() - start_time > wait_timeout:
            if not queue_switched:
                print("[INFO] The default 'short' queue is busy. Switching to 'standard' queue and retrying (timeout after 5 minutes)...")
                cluster.close()
                kwargs['queue'] = 'standard'
                dask_client, cluster = _create_cluster(**kwargs)
                start_time = time.time()
                queue_switched = True
            else:
                raise TimeoutError("[INFO] Cluster failed to start within timeout period of 5 minutes. This could be due to high demand on the cluster.")
        time.sleep(10)
    
    print(f"[INFO] Cluster is ready for computation! :) Dask dashboard available via 'localhost:{port}'")
    return dask_client, cluster


def _dashboard_port(port: int = 8787) -> int:
    """Finding a free port for the dask dashboard based on the user id."""
    uid = sp.check_output('id -u', shell=True).decode('utf-8').replace('\n', '')
    for i in uid:
        port += int(i)
    sp_check = 'lsof -i -P -n | grep LISTEN'
    while port in [int(x.split(':')[1].split(' (')[0]) for x in
                   sp.check_output(sp_check, shell=True).decode('utf-8').split('\n')
                   if x != '']:
        port += 1
    return port


def _create_cluster(**kwargs) -> tuple[Client, SLURMCluster]:
    """Create a dask_jobqueue.SLURMCluster and a distributed.Client."""
    cluster = SLURMCluster(**kwargs)
    dask_client = Client(cluster)
    cluster.adapt(minimum_jobs=1, maximum_jobs=3,
                # https://github.com/dask/dask-jobqueue/issues/498#issuecomment-1233716189
                worker_key=lambda state: state.address.split(':')[0],
                interval='10s')
    return dask_client, cluster


def is_cluster_ready(client: Client,
                     min_workers: int = 1,
                     recent_job_time: int = 120
                     ) -> bool:
    """
    Check if the cluster is ready for computation by checking the status of recent SLURM 
    jobs for dask workers.
    
    Parameters
    ----------
    client : Client
        The dask distributed Client object
    min_workers : int
        Minimum number of workers required
    recent_job_time : int
        Time in seconds to consider a job as recent. Default is 120 seconds.
    
    Returns
    -------
    bool
        True if cluster is ready for computation, False otherwise.
    """
    try:
        current_time = datetime.datetime.now()
        
        # Get all Slurm job IDs for current user and name "dask-worker"
        cmd = f"squeue -u {os.getenv('USER')} -n dask-worker -h -o '%i %S'"
        output = sp.check_output(cmd, shell=True).decode('utf-8').strip().split('\n')
        
        # Filter jobs that are N/A or started recently
        recent_job_ids = []
        for line in output:
            if not line:
                continue
            job_id, start_time = line.split()
            if start_time == 'N/A':
                recent_job_ids.append(job_id)
            else:
                try:
                    start_dt = datetime.datetime.strptime(start_time, '%Y-%m-%dT%H:%M:%S')
                    if (current_time - start_dt).total_seconds() <= recent_job_time:
                        recent_job_ids.append(job_id)
                except ValueError:
                    continue
        
        if not recent_job_ids:
            print("No recent SLURM jobs found for dask-workers")
            return False
        
        running_jobs = []
        pending_jobs = []
        
        # Check status of each job
        for job_id in recent_job_ids:
            job_info = _get_slurm_job_info(job_id)
            if not job_info:
                continue
            
            if job_info['state'] == 'RUNNING':
                running_jobs.append(job_id)
            elif job_info['state'] == 'PENDING':
                pending_jobs.append(job_id)
        
        # If we have any running jobs, check if we have enough workers
        if running_jobs:
            n_workers = len(client.scheduler_info()['workers'])
            if n_workers >= min_workers:
                return True
            else:
                print(f"Cluster has {n_workers} workers, but {min_workers} required")
                return False
        
        if pending_jobs:
            print(f"All jobs are pending. Job IDs: {', '.join(pending_jobs)}")
            return False
        
        return False
    
    except Exception as e:
        print(f"Error checking cluster status: {e}")
        return False


def _get_slurm_job_info(job_id: str) -> dict:
    """Get information about a SLURM job using squeue."""
    try:
        cmd = f"squeue -j {job_id} -o '%i|%T|%S' -h"
        output = sp.check_output(cmd, shell=True).decode('utf-8').strip()
        
        if not output:  # Job not found
            return {}
        
        job_id, state, start_time = output.split('|')
        
        return {
            'job_id': job_id,
            'state': state.strip(),
            'start_time': start_time.strip()
        }
    except sp.SubprocessError:
        return {}
