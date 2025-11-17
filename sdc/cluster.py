import os
from pathlib import Path
import shutil
import datetime
import time
import subprocess as sp
from dask_jobqueue import SLURMCluster
from distributed import Client

from distributed.utils import TimeoutError


def start_slurm_cluster(cores: int = 12,
                        processes: int = 3,
                        memory: str = '18 GiB',
                        walltime: str = '01:00:00',
                        wait_timeout: int = 300,
                        reservation: str = 'SALDI'
                        ) -> tuple[Client, SLURMCluster]:
    """
    Start a dask_jobqueue.SLURMCluster and a distributed.Client. The cluster will
    automatically scale up and down as needed.
    
    Parameters
    ----------
    cores : int, optional
        Total number of cores per job. Default is 12.
    processes : int, optional
        Number of processes per job. Default is 3.
    memory : str, optional
        Total amount of memory per job. Default is '18 GiB'.
    walltime : str, optional
        The walltime for the job in the format HH:MM:SS. Default is '01:00:00'.
    wait_timeout : int, optional
        Timeout in seconds to wait for each configuration to start. Default is 300 seconds
        (5 minutes).
    reservation : str, optional
        The SLURM reservation name to use. Default is 'SALDI'. If the reservation is
        not active, it will be ignored. If it is active, the resources will be set to
        8 cores, 2 processes, and 16 GiB memory to better fit within the reservation limits.
    
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
    
    log_directory = None
    home_directory = Path(home_directory)
    if home_directory.exists():
        now = datetime.datetime.now()
        _clean_old_logs(home_directory.joinpath('.sdc_logs'), now)
        log_directory = home_directory.joinpath('.sdc_logs', 
                                                now.strftime('%Y-%m-%dT%H:%M'))            
    
    port = _dashboard_port()
    scheduler_options = {'dashboard_address': f':{port}'}
    job_name = f"dask-worker-{port}"
    common_params = {
        'cores': cores,
        'processes': processes,
        'memory': memory,
        'walltime': walltime,
        'interface': 'ib0',
        'job_script_prologue': ['mkdir -p /scratch/$USER'],
        'worker_extra_args': ['--lifetime', '55m', '--lifetime-stagger', '4m'],
        'local_directory': os.path.join('/', 'scratch', user_name),
        'log_directory': str(log_directory) if log_directory else None,
        'scheduler_options': scheduler_options,
        'job_name': job_name
    }
    
    configurations = []
    if _check_reservation_active(reservation):
        configurations.append({**common_params, 'queue': 'short',
                  'job_extra_directives': [f'--reservation={reservation}'],
                  'cores': 8, 'processes': 2, 'memory': '16 GiB'})
    else:
        reservation = None
    configurations.append({**common_params, 'queue': 'short'})
    configurations.append({**common_params, 'queue': 'standard'})
    
    start_time = time.time()
    config_index = 0
    try:
        while config_index < len(configurations):
            config = configurations[config_index]
            print(f"[INFO] Trying to allocate requested resources using configuration "
                  f"{config_index + 1}/{len(configurations)}:\nqueue={config['queue']}, "
                  f"reservation={reservation if reservation else 'None'}")
            
            dask_client, cluster = _create_cluster(**config)

            while not _is_cluster_ready(dask_client, job_name=job_name):
                if time.time() - start_time > wait_timeout:
                    if config_index < len(configurations) - 1:
                        # Move to the next configuration
                        if reservation:
                            reservation = None
                        config_index += 1
                        cluster.close()
                        start_time = time.time()  # Reset the timer
                        break
                    else:
                        raise TimeoutError("[INFO] Cluster failed to start within "
                                           "timeout period. This could be due to high "
                                           "demand on the cluster.")
                time.sleep(10)
            else:
                # If we exited the while loop without breaking (i.e., cluster is ready)
                print(f"[INFO] Cluster is ready for computation! :) Dask dashboard "
                      f"available via 'localhost:{port}'")
                return dask_client, cluster

        # If we exhausted all configurations
        raise TimeoutError("[INFO] Cluster failed to start with any configuration within "
                           "the timeout period. This could be due to high demand on the "
                           "cluster.")
    except (SystemExit, KeyboardInterrupt):
        _cancel_slurm_jobs(job_name)
    except Exception as e:
        _cancel_slurm_jobs(job_name)
        raise e


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


def _check_reservation_active(reservation_name: str) -> bool:
    """Check if a SLURM reservation is active."""
    try:
        cmd = f"scontrol show reservation {reservation_name}"
        output = sp.check_output(cmd, shell=True).decode('utf-8').strip()
        if f"ReservationName={reservation_name}" in output and "State=ACTIVE" in output:
            return True
    except sp.SubprocessError:
        pass
    return False


def _create_cluster(**kwargs) -> tuple[Client, SLURMCluster]:
    """Create a dask_jobqueue.SLURMCluster and a distributed.Client."""
    cluster = SLURMCluster(**kwargs)
    dask_client = Client(cluster)
    cluster.adapt(minimum=1, maximum=2*kwargs['processes'],
                # https://github.com/dask/dask-jobqueue/issues/498#issuecomment-1233716189
                worker_key=lambda state: state.address.split(':')[0], interval='10s')
    return dask_client, cluster


def _is_cluster_ready(client: Client,
                     min_workers: int = 1,
                     recent_job_time: int = 120,
                     job_name: str = "dask-worker"
                     ) -> bool:
    """Check if the cluster is ready for computation by checking the status of recent SLURM jobs for dask workers."""
    try:
        current_time = datetime.datetime.now()
        
        # Get all Slurm job IDs for current user with the given job name
        cmd = f"squeue -u {os.getenv('USER')} -n {job_name} -h -o '%i %S'"
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
                    start_dt = datetime.datetime.strptime(start_time,
                                                          '%Y-%m-%dT%H:%M:%S')
                    if (current_time - start_dt).total_seconds() <= recent_job_time:
                        recent_job_ids.append(job_id)
                except ValueError:
                    continue
        
        if not recent_job_ids:
            print(f"No recent SLURM jobs found for {job_name}")
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


def _cancel_slurm_jobs(job_name: str):
    """Cancel all SLURM jobs for the current user with the given job name."""
    try:
        cmd = f"squeue -u {os.getenv('USER')} -n {job_name} -h -o '%i'"
        output = sp.check_output(cmd, shell=True).decode('utf-8').strip().split('\n')
        job_ids = [line.strip() for line in output if line.strip()]
        for job_id in job_ids:
            try:
                sp.run(f"scancel {job_id}", shell=True, check=True, timeout=5)
                print(f"Canceled job {job_id}")
            except sp.TimeoutExpired:
                print(f"Timeout while trying to cancel job {job_id}")
            except sp.SubprocessError as e:
                print(f"Failed to cancel job {job_id}: {e}")
    except Exception as e:
        print(f"Error canceling jobs: {e}")


def _clean_old_logs(log_directory, now):
    log_path = Path(log_directory)
    if not log_path.exists():
        return
    
    one_week_ago = now - datetime.timedelta(weeks=1)
    for dir_path in log_path.iterdir():
        if dir_path.is_dir():
            try:
                dir_date = datetime.datetime.strptime(dir_path.name, "%Y-%m-%dT%H:%M")
                if dir_date < one_week_ago:
                    shutil.rmtree(dir_path)
            except ValueError:
                print(f"Skipping {dir_path}: name does not match expected date format.")
            except Exception as e:
                print(f"Error processing {dir_path}: {e}")
