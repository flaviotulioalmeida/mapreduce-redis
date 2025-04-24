#!/usr/bin/env python3
"""
Worker Runner Module

This script starts multiple mapper and reducer workers to simulate a distributed environment.
"""
import os
import time
import argparse
import subprocess
from multiprocessing import Process


def start_worker(worker_type, worker_id, redis_host, redis_port):
    """
    Start a worker process.
    
    Args:
        worker_type (str): Type of worker ('mapper' or 'reducer')
        worker_id (str): Unique identifier for the worker
        redis_host (str): Redis server hostname
        redis_port (int): Redis server port
    """
    script = 'mapper.py' if worker_type == 'mapper' else 'reducer.py'
    
    cmd = [
        'python', script,
        f'--redis-host={redis_host}',
        f'--redis-port={redis_port}',
        f'--worker-id={worker_type}-{worker_id}'
    ]
    
    print(f"Starting {worker_type} worker {worker_id}: {' '.join(cmd)}")
    
    # Start the worker process
    process = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        universal_newlines=True,
        bufsize=1
    )
    
    # Print worker output with prefix
    prefix = f"[{worker_type.upper()}-{worker_id}] "
    for line in process.stdout:
        print(prefix + line.strip())
    
    process.wait()
    print(f"{prefix}Worker exited with code {process.returncode}")


def run_workers(num_mappers=3, num_reducers=2, redis_host='localhost', redis_port=6379):
    """
    Run multiple mapper and reducer workers.
    
    Args:
        num_mappers (int): Number of mapper workers to start
        num_reducers (int): Number of reducer workers to start
        redis_host (str): Redis server hostname
        redis_port (int): Redis server port
    """
    processes = []
    
    # Start mapper workers
    for i in range(num_mappers):
        p = Process(
            target=start_worker,
            args=('mapper', i, redis_host, redis_port)
        )
        p.start()
        processes.append(p)
    
    # Start reducer workers
    for i in range(num_reducers):
        p = Process(
            target=start_worker,
            args=('reducer', i, redis_host, redis_port)
        )
        p.start()
        processes.append(p)
    
    # Wait for all processes to finish
    for p in processes:
        p.join()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Start MapReduce workers")
    parser.add_argument("--num-mappers", type=int, default=3, help="Number of mapper workers to start")
    parser.add_argument("--num-reducers", type=int, default=2, help="Number of reducer workers to start")
    parser.add_argument("--redis-host", default="localhost", help="Redis server hostname")
    parser.add_argument("--redis-port", type=int, default=6379, help="Redis server port")
    
    args = parser.parse_args()
    
    run_workers(
        num_mappers=args.num_mappers,
        num_reducers=args.num_reducers,
        redis_host=args.redis_host,
        redis_port=args.redis_port
    )
