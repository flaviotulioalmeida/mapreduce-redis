#!/usr/bin/env python3
"""
MapReduce Runner Module

This script provides a convenient way to run the entire MapReduce process,
including data generation, coordination, and worker execution.
"""
import os
import time
import argparse
import subprocess
from pathlib import Path


def ensure_directories():
    """
    Ensure all necessary directories exist.
    """
    directories = ['data', 'chunks', 'intermediate', 'reducer_input', 'output']
    for directory in directories:
        Path(directory).mkdir(parents=True, exist_ok=True)
        print(f"Created directory: {directory}")


def generate_data(size_mb):
    """
    Generate test data.
    
    Args:
        size_mb (int): Size of the data file in MB
    """
    print(f"Generating {size_mb}MB of test data...")
    subprocess.run([
        'python', 'data_generator.py',
        f'--size-mb={size_mb}',
        '--output-file=data/data.txt'
    ], check=True)


def start_redis():
    """
    Start Redis server if not already running.
    
    Returns:
        subprocess.Popen: Redis server process
    """
    print("Checking if Redis is running...")
    try:
        # Try to ping Redis
        result = subprocess.run(
            ['redis-cli', 'ping'],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True,
            check=False
        )
        
        if result.stdout.strip() == 'PONG':
            print("Redis is already running")
            return None
        
    except (subprocess.SubprocessError, FileNotFoundError):
        pass
    
    print("Starting Redis server...")
    redis_process = subprocess.Popen(
        ['redis-server'],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT
    )
    
    # Wait for Redis to start
    time.sleep(2)
    return redis_process


def run_mapreduce(num_chunks, num_reducers, num_mappers, num_reducer_workers):
    """
    Run the MapReduce process.
    
    Args:
        num_chunks (int): Number of chunks to split the input file into
        num_reducers (int): Number of reducers to use
        num_mappers (int): Number of mapper worker processes
        num_reducer_workers (int): Number of reducer worker processes
    """
    print("Starting MapReduce process...")
    
    # Start coordinator in a separate process
    print("Starting coordinator...")
    coordinator_process = subprocess.Popen([
        'python', 'coordinator.py',
        f'--input-file=data/data.txt',
        f'--num-chunks={num_chunks}',
        f'--num-reducers={num_reducers}'
    ])
    
    # Give coordinator time to initialize and split the file
    time.sleep(5)
    
    # Start workers
    print(f"Starting {num_mappers} mapper workers and {num_reducer_workers} reducer workers...")
    workers_process = subprocess.Popen([
        'python', 'run_workers.py',
        f'--num-mappers={num_mappers}',
        f'--num-reducers={num_reducer_workers}'
    ])
    
    # Start monitor in a separate process
    print("Starting monitoring dashboard...")
    monitor_process = subprocess.Popen(['python', 'monitor.py'])
    
    # Wait for coordinator to finish
    coordinator_process.wait()
    print("Coordinator finished")
    
    # Wait for workers to finish
    workers_process.wait()
    print("Workers finished")
    
    # Terminate monitor
    monitor_process.terminate()
    print("Monitor terminated")


def main():
    parser = argparse.ArgumentParser(description="Run MapReduce with Redis")
    parser.add_argument("--generate-data", action="store_true", help="Generate test data")
    parser.add_argument("--size-mb", type=int, default=1000, help="Size of test data in MB")
    parser.add_argument("--num-chunks", type=int, default=10, help="Number of chunks to split into")
    parser.add_argument("--num-reducers", type=int, default=5, help="Number of reducers to use")
    parser.add_argument("--num-mappers", type=int, default=3, help="Number of mapper worker processes")
    parser.add_argument("--num-reducer-workers", type=int, default=2, 
                        help="Number of reducer worker processes")
    
    args = parser.parse_args()
    
    # Ensure directories exist
    ensure_directories()
    
    # Generate data if requested
    if args.generate_data:
        generate_data(args.size_mb)
    
    # Start Redis if not already running
    redis_process = start_redis()
    
    try:
        # Run MapReduce
        run_mapreduce(
            args.num_chunks,
            args.num_reducers,
            args.num_mappers,
            args.num_reducer_workers
        )
    finally:
        # Stop Redis if we started it
        if redis_process:
            print("Stopping Redis server...")
            redis_process.terminate()
            redis_process.wait()


if __name__ == "__main__":
    main()
