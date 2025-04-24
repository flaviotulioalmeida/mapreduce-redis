#!/usr/bin/env python3
"""
Reducer Worker Module

This script implements a reducer worker that:
1. Fetches a task from a Redis queue
2. Reads the assigned reducer input file
3. Applies a reduce function (sum values for each key)
4. Writes the result to a final output file
"""
import os
import json
import time
import redis
import argparse
from pathlib import Path


class ReducerWorker:
    def __init__(self, redis_host='localhost', redis_port=6379, 
                 reducer_input_dir='reducer_input', output_dir='output', worker_id=None):
        """
        Initialize the reducer worker.
        
        Args:
            redis_host (str): Redis server hostname
            redis_port (int): Redis server port
            reducer_input_dir (str): Directory containing reducer input files
            output_dir (str): Directory to store final output files
            worker_id (str): Unique identifier for this worker
        """
        self.redis_client = redis.Redis(host=redis_host, port=redis_port, decode_responses=True)
        self.reducer_input_dir = reducer_input_dir
        self.output_dir = output_dir
        self.worker_id = worker_id or f"reducer-{os.getpid()}"
        
        # Create output directory if it doesn't exist
        Path(output_dir).mkdir(parents=True, exist_ok=True)
    
    def reduce_function(self, key, values):
        """
        Reduce function for word count: sum all values for a key.
        
        Args:
            key (str): The key (word)
            values (list): List of values (counts) for the key
            
        Returns:
            int: Sum of all values
        """
        return sum(values)
    
    def process_reducer_input(self, reducer_file):
        """
        Process a reducer input file and apply the reduce function.
        
        Args:
            reducer_file (str): Name of the reducer input file
            
        Returns:
            str: Path to the output file
        """
        print(f"[{self.worker_id}] Processing {reducer_file}")
        input_path = os.path.join(self.reducer_input_dir, reducer_file)
        
        # Extract reducer number for the output file name
        reducer_num = reducer_file.split('_')[0]
        output_file = os.path.join(self.output_dir, f"{reducer_num}_output.txt")
        
        results = {}
        
        # Load the grouped key-value pairs
        with open(input_path, 'r', encoding='utf-8') as f:
            grouped_data = json.load(f)
        
        # Apply the reduce function to each key
        for key, values in grouped_data.items():
            results[key] = self.reduce_function(key, values)
        
        # Sort results by key for better readability
        sorted_results = {k: results[k] for k in sorted(results.keys())}
        
        # Write results to output file
        with open(output_file, 'w', encoding='utf-8') as f:
            for key, value in sorted_results.items():
                f.write(f"{key}\t{value}\n")
        
        print(f"[{self.worker_id}] Completed {reducer_file}, wrote results to {output_file}")
        return output_file
    
    def run(self):
        """
        Main worker loop: fetch tasks from Redis queue and process them.
        """
        print(f"[{self.worker_id}] Starting reducer worker")
        
        while True:
            # Try to get a task from the reducer queue
            task = self.redis_client.lpop('reducer_tasks')
            if not task:
                # Check if we should exit (coordinator signals completion)
                if self.redis_client.get('reducers_done') == 'true':
                    print(f"[{self.worker_id}] No more tasks, exiting")
                    break
                
                print(f"[{self.worker_id}] No tasks available, waiting...")
                time.sleep(1)
                continue
            
            try:
                # Process the reducer input and get the output file path
                output_file = self.process_reducer_input(task)
                
                # Publish completion message
                self.redis_client.publish('reducer_completed', json.dumps({
                    'worker_id': self.worker_id,
                    'reducer_file': task,
                    'output_file': output_file
                }))
                
                # Increment the completed tasks counter
                self.redis_client.incr('completed_reducer_tasks')
                
            except Exception as e:
                print(f"[{self.worker_id}] Error processing {task}: {e}")
                # Push the task back to the queue for retry
                self.redis_client.rpush('reducer_tasks', task)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="MapReduce Reducer Worker")
    parser.add_argument("--redis-host", default="localhost", help="Redis server hostname")
    parser.add_argument("--redis-port", type=int, default=6379, help="Redis server port")
    parser.add_argument("--reducer-input-dir", default="reducer_input", 
                        help="Directory containing reducer input files")
    parser.add_argument("--output-dir", default="output", help="Directory to store output files")
    parser.add_argument("--worker-id", help="Unique identifier for this worker")
    
    args = parser.parse_args()
    
    reducer = ReducerWorker(
        redis_host=args.redis_host,
        redis_port=args.redis_port,
        reducer_input_dir=args.reducer_input_dir,
        output_dir=args.output_dir,
        worker_id=args.worker_id
    )
    
    reducer.run()
