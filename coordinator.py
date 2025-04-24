#!/usr/bin/env python3
"""
Coordinator Module

This script orchestrates the entire MapReduce process:
1. Splits the input file into chunks
2. Pushes mapper tasks to Redis
3. Waits for all mappers to finish
4. Triggers the shuffle phase
5. Pushes reducer tasks to Redis
6. Waits for all reducers to finish
7. Merges reducer outputs into a final result
"""
import os
import json
import time
import redis
import argparse
import threading
from pathlib import Path

from file_splitter import split_file
from shuffler import Shuffler


class Coordinator:
    def __init__(self, redis_host='localhost', redis_port=6379, input_file='data.txt',
                 num_chunks=10, num_reducers=5, chunks_dir='chunks', intermediate_dir='intermediate',
                 reducer_input_dir='reducer_input', output_dir='output'):
        """
        Initialize the coordinator.
        
        Args:
            redis_host (str): Redis server hostname
            redis_port (int): Redis server port
            input_file (str): Path to the input file
            num_chunks (int): Number of chunks to split the input file into
            num_reducers (int): Number of reducers to use
            chunks_dir (str): Directory to store input chunks
            intermediate_dir (str): Directory to store mapper intermediate results
            reducer_input_dir (str): Directory to store reducer input files
            output_dir (str): Directory to store reducer output files
        """
        self.redis_client = redis.Redis(host=redis_host, port=redis_port, decode_responses=True)
        self.input_file = input_file
        self.num_chunks = num_chunks
        self.num_reducers = num_reducers
        self.chunks_dir = chunks_dir
        self.intermediate_dir = intermediate_dir
        self.reducer_input_dir = reducer_input_dir
        self.output_dir = output_dir
        
        # Create necessary directories
        for directory in [chunks_dir, intermediate_dir, reducer_input_dir, output_dir]:
            Path(directory).mkdir(parents=True, exist_ok=True)
        
        # Initialize Redis pubsub for completion notifications
        self.pubsub = self.redis_client.pubsub()
        self.pubsub.subscribe('mapper_completed', 'reducer_completed')
        
        # Start the pubsub listener thread
        self.pubsub_thread = threading.Thread(target=self._listen_for_completions)
        self.pubsub_thread.daemon = True
        self.pubsub_thread.start()
    
    def _listen_for_completions(self):
        """
        Listen for completion messages from workers.
        """
        for message in self.pubsub.listen():
            if message['type'] == 'message':
                channel = message['channel']
                data = json.loads(message['data'])
                
                if channel == 'mapper_completed':
                    print(f"Mapper {data['worker_id']} completed chunk {data['chunk']}")
                elif channel == 'reducer_completed':
                    print(f"Reducer {data['worker_id']} completed file {data['reducer_file']}")
    
    def reset_redis_state(self):
        """
        Reset Redis state for a new MapReduce job.
        """
        # Clear any existing tasks
        self.redis_client.delete('mapper_tasks', 'reducer_tasks')
        
        # Reset completion flags
        self.redis_client.delete('mappers_done', 'reducers_done')
        
        # Reset task counters
        self.redis_client.delete('completed_mapper_tasks', 'completed_reducer_tasks')
    
    def split_input_file(self):
        """
        Split the input file into chunks.
        """
        print(f"Splitting input file {self.input_file} into {self.num_chunks} chunks")
        split_file(self.input_file, self.num_chunks, self.chunks_dir)
    
    def push_mapper_tasks(self):
        """
        Push mapper tasks to Redis queue.
        """
        print("Pushing mapper tasks to Redis queue")
        
        # Get all chunk files
        chunk_files = [f for f in os.listdir(self.chunks_dir) if f.startswith('chunk') and f.endswith('.txt')]
        
        if not chunk_files:
            raise ValueError(f"No chunk files found in {self.chunks_dir}")
        
        # Push each chunk file as a task
        for chunk_file in chunk_files:
            self.redis_client.rpush('mapper_tasks', chunk_file)
        
        # Set the total number of mapper tasks
        self.redis_client.set('total_mapper_tasks', len(chunk_files))
        print(f"Pushed {len(chunk_files)} mapper tasks")
    
    def wait_for_mappers(self):
        """
        Wait for all mapper tasks to complete.
        """
        print("Waiting for all mapper tasks to complete")
        total_tasks = int(self.redis_client.get('total_mapper_tasks') or 0)
        
        while True:
            completed_tasks = int(self.redis_client.get('completed_mapper_tasks') or 0)
            print(f"Mapper progress: {completed_tasks}/{total_tasks} tasks completed")
            
            if completed_tasks >= total_tasks:
                print("All mapper tasks completed")
                self.redis_client.set('mappers_done', 'true')
                break
            
            time.sleep(2)
    
    def run_shuffle_phase(self):
        """
        Run the shuffle phase.
        """
        print("Starting shuffle phase")
        shuffler = Shuffler(
            intermediate_dir=self.intermediate_dir,
            reducer_input_dir=self.reducer_input_dir,
            num_reducers=self.num_reducers
        )
        shuffler.shuffle()
    
    def push_reducer_tasks(self):
        """
        Push reducer tasks to Redis queue.
        """
        print("Pushing reducer tasks to Redis queue")
        
        # Get all reducer input files
        reducer_files = [f for f in os.listdir(self.reducer_input_dir) if f.endswith('_input.json')]
        
        if not reducer_files:
            raise ValueError(f"No reducer input files found in {self.reducer_input_dir}")
        
        # Push each reducer file as a task
        for reducer_file in reducer_files:
            self.redis_client.rpush('reducer_tasks', reducer_file)
        
        # Set the total number of reducer tasks
        self.redis_client.set('total_reducer_tasks', len(reducer_files))
        print(f"Pushed {len(reducer_files)} reducer tasks")
    
    def wait_for_reducers(self):
        """
        Wait for all reducer tasks to complete.
        """
        print("Waiting for all reducer tasks to complete")
        total_tasks = int(self.redis_client.get('total_reducer_tasks') or 0)
        
        while True:
            completed_tasks = int(self.redis_client.get('completed_reducer_tasks') or 0)
            print(f"Reducer progress: {completed_tasks}/{total_tasks} tasks completed")
            
            if completed_tasks >= total_tasks:
                print("All reducer tasks completed")
                self.redis_client.set('reducers_done', 'true')
                break
            
            time.sleep(2)
    
    def merge_outputs(self):
        """
        Merge all reducer outputs into a final result file.
        """
        print("Merging reducer outputs into final result")
        final_result_file = 'finalresult.txt'
        
        # Get all reducer output files
        output_files = [f for f in os.listdir(self.output_dir) if f.endswith('_output.txt')]
        
        if not output_files:
            raise ValueError(f"No reducer output files found in {self.output_dir}")
        
        # Dictionary to store merged results
        merged_results = {}
        
        # Read all reducer output files
        for output_file in output_files:
            file_path = os.path.join(self.output_dir, output_file)
            print(f"Processing {file_path}")
            
            with open(file_path, 'r', encoding='utf-8') as f:
                for line in f:
                    key, value = line.strip().split('\t')
                    merged_results[key] = int(value)
        
        # Sort results by key
        sorted_results = {k: merged_results[k] for k in sorted(merged_results.keys())}
        
        # Write merged results to final result file
        with open(final_result_file, 'w', encoding='utf-8') as f:
            for key, value in sorted_results.items():
                f.write(f"{key}\t{value}\n")
        
        print(f"Final result written to {final_result_file}")
        return final_result_file
    
    def run(self):
        """
        Run the entire MapReduce process.
        """
        print("Starting MapReduce job")
        
        # Reset Redis state
        self.reset_redis_state()
        
        # Split input file
        self.split_input_file()
        
        # Map phase
        self.push_mapper_tasks()
        self.wait_for_mappers()
        
        # Shuffle phase
        self.run_shuffle_phase()
        
        # Reduce phase
        self.push_reducer_tasks()
        self.wait_for_reducers()
        
        # Merge outputs
        final_result = self.merge_outputs()
        
        print(f"MapReduce job completed. Final result: {final_result}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="MapReduce Coordinator")
    parser.add_argument("--redis-host", default="localhost", help="Redis server hostname")
    parser.add_argument("--redis-port", type=int, default=6379, help="Redis server port")
    parser.add_argument("--input-file", default="data.txt", help="Path to the input file")
    parser.add_argument("--num-chunks", type=int, default=10, 
                        help="Number of chunks to split the input file into")
    parser.add_argument("--num-reducers", type=int, default=5, help="Number of reducers to use")
    parser.add_argument("--chunks-dir", default="chunks", help="Directory to store input chunks")
    parser.add_argument("--intermediate-dir", default="intermediate", 
                        help="Directory to store mapper intermediate results")
    parser.add_argument("--reducer-input-dir", default="reducer_input", 
                        help="Directory to store reducer input files")
    parser.add_argument("--output-dir", default="output", help="Directory to store reducer output files")
    
    args = parser.parse_args()
    
    coordinator = Coordinator(
        redis_host=args.redis_host,
        redis_port=args.redis_port,
        input_file=args.input_file,
        num_chunks=args.num_chunks,
        num_reducers=args.num_reducers,
        chunks_dir=args.chunks_dir,
        intermediate_dir=args.intermediate_dir,
        reducer_input_dir=args.reducer_input_dir,
        output_dir=args.output_dir
    )
    
    coordinator.run()
