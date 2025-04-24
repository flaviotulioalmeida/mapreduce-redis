#!/usr/bin/env python3
"""
Mapper Worker Module

This script implements a mapper worker that:
1. Fetches a task from a Redis queue
2. Processes the assigned chunk file
3. Emits key-value pairs (word, 1) for each word
4. Writes the emitted pairs to an intermediate file
"""
import os
import json
import time
import redis
import argparse
import re
from pathlib import Path


class MapperWorker:
    def __init__(self, redis_host='localhost', redis_port=6379, chunks_dir='chunks', 
                 intermediate_dir='intermediate', worker_id=None):
        """
        Initialize the mapper worker.
        
        Args:
            redis_host (str): Redis server hostname
            redis_port (int): Redis server port
            chunks_dir (str): Directory containing input chunks
            intermediate_dir (str): Directory to store intermediate results
            worker_id (str): Unique identifier for this worker
        """
        self.redis_client = redis.Redis(host=redis_host, port=redis_port, decode_responses=True)
        self.chunks_dir = chunks_dir
        self.intermediate_dir = intermediate_dir
        self.worker_id = worker_id or f"mapper-{os.getpid()}"
        
        # Create intermediate directory if it doesn't exist
        Path(intermediate_dir).mkdir(parents=True, exist_ok=True)
    
    def map_function(self, line):
        """
        Map function for word count: emits (word, 1) for each word.
        
        Args:
            line (str): Input line to process
            
        Returns:
            list: List of (word, 1) tuples
        """
        # Convert to lowercase and split by non-alphanumeric characters
        words = re.findall(r'\w+', line.lower())
        return [(word, 1) for word in words if word]
    
    def process_chunk(self, chunk_file):
        """
        Process a chunk file and emit key-value pairs.
        
        Args:
            chunk_file (str): Name of the chunk file to process
            
        Returns:
            str: Path to the intermediate file
        """
        print(f"[{self.worker_id}] Processing {chunk_file}")
        chunk_path = os.path.join(self.chunks_dir, chunk_file)
        
        # Extract chunk number for the intermediate file name
        chunk_num = re.search(r'chunk(\d+)\.txt', chunk_file)
        if not chunk_num:
            raise ValueError(f"Invalid chunk file name format: {chunk_file}")
        
        intermediate_file = os.path.join(
            self.intermediate_dir, 
            f"mapper-{chunk_num.group(1)}-{self.worker_id}.json"
        )
        
        results = []
        
        # Process the chunk file
        with open(chunk_path, 'r', encoding='utf-8') as f:
            for line in f:
                mapped_values = self.map_function(line)
                results.extend(mapped_values)
        
        # Write results to intermediate file
        with open(intermediate_file, 'w', encoding='utf-8') as f:
            json.dump(results, f)
        
        print(f"[{self.worker_id}] Completed {chunk_file}, wrote results to {intermediate_file}")
        return intermediate_file
    
    def run(self):
        """
        Main worker loop: fetch tasks from Redis queue and process them.
        """
        print(f"[{self.worker_id}] Starting mapper worker")
        
        while True:
            # Try to get a task from the mapper queue
            task = self.redis_client.lpop('mapper_tasks')
            if not task:
                # Check if we should exit (coordinator signals completion)
                if self.redis_client.get('mappers_done') == 'true':
                    print(f"[{self.worker_id}] No more tasks, exiting")
                    break
                
                print(f"[{self.worker_id}] No tasks available, waiting...")
                time.sleep(1)
                continue
            
            try:
                # Process the chunk and get the intermediate file path
                intermediate_file = self.process_chunk(task)
                
                # Publish completion message
                self.redis_client.publish('mapper_completed', json.dumps({
                    'worker_id': self.worker_id,
                    'chunk': task,
                    'intermediate_file': intermediate_file
                }))
                
                # Increment the completed tasks counter
                self.redis_client.incr('completed_mapper_tasks')
                
            except Exception as e:
                print(f"[{self.worker_id}] Error processing {task}: {e}")
                # Push the task back to the queue for retry
                self.redis_client.rpush('mapper_tasks', task)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="MapReduce Mapper Worker")
    parser.add_argument("--redis-host", default="localhost", help="Redis server hostname")
    parser.add_argument("--redis-port", type=int, default=6379, help="Redis server port")
    parser.add_argument("--chunks-dir", default="chunks", help="Directory containing input chunks")
    parser.add_argument("--intermediate-dir", default="intermediate", 
                        help="Directory to store intermediate results")
    parser.add_argument("--worker-id", help="Unique identifier for this worker")
    
    args = parser.parse_args()
    
    mapper = MapperWorker(
        redis_host=args.redis_host,
        redis_port=args.redis_port,
        chunks_dir=args.chunks_dir,
        intermediate_dir=args.intermediate_dir,
        worker_id=args.worker_id
    )
    
    mapper.run()
