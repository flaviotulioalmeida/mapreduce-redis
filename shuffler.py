#!/usr/bin/env python3
"""
Shuffler Module

This script implements the shuffle phase of MapReduce:
1. Reads all intermediate files produced by mappers
2. Groups values by key
3. Partitions the keys into R reducers using a hash function
4. Writes grouped key-value lists to files for reducers
"""
import os
import json
import hashlib
import argparse
from collections import defaultdict
from pathlib import Path


class Shuffler:
    def __init__(self, intermediate_dir='intermediate', reducer_input_dir='reducer_input', num_reducers=5):
        """
        Initialize the shuffler.
        
        Args:
            intermediate_dir (str): Directory containing mapper intermediate files
            reducer_input_dir (str): Directory to store reducer input files
            num_reducers (int): Number of reducers to partition data for
        """
        self.intermediate_dir = intermediate_dir
        self.reducer_input_dir = reducer_input_dir
        self.num_reducers = num_reducers
        
        # Create reducer input directory if it doesn't exist
        Path(reducer_input_dir).mkdir(parents=True, exist_ok=True)
    
    def partition_function(self, key):
        """
        Partition function to determine which reducer should process a key.
        
        Args:
            key (str): The key to partition
            
        Returns:
            int: The reducer index (0 to num_reducers-1)
        """
        # Use a hash function to distribute keys evenly
        hash_value = int(hashlib.md5(str(key).encode()).hexdigest(), 16)
        return hash_value % self.num_reducers
    
    def shuffle(self):
        """
        Perform the shuffle operation.
        
        1. Read all intermediate files
        2. Group values by key
        3. Partition keys to reducers
        4. Write reducer input files
        """
        print("Starting shuffle phase...")
        
        # Dictionary to store grouped values by key for each reducer
        grouped_data = [defaultdict(list) for _ in range(self.num_reducers)]
        
        # Read all intermediate files
        intermediate_files = [f for f in os.listdir(self.intermediate_dir) if f.endswith('.json')]
        print(f"Found {len(intermediate_files)} intermediate files")
        
        for filename in intermediate_files:
            file_path = os.path.join(self.intermediate_dir, filename)
            print(f"Processing {file_path}")
            
            with open(file_path, 'r', encoding='utf-8') as f:
                try:
                    # Load key-value pairs from the intermediate file
                    key_value_pairs = json.load(f)
                    
                    # Group values by key and assign to reducers
                    for key, value in key_value_pairs:
                        reducer_idx = self.partition_function(key)
                        grouped_data[reducer_idx][key].append(value)
                        
                except json.JSONDecodeError as e:
                    print(f"Error decoding JSON from {file_path}: {e}")
        
        # Write reducer input files
        for reducer_idx in range(self.num_reducers):
            output_file = os.path.join(self.reducer_input_dir, f"reducer{reducer_idx}_input.json")
            
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(dict(grouped_data[reducer_idx]), f)
            
            print(f"Created reducer input file: {output_file} with {len(grouped_data[reducer_idx])} keys")
        
        print("Shuffle phase completed")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="MapReduce Shuffler")
    parser.add_argument("--intermediate-dir", default="intermediate", 
                        help="Directory containing mapper intermediate files")
    parser.add_argument("--reducer-input-dir", default="reducer_input", 
                        help="Directory to store reducer input files")
    parser.add_argument("--num-reducers", type=int, default=5, 
                        help="Number of reducers to partition data for")
    
    args = parser.parse_args()
    
    shuffler = Shuffler(
        intermediate_dir=args.intermediate_dir,
        reducer_input_dir=args.reducer_input_dir,
        num_reducers=args.num_reducers
    )
    
    shuffler.shuffle()
