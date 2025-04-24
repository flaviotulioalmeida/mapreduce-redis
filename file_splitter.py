#!/usr/bin/env python3
"""
File Splitter Module

This script splits a large text file into multiple chunks of roughly equal size.
It ensures that splits occur at line boundaries to maintain data integrity.
"""
import os
import math
import argparse
from pathlib import Path


def split_file(input_file, num_chunks=10, output_dir="chunks"):
    """
    Split a large file into roughly equal-sized chunks.
    
    Args:
        input_file (str): Path to the input file
        num_chunks (int): Number of chunks to split the file into
        output_dir (str): Directory to store the chunks
    """
    # Create output directory if it doesn't exist
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    
    # Get file size
    file_size = os.path.getsize(input_file)
    chunk_size = math.ceil(file_size / num_chunks)
    
    print(f"Splitting {input_file} ({file_size} bytes) into {num_chunks} chunks of ~{chunk_size} bytes each")
    
    with open(input_file, 'r', encoding='utf-8') as f:
        chunk_num = 0
        current_size = 0
        current_chunk = []
        
        for line in f:
            line_size = len(line.encode('utf-8'))  # Get byte size of the line
            current_size += line_size
            current_chunk.append(line)
            
            # If current chunk is big enough, write it to a file
            if current_size >= chunk_size and chunk_num < num_chunks - 1:
                write_chunk(current_chunk, output_dir, chunk_num)
                chunk_num += 1
                current_size = 0
                current_chunk = []
        
        # Write the last chunk (which might be smaller or larger than chunk_size)
        if current_chunk:
            write_chunk(current_chunk, output_dir, chunk_num)
    
    print(f"Successfully split file into {num_chunks} chunks in {output_dir}/")


def write_chunk(chunk, output_dir, chunk_num):
    """
    Write a chunk of lines to a file.
    
    Args:
        chunk (list): List of lines to write
        output_dir (str): Directory to store the chunk
        chunk_num (int): Chunk number
    """
    output_file = os.path.join(output_dir, f"chunk{chunk_num}.txt")
    with open(output_file, 'w', encoding='utf-8') as f:
        f.writelines(chunk)
    print(f"Created {output_file}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Split a large file into chunks")
    parser.add_argument("input_file", help="Path to the input file")
    parser.add_argument("--num-chunks", type=int, default=10, help="Number of chunks to split into")
    parser.add_argument("--output-dir", default="chunks", help="Directory to store the chunks")
    
    args = parser.parse_args()
    split_file(args.input_file, args.num_chunks, args.output_dir)
