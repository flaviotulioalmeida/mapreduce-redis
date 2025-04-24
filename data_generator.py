#!/usr/bin/env python3
"""
Data Generator Module

This script generates a large text file with random words for testing the MapReduce system.
"""
import os
import random
import argparse
from pathlib import Path


def generate_data(output_file='data.txt', size_mb=1000, words_per_line=10):
    """
    Generate a large text file with random words.
    
    Args:
        output_file (str): Path to the output file
        size_mb (int): Approximate size of the file in MB
        words_per_line (int): Number of words per line
    """
    # Common English words for the dataset
    common_words = [
        "the", "be", "to", "of", "and", "a", "in", "that", "have", "I", 
        "it", "for", "not", "on", "with", "he", "as", "you", "do", "at", 
        "this", "but", "his", "by", "from", "they", "we", "say", "her", "she", 
        "or", "an", "will", "my", "one", "all", "would", "there", "their", "what", 
        "so", "up", "out", "if", "about", "who", "get", "which", "go", "me", 
        "when", "make", "can", "like", "time", "no", "just", "him", "know", "take", 
        "people", "into", "year", "your", "good", "some", "could", "them", "see", "other", 
        "than", "then", "now", "look", "only", "come", "its", "over", "think", "also", 
        "back", "after", "use", "two", "how", "our", "work", "first", "well", "way", 
        "even", "new", "want", "because", "any", "these", "give", "day", "most", "us"
    ]
    
    # Calculate approximate number of lines needed to reach the desired file size
    # Assuming average word length of 5 characters + 1 space
    avg_chars_per_word = 6
    avg_chars_per_line = avg_chars_per_word * words_per_line
    target_chars = size_mb * 1024 * 1024  # Convert MB to bytes
    num_lines = target_chars // avg_chars_per_line
    
    print(f"Generating {size_mb} MB of data (~{num_lines} lines) to {output_file}")
    
    # Create parent directory if it doesn't exist
    Path(os.path.dirname(output_file) or '.').mkdir(parents=True, exist_ok=True)
    
    with open(output_file, 'w', encoding='utf-8') as f:
        for _ in range(int(num_lines)):
            # Generate a line with random words
            line = ' '.join(random.choice(common_words) for _ in range(words_per_line))
            f.write(line + '\n')
    
    # Get actual file size
    actual_size_mb = os.path.getsize(output_file) / (1024 * 1024)
    print(f"Generated {actual_size_mb:.2f} MB of data in {output_file}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate test data for MapReduce")
    parser.add_argument("--output-file", default="data.txt", help="Path to the output file")
    parser.add_argument("--size-mb", type=int, default=1000, help="Approximate size of the file in MB")
    parser.add_argument("--words-per-line", type=int, default=10, help="Number of words per line")
    
    args = parser.parse_args()
    
    generate_data(
        output_file=args.output_file,
        size_mb=args.size_mb,
        words_per_line=args.words_per_line
    )
