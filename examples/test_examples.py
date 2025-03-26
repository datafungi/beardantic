#!/usr/bin/env python
"""
Test script to verify that all examples work correctly with the new directory structure.
"""
import os
import subprocess
import sys
from pathlib import Path

def run_example(example_path):
    """Run an example script and return whether it succeeded."""
    print(f"\n=== Running {example_path} ===")
    try:
        result = subprocess.run(
            [sys.executable, example_path],
            check=True,
            capture_output=True,
            text=True
        )
        print(f"Success! Output:\n{result.stdout}")
        return True
    except subprocess.CalledProcessError as e:
        print(f"Error running {example_path}:")
        print(f"Exit code: {e.returncode}")
        print(f"Output: {e.stdout}")
        print(f"Error: {e.stderr}")
        return False

def main():
    """Run all example scripts to verify they work correctly."""
    examples_dir = Path(__file__).parent
    
    # Find all Python files in the examples directory and subdirectories
    example_files = []
    for root, _, files in os.walk(examples_dir):
        for file in files:
            if file.endswith('.py') and file != 'test_examples.py':
                example_files.append(os.path.join(root, file))
    
    # Run each example
    success_count = 0
    for example in example_files:
        if run_example(example):
            success_count += 1
    
    # Print summary
    print(f"\n=== Summary ===")
    print(f"Ran {len(example_files)} examples")
    print(f"Successful: {success_count}")
    print(f"Failed: {len(example_files) - success_count}")
    
    return 0 if success_count == len(example_files) else 1

if __name__ == "__main__":
    sys.exit(main())
