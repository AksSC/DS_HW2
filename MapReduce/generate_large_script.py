import numpy as np
import csv
import random
import os
import sys

# --- Configuration ---
# All parameters are now taken from the command line.
CLUSTER_STD_DEV = 2.5 

# --- File Names ---
os.makedirs('data', exist_ok=True) # Use a new directory
POINTS_FILE = 'data/points.csv'
INITIAL_CENTERS_FILE = 'data/initial_centers.csv'

def generate_and_write_points(n_points, n_dim, k, std_dev, true_centers):
    """
    Generates points one by one and writes them directly to a CSV file
    to avoid using large amounts of memory.
    """
    with open(POINTS_FILE, 'w', newline='') as f:
        writer = csv.writer(f)
        for i in range(n_points):
            if (i + 1) % 100000 == 0:
                print(f"  -> Generated {i+1}/{n_points} points...")
            
            # Pick a random true center
            center = random.choice(true_centers)
            # Create a point using a normal distribution around the center
            point = np.random.normal(loc=center, scale=std_dev)
            writer.writerow(point)

def write_to_csv(filepath, data):
    """Writes a list of lists or numpy array to a CSV file."""
    with open(filepath, 'w', newline='') as f:
        writer = csv.writer(f)
        for row in data:
            writer.writerow(row)

def main():
    """
    Main function to generate a large dataset without holding it in memory.
    This script ONLY generates points.csv and initial_centers.csv.
    It DOES NOT run a sequential K-Means, as the data is too large.
    """
    if len(sys.argv) != 4:
        print("Usage: python3 generate_large_data.py <num_points> <num_dimensions> <K>")
        sys.exit(1)

    NUM_POINTS = int(sys.argv[1])
    NUM_DIMENSIONS = int(sys.argv[2])
    K = int(sys.argv[3])

    print(f"Generating a large dataset with {NUM_POINTS} points...")
    
    # --- 1. Generate True Centers and Initial Centers ---
    # We still need the "true" centers around which to generate data.
    true_centers = np.random.uniform(-50, 50, size=(K, NUM_DIMENSIONS))
    
    # We can't pick initial centers from the dataset, as we never hold it all.
    # A simple and effective strategy is to just generate another K random points.
    initial_centers = np.random.uniform(-50, 50, size=(K, NUM_DIMENSIONS))
    
    # --- 2. Write Initial Centers File ---
    write_to_csv(INITIAL_CENTERS_FILE, initial_centers)
    print(f"-> Successfully created '{INITIAL_CENTERS_FILE}'")

    # --- 3. Generate and Stream Points File ---
    print(f"-> Starting to stream points to '{POINTS_FILE}'...")
    generate_and_write_points(NUM_POINTS, NUM_DIMENSIONS, K, CLUSTER_STD_DEV, true_centers)
    
    print("\nLarge data generation complete!")
    print(f"You can now run your MapReduce job on the files in the 'data/' directory.")

if __name__ == "__main__":
    main()
