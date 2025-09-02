import numpy as np
import csv
import random

# --- Configuration ---
# Feel free to change these parameters to test different scenarios
NUM_POINTS = 10000  # Number of data points to generate
NUM_DIMENSIONS = 3    # Number of dimensions for each point
K = 15                # Number of clusters
MAX_ITERATIONS = 50   # Max iterations for the naive K-Means
CLUSTER_STD_DEV = 2.5 # Standard deviation for clusters (how spread out they are)

# --- File Names ---
POINTS_FILE = 'data/points.csv'
INITIAL_CENTERS_FILE = 'data/initial_centers.csv'
EXPECTED_CENTERS_FILE = 'data/expected_centers.csv'
EXPECTED_ASSIGNMENTS_FILE = 'data/expected_assignments.csv'

def generate_clustered_data(n_points, n_dim, k, std_dev):
    """
    Generates realistic data points grouped around K centers.
    """
    print(f"Generating {n_points} data points in {n_dim} dimensions for {k} clusters...")
    
    # 1. Create K "true" cluster centers randomly
    true_centers = np.random.uniform(-50, 50, size=(k, n_dim))
    
    # 2. Generate points around these true centers
    points = []
    for i in range(n_points):
        # Pick a random true center
        center = random.choice(true_centers)
        # Create a point using a normal distribution around the center
        point = np.random.normal(loc=center, scale=std_dev)
        points.append(point)
        
    return np.array(points)

def write_to_csv(filepath, data):
    """
    Writes a list of lists or numpy array to a CSV file.
    """
    with open(filepath, 'w', newline='') as f:
        writer = csv.writer(f)
        for row in data:
            writer.writerow(row)

def naive_kmeans(points, initial_centroids, max_iter):
    """
    A simple, non-distributed implementation of K-Means.
    This serves as the ground truth to verify the MapReduce output.
    """
    centroids = np.copy(initial_centroids)
    
    for i in range(max_iter):
        print(f"  -> Naive K-Means Iteration {i+1}/{max_iter}")
        
        # 1. Assignment Step
        # For each point, find the index of the closest centroid
        distances = np.sqrt(((points - centroids[:, np.newaxis])**2).sum(axis=2))
        assignments = np.argmin(distances, axis=0)
        
        # 2. Update Step
        new_centroids = np.zeros_like(centroids)
        # Using a for loop for clarity, but this can be vectorized
        for cluster_id in range(len(centroids)):
            # Get all points assigned to this cluster
            points_in_cluster = points[assignments == cluster_id]
            # If a cluster has no points, we don't move its centroid
            if len(points_in_cluster) > 0:
                new_centroids[cluster_id] = points_in_cluster.mean(axis=0)
            else:
                new_centroids[cluster_id] = centroids[cluster_id]

        # 3. Convergence Check
        if np.allclose(centroids, new_centroids):
            print("  -> Naive K-Means converged.")
            break
            
        centroids = new_centroids
        
    return centroids, assignments

def main():
    """
    Main function to generate data, run naive K-Means, and write all files.
    """
    # --- 1. Generate Data and Initial Centers ---
    points = generate_clustered_data(NUM_POINTS, NUM_DIMENSIONS, K, CLUSTER_STD_DEV)
    
    # Select K random unique points from the dataset as initial centers
    initial_center_indices = np.random.choice(len(points), size=K, replace=False)
    initial_centers = points[initial_center_indices]
    
    # --- 2. Write Input Files for MapReduce ---
    print(f"\nWriting input files...")
    write_to_csv(POINTS_FILE, points)
    print(f"-> Successfully created '{POINTS_FILE}'")
    write_to_csv(INITIAL_CENTERS_FILE, initial_centers)
    print(f"-> Successfully created '{INITIAL_CENTERS_FILE}'")

    # --- 3. Run Naive K-Means to get the expected output ---
    print(f"\nRunning naive K-Means to compute ground truth...")
    final_centers, final_assignments = naive_kmeans(points, initial_centers, MAX_ITERATIONS)

    # --- 4. Write Expected Output Files ---
    print(f"\nWriting expected output files for verification...")
    write_to_csv(EXPECTED_CENTERS_FILE, final_centers)
    print(f"-> Successfully created '{EXPECTED_CENTERS_FILE}'")
    
    # Write the final assignments in the "key\tvalue" format like MapReduce
    with open(EXPECTED_ASSIGNMENTS_FILE, 'w') as f:
        for i, point in enumerate(points):
            cluster_id = final_assignments[i]
            point_str = ",".join(map(str, point))
            f.write(f"{cluster_id}\t{point_str}\n")
    print(f"-> Successfully created '{EXPECTED_ASSIGNMENTS_FILE}'")
    
    print("\nAll done! You can now use the generated CSV files to test your MapReduce implementation.")

if __name__ == "__main__":
    main()
