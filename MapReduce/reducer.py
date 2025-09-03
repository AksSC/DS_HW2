#!/usr/bin/env python3
import sys

def read_old_centroids(file_path):
    """Reads the previous iteration's centroids into a dictionary."""
    centroids = {}
    with open(file_path, 'r') as f:
        for i, line in enumerate(f):
            parts = line.strip().split(',')
            centroids[i] = tuple(map(float, parts))
    return centroids

def main():
    """
    Reads pre-aggregated data from multiple combiners. For each cluster_id,
    it sums the partial sums and partial counts to get a total sum and count.
    From this, it calculates the new centroid.
    
    It handles empty clusters by pre-loading the old centroids and outputting
    their old position if no new data is received for them.
    
    Input: (from stdin, sorted by key)
        <cluster_id>\t<partial_sum_x,...\t<partial_count>
        ...
        
    Output: (to stdout)
        <x,y,z,...>  (the new centroid)
    """
    old_centroids_file = sys.argv[1]
    
    # Pre-load old centroids to handle empty clusters
    final_centroids = read_old_centroids(old_centroids_file)

    # Dictionaries to store the total sums and counts from all combiners
    total_sums = {}
    total_counts = {}

    # Read the pre-aggregated data from stdin
    for line in sys.stdin:
        cluster_id_str, partial_sum_str, partial_count_str = line.strip().split('\t')
        cluster_id = int(cluster_id_str)
        partial_sum = list(map(float, partial_sum_str.split(',')))
        partial_count = int(partial_count_str)
        
        if cluster_id not in total_sums:
            total_sums[cluster_id] = [0.0] * len(partial_sum)
            total_counts[cluster_id] = 0
            
        for i in range(len(partial_sum)):
            total_sums[cluster_id][i] += partial_sum[i]
        total_counts[cluster_id] += partial_count

    # Calculate the new centroids from the total sums and counts
    for cid, t_sum in total_sums.items():
        count = total_counts[cid]
        new_centroid = [s / count for s in t_sum]
        final_centroids[cid] = tuple(new_centroid)

    # Print all K final centroids, preserving old ones if a cluster was empty
    for i in range(len(final_centroids)):
        centroid_output = ",".join(map(str, final_centroids[i]))
        print(centroid_output)


if __name__ == "__main__":
    main()

