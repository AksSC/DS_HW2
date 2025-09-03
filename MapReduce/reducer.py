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
    # The path to the previous centroids file is now a command-line argument.
    old_centroids_file = sys.argv[1]
    
    # This dictionary will hold the state. We initialize it with the old centroids.
    # It will be updated with new values, but will always contain K items.
    final_centroids = read_old_centroids(old_centroids_file)

    # These dictionaries will be used to calculate the new means.
    point_sums = {}
    point_counts = {}

    # Read point assignments from stdin and accumulate sums and counts.
    for line in sys.stdin:
        centroid_id_str, point_str = line.strip().split('\t')
        centroid_id = int(centroid_id_str)
        point = list(map(float, point_str.split(',')))

        if centroid_id not in point_sums:
            point_sums[centroid_id] = [0.0] * len(point)
            point_counts[centroid_id] = 0
        
        for i in range(len(point)):
            point_sums[centroid_id][i] += point[i]
        point_counts[centroid_id] += 1

    # Now, update our main 'final_centroids' dictionary with the new calculations.
    for cid, p_sum in point_sums.items():
        count = point_counts[cid]
        new_centroid = [s / count for s in p_sum]
        final_centroids[cid] = tuple(new_centroid)

    # Finally, print all K centroids.
    # We iterate from 0 to K-1 (the length of the initial dictionary).
    # If a centroid was updated, we print the new value.
    # If it was never updated (empty cluster), we print its original old value.
    for i in range(len(final_centroids)):
        centroid_output = ",".join(map(str, final_centroids[i]))
        print(centroid_output)


if __name__ == "__main__":
    main()
