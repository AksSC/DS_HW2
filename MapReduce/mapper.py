#!/usr/bin/env python3
import sys
import math

def read_centroids(file_path):
    """Reads centroids from a file into a list of tuples."""
    centroids = []
    with open(file_path, 'r') as f:
        for line in f:
            parts = line.strip().split(',')
            centroids.append(tuple(map(float, parts)))
    return centroids

def euclidean_distance(point1, point2):
    """Calculates the Euclidean distance between two points."""
    distance = 0
    for i in range(len(point1)):
        distance += (point1[i] - point2[i]) ** 2
    return math.sqrt(distance)

def main():
    # The path to the centroids file is passed as a command-line argument
    centroid_file = sys.argv[1]
    centroids = read_centroids(centroid_file)
    
    # Read data points from standard input
    for line in sys.stdin:
        # Assuming points are comma-separated, e.g., "x,y,z"
        point_str = line.strip().split(',')
        point = tuple(map(float, point_str))
        
        min_dist = float('inf')
        closest_centroid_id = -1
        
        # Find the closest centroid
        for i, centroid in enumerate(centroids):
            dist = euclidean_distance(point, centroid)
            if dist < min_dist:
                min_dist = dist
                closest_centroid_id = i
        
        # Output: key=centroid_id, value=point_coordinates
        # The point coordinates are joined back into a string
        point_output_str = ",".join(map(str, point))
        print(f"{closest_centroid_id}\t{point_output_str}")

if __name__ == "__main__":
    main()