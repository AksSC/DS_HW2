#!/usr/bin/env python3
import sys

def main():
    """
    Reads sorted (key, value) pairs from a single mapper, where the key is a
    cluster_id and the value is a point's coordinates. It pre-aggregates
    these points, calculating a local sum and count for each cluster_id.
    
    This drastically reduces the amount of data that needs to be shuffled
    globally before the reduce phase.
    
    Input: (from stdin, sorted by key)
        <cluster_id>\t<x,y,z,...>
        <cluster_id>\t<x,y,z,...>
        ...
        
    Output: (to stdout)
        <cluster_id>\t<sum_x,sum_y,...\t<count>
    """
    current_cluster_id = None
    point_sum = None
    point_count = 0
    
    # Read from standard input
    for line in sys.stdin:
        cluster_id_str, point_str = line.strip().split('\t')
        cluster_id = int(cluster_id_str)
        point = list(map(float, point_str.split(',')))
        
        if current_cluster_id is None:
            current_cluster_id = cluster_id
        
        # If we are still processing the same cluster, accumulate points
        if cluster_id == current_cluster_id:
            if point_sum is None:
                point_sum = [0.0] * len(point)
            for i in range(len(point)):
                point_sum[i] += point[i]
            point_count += 1
        else:
            # The cluster ID has changed, so we output the aggregated result
            # for the previous cluster.
            sum_str = ",".join(map(str, point_sum))
            print(f"{current_cluster_id}\t{sum_str}\t{point_count}")
            
            # Reset for the new cluster
            current_cluster_id = cluster_id
            point_sum = point
            point_count = 1
            
    # Output the last aggregated cluster after the loop finishes
    if current_cluster_id is not None:
        sum_str = ",".join(map(str, point_sum))
        print(f"{current_cluster_id}\t{sum_str}\t{point_count}")

if __name__ == "__main__":
    main()
