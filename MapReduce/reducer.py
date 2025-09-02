#!/usr/bin/env python3
import sys

def main():
    current_centroid_id = None
    point_sum = None
    point_count = 0
    
    # Read sorted input from stdin
    for line in sys.stdin:
        centroid_id_str, point_str = line.strip().split('\t')
        centroid_id = int(centroid_id_str)
        point = list(map(float, point_str.split(',')))
        
        if current_centroid_id is None:
            current_centroid_id = centroid_id
        
        # If we are still on the same centroid, accumulate points
        if centroid_id == current_centroid_id:
            if point_sum is None:
                point_sum = [0.0] * len(point)
            for i in range(len(point)):
                point_sum[i] += point[i]
            point_count += 1
        else:
            # The centroid_id has changed, so we calculate and output the new centroid
            new_centroid = [s / point_count for s in point_sum]
            print(",".join(map(str, new_centroid)))
            
            # Reset for the new centroid
            current_centroid_id = centroid_id
            point_sum = point
            point_count = 1
            
    # Don't forget to output the last centroid after the loop finishes
    if current_centroid_id is not None:
        new_centroid = [s / point_count for s in point_sum]
        print(",".join(map(str, new_centroid)))

if __name__ == "__main__":
    main()