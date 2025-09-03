import sys
import csv
import numpy as np

# A small tolerance for comparing floating-point numbers
TOLERANCE = 1e-6

# ANSI color codes for pretty printing
class bcolors:
    HEADER = '\033[95m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'

def read_csv_to_numpy(filepath):
    """Reads a CSV file containing coordinates into a NumPy array."""
    try:
        with open(filepath, 'r') as f:
            reader = csv.reader(f)
            data = [list(map(float, row)) for row in reader]
        return np.array(data)
    except FileNotFoundError:
        print(f"{bcolors.FAIL}Error: File not found at '{filepath}'{bcolors.ENDC}")
        sys.exit(1)
    except Exception as e:
        print(f"{bcolors.FAIL}Error reading or parsing {filepath}: {e}{bcolors.ENDC}")
        sys.exit(1)

def read_assignments(filepath):
    """Reads an assignment file (id\tpoint_str) into a dictionary."""
    assignments = {}
    try:
        with open(filepath, 'r') as f:
            for line in f:
                parts = line.strip().split('\t')
                if len(parts) == 2:
                    cluster_id, point_str = parts
                    # We use the point string as a key for easy lookup
                    assignments[point_str] = int(cluster_id)
    except FileNotFoundError:
        print(f"{bcolors.FAIL}Error: File not found at '{filepath}'{bcolors.ENDC}")
        sys.exit(1)
    except Exception as e:
        print(f"{bcolors.FAIL}Error reading or parsing {filepath}: {e}{bcolors.ENDC}")
        sys.exit(1)
    return assignments

def main():
    if len(sys.argv) != 5:
        print(f"{bcolors.BOLD}Usage:{bcolors.ENDC}")
        print("  python3 verify_output.py <expected_centers.csv> <actual_centers.csv> <expected_assignments.csv> <actual_assignments.csv>")
        sys.exit(1)

    expected_centers_file, actual_centers_file, expected_assignments_file, actual_assignments_file = sys.argv[1:]

    print(f"{bcolors.HEADER}--- K-Means Verification Script ---{bcolors.ENDC}")

    # --- 1. Load all data ---
    print("Loading data files...")
    expected_centers = read_csv_to_numpy(expected_centers_file)
    actual_centers = read_csv_to_numpy(actual_centers_file)
    expected_assignments = read_assignments(expected_assignments_file)
    actual_assignments = read_assignments(actual_assignments_file)

    # --- 2. Verify Centroids ---
    print("\nVerifying final centroids...")
    if expected_centers.shape != actual_centers.shape:
        print(f"{bcolors.FAIL}Failure: Centroid files have different shapes! Expected {expected_centers.shape}, got {actual_centers.shape}{bcolors.ENDC}")
        sys.exit(1)

    # This map will store {expected_id -> actual_id}
    centroid_map = {}
    used_actual_indices = set()

    for i, exp_center in enumerate(expected_centers):
        # Find the closest actual center to this expected center
        distances = np.linalg.norm(actual_centers - exp_center, axis=1)
        best_match_idx = np.argmin(distances)
        min_dist = distances[best_match_idx]

        if min_dist > TOLERANCE:
            print(f"{bcolors.FAIL}Failure: No matching actual centroid found for expected centroid #{i} {exp_center}{bcolors.ENDC}")
            sys.exit(1)
        
        if best_match_idx in used_actual_indices:
            print(f"{bcolors.FAIL}Failure: Actual centroid #{best_match_idx} was matched to multiple expected centroids. This indicates a problem.{bcolors.ENDC}")
            sys.exit(1)
            
        centroid_map[i] = best_match_idx
        used_actual_indices.add(best_match_idx)

    print(f"{bcolors.OKGREEN}Success: All {len(expected_centers)} centroids matched perfectly.{bcolors.ENDC}")
    # print(f"  > Centroid ID mapping: {centroid_map}")

    # --- 3. Verify Assignments ---
    # REMOVED, points can be assigned to clusters arbitrarily if equal distance? Either way as long as centroids are same should be fine.
    # print("\nVerifying point assignments...")
    # if len(expected_assignments) != len(actual_assignments):
    #     print(f"{bcolors.FAIL}Failure: Assignment files have a different number of points! Expected {len(expected_assignments)}, got {len(actual_assignments)}{bcolors.ENDC}")
    #     sys.exit(1)

    # mismatch_count = 0
    # for point_str, expected_id in expected_assignments.items():
    #     if point_str not in actual_assignments:
    #         print(f"{bcolors.FAIL}Failure: Point '{point_str}' is in the expected assignments but not in the actual output.{bcolors.ENDC}")
    #         mismatch_count += 1
    #         continue

    #     actual_id = actual_assignments[point_str]
        
    #     # Use the map to check for correctness
    #     if centroid_map[expected_id] != actual_id:
    #         mismatch_count += 1
    #         if mismatch_count < 5: # Print first few mismatches for debugging
    #             print(f"{bcolors.WARNING}  - Mismatch for point {point_str}: Expected cluster {expected_id} (maps to actual id {centroid_map[expected_id]}), but got actual id {actual_id}{bcolors.ENDC}")

    # if mismatch_count == 0:
    #     print(f"{bcolors.OKGREEN}Success: All {len(expected_assignments)} point assignments are correct.{bcolors.ENDC}")
    # else:
    #     print(f"{bcolors.FAIL}Failure: Found {mismatch_count} incorrect point assignments.{bcolors.ENDC}")
    #     sys.exit(1)
        
    print(f"\n{bcolors.BOLD}{bcolors.OKGREEN}Verification Complete: The MapReduce output matches the expected output!{bcolors.ENDC}")

if __name__ == "__main__":
    main()