#!/bin/bash

# --- Argument Parsing ---
if [ "$#" -ne 5 ]; then
    echo "Usage: ./run_mapreduce_parallel.sh K points.csv centers.txt max_iter output_dir"
    exit 1
fi

K=$1
POINTS_FILE=$2
INITIAL_CENTERS_FILE=$3
MAX_ITER=$4
OUTPUT_DIR=$5

# --- SLURM Environment Variable for Number of Mappers ---
# SLURM provides the number of tasks in the environment. Default to 8 if not in a SLURM job.
NUM_MAPPERS=${SLURM_NTASKS:-8}

# --- Setup ---
mkdir -p "$OUTPUT_DIR/tmp" # Temporary directory for chunks and map outputs
cp "$INITIAL_CENTERS_FILE" "$OUTPUT_DIR/centroids_0.txt"
echo "Starting K-Means with $NUM_MAPPERS parallel mappers."

# --- Split the input file for the mappers ---
# The 'split' command creates files like chunk_aa, chunk_ab, etc.
split -n l/$NUM_MAPPERS -d --additional-suffix=.txt "$POINTS_FILE" "$OUTPUT_DIR/tmp/chunk_"

# --- Main Iteration Loop ---
for i in $(seq 1 $MAX_ITER)
do
    echo "--- Iteration $i ---"
    PREV_CENTROIDS="$OUTPUT_DIR/centroids_$(($i-1)).txt"
    NEW_CENTROIDS="$OUTPUT_DIR/centroids_$i.txt"
    
    # --- Parallel Map Step ---
    # srun launches NUM_MAPPERS instances of the command.
    # SLURM_PROCID gives each task a unique ID from 0 to NUM_MAPPERS-1.
    # Each mapper reads its own chunk and writes to its own output file.
    srun --ntasks=$NUM_MAPPERS bash -c '
        TASK_ID=$(printf "%02d" $SLURM_PROCID)
        INPUT_CHUNK="$1/tmp/chunk_${TASK_ID}.txt"
        MAP_OUTPUT="$1/tmp/map_out_${TASK_ID}.txt"
        python3 mapper.py "$2" < "$INPUT_CHUNK" > "$MAP_OUTPUT"
    ' bash "$OUTPUT_DIR" "$PREV_CENTROIDS" # Pass arguments to the bash -c command

    # --- Aggregate, Sort, and Reduce Step ---
    # Consolidate all mapper outputs
    cat "$OUTPUT_DIR/tmp/map_out_"*.txt > "$OUTPUT_DIR/tmp/combined_map_out.txt"
    
    # Sort the combined output (Shuffle & Sort) and pipe to the reducer
    sort -k1,1n "$OUTPUT_DIR/tmp/combined_map_out.txt" | python3 reducer.py > "$NEW_CENTROIDS"

    # --- Convergence Check ---
    if diff -q "$PREV_CENTROIDS" "$NEW_CENTROIDS" > /dev/null; then
        echo "Convergence reached at iteration $i."
        CONVERGED=1
        break
    fi
done

if [ -z "$CONVERGED" ]; then
    echo "Reached max iterations ($MAX_ITER) without convergence."
fi

# --- Final Output Generation ---
# The final centroids are the ones from the last successful iteration.
FINAL_CENTROIDS="$OUTPUT_DIR/centroids_$i.txt"
echo "Final centroids written to $FINAL_CENTROIDS"

# Create the final point-to-cluster assignment file [cite: 128]
echo "Generating final cluster assignments..."
ASSIGNMENT_FILE="$OUTPUT_DIR/assignments.txt"
cat $POINTS_FILE | python3 mapper.py $FINAL_CENTROIDS > $ASSIGNMENT_FILE

echo "K-Means finished. Final output is in $OUTPUT_DIR"