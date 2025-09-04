#!/bin/bash

# --- Argument Parsing ---
if [ "$#" -ne 5 ]; then
    echo "Usage: ./run_mapreduce.sh K points.csv centers.txt max_iter output_dir"
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
    # echo "--- Iteration $i ---"
    PREV_CENTROIDS="$OUTPUT_DIR/centroids_$(($i-1)).txt"
    NEW_CENTROIDS="$OUTPUT_DIR/centroids_$i.txt"
    
    # --- Parallel Map -> Combine Step ---
    # Each task now runs a full Map -> Sort -> Combine pipeline locally.
    srun --ntasks=$NUM_MAPPERS bash -c '
        TASK_ID=$(printf "%02d" $SLURM_PROCID)
        INPUT_CHUNK="$1/tmp/chunk_${TASK_ID}.txt"
        COMBINED_OUTPUT="$1/tmp/combined_out_${TASK_ID}.txt"
        python3 mapper.py "$2" < "$INPUT_CHUNK" | sort -k1,1n | python3 combiner.py > "$COMBINED_OUTPUT"
    ' bash "$OUTPUT_DIR" "$PREV_CENTROIDS" # Pass arguments to the bash -c command

    # --- Aggregate, Sort, and Reduce Step ---
    # Consolidate all mapper outputs
    cat "$OUTPUT_DIR/tmp/combined_out_"*.txt > "$OUTPUT_DIR/tmp/global_combined_out.txt"
    rm "$OUTPUT_DIR/tmp/combined_out_"*.txt
    
    sort -k1,1n "$OUTPUT_DIR/tmp/global_combined_out.txt" | python3 reducer.py "$PREV_CENTROIDS" > "$NEW_CENTROIDS"
    rm "$OUTPUT_DIR/tmp/global_combined_out.txt"

    # --- Convergence Check ---
    if diff -q "$PREV_CENTROIDS" "$NEW_CENTROIDS" > /dev/null; then
        echo "Convergence reached at iteration $i."
        # Final assignment generation for converged state
        FINAL_CENTROIDS_PATH=$NEW_CENTROIDS
        srun --ntasks=$NUM_MAPPERS bash -c '
            TASK_ID=$(printf "%02d" $SLURM_PROCID)
            INPUT_CHUNK="$1/tmp/chunk_${TASK_ID}.txt"
            ASSIGNMENT_OUTPUT="$1/assignments_${TASK_ID}.txt"
            python3 mapper.py "$2" < "$INPUT_CHUNK" > "$ASSIGNMENT_OUTPUT"
        ' bash "$OUTPUT_DIR" "$FINAL_CENTROIDS_PATH"
        cat "$OUTPUT_DIR/assignments_"*.txt > "$OUTPUT_DIR/assignments.txt"
        cp "$FINAL_CENTROIDS_PATH" "$OUTPUT_DIR/centroids_final.txt"
        rm "$OUTPUT_DIR/assignments_"*.txt
        break
    fi

    # Handle final output if max iterations is reached
    if [ "$i" -eq "$MAX_ITER" ]; then
        echo "Reached max iterations ($MAX_ITER) without convergence."
        FINAL_CENTROIDS_PATH=$NEW_CENTROIDS
        srun --ntasks=$NUM_MAPPERS bash -c '
            TASK_ID=$(printf "%02d" $SLURM_PROCID)
            INPUT_CHUNK="$1/tmp/chunk_${TASK_ID}.txt"
            ASSIGNMENT_OUTPUT="$1/assignments_${TASK_ID}.txt"
            python3 mapper.py "$2" < "$INPUT_CHUNK" > "$ASSIGNMENT_OUTPUT"
        ' bash "$OUTPUT_DIR" "$FINAL_CENTROIDS_PATH"
        cat "$OUTPUT_DIR/assignments_"*.txt > "$OUTPUT_DIR/assignments.txt"
        cp "$FINAL_CENTROIDS_PATH" "$OUTPUT_DIR/centroids_final.txt"
        rm "$OUTPUT_DIR/assignments_"*.txt
    fi
done

echo "K-Means finished. Final assignment is in $OUTPUT_DIR/assignments.txt"
rm -rf "$OUTPUT_DIR/tmp"