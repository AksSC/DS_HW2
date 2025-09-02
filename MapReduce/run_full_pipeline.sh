#!/bin/bash

# --- Argument Validation ---
if [ "$#" -ne 4 ]; then
    echo "Error: Incorrect number of arguments."
    echo "Usage: ./run_full_pipeline.sh <num_points> <num_dimensions> <k> <max_iterations>"
    exit 1
fi

chmod +x submit_generate.slurm submit_kmeans.slurm submit_verify.slurm
chmod +x generate_and_test.py run_mapreduce_parallel.sh mapper.py reducer.py verify_output.py

# Assign arguments to named variables for clarity
NUM_POINTS=$1
NUM_DIMS=$2
K=$3
MAX_ITER=$4

echo "--- Submitting Full K-Means Pipeline ---"
echo "Parameters: Points=$NUM_POINTS, Dims=$NUM_DIMS, K=$K, Iterations=$MAX_ITER"
echo "------------------------------------------"

# --- Step 1: Submit the Data Generation Job ---
# The 'sbatch' command prints the job ID, which we capture.
# The '--parsable' flag makes the output just the job ID, which is cleaner.
echo "Submitting data generation job..."
GENERATE_JOB_ID=$(sbatch --parsable submit_generate.slurm "$NUM_POINTS" "$NUM_DIMS" "$K" "$MAX_ITER")
echo "-> Generation Job ID: $GENERATE_JOB_ID"

# --- Step 2: Submit the K-Means Job with a Dependency ---
# This job will only start after the generation job (ID: $GENERATE_JOB_ID) completes successfully.
echo "Submitting K-Means job (will wait for generation to finish)..."
KMEANS_JOB_ID=$(sbatch --parsable --dependency=afterok:$GENERATE_JOB_ID submit_kmeans.slurm "$K" "$MAX_ITER")
echo "-> K-Means Job ID: $KMEANS_JOB_ID"

# --- Step 3: Submit the Verification Job with a Dependency ---
# This job will only start after the K-Means job (ID: $KMEANS_JOB_ID) completes successfully.
echo "Submitting verification job (will wait for K-Means to finish)..."
VERIFY_JOB_ID=$(sbatch --parsable --dependency=afterok:$KMEANS_JOB_ID submit_verify.slurm)
echo "-> Verification Job ID: $VERIFY_JOB_ID"

echo "------------------------------------------"
echo "All jobs submitted. Monitor progress with: squeue -u \$USER"
