#!/bin/bash
set -e # Exit immediately if a command exits with a non-zero status.

# --- Configuration ---
# The list of core counts you want to test.
CORE_COUNTS=(1 2 4 8 16 24 32 48 64 96)

chmod +x submit_generate.slurm submit_kmeans.slurm submit_verify.slurm
chmod +x generate_script.py run_mapreduce.sh mapper.py reducer.py verify_script.py

# Parameters for the single, large dataset we will use for all tests.
NUM_POINTS=500000
NUM_DIMS=5
K=50
MAX_ITER=200

# Output file for raw timing data.
JOB_IDS_FILE="job_ids.csv"

# Can further remove this from here, just do sbatch outside and then once generation is done, run this script, to avoid waiting like this tch tch
# --- Step 1: Generate the Benchmark Dataset (only once) ---
echo "--- Submitting Data Generation Job ---"
# We wait for this job to complete before starting the tests.
sbatch --wait submit_generate.slurm "$NUM_POINTS" "$NUM_DIMS" "$K" "$MAX_ITER"
echo "-> Data generation complete."
echo "------------------------------------------"


# --- Step 2: Run K-Means for each core count and record time ---
echo "--- Starting Scalability Tests ---"

# Create the CSV header
echo "Cores,KMeansJobID" > "$JOB_IDS_FILE"

# Loop through the defined core counts
for cores in "${CORE_COUNTS[@]}"; do
    OUTPUT_DIR="output_${cores}_cores"
    mkdir -p "$OUTPUT_DIR"
    echo "-> Submitting K-Means job with $cores core(s)..."
    
    # Submit the job and specify the number of tasks (--ntasks) for this run.
    # Capture the job ID from the output.
    JOB_ID=$(sbatch --parsable --ntasks="$cores" submit_kmeans.slurm "$K" "$MAX_ITER" "$OUTPUT_DIR")
    
    echo "   KMeans Job ID: $JOB_ID"

    VERIFY_JOB_ID=$(sbatch --parsable --dependency=afterok:"$JOB_ID" submit_verify.slurm "$OUTPUT_DIR")
    echo "   Verification Job ID: $VERIFY_JOB_ID (depends on $JOB_ID)"

    # Save the job IDs to our CSV file
    echo "$cores,$JOB_ID" >> "$JOB_IDS_FILE"
done

echo "------------------------------------------"
echo "All scalability tests are submitted."
echo "Monitor progress with 'squeue -u \$USER'."
echo "Once all jobs are finished, run './collect_results.sh' to get the timings."
