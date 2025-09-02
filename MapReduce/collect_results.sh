#!/bin/bash
set -e # Exit immediately if a command fails

# --- Configuration ---
JOB_IDS_FILE="job_ids.csv"
RESULTS_FILE="scalability_results.csv"

echo "--- Collecting Results for Completed Jobs ---"

if [ ! -f "$JOB_IDS_FILE" ]; then
    echo "Error: Job ID file '$JOB_IDS_FILE' not found. Please run the submission script first."
    exit 1
fi

# Create the header for the final results file
echo "Cores,ExecutionTime" > "$RESULTS_FILE"

# Read the job_ids.csv file, skipping the header line
tail -n +2 "$JOB_IDS_FILE" | while IFS=, read -r cores job_id; do
    echo "-> Fetching time for job $job_id ($cores cores)..."
    
    # Use sacct to get the Elapsed time for the specific job ID
    ELAPSED_TIME=$(sacct -j "$job_id" --format=Elapsed --noheader | head -n 1 | tr -d '[:space:]')
    
    if [ -z "$ELAPSED_TIME" ]; then
        echo "   Warning: Could not retrieve time for job $job_id. Was it cancelled or did it fail?"
        continue
    fi
    
    echo "   Elapsed Time: $ELAPSED_TIME"
    
    # Append the result to the final CSV file
    echo "$cores,$ELAPSED_TIME" >> "$RESULTS_FILE"
done

echo "------------------------------------------"
echo "Result collection complete."
echo "Final timing data saved to '$RESULTS_FILE'."    
