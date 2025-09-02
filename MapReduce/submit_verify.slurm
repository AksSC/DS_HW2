#!/bin/bash
#SBATCH --job-name=verify_kmeans
#SBATCH --output=verify_output_%j.txt
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=1
#SBATCH --mem=4G            # Request 4 GB of memory, which should be plenty
#SBATCH --time=00:10:00     # 10-minute time limit

echo "--- Starting K-Means Verification ---"

# Ensure python is available (uncomment if you have modules)
module load python/3.12.5

source "$HOME/DS_HW2/kmeans_env/bin/activate"

# Run the verification script with the correct file paths
# Make sure these paths are correct for your directory structure
python3 verify_script.py data/expected_centers.csv output/centroids_final.txt data/expected_assignments.csv output/assignments.txt

echo "--- Verification Complete ---"