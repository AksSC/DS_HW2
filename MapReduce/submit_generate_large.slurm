#!/bin/bash
#SBATCH --job-name=generate_data_large_kmeans
#SBATCH --output=generate_large_output_%j.txt
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=1
#SBATCH --mem=4G            # Request 4 GB of memory, which should be plenty
#SBATCH --time=02:00:00     # 1 hour time limit

# --- Argument Validation ---
if [ "$#" -ne 3 ]; then
    echo "Error: Incorrect number of arguments."
    echo "Usage: sbatch submit_generate_large.slurm <num_points> <num_dimensions> <k>"
    exit 1
fi

chmod +x generate_large_script.py

echo "--- Starting K-Means Data Generation ---"
echo "Number of Points: $1"
echo "Number of Dimensions: $2"
echo "K (Clusters): $3"

# Ensure python is available (uncomment if you have modules)
module load python/3.12.5
source "$HOME/kmeans_env/bin/activate"

python3 generate_large_script.py "$1" "$2" "$3"

echo "--- Generation Complete ---"