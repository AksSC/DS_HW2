#!/bin/bash
#SBATCH --job-name=generate_data_kmeans
#SBATCH --output=generate_output_%j.txt
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=1
#SBATCH --mem=8G            # Request 4 GB of memory, which should be plenty
#SBATCH --time=00:10:00     # 10-minute time limit

# --- Argument Validation ---
if [ "$#" -ne 4 ]; then
    echo "Error: Incorrect number of arguments."
    echo "Usage: sbatch submit_generate.slurm <num_points> <num_dimensions> <k> <max_iterations>"
    exit 1
fi

chmod +x generate_script.py

echo "--- Starting K-Means Data Generation ---"
echo "Number of Points: $1"
echo "Number of Dimensions: $2"
echo "K (Clusters): $3"
echo "Max Iterations: $4"

# Ensure python is available (uncomment if you have modules)
module load python/3.12.5
source "$HOME/kmeans_env/bin/activate"

python3 generate_script.py "$1" "$2" "$3" "$4"

echo "--- Generation Complete ---"