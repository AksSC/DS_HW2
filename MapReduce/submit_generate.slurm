#!/bin/bash
#SBATCH --job-name=generate_data_kmeans
#SBATCH --output=generate_output_%j.txt
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=1
#SBATCH --mem=8G            # Request 4 GB of memory, which should be plenty
#SBATCH --time=00:10:00     # 10-minute time limit

echo "--- Starting K-Means Data Generation ---"

# Ensure python is available (uncomment if you have modules)
module load python/3.12.5

source "$HOME/kmeans_env/bin/activate"

python3 generate_script.py

echo "--- Generation Complete ---"