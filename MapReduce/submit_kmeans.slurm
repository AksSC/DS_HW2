#!/bin/bash
#SBATCH --job-name=kmeans_mapreduce
#SBATCH --output=kmeans_output_%j.txt
#SBATCH --nodes=1
#SBATCH --ntasks=24          # number of mappers as well - need to change and try things for scalability tests
#SBATCH --cpus-per-task=1   
#SBATCH --mem=8G
#SBATCH --time=10:00:00     # 180-minute time limit (might be required for the 1 mapper??)

# --- Load any necessary modules (if required on your cluster) ---
# module load python/3.8

if [ "$#" -ne 3 ]; then
    echo "Error: Incorrect number of arguments."
    echo "Usage: sbatch submit_kmeans.slurm <K> <max_iterations> <output_dir>"
    exit 1
fi

echo "--- Starting K-Means MapReduce Job ---"
echo "K: $1"
echo "Max Iterations: $2"
echo "Output Directory: $3"

# Environment setup
module load python/3.12.5
source "$HOME/kmeans_env/bin/activate"

# --- Run the main driver script ---
# Example values, replace with your actual files and parameters
./run_mapreduce.sh "$1" data/points.csv data/initial_centers.csv "$2" "$3"

echo "--- K-Means MapReduce Job Complete ---"