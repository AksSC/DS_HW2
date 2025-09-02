#!/bin/bash
#SBATCH --job-name=kmeans_mapreduce
#SBATCH --output=kmeans_output_%j.txt
#SBATCH --nodes=1
#SBATCH --ntasks=24          # number of mappers as well - need to change and try things for scalability tests
#SBATCH --cpus-per-task=1   
#SBATCH --mem=8G
#SBATCH --time=00:30:00     # 30-minute time limit

# --- Load any necessary modules (if required on your cluster) ---
# module load python/3.8

# --- Ensure scripts are executable ---
chmod +x run_mapreduce.sh mapper.py reducer.py

# --- Run the main driver script ---
# Example values, replace with your actual files and parameters
./run_mapreduce.sh 2 data/points.csv data/initial_centers.csv 50 output