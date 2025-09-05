# K-Means Clustering with MapReduce

## 1. Introduction

This project implements the iterative K-Means clustering algorithm using a "from-scratch" MapReduce framework built with standard Linux tools and the SLURM workload manager. The implementation is designed for scalability and has been optimized using a combiner to minimize data shuffling overhead. The entire workflow, from data generation to performance testing, is automated with a series of shell scripts.

## 2. File Structure

* `mapper.py`, `combiner.py`, `reducer.py`: Core Python scripts containing the MapReduce logic.
* `run_mapreduce.sh`: The main driver script that executes one iteration of the K-Means algorithm.
* `generate_script.py`: Generates small-to-medium datasets and a "ground truth" sequential K-Means result for validation.
* `generate_large_script.py`: A memory-efficient script for generating very large datasets that do not fit in memory.
* `verify_script.py`: Compares the MapReduce output against the ground truth.
* `*.slurm`: SLURM submission scripts for individual job steps (generation, k-means, verification).
* `run_full_pipeline.sh`, `submit_scalability_tests.sh`, `collect_results.sh`: Master scripts for automating the entire workflow.

## 3. Compilation and Environment Setup

The implementation requires Python 3 and the `numpy` library. The following steps should be performed once on the cluster's login node to set up the environment.

```bash
# 1. Navigate to your home directory and load the required Python module
cd ~
module load python/3.12.5

# 2. Create and activate a dedicated Python virtual environment
python3 -m venv kmeans_env
source kmeans_env/bin/activate

# 3. Upgrade pip and install numpy
pip install --upgrade pip
pip install numpy

# 4. (Optional) For running the performance analysis script, install pandas and matplotlib
pip install pandas matplotlib
```

## 4. Execution Instructions

All scripts should be made executable before running:

```bash
chmod +x *.sh *.py
```

The entire workflow is automated. The following instructions cover running a single, validated experiment and running the full scalability analysis.

### A. Single Validated Run (for smaller datasets)

This is the recommended way to run a single, end-to-end experiment that generates data, runs the MapReduce job, and verifies the output.

Use the `run_full_pipeline.sh` script:

```bash
# Usage: ./run_full_pipeline.sh <num_points> <num_dimensions> <K> <max_iterations>

# Example: Run with 100,000 points, 3 dimensions, K=20, and 50 max iterations
./run_full_pipeline.sh 100000 3 20 50
```

This will submit a chain of dependent SLURM jobs. You can monitor their progress with `squeue -u $USER`. The final output and verification logs will be in the respective job output files (e.g., `kmeans_output_*.txt`, `verify_output_*.txt`).

### B. Large-Scale Performance & Scalability Testing

This workflow is for testing the implementation on datasets too large to be validated sequentially.

#### Step 1: Generate a Large Dataset

Use the `submit_generate_large.slurm` script.

```bash
# This will generate a large dataset in the data directory
# Usage: sbatch submit_generate_large.slurm <num_points> <num_dimensions> <K>
sbatch submit_generate_large.slurm 10000000 2 50
```

Wait for this job to complete before proceeding.

#### Step 2: Submit All Scalability Test Jobs

Use the `submit_scalability_tests.sh` script. This will submit a series of K-Means jobs for different core counts (from 1 to 96, as configured in the script).

```bash
# Usage: ./submit_scalability_tests.sh <K> <max_iterations>

# Example: Run scalability tests for K=50 and 100 iterations
./submit_all_tests.sh 50 100
```

This will submit many jobs to the queue. You can log off and check their status later with `squeue -u $USER`.

#### Step 3: Collect Performance Results

Once all jobs from Step 2 are finished, run the collection script.

```bash
./collect_results.sh
```

This will query SLURM's accounting database for the execution time of each completed job and save the results in `scalability_results.csv`.

#### Step 4: Analyze Results (Optional)

To generate the performance table and plots, run the analysis script.

```bash
python3 analyze_results.py
```

This will print a markdown table to the console and save `speedup_plot.png` and `efficiency_plot.png` to the directory. I prefer doing this from my local system rather than on the login node.
