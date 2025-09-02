import pandas as pd
import matplotlib.pyplot as plt
import sys

# --- Configuration ---
RESULTS_CSV_FILE = 'scalability_results.csv'
TIME_TABLE_FILE = 'performance_table.md'
SPEEDUP_PLOT_FILE = 'speedup_plot.png'
EFFICIENCY_PLOT_FILE = 'efficiency_plot.png'

def time_to_seconds(time_str):
    """Converts SLURM's elapsed time format (e.g., 00:05:12) to seconds."""
    parts = list(map(int, time_str.split(':')))
    seconds = 0
    if len(parts) == 3: # HH:MM:SS
        seconds = parts[0] * 3600 + parts[1] * 60 + parts[2]
    elif len(parts) == 2: # MM:SS
        seconds = parts[0] * 60 + parts[1]
    return seconds

def main():
    """Reads raw timing data, calculates metrics, and generates outputs."""
    try:
        # Read the CSV file into a pandas DataFrame
        df = pd.read_csv(RESULTS_CSV_FILE)
    except FileNotFoundError:
        print(f"Error: The results file '{RESULTS_CSV_FILE}' was not found.")
        print("Please run the scalability tests first.")
        sys.exit(1)

    # --- Data Processing ---
    # Convert the time string to total seconds for easier calculation
    df['TimeSeconds'] = df['ExecutionTime'].apply(time_to_seconds)
    
    # The baseline time is the time it took to run on a single core
    try:
        baseline_time = df[df['Cores'] == 1]['TimeSeconds'].iloc[0]
    except IndexError:
        print("Error: Baseline time for 1 core not found in the results file.")
        print("A 1-core run is required to calculate speedup and efficiency.")
        sys.exit(1)

    # Calculate Speedup and Efficiency
    df['Speedup'] = baseline_time / df['TimeSeconds']
    df['Efficiency'] = df['Speedup'] / df['Cores']
    df['IdealSpeedup'] = df['Cores']

    # --- Generate Markdown Table for Report ---
    # Round the values for cleaner presentation
    report_df = df.round({'TimeSeconds': 2, 'Speedup': 2, 'Efficiency': 3})
    
    # Select and rename columns for the final report table
    report_df = report_df[['Cores', 'ExecutionTime', 'TimeSeconds', 'Speedup', 'Efficiency']]
    report_df.rename(columns={'TimeSeconds': 'Time (s)'}, inplace=True)

    print("--- Performance Analysis Results ---")
    table = report_df.to_markdown(index=False)
    print(table)
    
    # Save the table to a file
    with open(TIME_TABLE_FILE, 'w') as f:
        f.write(table)
    print(f"\n-> Performance table saved to '{TIME_TABLE_FILE}'")


    # --- Generate Plots ---
    plt.style.use('seaborn-v0_8-whitegrid')

    # 1. Speedup Plot
    plt.figure(figsize=(10, 6))
    plt.plot(df['Cores'], df['Speedup'], 'o-', label='Actual Speedup', markersize=8)
    plt.plot(df['Cores'], df['IdealSpeedup'], 'r--', label='Ideal (Linear) Speedup')
    plt.title('K-Means Strong Scaling Speedup', fontsize=16)
    plt.xlabel('Number of Cores (P)', fontsize=12)
    plt.ylabel('Speedup (T_1 / T_p)', fontsize=12)
    plt.legend(fontsize=10)
    plt.grid(True)
    plt.xticks(df['Cores'])
    plt.savefig(SPEEDUP_PLOT_FILE)
    print(f"-> Speedup plot saved to '{SPEEDUP_PLOT_FILE}'")

    # 2. Efficiency Plot
    plt.figure(figsize=(10, 6))
    plt.plot(df['Cores'], df['Efficiency'] * 100, 'o-', label='Actual Efficiency', color='green', markersize=8)
    plt.axhline(y=100, color='r', linestyle='--', label='Ideal Efficiency (100%)')
    plt.title('K-Means Strong Scaling Efficiency', fontsize=16)
    plt.xlabel('Number of Cores (P)', fontsize=12)
    plt.ylabel('Efficiency (Speedup / P) %', fontsize=12)
    plt.ylim(0, 110) # Set y-axis from 0% to 110%
    plt.legend(fontsize=10)
    plt.grid(True)
    plt.xticks(df['Cores'])
    plt.savefig(EFFICIENCY_PLOT_FILE)
    print(f"-> Efficiency plot saved to '{EFFICIENCY_PLOT_FILE}'")


if __name__ == "__main__":
    main()
    