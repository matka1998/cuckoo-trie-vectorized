import pandas as pd
import matplotlib.pyplot as plt
import os
import sys

BRANCH_MAPPING = {"main": "with split",
                    "c951393f7a9b7abb7ce005f6fee3d0ee5f7c21ef": "original",
                    "feature/vectorization": "vectorization with split",
                    "feature/vectorization_without_split": "vectorization without split",
                    }

COLOR_MAPPING = {"main": "blue",
                 "c951393f7a9b7abb7ce005f6fee3d0ee5f7c21ef": "red",
                 "feature/vectorization": "green",
                 "feature/vectorization_without_split": "orange",
                 }

def plot_benchmarks(csv_path, output_dir="benchmark_plots"):
    # Load CSV
    df = pd.read_csv(csv_path)

    # Make sure output folder exists
    os.makedirs(output_dir, exist_ok=True)

    # Generate a plot for each benchmark_name
    for name, group in df.groupby("benchmark_name"):
        plt.figure(figsize=(10, 6))

        # X-axis: combine setting + dataset
        x_labels = [BRANCH_MAPPING[setting] + " | " + dataset 
                   for setting, dataset in zip(group["setting"], group["dataset"])]
        y_values = group["benchmark_time"]
        
        # Get colors for each bar based on setting
        colors = [COLOR_MAPPING[setting] for setting in group["setting"]]

        bars = plt.bar(x_labels, y_values, color=colors)
        for bar, y in zip(bars, y_values):
            plt.text(bar.get_x() + bar.get_width() / 2.0,
                     bar.get_height(),
                     f"{y:.2f}",
                     ha="center", va="bottom")
        plt.xticks(rotation=45, ha="right")
        plt.ylabel("benchmark_time")
        plt.xlabel("setting | dataset")
        plt.title(f"Benchmark: {name}")
        plt.tight_layout()

        # Save figure
        plot_path = os.path.join(output_dir, f"{name}.png")
        plt.savefig(plot_path)
        plt.close()

    print(f"Plots saved in: {os.path.abspath(output_dir)}")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python plot_benchmarks.py <csv_file> [output_dir]")
        sys.exit(1)

    csv_file = sys.argv[1]
    output_dir = sys.argv[2] if len(sys.argv) > 2 else "benchmark_plots"
    plot_benchmarks(csv_file, output_dir)
