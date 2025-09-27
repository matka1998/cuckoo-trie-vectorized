#!/bin/bash

set -e
#TODO: Maybe run with different flags?
ITERATIONS=${1:-2} # TODO: Choose number of iterations instead of 2
RESULT_FILE="result.csv"
BENCHMARK_BINARY="benchmark"

SETTINGS=(
    "main"
    "feature/vectorization"
    "feature/vectorization_without_split"
    "c951393f7a9b7abb7ce005f6fee3d0ee5f7c21ef"
)

SUB_BENCHMARKS=( # TODO: Choose the benchmarks to run
    "insert"
    "pos-lookup"
    #"mt-pos-lookup"
    #"mw-insert-pos-lookup"
    #"range-read"
    #"range-prefetch"
    #"range-skip"
    #"mem-usage"
    #"mt-insert"
    "ycsb-a"
    # Continue..
)

echo "Running benchmark suite with $ITERATIONS iterations per test"
echo "Results will be saved to $RESULT_FILE"

echo "benchmark_name,setting,benchmark_time" > "$RESULT_FILE"

run_benchmark() {
    local benchmark_name="$1"
    local setting="$2"
    local binary_path="$3"

    echo "Running $benchmark_name with setting: $setting"

    # TODO: Choose the datasets to run
    for ((i=1; i<=ITERATIONS; i++)); do
        "$binary_path" "$benchmark_name" rand-8 > output.txt
    done

    benchmark_time=$(grep "RESULT:" output.txt | grep -o "ms=[0-9]*" | cut -d'=' -f2)

    echo "$benchmark_name,$setting,$benchmark_time" >> "$RESULT_FILE"
    echo "  Completed in ${benchmark_time}ms"
}

build_benchmark() {
    local setting="$1"

    echo "Building benchmark for setting: $setting"
    local branch_name="$1"

    echo "Checking out branch: $branch_name"
    git checkout "$branch_name" || {
        echo "Error: Could not checkout branch $branch_name"
        echo "Available branches:"
        git branch -a
        exit 1
    }

    echo "Cleaning and building..."
    make clean
    make || {
        echo "Error: Build failed for setting $setting"
        exit 1
    }

    if [ ! -f "$BENCHMARK_BINARY" ]; then
        echo "Error: Benchmark binary '$BENCHMARK_BINARY' not found after build"
        exit 1
    fi

    echo "Build completed successfully"
}

plot_results() {
    echo "Generating plots..."

    python3 - << 'EOF'
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import sys
import os

if not os.path.exists('result.csv'):
    print("Error: result.csv not found")
    sys.exit(1)

try:
    df = pd.read_csv('result.csv')

    if df.empty:
        print("Error: No data found in result.csv")
        sys.exit(1)

    plt.style.use('default')
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 6))

    sns.barplot(data=df, x='benchmark_name', y='benchmark_time', hue='setting', ax=ax1)
    ax1.set_title('Benchmark Times by Setting')
    ax1.set_ylabel('Time (seconds)')
    ax1.set_xlabel('Benchmark')
    ax1.tick_params(axis='x', rotation=45)
    ax1.legend(bbox_to_anchor=(1.05, 1), loc='upper left')

    pivot_df = df.pivot(index='benchmark_name', columns='setting', values='benchmark_time')
    sns.heatmap(pivot_df, annot=True, fmt='.4f', cmap='YlOrRd', ax=ax2)
    ax2.set_title('Benchmark Times Heatmap')
    ax2.set_ylabel('Benchmark')
    ax2.set_xlabel('Setting')

    plt.tight_layout()
    plt.savefig('benchmark_results.png', dpi=300, bbox_inches='tight')
    plt.savefig('benchmark_results.pdf', bbox_inches='tight')

    print("Plots saved as:")
    print("  - benchmark_results.png")
    print("  - benchmark_results.pdf")

    print("\nSummary Statistics:")
    print(df.groupby('setting')['benchmark_time'].agg(['mean', 'std', 'min', 'max']))

except ImportError as e:
    print(f"Error: Missing required Python packages: {e}")
    print("Please install required packages: pip install pandas matplotlib seaborn")
    sys.exit(1)
except Exception as e:
    print(f"Error generating plots: {e}")
    sys.exit(1)
EOF

    echo "Plotting completed"
}

check_dependencies() {
    echo "Checking dependencies..."

    if ! command -v git &> /dev/null; then
        echo "Error: git is required but not installed"
        exit 1
    fi

    if ! command -v make &> /dev/null; then
        echo "Error: make is required but not installed"
        exit 1
    fi

    if ! command -v bc &> /dev/null; then
        echo "Error: bc is required but not installed"
        exit 1
    fi

    

    if ! command -v python3 &> /dev/null; then
        echo "Warning: python3 not found, plotting will be skipped"
        SKIP_PLOTTING=true
    fi

    echo "Dependencies check completed"
}

main() {
    echo "=== Benchmark Suite Runner ==="
    echo "Started at: $(date)"
    echo

    check_dependencies

    if [ -f "$RESULT_FILE" ]; then
        echo "Warning: $RESULT_FILE already exists and will be overwritten"
        read -p "Continue? (y/N): " -n 1 -r
        echo
        if [[ ! $REPLY =~ ^[Yy]$ ]]; then
            echo "Aborted"
            exit 1
        fi
    fi

    echo "benchmark_name,setting,benchmark_time" > "$RESULT_FILE"

    original_branch=$(git rev-parse --abbrev-ref HEAD)
    echo "Current branch: $original_branch"

    for setting in "${SETTINGS[@]}"; do
        echo
        echo "=== Processing setting: $setting ==="

        build_benchmark "$setting"

        for benchmark in "${SUB_BENCHMARKS[@]}"; do
            run_benchmark "$benchmark" "$setting" "./$BENCHMARK_BINARY"
        done
    done

    echo
    echo "=== Restoring original branch ==="
    git checkout "$original_branch"

    echo
    echo "=== Results Summary ==="
    echo "Total benchmarks run: $((${#SETTINGS[@]} * ${#SUB_BENCHMARKS[@]}))"
    echo "Results saved to: $RESULT_FILE"
    echo

    if [ "$SKIP_PLOTTING" != "true" ]; then
        plot_results
    else
        echo "Skipping plotting (python3 not available)"
    fi

    echo
    echo "Benchmark suite completed at: $(date)"
    echo "Results:"
    column -t -s',' "$RESULT_FILE"
}

if [ "$1" = "--help" ] || [ "$1" = "-h" ]; then
    echo "Usage: $0 [ITERATIONS]"
    echo
    echo "Run benchmark suite across different settings"
    echo
    echo "Arguments:"
    echo "  ITERATIONS    Number of iterations per benchmark (default: 1000000)"
    echo
    echo "Settings tested:"
    for setting in "${SETTINGS[@]}"; do
        echo "  - $setting"
    done
    echo
    echo "Sub-benchmarks:"
    for benchmark in "${SUB_BENCHMARKS[@]}"; do
        echo "  - $benchmark"
    done
    echo
    echo "Output:"
    echo "  - result.csv: Raw benchmark data"
    echo "  - benchmark_results.png: Visualization (if python3 available)"
    echo "  - benchmark_results.pdf: Visualization (if python3 available)"
    exit 0
fi

main "$@"