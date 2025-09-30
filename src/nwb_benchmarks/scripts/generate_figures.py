"""Script to generate and save benchmark figures."""
from pathlib import Path

from nwb_benchmarks.database._processing import BenchmarkDatabase
from nwb_benchmarks.database._visualization import BenchmarkVisualizer

def main():
    # Initialize database handler
    db = BenchmarkDatabase()
    db.create_database()

    # Initialize visualizer
    visualizer = BenchmarkVisualizer(output_directory=Path(__file__).parent / "figures")
    
    # Generate all plots
    visualizer.generate_all_plots(db)


if __name__ == "__main__":
    main()
