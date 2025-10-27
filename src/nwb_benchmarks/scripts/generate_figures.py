"""Script to generate and save benchmark figures."""

from pathlib import Path

from nwb_benchmarks.database import BenchmarkDatabase, BenchmarkVisualizer

LBL_MAC_MACHINE_ID = "87fee773e425b4b1d3978fbf762d57effb0e8df8"


def main():
    # Initialize database handler
    db = BenchmarkDatabase(machine_id=LBL_MAC_MACHINE_ID)
    db.create_database()

    # Initialize visualizer
    visualizer = BenchmarkVisualizer(output_directory=Path(__file__).parent / "figures")

    # Generate all plots
    visualizer.plot_all(db)


if __name__ == "__main__":
    main()
