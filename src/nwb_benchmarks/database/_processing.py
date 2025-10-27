import re
from functools import lru_cache
from pathlib import Path
from typing import Optional

import polars as pl

from nwb_benchmarks.database._parquet import repackage_as_parquet

PACKAGES_OF_INTEREST = ["h5py", "fsspec", "lindi", "remfile", "zarr", "hdmf-zarr", "hdmf", "pynwb"]


class BenchmarkDatabase:
    """Handles database preprocessing and loading for NWB benchmarks."""

    def __init__(
        self,
        results_directory: Optional[Path] = None,
        db_directory: Optional[Path] = None,
        machine_id: str = None,
    ):
        """Initialize database handler with directories and machine ID.

        Args:
            results_directory: Directory containing benchmark results
            db_directory: Directory for database storage
            machine_id: Machine ID for filtering results
        """
        self.machine_id = machine_id
        self.packages_of_interest = PACKAGES_OF_INTEREST

        # Set default directories if not provided
        cache_dir = Path.home() / ".cache" / "nwb-benchmarks"
        self.results_directory = results_directory or cache_dir / "nwb-benchmarks-results"
        self.db_directory = db_directory or cache_dir / "nwb-benchmarks-database"

        self._results_df = None
        self._environments_df = None

    def create_database(self, minimum_results_version: str = "3.0.0", minimum_machines_version: str = "1.4.0") -> None:
        """Create new database file with latest results."""
        repackage_as_parquet(
            directory=self.results_directory,
            output_directory=self.db_directory,
            minimum_results_version=minimum_results_version,
            minimum_machines_version=minimum_machines_version,
        )

    @staticmethod
    @lru_cache(maxsize=128)
    def split_camel_case(text: str) -> str:
        """Split camel case text into words."""
        text = re.sub(r"PyNWBS3", "PyNWB S3", text)
        text = re.sub(r"NWBROS3", "NWB ROS3", text)
        result = re.sub("([a-z0-9])([A-Z])", r"\1 \2", text)
        result = re.sub("([A-Z]+)([A-Z][a-z])", r"\1 \2", result)
        result = re.sub("Py NWB", "PyNWB", result)
        return result

    @lru_cache(maxsize=128)
    def clean_benchmark_name_test(self, name: str) -> str:
        """Clean benchmark test names."""
        short_name = (
            name.replace("ContinuousSliceBenchmark", "")
            .replace("FileReadBenchmark", "")
            .replace("DownloadBenchmark", "")
        )
        return self.split_camel_case(short_name).lower()

    def _preprocess_results(self, df: pl.LazyFrame) -> pl.DataFrame:
        """Apply all preprocessing transformations to the results dataframe."""
        print("Preprocessing benchmark results...")

        # clean benchmark expression
        clean_benchmark_operation_expr = (
            pl.col("benchmark_name_operation")
            .str.replace("time_read_", "")
            .str.replace("track_network_read_", "")
            .str.replace("time_download_", "")
            .str.replace_all("_", " ")
        )

        return (
            df
            # Filter for specific machine early to reduce data volume
            .filter(pl.col("machine_id") == self.machine_id if self.machine_id is not None else pl.lit(True))
            # Extract benchmark name components
            .with_columns(
                [
                    pl.col("parameter_case_name").str.extract(r"^(Ophys|Ecephys|Icephys)").alias("modality"),
                    pl.col("benchmark_name").str.split(".").list.get(0).alias("benchmark_name_type"),
                    pl.col("benchmark_name").str.split(".").list.get(1).alias("benchmark_name_test"),
                    pl.col("benchmark_name").str.split(".").list.get(2).alias("benchmark_name_operation"),
                ]
            )
            # Clean operation names using expressions
            .with_columns(clean_benchmark_operation_expr.alias("benchmark_name_operation"))
            # Extract benchmark labels and clean test names
            .with_columns(
                [
                    pl.col("benchmark_name_test")
                    .str.extract(r"(ContinuousSliceBenchmark|FileReadBenchmark|DownloadBenchmark)")
                    .alias("benchmark_name_label"),
                    pl.col("benchmark_name_test")
                    .map_elements(self.clean_benchmark_name_test, return_dtype=pl.String)
                    .alias("benchmark_name_test"),
                ]
            )
            # Handle preloaded information
            .with_columns(
                [
                    pl.col("benchmark_name_test").str.contains("Preloaded").alias("is_preloaded"),
                    pl.col("benchmark_name_test").str.replace(" Preloaded", "").alias("benchmark_name_test"),
                ]
            )
            # Extract scaling information
            .with_columns(
                pl.col("parameter_case_slice_range")
                .list.first()
                .str.extract(r"slice\(0, (\d+),", group_index=1)
                .cast(pl.Int64)
                .alias("scaling_value"),
            ).with_columns((pl.col("scaling_value").rank(method="dense")).over("modality").alias("slice_number"))
            # Create unified cleaned benchmark name
            .with_columns(
                pl.when(pl.col("benchmark_name_label") == "ContinuousSliceBenchmark")
                .then(pl.col("benchmark_name_test"))
                .when(pl.col("benchmark_name_label").is_in(["FileReadBenchmark", "DownloadDandiAPIBenchmark"]))
                .then(pl.col("benchmark_name_operation"))
                .otherwise(pl.col("benchmark_name_test"))  # fallback
                .alias("benchmark_name_clean")
            )
        )

    def _preprocess_environments(self, df: pl.LazyFrame) -> pl.LazyFrame:
        """Apply all preprocessing transformations to the environments dataframe."""

        return (
            df
            # get only relevant package columns
            .select(["environment_id", *self.packages_of_interest])
            # remove build information
            .with_columns([pl.col(pkg).str.extract(r"^([\d.]+)", group_index=1) for pkg in self.packages_of_interest])
            # unpivot packages into long format for plotting
            .unpivot(
                index="environment_id",
                on=self.packages_of_interest,
                variable_name="package_name",
                value_name="package_version",
            ).filter(pl.col("package_version").is_not_null())
        )

    def get_results(self) -> pl.LazyFrame:
        """
        Load and preprocess benchmark results with caching.

        Returns:
            Preprocessed benchmark results as a DataFrame
        """
        if self._results_df is None:
            lazy_df = pl.scan_parquet(self.db_directory / "results.parquet")
            self._results_df = self._preprocess_results(lazy_df)

        return self._results_df

    def get_environments(self) -> pl.LazyFrame:
        """
        Load and preprocess benchmark environments with caching.

        Returns:
            Preprocessed benchmark environments as a DataFrame
        """
        if self._environments_df is None:
            lazy_df = pl.scan_parquet(self.db_directory / "environments.parquet")
            self._environments_df = self._preprocess_environments(lazy_df)

        return self._environments_df

    def join_results_with_environments(self) -> pl.LazyFrame:
        """Join streaming package versions with results using the environments table."""
        return self.get_results().join(
            self.get_environments(),
            on="environment_id",
            how="left",
        )

    def filter_tests(self, benchmark_type: str) -> pl.LazyFrame:
        """Filter benchmark tests."""
        results_df = self.get_results()
        return results_df.filter(pl.col("benchmark_name_type") == benchmark_type)
