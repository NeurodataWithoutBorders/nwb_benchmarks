import textwrap
from pathlib import Path
from typing import Any, Dict, List, Optional
from packaging import version

import matplotlib
import matplotlib.pyplot as plt
import pandas as pd
import polars as pl
import seaborn as sns

from nwb_benchmarks.database._processing import BenchmarkDatabase


class BenchmarkVisualizer:
    """Handles plotting and visualization of benchmark results."""

    # Default benchmark orders
    slicing_order = [
        "HDF5 PyNWB Fsspec S3 No Cache",
        "HDF5 PyNWB Fsspec Https No Cache",
        "HDF5 PyNWB Remfile No Cache",
        "HDF5 PyNWB Fsspec S3 With Cache",
        "HDF5 PyNWB Fsspec Https With Cache",
        "HDF5 PyNWB Remfile With Cache",
        "HDF5 PyNWB ROS3",
        "Lindi Local JSON",
        "Zarr PyNWB S3",
        "Zarr PyNWB S3 Force No Consolidated",
    ]

    file_open_order = [
        "hdf5 h5py remfile no cache",
        "hdf5 h5py fsspec https no cache",
        "hdf5 h5py fsspec s3 no cache",
        "hdf5 h5py remfile with cache",
        "hdf5 h5py fsspec https with cache",
        "hdf5 h5py fsspec s3 with cache",
        "hdf5 h5py ros3",
        "lindi h5py",
        "zarr https",
        "zarr https force no consolidated",
        "zarr s3",
        "zarr s3 force no consolidated",
    ]

    pynwb_reading_order = [
        "hdf5 pynwb remfile no cache",
        "hdf5 pynwb fsspec https no cache",
        "hdf5 pynwb fsspec s3 no cache",
        "hdf5 pynwb remfile with cache",
        "hdf5 pynwb fsspec https with cache",
        "hdf5 pynwb fsspec s3 with cache",
        "hdf5 pynwb ros3",
        "lindi pynwb",
        "zarr pynwb https",
        "zarr pynwb https force no consolidated",
        "zarr pynwb s3",
        "zarr pynwb s3 force no consolidated",
    ]

    download_order = ["Lindi"]

    def __init__(self, output_directory: Optional[Path] = None):
        """Initialize visualizer with output directory.

        Args:
            output_directory: Directory for saving figures
        """
        self.output_directory = output_directory or Path(__file__).parent / "figures"
        self.output_directory.mkdir(parents=True, exist_ok=True)
        self._setup_matplotlib()

    @staticmethod
    def _setup_matplotlib():
        """Setup matplotlib settings for editable text in Illustrator."""
        matplotlib.rcParams["pdf.fonttype"] = 42
        matplotlib.rcParams["ps.fonttype"] = 42

    def add_mean_sem_annotations(self, value: str, group: str, order: List[str], **kwargs):
        """Add mean ± SEM annotations to plot."""
        stats_df = kwargs.get("data").groupby(group)[value].agg(["mean", "std", "max", "count"])

        for i, label in enumerate(order):
            if label in stats_df.index:
                stats = stats_df.loc[label, :]
                mean_sem_text = self._format_stat_text(stats["mean"], stats["std"], stats["count"])

                plt.text(
                    x=stats["max"],
                    y=i,
                    s=mean_sem_text,
                    verticalalignment="center",
                    horizontalalignment="left",
                    fontsize=8,
                )

    @staticmethod
    def _format_stat_text(mean: float, std: float, count: int) -> str:
        """Format statistical text based on value magnitude."""
        if mean > 1000 or mean < 0.01:
            return f"  {mean:.2e} ± {std:.2e}, n={int(count)}"
        return f"  {mean:.2f} ± {std:.2f}, n={int(count)}"

    def plot_benchmark_dist(
        self,
        df: pd.DataFrame,
        group: str,
        metric_order: List[str],
        filename: str,
        row: Optional[str] = None,
        sharex: bool = True,
        add_annotations: bool = True,
        kind: str = "box",
        palette: str = "Paired",
        catplot_kwargs: Optional[Dict[str, Any]] = None,
    ):
        """Create distribution plot for benchmarks."""
        catplot_kwargs = catplot_kwargs or {}

        g = sns.catplot(
            data=df,
            x="value",
            y=group,
            col="modality",
            row=row,
            hue=group,
            sharex=sharex,
            palette=palette,
            kind=kind,
            order=metric_order,
            legend=False,
            **catplot_kwargs,
        )

        if add_annotations:
            g.map_dataframe(self.add_mean_sem_annotations, value="value", group=group, order=metric_order)

        g.set(xlabel="Time (s)", ylabel=df["benchmark_name_label"].iloc[0])

        for ax in g.axes.flat:
            wrapped_title = "\n".join(textwrap.wrap(ax.get_title(), width=50))
            ax.set_title(wrapped_title)

        sns.despine()
        plt.tight_layout()
        plt.savefig(filename, dpi=300)
        plt.close()

    def plot_benchmark_slices_vs_time(
        self,
        df: pl.LazyFrame,
        group: str,
        metric_order: List[str],
        filename: Path,
        row: Optional[str] = None,
        sharex: bool = True,
    ):
        """Plot benchmark performance vs slice size."""
        g = sns.catplot(
            data=df.to_pandas(),
            x="slice_number",
            y="value",
            col="modality",
            row=row,
            hue=group,
            hue_order=metric_order,
            sharex=sharex,
            palette="Paired",
            kind="point",
            sharey=False,
        )

        g.set(xlabel="Relative slice size", ylabel="Time (s)")
        sns.despine()
        plt.savefig(filename, dpi=300)
        plt.close()

    def _create_heatmap_df(self, df: pl.DataFrame, group: str, metric_order: List[str]) -> pd.DataFrame:
        """Prepare data for heatmap visualization."""
        return (
            df.to_pandas()
            .pivot_table(index=group, columns="modality", values="value", aggfunc="mean")
            .reindex(metric_order)
            .reindex(["Ecephys", "Ophys", "Icephys"], axis=1)
        )

    def plot_benchmark_heatmap(
        self, df: pl.LazyFrame, group: str, metric_order: List[str], ax: Optional[plt.Axes] = None
    ) -> plt.Axes:
        """Create heatmap visualization of benchmark results."""
        heatmap_df = self._create_heatmap_df(df, group, metric_order)

        if ax is None:
            _, ax = plt.subplots(figsize=(10, 8))

        sns.heatmap(data=heatmap_df, annot=True, fmt=".2f", cmap="OrRd", ax=ax)
        ax.set(xlabel="", ylabel="")

        # Add star for best method in each modality
        for j, col in enumerate(heatmap_df.columns):
            min_idx = heatmap_df[col].idxmin()
            i = heatmap_df.index.get_loc(min_idx)
            ax.text(j + 0.5, i + 0.5, "     *", fontsize=20, ha="center", va="center", color="black", weight="bold")

        return ax

    def _create_plot_kwargs(
        self, df, group: str, order: List[str], filename: Path, kind: str = "box", **extra_kwargs
    ) -> Dict[str, Any]:
        """Create common plot kwargs to reduce duplication."""
        plot_kwargs = {
            "df": df,
            "group": group,
            "metric_order": order,
            "filename": filename,
            "kind": kind,
        }

        if kind == "box":
            plot_kwargs["catplot_kwargs"] = {"showfliers": False, "boxprops": dict(linewidth=0)}

        plot_kwargs.update(extra_kwargs)

        return plot_kwargs

    def _get_filename_prefix(self, network_tracking: bool) -> str:
        """Get filename prefix based on network tracking."""
        return "network_tracking_" if network_tracking else ""

    def plot_read_benchmarks(
        self,
        db: BenchmarkDatabase,
        order: List[str],
        benchmark_type: str = "time_remote_file_reading",
        col_name: str = "benchmark_name_operation",
        network_tracking: bool = False,
        kind: str = "box",
        suffix: str = "pynwb",
    ):
        """Plot read benchmark results."""
        print(f"Plotting read benchmarks for {benchmark_type}...")

        filtered_df = db.filter_tests(benchmark_type).collect()
        prefix = self._get_filename_prefix(network_tracking)

        # Create base plot kwargs
        base_kwargs = self._create_plot_kwargs(
            df=filtered_df.to_pandas(),
            group=col_name,
            order=order,
            filename=self.output_directory / f"{prefix}file_read{suffix}.pdf",
            kind=kind,
        )

        # Add network tracking specific options
        if network_tracking:
            base_kwargs.update({"row": "variable", "sharex": "row"})

        # Plot box plot
        self.plot_benchmark_dist(**base_kwargs)

        # Plot scatter plot
        base_kwargs.update(
            {
                "catplot_kwargs": dict(),
                "kind": "strip",
                "add_annotations": False,
                "filename": self.output_directory / f"{prefix}file_read_scatter{suffix}.pdf",
            }
        )
        self.plot_benchmark_dist(**base_kwargs)

    def plot_slice_benchmarks(
        self,
        db: BenchmarkDatabase,
        order: List[str],
        benchmark_type: str = "time_remote_slicing",
        col_name: str = "benchmark_name_test",
        network_tracking: bool = False,
        kind: str = "box",
    ):
        """Plot slice benchmark results."""
        print(f"Plotting slice benchmarks for {benchmark_type}...")

        filtered_df = db.filter_tests(benchmark_type).collect()
        prefix = self._get_filename_prefix(network_tracking)

        # Create base plot kwargs
        base_kwargs = self._create_plot_kwargs(
            df=filtered_df,
            group=col_name,
            order=order,
            filename=None,
            kind=kind,
            row="is_preloaded",
        )

        if network_tracking:
            base_kwargs.update({"df": filtered_df.filter(~pl.col("is_preloaded")), "row": "variable", "sharex": "row"})

        # Plot box plot for each slice value
        for slice_num, slice_df in enumerate(base_kwargs["df"].partition_by("slice_number")):
            base_kwargs.update(
                {
                    "df": slice_df.to_pandas(),
                    "filename": self.output_directory / f"{prefix}slicing_range{slice_num}.pdf",
                }
            )
            self.plot_benchmark_dist(**base_kwargs)

        # Plot scatter plot
        base_kwargs.update(
            {
                "df": filtered_df.to_pandas(),
                "catplot_kwargs": dict(),
                "kind": "strip",
                "add_annotations": False,
                "filename": self.output_directory / f"{prefix}slicing_scatter.pdf",
            }
        )
        self.plot_benchmark_dist(**base_kwargs)

    def plot_download_vs_stream_benchmarks(
        self,
        db: BenchmarkDatabase,
        order: List[str],
        benchmark_type: str = "time_remote_slicing",
        network_tracking: bool = False,
    ):
        """Plot download vs stream benchmark comparison."""
        print("Plotting download vs stream benchmark comparison...")

        slice_df = db.filter_tests(benchmark_type).collect()
        prefix = self._get_filename_prefix(network_tracking)

        plot_kwargs = {
            "df": slice_df,
            "group": "benchmark_name_test",
            "metric_order": order,
            "filename": self.output_directory / f"{prefix}slicing_vs_time.pdf",
            "row": "variable" if network_tracking else "is_preloaded",
            "sharex": "row" if network_tracking else True,
        }

        self.plot_benchmark_slices_vs_time(**plot_kwargs)

    def plot_method_rankings(self, db: BenchmarkDatabase):
        """Create heatmap showing method rankings across benchmarks."""
        print("Plotting method rankings heatmap...")

        slice_df = db.filter_tests("time_remote_slicing").collect()
        read_df = db.filter_tests("time_remote_file_reading").collect()

        fig, axes = plt.subplots(3, 1, figsize=(8, 16))
        axes[0] = self.plot_benchmark_heatmap(
            df=read_df, group="benchmark_name_operation", metric_order=self.file_open_order, ax=axes[0]
        )

        axes[1] = self.plot_benchmark_heatmap(
            df=read_df, group="benchmark_name_operation", metric_order=self.pynwb_reading_order, ax=axes[1]
        )

        axes[2] = self.plot_benchmark_heatmap(
            df=slice_df, group="benchmark_name_test", metric_order=self.slicing_order, ax=axes[2]
        )

        axes[0].set_title("Remote File Reading")
        axes[1].set_title("Remote File Reading - PyNWB")
        axes[2].set_title("Remote Slicing")

        plt.tight_layout()
        plt.savefig(self.output_directory / "method_rankings_heatmap.pdf", dpi=300)
        plt.close()

    @staticmethod
    def set_package_version_categorical(group):
        # TODO - idk if this is actually sorting or not
        sorted_versions = sorted(
            group['package_version'].unique(), 
            key=lambda v: version.parse(v),
        )
        group['package_version'] = pd.Categorical(
            group['package_version'],
            categories=sorted_versions,
            ordered=True
        )
        return group

    def plot_performance_over_time(self, db: BenchmarkDatabase, order: List[str], 
                                   hue: str = "benchmark_name_operation",
                                   benchmark_type: str = "time_remote_file_reading"):
        """Plot performance changes over time for a given benchmark type."""
        print(f"Plotting performance over time")
        
        # get polars dataframe and filter
        df = db.join_results_with_environments()
        df = (
            df
            .filter(pl.col("benchmark_name_type") == benchmark_type)
            .collect()
            .to_pandas()
            .groupby('package_name')
            .apply(self.set_package_version_categorical, include_groups=False)
            .reset_index(level=0)
        )
        
        g = sns.catplot(
            data=df,
            x="package_version",
            y="value",
            col="modality",
            row="package_name",
            hue=hue,
            hue_order=order,
            palette="Paired",
            kind="point",
            sharey=True,
            sharex=False,
        )

        g.set(xlabel="Package version", ylabel="Time (s)")

        sns.despine()
        plt.savefig(self.output_directory / f"performance_over_{benchmark_type}.pdf", dpi=300)
        plt.close()

    def plot_all(self, db: BenchmarkDatabase):
        """Generate all benchmark visualization plots."""
        # 1. WHICH LIBRARY SHOULD I USE TO STREAM DATA

        # Remote file reading benchmarks
        self.plot_read_benchmarks(db, self.file_open_order, suffix="")
        self.plot_read_benchmarks(db, self.pynwb_reading_order, suffix="_pynwb")
        self.plot_read_benchmarks(
            db,
            self.download_order,
            benchmark_type="time_download_lindi",
            col_name="benchmark_name_test",
            suffix="_lindidownload",
        )

        # Remote file slicing
        self.plot_slice_benchmarks(db, self.slicing_order)

        # Network tracking analysis
        benchmark_type = "network_tracking_remote_file_reading"
        self.plot_read_benchmarks(db, self.file_open_order, benchmark_type=benchmark_type, network_tracking=True)
        self.plot_read_benchmarks(
            db, self.pynwb_reading_order, benchmark_type=benchmark_type, network_tracking=True, suffix="pynwb"
        )

        benchmark_type = "network_tracking_remote_slicing"
        self.plot_slice_benchmarks(db, self.slicing_order, benchmark_type=benchmark_type, network_tracking=True)

        # Method rankings
        self.plot_method_rankings(db)

        # 2. TODO - WHEN TO DOWNLOAD VS. STREAM DATA?
        # need to plot baseline download line + time to open + slice vs. number of slices
        # this approach has outline but does not include time to open or baseline download time
        self.plot_download_vs_stream_benchmarks(db, self.slicing_order)

        # 3. TODO - HOW DOES PERFORMANCE CHANGE ACROSS VERSIONS/TIME
        # need to add regression tests for different libraries selecting a specific approach
        # different versions (6/2024, 6/2025, now, etc.)
        # performance on a single test (speed) vs. version (h5py, fsspec, etc.)
        self.plot_performance_over_time(db, 
                                        self.pynwb_reading_order, 
                                        hue="benchmark_name_operation", 
                                        benchmark_type="time_remote_file_reading")
        self.plot_performance_over_time(db, 
                                        self.slicing_order, 
                                        hue="benchmark_name_test", 
                                        benchmark_type="time_remote_slicing")
