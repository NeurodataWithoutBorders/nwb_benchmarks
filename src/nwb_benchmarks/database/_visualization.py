import textwrap
from pathlib import Path
from typing import Any, Dict, List, Optional

import matplotlib
import matplotlib.pyplot as plt
import pandas as pd
import polars as pl
import seaborn as sns
import numpy as np
from packaging import version

from nwb_benchmarks.database._processing import BenchmarkDatabase

DEFAULT_BENCHMARK_ORDER = [
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


class BenchmarkVisualizer:
    """Handles plotting and visualization of benchmark results."""

    file_open_order = DEFAULT_BENCHMARK_ORDER
    pynwb_read_order = [
        method.replace("h5py", "pynwb").replace("zarr", "zarr pynwb") for method in DEFAULT_BENCHMARK_ORDER
    ]
    download_order = ["hdf5 dandi api", "zarr dandi api", "lindi dandi api"]
    # TODO - where does lindi local json value go / what should it be called

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

    @staticmethod
    def _format_stat_text(mean: float, std: float, count: int) -> str:
        """Format statistical text based on value magnitude."""
        if mean > 1000 or mean < 0.01:
            return f"  {mean:.2e} ± {std:.2e}, n={int(count)}"
        return f"  {mean:.2f} ± {std:.2f}, n={int(count)}"

    def _add_mean_sem_annotations(self, value: str, group: str, order: List[str], **kwargs):
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

    def _get_filename_prefix(self, network_tracking: bool) -> str:
        """Get filename prefix based on network tracking."""
        return "network_tracking_" if network_tracking else ""

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

    @staticmethod
    def _set_package_version_categorical(group):
        sorted_versions = sorted(
            group["package_version"].unique(),
            key=lambda v: version.parse(v),
        )
        group["package_version"] = pd.Categorical(group["package_version"], categories=sorted_versions, ordered=True)
        return group

    @staticmethod
    def _add_annotations_df(intersections_df: pl.DataFrame, order: List[str], **kwargs):
        """Add intersection annotations to plot."""
        ax = plt.gca()

        # Get the grouping values from data
        all_modalities = kwargs['data']['modality'].unique()
        assert len(all_modalities) == 1, "Expected a single modality per subplot."

        is_preloaded = kwargs['data']['is_preloaded'].unique()
        assert len(is_preloaded) == 1, "Expected a single preloaded parameter per subplot."

        summary_text = ["# slices to intersect with download time \n"]
        modality_intersections = intersections_df.query(f'modality == "{all_modalities[0]}" and is_preloaded == {is_preloaded[0]}')
        for label in order:
            row = modality_intersections.query(f'benchmark_name_clean == "{label}"')
            if not row.empty:
                summary_text.append(f"{label}: {row['intersection_slice'].tolist()[0]:.2f}\n")
        
        ax.text(0.1, 0.9, ''.join(summary_text), transform=ax.transAxes, fontsize=8, ha='left', va='top',)

    def _create_heatmap_df(self, df: pl.DataFrame, group: str, metric_order: List[str]) -> pd.DataFrame:
        """Prepare data for heatmap visualization."""
        return (
            df.to_pandas()
            .pivot_table(index=group, columns="modality", values="value", aggfunc="mean")
            .reindex(metric_order)
            .reindex(["Ecephys", "Ophys", "Icephys"], axis=1)
        )

    def plot_benchmark_heatmap(
        self,
        df: pl.LazyFrame,
        metric_order: List[str],
        group: str = "benchmark_name_clean",
        ax: Optional[plt.Axes] = None,
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
        if df.empty:
            print(f"Warning: No data available to plot for {filename}. Skipping plot.")
            return

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
            g.map_dataframe(self._add_mean_sem_annotations, value="value", group=group, order=metric_order)

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
        df: pd.DataFrame,
        group: str,
        y_value: str,
        metric_order: List[str],
        filename: Path,
        row: Optional[str] = None,
        sharex: bool = True,
        intersections_df: Optional[pl.DataFrame] = None,
    ):
        """Plot benchmark performance vs slice size."""
        if df.empty:
            print(f"Warning: No data available to plot for {filename}. Skipping plot.")
            return
    
        g = sns.catplot(
            data=df,
            x="slice_number",
            y=y_value,
            col="modality",
            row=row,
            hue=group,
            hue_order=metric_order,
            sharex=sharex,
            palette="Paired",
            sharey=False,
            kind="point",
        )

        # Add intersection annotations
        if intersections_df is not None:
            g.map_dataframe(self._add_annotations_df, intersections_df=intersections_df, order=metric_order)

        g.set(xlabel="Relative slice size", ylabel="Time (s)")
        sns.despine()
        plt.savefig(filename, dpi=300)
        plt.close()

    def plot_read_benchmarks(
        self,
        db: BenchmarkDatabase,
        order: List[str] = None,
        benchmark_type: str = "time_remote_file_reading",
        col_name: str = "benchmark_name_clean",
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
            order=self.pynwb_read_order if order is None else order,
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
        order: List[str] = None,
        benchmark_type: str = "time_remote_slicing",
        col_name: str = "benchmark_name_clean",
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
            order=self.pynwb_read_order if order is None else order,
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

    @staticmethod
    def calculate_intersection(group_a, group_b):
        """Calculate intersection point for a single group"""
        m1, b1 = np.polyfit(group_a["slice_number"], group_a["total_time"], 1)
        m2, b2 = np.polyfit(group_b["slice_number"], group_b["total_time"], 1)
        
        if abs(m1 - m2) < 1e-10:  # parallel lines
            return None, None
        
        intersection_x = (b2 - b1) / (m1 - m2)
        intersection_y = m1 * intersection_x + b1

        return intersection_x, intersection_y

    def plot_download_vs_stream_benchmarks(
        self,
        db: BenchmarkDatabase,
        order: List[str] = None,
        network_tracking: bool = False,
    ):
        """Plot download vs stream benchmark comparison."""
        print("Plotting download vs stream benchmark comparison...")
        
        # plot time vs number of slices
        prefix = self._get_filename_prefix(network_tracking)
        base_filename = self.output_directory / f"{prefix}slicing"
        slice_df_combined = db.combine_read_and_slice_times(read_col_name="time_remote_file_reading",
                                                            slice_col_name="time_remote_slicing")
        plot_kwargs = {
            "df": slice_df_combined.collect().to_pandas(),
            "group": "benchmark_name_clean",
            "metric_order": self.pynwb_read_order if order is None else order,
            "row": "variable" if network_tracking else "is_preloaded",
            "sharex": "row" if network_tracking else True,
        }
        self.plot_benchmark_slices_vs_time(y_value="value", filename=f"{base_filename}_vs_time.pdf", **plot_kwargs)

        # add plots of total time (read + slice) with baseline number of slices (indicates file read only time)
        slice_df_combined_with_baseline = db.combine_read_and_slice_times(read_col_name="time_remote_file_reading",
                                                                          slice_col_name="time_remote_slicing",
                                                                          with_baseline=True)
        plot_kwargs.update({"df": slice_df_combined_with_baseline.collect().to_pandas()})
        self.plot_benchmark_slices_vs_time(
            y_value="total_time", filename=f"{base_filename}_vs_read_and_slice_time.pdf", **plot_kwargs
        )

        # add plots of local read + slice times
        local_df_combined = db.combine_read_and_slice_times(read_col_name="time_local_file_reading",
                                                            slice_col_name="time_local_slicing",
                                                            with_baseline=True)
        plot_kwargs.update({"df": local_df_combined.collect().to_pandas(),
                            'metric_order': None})
        self.plot_benchmark_slices_vs_time(
            y_value="total_time", filename=f"{base_filename}_vs_local_read_and_slice_time.pdf", **plot_kwargs
        )

        # combine download + local read + slice times
        download_df = db.filter_tests("time_download")
        local_df_combined_with_download = local_df_combined.join(
                download_df
                .with_columns(pl.col("benchmark_name_clean").str.replace("dandi api", "pynwb").alias("benchmark_name_clean"))
                .group_by(["modality", "benchmark_name_clean"])
                .agg(pl.col("value").mean().alias("avg_download_time")),
                on=["modality", "benchmark_name_clean"],
                how="left",
            ).with_columns([
                (pl.col("avg_download_time") + pl.col("total_time")).alias("total_time"),
                (pl.col("benchmark_name_clean").str.replace("pynwb", "").alias("benchmark_name_clean"))
            ])
        plot_kwargs.update({"df": local_df_combined_with_download.collect().to_pandas(),
                            'metric_order': ['hdf5 ', 'zarr ', 'lindi ']})
        self.plot_benchmark_slices_vs_time(
            y_value="total_time", filename=f"{base_filename}_vs_download_time.pdf", **plot_kwargs
        )
        
        # calculate intersection points and add to plots
        intersections = []
        for (modality, benchmark_name, is_preloaded), remote_group in slice_df_combined.collect().group_by(["modality", "benchmark_name_clean", "is_preloaded"]):            
            local_group = local_df_combined_with_download.filter(
                (pl.col("modality") == modality) & (pl.col("benchmark_name_clean") == f"{benchmark_name.split(' ')[0]} ") & (pl.col("is_preloaded") == is_preloaded)
            ).collect()
            
            # calculate intersection
            if not local_group.is_empty():
                int_x, int_y = self.calculate_intersection(remote_group, local_group)
                intersections.append({
                    "modality": modality,
                    'is_preloaded': is_preloaded,
                    "benchmark_name_clean": benchmark_name,
                    "intersection_slice": int_x,
                    "intersection_time": int_y,
                })

        intersections_df = pl.DataFrame(intersections)

        # add intersection information 
        plot_kwargs = {
            "df": slice_df_combined.collect().to_pandas(),
            "group": "benchmark_name_clean",
            "metric_order": self.pynwb_read_order if order is None else order,
            "row": "variable" if network_tracking else "is_preloaded",
            "sharex": "row" if network_tracking else True,
            "intersections_df": intersections_df.to_pandas(),
        }
        self.plot_benchmark_slices_vs_time(y_value="value", filename=f"{base_filename}_vs_time.pdf", **plot_kwargs)

    def plot_method_rankings(self, db: BenchmarkDatabase):
        """Create heatmap showing method rankings across benchmarks."""
        print("Plotting method rankings heatmap...")

        slice_df = db.filter_tests("time_remote_slicing").collect()
        read_df = db.filter_tests("time_remote_file_reading").collect()

        fig, axes = plt.subplots(3, 1, figsize=(8, 16))
        axes[0] = self.plot_benchmark_heatmap(df=read_df, metric_order=self.file_open_order, ax=axes[0])

        axes[1] = self.plot_benchmark_heatmap(df=read_df, metric_order=self.pynwb_read_order, ax=axes[1])

        axes[2] = self.plot_benchmark_heatmap(df=slice_df, metric_order=self.pynwb_read_order, ax=axes[2])

        axes[0].set_title("Remote File Reading")
        axes[1].set_title("Remote File Reading - PyNWB")
        axes[2].set_title("Remote Slicing")

        plt.tight_layout()
        plt.savefig(self.output_directory / "method_rankings_heatmap.pdf", dpi=300)
        plt.close()

    def plot_performance_across_versions(
        self,
        db: BenchmarkDatabase,
        order: List[str] = None,
        hue: str = "benchmark_name_clean",
        benchmark_type: str = "time_remote_file_reading",
    ):
        """Plot performance changes over time for a given benchmark type."""
        print(f"Plotting performance over time")

        # get polars dataframe and filter
        df = db.join_results_with_environments()
        df = (
            df.filter(pl.col("benchmark_name_type") == benchmark_type)
            .collect()
            .to_pandas()
            .groupby("package_name")
            .apply(self._set_package_version_categorical, include_groups=False)
            .reset_index(level=0)
        )

        g = sns.catplot(
            data=df,
            x="package_version",
            y="value",
            col="modality",
            row="package_name",
            hue=hue,
            hue_order=self.pynwb_read_order if order is None else order,
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
        # Remote file reading / slicing benchmarks
        self.plot_read_benchmarks(db, suffix="_pynwb")
        self.plot_read_benchmarks(db, order=self.file_open_order, suffix="")
        self.plot_slice_benchmarks(db)

        # Network tracking analysis
        benchmark_type = "network_tracking_remote_file_reading"
        self.plot_read_benchmarks(db, order=self.file_open_order, benchmark_type=benchmark_type, network_tracking=True)
        self.plot_read_benchmarks(db, benchmark_type=benchmark_type, network_tracking=True, suffix="pynwb")
        self.plot_slice_benchmarks(db, benchmark_type="network_tracking_remote_slicing", network_tracking=True)

        # Method rankings
        self.plot_method_rankings(db)

        # 2. WHEN TO DOWNLOAD VS. STREAM DATA?
        # baseline download line + time to open + slice locally vs. number of slices
        # time to open + slice locally vs. number of slices
        self.plot_download_vs_stream_benchmarks(db)

        # 3. HOW DOES PERFORMANCE CHANGE ACROSS VERSIONS/TIME
        # performance on a single test (time to read/slice) vs. version (h5py, fsspec, etc.)
        self.plot_performance_across_versions(db, benchmark_type="time_remote_file_reading")
        self.plot_performance_across_versions(db, benchmark_type="time_remote_slicing")
