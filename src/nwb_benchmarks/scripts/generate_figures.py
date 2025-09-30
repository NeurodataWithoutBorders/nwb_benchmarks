import re
import textwrap
from pathlib import Path

import matplotlib
import matplotlib.pyplot as plt
import polars as pl
import seaborn as sns
import pandas as pd

from nwb_benchmarks.database._processing import repackage_as_parquet

# setup font settings for editable text in Illustrator
matplotlib.rcParams["pdf.fonttype"] = 42
matplotlib.rcParams["ps.fonttype"] = 42

# use lbl-mac machine_id for initial comparisons
lbl_mac_id = "87fee773e425b4b1d3978fbf762d57effb0e8df8"  # "SincereCornucopia"

# create new database file with latest results in local github repository
results_directory = Path.home() / ".cache" / "nwb-benchmarks" / "nwb-benchmarks-results"
db_directory = Path.home() / ".cache" / "nwb-benchmarks" / "nwb-benchmarks-database"
output_directory = Path(__file__).parent / "figures"

repackage_as_parquet(
    directory=results_directory,
    output_directory=db_directory,
    minimum_results_version="3.0.0",
    minimum_machines_version="1.4.0",
)

# Load and preprocess data
results_df = (
    pl.scan_parquet(db_directory / "results.parquet")
    .filter(pl.col("machine_id") == lbl_mac_id)
    .with_columns(
        [
            pl.col("parameter_case_name").str.extract(r"^(Ophys|Ecephys|Icephys)").alias("modality"),
            pl.col("benchmark_name").str.split(".").list.get(0).alias("benchmark_name_type"),
            (pl.col("benchmark_name").str.split(".").list.get(1)
             .map_elements(lambda x: clean_benchmark_name_test(x))
             .alias("benchmark_name_test")),
            pl.col("benchmark_name").str.split(".").list.get(2).alias("benchmark_name_operation"),
        ]
    )
)


# Utility functions
def clean_benchmark_name_operation(name, prefix):
    return name.replace(f"{prefix}_", "").replace("_", " ")


def clean_benchmark_name_test(name):
    short_name = (name
                  .replace("ContinuousSliceBenchmark", "")
                  .replace("FileReadBenchmark", "")
                  .replace("DownloadBenchmark", ""))
    return split_camel_case(short_name)


def split_camel_case(text):
    text = re.sub(r"PyNWBS3", "PyNWB S3", text)  # special handling for NWB
    text = re.sub(r"NWBROS3", "NWB ROS3", text)  # special handling for NWB ROS3
    result = re.sub("([a-z0-9])([A-Z])", r"\1 \2", text)
    result = re.sub("([A-Z]+)([A-Z][a-z])", r"\1 \2", result)
    result = re.sub("Py NWB", "PyNWB", result)  # revert Py NWB back to PyNWB

    return result


def add_mean_sem_annotations(value, group, order, scientific_notation_threshold=1000, **kwargs):
    stats_df = kwargs.get("data").groupby(group)[value].agg(["mean", "std", "max", "count"])
    for i, label in enumerate(order):
        if label in stats_df.index:
            stats = stats_df.loc[label, :]
            if stats['mean'] > scientific_notation_threshold or stats['mean'] < 0.01:
                mean_sem_text = f"  {stats['mean']:.2e} ± {stats['std']:.2e}, n={int(stats['count'])}"
            else:
                mean_sem_text = f"  {stats['mean']:.2f} ± {stats['std']:.2f}, n={int(stats['count'])}"

            plt.text(
                x=stats["max"],
                y=i,
                s=mean_sem_text,
                verticalalignment="center",
                horizontalalignment="left",
                fontsize=8,
            )


def plot_benchmark_dist(df: pd.DataFrame, group: str, metric: str, metric_order: list, filename: str, row: str = None, sharex: bool = True, add_annotations: bool = True, kind='box', palette='Paired', catplot_kwargs=dict()):
    g = sns.catplot(
        data=df,
        x="value",
        y=group,
        col="modality",
        row=row,
        hue=group,
        sharex=sharex,
        palette="Paired",
        kind=kind,
        order=metric_order,
        legend=False,
        **catplot_kwargs,
    )

    if add_annotations:
        g.map_dataframe(add_mean_sem_annotations, value="value", group=group, order=metric_order)

    g.set(xlabel="Time (s)", ylabel=metric)
    for ax in g.axes.flat:
        wrapped_title = "\n".join(textwrap.wrap(ax.get_title(), width=50))
        ax.set_title(wrapped_title)

    sns.despine()
    plt.tight_layout()
    plt.savefig(filename, dpi=300)
    plt.close()


def plot_benchmark_slices_vs_time(df, group, metric_order, filename, row=None, sharex=True):
    g = sns.catplot(
        data=df.collect().to_pandas(),
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


def plot_benchmark_heatmap(df, group, metric_order, ax=None):
    heatmap_df = (
        df.collect()
        .to_pandas()
        .pivot_table(index=group, columns="modality", values="value", aggfunc="mean")
        .reindex(metric_order)
        .reindex(["Ecephys", "Ophys", "Icephys"], axis=1)
    )

    if ax is None:
        _, ax = plt.subplots(figsize=(10, 8))

    sns.heatmap(data=heatmap_df, annot=True, fmt=".2f", cmap="OrRd", ax=ax)
    ax.set(xlabel="", ylabel="")

    # add star for best method in each modality
    for i, row in enumerate(heatmap_df.index):
        for j, col in enumerate(heatmap_df.columns):
            if heatmap_df.loc[row, col] == heatmap_df.min()[col]:
                ax.text(j + 0.5, i + 0.5, "     *", fontsize=20, ha="center", va="center", color="black", weight="bold")

    return ax


def filter_read_tests(benchmark_type, prefix):
    return results_df.filter(pl.col("benchmark_name_type") == benchmark_type).with_columns(
        pl.col("benchmark_name_operation")
        .map_elements(lambda x: clean_benchmark_name_operation(x, prefix))
        .alias("benchmark_name_operation")
    )


def filter_slice_tests(benchmark_type):
    return (
        results_df.filter(pl.col("benchmark_name_type") == benchmark_type)
        .with_columns(
            pl.col("parameter_case_slice_range")
            .list.first()
            .str.extract(r"slice\(0, (\d+),", group_index=1)
            .cast(pl.Int64)
            .alias("scaling_value")
        )
        .with_columns((pl.col("scaling_value").rank(method="dense") - 1).over("modality").alias("slice_number"))
        .with_columns(
            [
                pl.col("benchmark_name_test").str.contains("Preloaded").alias("is_preloaded"),
                pl.col("benchmark_name_test").str.replace(" Preloaded", "").alias("benchmark_name_test"),
            ]
        )
    )


def plot_read_benchmarks(order, benchmark_type="time_remote_file_reading", col_name="benchmark_name_operation", prefix="time_read", network_tracking=False, kind='box', suffix='pynwb'):
    filtered_df = filter_read_tests(benchmark_type, prefix)

    filename_prefix = "network_tracking_" if network_tracking else ""
    plot_kwargs = {
        "df": filtered_df.collect().to_pandas(),
        "group": col_name,
        "metric": "File Read Benchmark",
        "metric_order": order,
        "filename": output_directory / f"{filename_prefix}file_read{suffix}.pdf",
        "kind": kind,
        "catplot_kwargs": {
            "showfliers": False,
            "boxprops": dict(linewidth=0),
        },
    }

    if network_tracking:
        plot_kwargs.update({"row": "variable", "sharex": "row"})

    # plot box plot
    plot_benchmark_dist(**plot_kwargs)

    # plot scatter plot
    plot_kwargs.update({"catplot_kwargs": dict(),
                        "kind": "strip",
                        "add_annotations": False,
                        "filename": output_directory / f"{filename_prefix}file_read_scatter{suffix}.pdf"})
    plot_benchmark_dist(**plot_kwargs)


def plot_slice_benchmarks(order, benchmark_type="time_remote_slicing", col_name="benchmark_name_test", network_tracking=False, kind='box'):
    filtered_df = filter_slice_tests(benchmark_type)

    filename_prefix = "network_tracking_" if network_tracking else ""
    plot_kwargs = {
        "df": filtered_df,
        "group": col_name,
        "metric": "Continuous Slice Benchmark",
        "metric_order": order,
        "row": "is_preloaded",
        "kind": kind,
        "catplot_kwargs": {
            "showfliers": False,
            "boxprops": dict(linewidth=0),
        },
    }

    if network_tracking:
        # plot non-preloaded tests only
        plot_kwargs.update({"df": filtered_df.filter(pl.col("is_preloaded") == False),
                            "row": "variable", "sharex": "row"})
        
    # plot box plot for each slice value
    for slice_num, slice_df in enumerate(plot_kwargs["df"].collect().partition_by("slice_number")):
        plot_kwargs.update({"df": slice_df.to_pandas(),
                            "filename": output_directory / f"{filename_prefix}slicing_{slice_num}.pdf",})
        plot_benchmark_dist(**plot_kwargs)

    # plot scatter plot
    plot_kwargs.update({"df": filtered_df.collect().to_pandas(),
                        "catplot_kwargs": dict(),
                        "kind": "strip",
                        "add_annotations": False,
                        "filename": output_directory / f"{filename_prefix}slicing_scatter.pdf"})
    plot_benchmark_dist(**plot_kwargs)


def plot_download_vs_stream_benchmarks(order, benchmark_type="time_remote_slicing", network_tracking=False):
    slice_df = filter_slice_tests(benchmark_type)

    # plot just the slice information
    filename_prefix = "network_tracking_" if network_tracking else ""
    plot_kwargs = {
        "df": slice_df,
        "group": "benchmark_name_test",
        "metric_order": order,
        "row": "is_preloaded",
        "filename": output_directory / f"{filename_prefix}slicing_vs_time.pdf",
    }

    if network_tracking:
        plot_kwargs.update({"row": "variable", "sharex": "row", "add_annotations": False})

    plot_benchmark_slices_vs_time(**plot_kwargs)

    # plot slice + read vs. download information
    # TODO - need to capture download time for each test case


def plot_method_rankings(slice_order, read_order, pynwb_order):
    slice_df = filter_slice_tests("time_remote_slicing")
    read_df = filter_read_tests("time_remote_file_reading", "time_read")

    fig, axes = plt.subplots(3, 1, figsize=(8, 16))
    axes[0] = plot_benchmark_heatmap(df=read_df, group="benchmark_name_operation",  metric_order=read_order, ax=axes[0])
    axes[1] = plot_benchmark_heatmap(df=read_df, group="benchmark_name_operation",  metric_order=pynwb_order, ax=axes[1])
    axes[2] = plot_benchmark_heatmap(df=slice_df, group="benchmark_name_test",  metric_order=slice_order, ax=axes[2])

    axes[0].set_title("Remote File Reading")
    axes[1].set_title("Remote File Reading - PyNWB")
    axes[2].set_title("Remote Slicing")

    plt.tight_layout()
    plt.savefig(output_directory / "method_rankings_heatmap.pdf", dpi=300)
    plt.close()


# Common benchmark orders
slicing_order = [
    "HDF5 PyNWB Fsspec S3 No Cache",
    "HDF5 PyNWB Fsspec S3 With Cache",
    "HDF5 PyNWB Fsspec Https No Cache",
    "HDF5 PyNWB Fsspec Https With Cache",
    "HDF5 PyNWB Remfile No Cache",
    "HDF5 PyNWB Remfile With Cache",
    "HDF5 PyNWB ROS3",
    "Lindi Local JSON",
    "Zarr PyNWB S3",
    "Zarr PyNWB S3 Force No Consolidated",
]

file_open_order = [
    "hdf5 h5py remfile no cache",
    "hdf5 h5py remfile with cache",
    "hdf5 h5py fsspec https no cache",
    "hdf5 h5py fsspec https with cache",
    "hdf5 h5py fsspec s3 no cache",
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
    "hdf5 pynwb remfile with cache",
    "hdf5 pynwb fsspec https no cache",
    "hdf5 pynwb fsspec https with cache",
    "hdf5 pynwb fsspec s3 no cache",
    "hdf5 pynwb fsspec s3 with cache",
    "hdf5 pynwb ros3",
    "lindi pynwb",
    "zarr pynwb https",
    "zarr pynwb https force no consolidated",
    "zarr pynwb s3",
    "zarr pynwb s3 force no consolidated",
]

download_order = [
    "Lindi"
]
############################### WHEN TO DOWNLOAD VS. STREAM DATA? #############################

# selecting a specific approach, for each modality when should I download vs. stream
# 1 plot for each modality (ecephys, ophys, icephys)
# baseline download line (number of slices that pushes you over the download time?)

# time to open + slice vs. number of slices
plot_download_vs_stream_benchmarks(slicing_order)

######################### WHICH LIBRARY SHOULD I USE TO STREAM DATA? #########################

# 1. Remote file reading - which methods are fastest?
plot_read_benchmarks(file_open_order, suffix='')
plot_read_benchmarks(pynwb_reading_order, suffix='_pynwb')
plot_read_benchmarks(download_order, benchmark_type="time_download_lindi", 
                     col_name="benchmark_name_test", prefix="time_download", suffix='_lindidownload')

# 2. Remote file slicing
plot_slice_benchmarks(slicing_order)

# 3. Remote file downloading

# 3. Network tracking - which methods make the most requests?
benchmark_type = "network_tracking_remote_file_reading"
prefix = "track_network_read"
plot_read_benchmarks(file_open_order, benchmark_type=benchmark_type, prefix=prefix, network_tracking=True)
plot_read_benchmarks(pynwb_reading_order, benchmark_type=benchmark_type, prefix=prefix, network_tracking=True, suffix='pynwb')

benchmark_type = "network_tracking_remote_slicing"
plot_slice_benchmarks(slicing_order, benchmark_type=benchmark_type, network_tracking=True)

# 4. What is the best software across all read / slicing?
plot_method_rankings(slicing_order, file_open_order, pynwb_reading_order)


###################### HOW DOES PERFORMANCE CHANGE ACROSS VERSIONS/TIME ######################

# TODO - need to add regression tests for different libraries selecting a specific approach
# different versions (6/2023, 6/2024, 6/2025)
# performance on a single test (speed) vs. version (h5py, ros3, etc.)
