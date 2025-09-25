import polars as pl
import matplotlib.pyplot as plt
import seaborn as sns
import re
import textwrap

from pathlib import Path
from nwb_benchmarks.database._processing import repackage_as_parquet

# use lbl-mac machine_id for initial comparisons
lbl_mac_id = '87fee773e425b4b1d3978fbf762d57effb0e8df8'  # "SincereCornucopia"

# create new database file with latest results in local github repository
results_directory = Path.home() / ".cache" / "nwb-benchmarks" / "nwb-benchmarks-results"
db_directory = Path.home() / ".cache" / "nwb-benchmarks" / "nwb-benchmarks-database"
output_directory = Path(__file__).parent / "figures"

repackage_as_parquet(directory=results_directory, output_directory=db_directory, 
                     minimum_results_version="3.0.0", minimum_machines_version="1.4.0")

# Load and preprocess data
results_df = (
    pl.scan_parquet(db_directory / "results.parquet")
    .filter(pl.col("machine_id") == lbl_mac_id)
    .with_columns([
        pl.col("parameter_case_name").str.extract(r"^(Ophys|Ecephys|Icephys)").alias("modality"),
        pl.col("benchmark_name").str.split('.').list.get(0).alias("benchmark_name_type"),
        pl.col("benchmark_name").str.split(".").list.get(1).alias("benchmark_name_test"),
        pl.col("benchmark_name").str.split(".").list.get(2).alias("benchmark_name_operation"),
    ])
)

# Utility functions
def clean_benchmark_name_operation(name, method, prefix):
    return name.replace(f'{prefix}_{method}_', '').replace('_', ' ')

def clean_benchmark_name_test(name, method):
    short_name = name.replace(f'{method}', '').replace('ContinuousSliceBenchmark', '')
    return split_camel_case(short_name)

def split_camel_case(text):
    result = re.sub('([a-z0-9])([A-Z])', r'\1 \2', text)
    result = re.sub('([A-Z]+)([A-Z][a-z])', r'\1 \2', result)
    return result

def add_mean_sem_annotations(value, group, order, **kwargs):
    stats = kwargs.get('data').groupby(group)[value].agg(['mean', 'std', 'max'])
    for i, label in enumerate(order):
        mean_sem_text = f"  {stats.loc[label, 'mean']:.2f} ± {stats.loc[label, 'std']:.2f}"
        plt.text(x=stats.loc[label, 'max'], y=i, s=mean_sem_text,
                verticalalignment='center', horizontalalignment='left', fontsize=8)

def plot_benchmark_dist(df, group, metric, metric_order, filename, row=None, sharex=True, add_annotations=True):
    g = sns.catplot(
        data=df.collect().to_pandas(), 
        x="value", y=group, col='modality', row=row, hue=group,
        sharex=sharex, palette="Paired", kind='box', showfliers=False,
        boxprops=dict(linewidth=0), order=metric_order, legend=False
    )
    
    if add_annotations:
        g.map_dataframe(add_mean_sem_annotations, value="value", group=group, order=metric_order)
    
    g.set(xlabel='Time (s)', ylabel=metric)
    for ax in g.axes.flat:
        wrapped_title = '\n'.join(textwrap.wrap(ax.get_title(), width=50))
        ax.set_title(wrapped_title)
    
    sns.despine()
    plt.tight_layout()
    plt.savefig(filename)
    plt.close()

def plot_benchmark_slices_vs_time(df, group, metric, metric_order, filename, row=None, sharex=True):
    g = sns.catplot(
        data=df.collect().to_pandas(), 
        x="slice_number", y='value', col='modality', row=row, hue=group, hue_order=metric_order,
        sharex=sharex, palette="Paired", kind='point', sharey=False)
    
    g.set(xlabel="Relative slice size", ylabel='Time (s)')
    
    sns.despine()
    plt.tight_layout()
    plt.savefig(filename)
    plt.close()

def filter_read_tests(benchmark_type, benchmark_test, benchmark_method, prefix):
    return (results_df
            .filter(pl.col("benchmark_name_type") == benchmark_type)
            .filter(pl.col("benchmark_name_test") == benchmark_test)
            .with_columns(
                pl.col('benchmark_name_operation')
                .map_elements(lambda x: clean_benchmark_name_operation(x, benchmark_method, prefix))
                .alias('benchmark_name_operation')
            ))

def filter_slice_tests(benchmark_type, benchmark_method):
    return (results_df
            .filter(pl.col("benchmark_name_type") == benchmark_type)
            .filter(pl.col("benchmark_name_test").str.contains(benchmark_method))
            .with_columns(
                pl.col('benchmark_name_test')
                .map_elements(lambda x: clean_benchmark_name_test(x, benchmark_method))
                .alias('benchmark_name_test'))
            .with_columns(
                pl.col("parameter_case_slice_range").list.first().str.extract(r"slice\(0, (\d+),", group_index=1).cast(pl.Int64)
                .alias("scaling_value"))
            .with_columns(
                (pl.col("scaling_value").rank(method="dense") - 1).over('modality')
                .alias("slice_number"))
            )

def plot_read_benchmarks(test, method, order, benchmark_type='time_remote_file_reading', 
                      prefix='time_read', network_tracking=False):
    filtered_df = filter_read_tests(benchmark_type, test, method, prefix)
    
    filename_prefix = "network_tracking_" if network_tracking else ""
    plot_kwargs = {
        'df': filtered_df,
        'group': 'benchmark_name_operation',
        'metric': test,
        'metric_order': order,
        'filename': output_directory / f"{filename_prefix}file_read_{method}.png"
    }
    
    if network_tracking:
        plot_kwargs.update({'row': 'variable', 'sharex': False, 'add_annotations': False})
    
    plot_benchmark_dist(**plot_kwargs)

def plot_slice_benchmarks(method, order, benchmark_type='time_remote_slicing', network_tracking=False):
    filtered_df = filter_slice_tests(benchmark_type, method)
    
    filename_prefix = "network_tracking_" if network_tracking else ""
    plot_kwargs = {
        'df': filtered_df,
        'group': 'benchmark_name_test',
        'metric': f'{method} Continuous Slice Benchmark',
        'metric_order': order,
        'filename': output_directory / f"{filename_prefix}slicing_{method}.png"
    }
    
    if network_tracking:
        plot_kwargs.update({'row': 'variable', 'sharex': 'row', 'add_annotations': False})
    
    plot_benchmark_dist(**plot_kwargs)

def plot_download_vs_stream_benchmarks(method, order, benchmark_type='time_remote_slicing', network_tracking=False):
    slice_df = filter_slice_tests(benchmark_type, method)
    
    # plot just the slice information
    filename_prefix = "network_tracking_" if network_tracking else ""
    plot_kwargs = {
        'df': slice_df,
        'group': 'benchmark_name_test',
        'metric': f'{method} Continuous Slice Benchmark',
        'metric_order': order,
        'filename': output_directory / f"{filename_prefix}slicing_vs_time_{method}.png"
    }
    
    if network_tracking:
        plot_kwargs.update({'row': 'variable', 'sharex': 'row', 'add_annotations': False})
    
    plot_benchmark_slices_vs_time(**plot_kwargs)

    # plot slice + read vs. download information
    # TODO - need to capture download time for each test case




# Common benchmark orders
hdf5_order = ['remfile no cache', 'remfile with cache', 
              'fsspec https no cache', 'fsspec https with cache',
              'fsspec s3 no cache', 'fsspec s3 with cache',
              'ros3',]

slicing_hdf5_order = ['Fsspec S3 No Cache', 'Fsspec S3 With Cache',
                      'Fsspec S3 Preloaded No Cache', 'Fsspec S3 Preloaded With Cache',
                      'Fsspec Https No Cache', 'Fsspec Https With Cache',
                      'Fsspec Https Preloaded No Cache', 'Fsspec Https Preloaded With Cache',
                      'Remfile No Cache', 'Remfile With Cache',
                      'Remfile Preloaded No Cache', 'Remfile Preloaded With Cache', 'ROS3']

zarr_order = ['https', 'https force no consolidated', 's3', 's3 force no consolidated']
zarr_pynwb_order = ['s3', 's3 force no consolidated']
lindi_order = ['h5py', 'pynwb']

############################### WHEN TO DOWNLOAD VS. STREAM DATA? #############################

# selecting a specific approach, for each modality when should I download vs. stream
# 1 plot for each modality (ecephys, ophys, icephys) 
# baseline download line (number of slices that pushes you over the download time?)

# time to open + slice vs. number of slices
# TODO - need the download time for the specific approach we want to focus on
plot_download_vs_stream_benchmarks('HDF5PyNWB', slicing_hdf5_order)
plot_download_vs_stream_benchmarks('Lindi', ['Local JSON'])
plot_download_vs_stream_benchmarks('ZarrPyNWB', ['S3', 'S3 Force No Consolidated'])


######################### WHICH LIBRARY SHOULD I USE TO STREAM DATA? #########################

# 1. Remote file reading - which methods are fastest?
plot_read_benchmarks('HDF5H5pyFileReadBenchmark', 'hdf5_h5py', hdf5_order)
plot_read_benchmarks('HDF5PyNWBFileReadBenchmark', 'hdf5_pynwb', hdf5_order)
plot_read_benchmarks('LindiLocalJSONFileReadBenchmark', 'lindi', lindi_order)
plot_read_benchmarks('ZarrZarrPythonFileReadBenchmark', 'zarr', zarr_order)
plot_read_benchmarks('ZarrPyNWBFileReadBenchmark', 'zarr_pynwb', zarr_pynwb_order)

# 2. Remote file slicing
plot_slice_benchmarks('HDF5PyNWB', slicing_hdf5_order)
plot_slice_benchmarks('Lindi', ['Local JSON'])
plot_slice_benchmarks('ZarrPyNWB', ['S3', 'S3 Force No Consolidated'])

# 3. Network tracking - which methods make the most requests?
benchmark_type = "network_tracking_remote_file_reading"
prefix = "track_network_read"
plot_read_benchmarks('HDF5H5pyFileReadBenchmark', 'hdf5_h5py', hdf5_order,
                  benchmark_type=benchmark_type, prefix=prefix, network_tracking=True)

plot_read_benchmarks('HDF5PyNWBFileReadBenchmark', 'hdf5_pynwb', hdf5_order,
                  benchmark_type=benchmark_type, prefix=prefix, network_tracking=True)

plot_read_benchmarks('LindiLocalJSONFileReadBenchmark', 'lindi', lindi_order,
                  benchmark_type=benchmark_type, prefix=prefix, network_tracking=True)

plot_read_benchmarks('ZarrZarrPythonFileReadBenchmark', 'zarr', zarr_order,
                  benchmark_type=benchmark_type, prefix=prefix, network_tracking=True)

plot_read_benchmarks('ZarrPyNWBFileReadBenchmark', 'zarr_pynwb', zarr_pynwb_order,
                  benchmark_type=benchmark_type, prefix=prefix, network_tracking=True)

benchmark_type = "network_tracking_remote_slicing"
plot_slice_benchmarks('HDF5PyNWB', slicing_hdf5_order,
                           benchmark_type=benchmark_type, network_tracking=True)
plot_slice_benchmarks('Lindi', ['Local JSON'],
                           benchmark_type=benchmark_type, network_tracking=True)
plot_slice_benchmarks('ZarrPyNWB', ['S3', 'S3 Force No Consolidated'],
                           benchmark_type=benchmark_type, network_tracking=True)


###################### HOW DOES PERFORMANCE CHANGE ACROSS VERSIONS/TIME ######################

# TODO - need to add regression tests for different libraries selecting a specific approach
# different versions (6/2023, 6/2024, 6/2025)
# performance on a single test (speed) vs. version (h5py, ros3, etc.)