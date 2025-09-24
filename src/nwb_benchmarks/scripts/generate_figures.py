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
                     minimum_results_version="3.0.0",
                     minimum_machines_version="1.4.0",)

# setup df for lazy loading
results_df = pl.scan_parquet(db_directory / "results.parquet")

# preprocess results df for easier querying
# TODO - it would be helpful to not encode so much info in the benchmark_name
results_df = (
    results_df
    .filter(pl.col("machine_id") == lbl_mac_id)
    .with_columns([
        pl.col("parameter_case_name").str.extract(r"^(Ophys|Ecephys|Icephys)").alias("modality")
    ])
    .with_columns([
        pl.col("benchmark_name").str.split('.').list.get(0).alias("benchmark_name_type"),
        pl.col("benchmark_name").str.split(".").list.get(1).alias("benchmark_name_test"),
        pl.col("benchmark_name").str.split(".").list.get(2).alias("benchmark_name_operation"),
        ]
    )
)

# setup plot utils
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
    stats = (kwargs.get('data')
             .groupby(group)[value]
             .agg(['mean', 'std', 'max']))
    
    for i, label in enumerate(order):
        mean_sem_text = f"  {stats.loc[label, 'mean']:.2f} ± {stats.loc[label, 'std']:.2f}"
        plt.text(x=stats.loc[label, 'max'], y=i, s=mean_sem_text, verticalalignment='center', horizontalalignment='left', fontsize=8)

def plot_benchmark_time_distributions(df, group, metric, metric_order, filename, row=None, sharex=True, add_annotations=True):
    g = sns.catplot(data=df.collect().to_pandas(), x="value", y=group, col='modality', row=row, hue=group, sharex=sharex,
                    palette="Set2", kind='box', showfliers=False, boxprops=dict(linewidth=0), order=metric_order, legend=False)

    if add_annotations:
        g.map_dataframe(add_mean_sem_annotations, value="value", group=group, order=benchmark_order)

    g.set(xlabel='Time (s)', ylabel=metric)
    for ax in g.axes.flat:
        wrapped_title = '\n'.join(textwrap.wrap(ax.get_title(), width=50))
        ax.set_title(wrapped_title)
    sns.despine()
    plt.tight_layout()
    plt.savefig(filename)

def filter_read_tests(benchmark_type, benchmark_test, benchmark_method, prefix):
    filtered_df = (results_df
                    .filter(pl.col("benchmark_name_type") == benchmark_type)
                    .filter(pl.col("benchmark_name_test") == benchmark_test)
                    .with_columns(pl.col('benchmark_name_operation')
                                    .map_elements(lambda x: clean_benchmark_name_operation(x, method=benchmark_method, prefix=prefix))
                                    .alias('benchmark_name_operation')))
    return filtered_df

def filter_slice_tests(benchmark_type, benchmark_method):
    filtered_df = (results_df
                    .filter(pl.col("benchmark_name_type") == benchmark_type)
                    .filter(pl.col("benchmark_name_test").str.contains(benchmark_method))
                    .with_columns(pl.col('benchmark_name_test')
                                    .map_elements(lambda x: clean_benchmark_name_test(x, method=benchmark_method))
                                    .alias('benchmark_name_test')))
    return filtered_df


##############################################################################################
######################### WHICH LIBRARY SHOULD I USE TO STREAM DATA? #########################
##############################################################################################

############## 1. Remote file reading - which of these methods are fastest? 

benchmark_type = 'time_remote_file_reading'
benchmark_prefix = "time_read"
group='benchmark_name_operation'

# filtering results from an h5py file
benchmark_test = 'HDF5H5pyFileReadBenchmark'
benchmark_method = 'hdf5_h5py'
benchmark_order = ['remfile no cache', 'remfile with cache', 'ros3',
                   'fsspec https no cache', 'fsspec https with cache',
                   'fsspec s3 no cache', 'fsspec s3 with cache',]
filtered_df = filter_read_tests(benchmark_type, benchmark_test, benchmark_method, benchmark_prefix)
plot_benchmark_time_distributions(df=filtered_df, 
                                  group=group,
                                  metric=benchmark_test,
                                  metric_order=benchmark_order,
                                  filename=output_directory / f"file_read_{benchmark_method}.png",)

# filtering results from a pynwb file (using io.read)
benchmark_test = 'HDF5PyNWBFileReadBenchmark'
benchmark_method = 'hdf5_pynwb'
filtered_df = filter_read_tests(benchmark_type, benchmark_test, benchmark_method, benchmark_prefix)
plot_benchmark_time_distributions(df=filtered_df, 
                                  group=group,
                                  metric=benchmark_test,
                                  metric_order=benchmark_order,
                                  filename=output_directory / f"file_read_{benchmark_method}.png",)

# filtering results from a lindi file
benchmark_test = 'LindiLocalJSONFileReadBenchmark'
benchmark_method = 'lindi'
benchmark_order = ['h5py', 'pynwb']
filtered_df = filter_read_tests(benchmark_type, benchmark_test, benchmark_method, benchmark_prefix)
plot_benchmark_time_distributions(df=filtered_df, 
                                  group=group,
                                  metric=benchmark_test,
                                  metric_order=benchmark_order,
                                  filename=output_directory / f"file_read_{benchmark_method}.png",)

# filtering results from a zarr file
benchmark_test = 'ZarrZarrPythonFileReadBenchmark'
benchmark_method = 'zarr'
benchmark_order = ['https', 'https force no consolidated', 's3', 's3 force no consolidated']
filtered_df = filter_read_tests(benchmark_type, benchmark_test, benchmark_method, benchmark_prefix)
plot_benchmark_time_distributions(df=filtered_df, 
                                  group=group,
                                  metric=benchmark_test,
                                  metric_order=benchmark_order,
                                  filename=output_directory / f"file_read_{benchmark_method}.png",)

# ways to open a zarr file with pynwb
benchmark_test = 'ZarrPyNWBFileReadBenchmark'
benchmark_method = 'zarr_pynwb'
benchmark_order = ['s3', 's3 force no consolidated']
filtered_df = filter_read_tests(benchmark_type, benchmark_test, benchmark_method, benchmark_prefix)
plot_benchmark_time_distributions(df=filtered_df, 
                                  group=group,
                                  metric=benchmark_test,
                                  metric_order=benchmark_order,
                                  filename=output_directory / f"file_read_{benchmark_method}.png",)

######################### 2. Remote file slicing (measuring after file opened, just access)
# TODO - not broken down by slice sizes - how to compare across modalities?

benchmark_type = 'time_remote_slicing'
group = 'benchmark_name_test'

benchmark_method = 'HDF5PyNWB'
benchmark_order = ['Fsspec S3 No Cache', 'Fsspec S3 With Cache',
                   'Fsspec S3 Preloaded No Cache', 'Fsspec S3 Preloaded With Cache',
                   'Fsspec Https No Cache', 'Fsspec Https With Cache',
                   'Fsspec Https Preloaded No Cache', 'Fsspec Https Preloaded With Cache',
                   'Remfile No Cache', 'Remfile With Cache',
                   'Remfile Preloaded No Cache', 'Remfile Preloaded With Cache',
                   'ROS3',]
filtered_df = filter_slice_tests(benchmark_type, benchmark_method)
plot_benchmark_time_distributions(df=filtered_df, 
                                  group=group,
                                  metric=f'{benchmark_method} Continuous Slice Benchmark',
                                  metric_order=benchmark_order,
                                  filename=output_directory / f"slicing_{benchmark_method}.png",)

benchmark_method = 'Lindi'
benchmark_order = ['Local JSON']
filtered_df = filter_slice_tests(benchmark_type, benchmark_method)
plot_benchmark_time_distributions(df=filtered_df, 
                                  group=group,
                                  metric=f'{benchmark_method} Continuous Slice Benchmark',
                                  metric_order=benchmark_order,
                                  filename=output_directory / f"slicing_{benchmark_method}.png",)

benchmark_method = 'ZarrPyNWB'
benchmark_order = ['S3', 'S3 Force No Consolidated']
filtered_df = filter_slice_tests(benchmark_type, benchmark_method)
plot_benchmark_time_distributions(df=filtered_df, 
                                  group=group,
                                  metric=f'{benchmark_method} Continuous Slice Benchmark',
                                  metric_order=benchmark_order,
                                  filename=output_directory / f"slicing_{benchmark_method}.png",)

######################### 3. Remote network tracking - which of these methods makes the most requests? 
# filtering results from an h5py file
benchmark_type = 'network_tracking_remote_file_reading'
benchmark_prefix = 'track_network_read'
group = 'benchmark_name_operation'

benchmark_test = 'HDF5H5pyFileReadBenchmark'
benchmark_method = 'hdf5_h5py'
benchmark_order = ['remfile no cache', 'remfile with cache', 'ros3',
                   'fsspec https no cache', 'fsspec https with cache',
                   'fsspec s3 no cache', 'fsspec s3 with cache',]
filtered_df = filter_read_tests(benchmark_type, benchmark_test, benchmark_method, benchmark_prefix)
plot_benchmark_time_distributions(df=filtered_df, 
                                  group=group,
                                  metric=benchmark_test,
                                  metric_order=benchmark_order,
                                  row='variable',
                                  sharex=False,
                                  filename=output_directory / f"network_tracking_file_read_{benchmark_method}.png",
                                  add_annotations=False,)

# filtering results from a pynwb file (using io.read)
benchmark_type = 'network_tracking_remote_file_reading'
benchmark_test = 'HDF5PyNWBFileReadBenchmark'
benchmark_method = 'hdf5_pynwb'
filtered_df = filter_read_tests(benchmark_type, benchmark_test, benchmark_method, benchmark_prefix)
plot_benchmark_time_distributions(df=filtered_df, 
                                  group=group,
                                  metric=benchmark_test,
                                  metric_order=benchmark_order,
                                  row='variable',
                                  sharex=False,
                                  filename=output_directory / f"network_tracking_file_read_{benchmark_method}.png",
                                  add_annotations=False,)

# filtering results from a lindi file
benchmark_type = 'network_tracking_remote_file_reading'
benchmark_test = 'LindiLocalJSONFileReadBenchmark'
benchmark_method = 'lindi'
benchmark_order = ['h5py', 'pynwb']
filtered_df = filter_read_tests(benchmark_type, benchmark_test, benchmark_method, benchmark_prefix)
plot_benchmark_time_distributions(df=filtered_df, 
                                  group=group,
                                  metric=benchmark_test,
                                  metric_order=benchmark_order,
                                  row='variable',
                                  sharex=False,
                                  filename=output_directory / f"network_tracking_file_read_{benchmark_method}.png",
                                  add_annotations=False,)

# filtering results from a zarr file
benchmark_type = 'network_tracking_remote_file_reading'
benchmark_test = 'ZarrZarrPythonFileReadBenchmark'
benchmark_method = 'zarr'
benchmark_order = ['https', 'https force no consolidated', 's3', 's3 force no consolidated']
filtered_df = filter_read_tests(benchmark_type, benchmark_test, benchmark_method, benchmark_prefix)
plot_benchmark_time_distributions(df=filtered_df, 
                                  group=group,
                                  metric=benchmark_test,
                                  metric_order=benchmark_order,
                                  row='variable',
                                  sharex=False,
                                  filename=output_directory / f"network_tracking_file_read_{benchmark_method}.png",
                                  add_annotations=False,)

# filtering results from a zarr file with pynwb
benchmark_type = 'network_tracking_remote_file_reading'
benchmark_test = 'ZarrPyNWBFileReadBenchmark'
benchmark_method = 'zarr_pynwb'
benchmark_order = ['s3', 's3 force no consolidated']
filtered_df = filter_read_tests(benchmark_type, benchmark_test, benchmark_method, benchmark_prefix)
plot_benchmark_time_distributions(df=filtered_df, 
                                  group=group,
                                  metric=benchmark_test,
                                  metric_order=benchmark_order,
                                  row='variable',
                                  sharex=False,
                                  filename=output_directory / f"network_tracking_file_read_{benchmark_method}.png",
                                  add_annotations=False,)


benchmark_type = 'network_tracking_remote_slicing'
benchmark_method = 'HDF5PyNWB'
benchmark_order = ['Fsspec S3 No Cache', 'Fsspec S3 With Cache',
                   'Fsspec S3 Preloaded No Cache', 'Fsspec S3 Preloaded With Cache',
                   'Fsspec Https No Cache', 'Fsspec Https With Cache',
                   'Fsspec Https Preloaded No Cache', 'Fsspec Https Preloaded With Cache',
                   'Remfile No Cache', 'Remfile With Cache',
                   'Remfile Preloaded No Cache', 'Remfile Preloaded With Cache',
                   'ROS3',]
filtered_df = filter_slice_tests(benchmark_type, benchmark_method)
plot_benchmark_time_distributions(df=filtered_df, 
                                  group='benchmark_name_test',
                                  row='variable',
                                  sharex='row',
                                  metric=f'{benchmark_method} Continuous Slice Benchmark',
                                  metric_order=benchmark_order,
                                  add_annotations=False,
                                  filename=output_directory / f"network_tracking_slicing_{benchmark_method}.png",)

benchmark_method = 'Lindi'
benchmark_order = ['Local JSON']
filtered_df = filter_slice_tests(benchmark_type, benchmark_method)
plot_benchmark_time_distributions(df=filtered_df, 
                                  group='benchmark_name_test',
                                  row='variable',
                                  sharex='row',
                                  metric=f'{benchmark_method} Continuous Slice Benchmark',
                                  metric_order=benchmark_order,
                                  add_annotations=False,
                                  filename=output_directory / f"network_tracking_slicing_{benchmark_method}.png",)

benchmark_method = 'ZarrPyNWB'
benchmark_order = ['S3', 'S3 Force No Consolidated']
filtered_df = filter_slice_tests(benchmark_type, benchmark_method)
plot_benchmark_time_distributions(df=filtered_df, 
                                  group='benchmark_name_test',
                                  row='variable',
                                  sharex='row',                             
                                  metric=f'{benchmark_method} Continuous Slice Benchmark',
                                  metric_order=benchmark_order,
                                  add_annotations=False,
                                  filename=output_directory / f"network_tracking_slicing_{benchmark_method}.png",)

##############################################################################################
########################### WHEN SHOULD I DOWNLOAD VS. STREAM DATA? ##########################
##############################################################################################

# TODO - need the download time for the specific approach we want to focus on
# selecting a specific approach, for each modality when should I download vs. stream
# 1 plot for each modality (ecephys, ophys, icephys) 
# time to open + slice vs. number of slices
# baseline download line (number of slices that pushes you over the download time?)

##############################################################################################
###################### HOW DOES PERFORMANCE CHANGE ACROSS VERSIONS/TIME ######################
##############################################################################################

# TODO - need to add regression tests for different libraries selecting a specific approach
# different versions (6/2023, 6/2024, 6/2025)
# performance on a single test (speed) vs. version (h5py, ros3, etc.)

