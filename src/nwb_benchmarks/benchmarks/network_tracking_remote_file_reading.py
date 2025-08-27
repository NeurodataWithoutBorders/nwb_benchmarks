"""Basic benchmarks for profiling network statistics for streaming access to NWB files and their contents."""

import os

from asv_runner.benchmarks.mark import skip_benchmark_if

from nwb_benchmarks import TSHARK_PATH
from nwb_benchmarks.core import (
    BaseBenchmark,
    create_lindi_reference_file_system,
    download_file,
    network_activity_tracker,
    read_hdf5_fsspec_no_cache,
    read_hdf5_fsspec_with_cache,
    read_hdf5_lindi,
    read_hdf5_nwbfile_fsspec_no_cache,
    read_hdf5_nwbfile_fsspec_with_cache,
    read_hdf5_nwbfile_lindi,
    read_hdf5_nwbfile_remfile,
    read_hdf5_nwbfile_remfile_with_cache,
    read_hdf5_nwbfile_ros3,
    read_hdf5_remfile,
    read_hdf5_remfile_with_cache,
    read_hdf5_ros3,
    read_zarr,
    read_zarr_nwbfile,
)

from .params_remote_file_reading import (
    lindi_hdf5_parameter_cases,
    lindi_remote_rfs_parameter_cases,
    parameter_cases,
    zarr_parameter_cases,
)


class FsspecNoCacheFileReadBenchmark(BaseBenchmark):
    """Benchmark reading a remote NWB file using fsspec without caching."""

    parameter_cases = parameter_cases

    @skip_benchmark_if(TSHARK_PATH is None)
    def track_network_activity_during_read_h5py(self, s3_url: str):
        """Track network activity during reading HDF5 files with h5py using fsspec without caching."""
        with network_activity_tracker(tshark_path=TSHARK_PATH) as network_tracker:
            self.file, self.bytestream = read_hdf5_fsspec_no_cache(s3_url=s3_url)
        return network_tracker.asv_network_statistics

    @skip_benchmark_if(TSHARK_PATH is None)
    def track_network_activity_during_read_pynwb(self, s3_url: str):
        """Track network activity during reading NWB files with h5py and PyNWB using fsspec without caching."""
        with network_activity_tracker(tshark_path=TSHARK_PATH) as network_tracker:
            self.nwbfile, self.io, self.file, self.bytestream = read_hdf5_nwbfile_fsspec_no_cache(s3_url=s3_url)
        return network_tracker.asv_network_statistics


class FsspecWithCacheFileReadBenchmark(BaseBenchmark):
    """Benchmark reading a remote NWB file using fsspec with caching."""

    parameter_cases = parameter_cases

    def teardown(self, s3_url: str):
        """Clean up temporary directories."""
        if hasattr(self, "tmpdir"):
            self.tmpdir.cleanup()

    @skip_benchmark_if(TSHARK_PATH is None)
    def track_network_activity_during_read_h5py(self, s3_url: str):
        """Track network activity during reading HDF5 files with h5py using fsspec with caching."""
        with network_activity_tracker(tshark_path=TSHARK_PATH) as network_tracker:
            self.file, self.bytestream, self.tmpdir = read_hdf5_fsspec_with_cache(s3_url=s3_url)
        return network_tracker.asv_network_statistics

    @skip_benchmark_if(TSHARK_PATH is None)
    def track_network_activity_during_read_pynwb(self, s3_url: str):
        """Track network activity during reading NWB files with h5py and PyNWB using fsspec with caching."""
        with network_activity_tracker(tshark_path=TSHARK_PATH) as network_tracker:
            self.nwbfile, self.io, self.file, self.bytestream, self.tmpdir = read_hdf5_nwbfile_fsspec_with_cache(
                s3_url=s3_url
            )
        return network_tracker.asv_network_statistics


class RemfileNoCacheFileReadBenchmark(BaseBenchmark):
    """Benchmark reading a remote NWB file using Remfile without caching."""

    parameter_cases = parameter_cases

    @skip_benchmark_if(TSHARK_PATH is None)
    def track_network_activity_during_read_h5py(self, s3_url: str):
        """Track network activity during reading HDF5 files with h5py using Remfile without caching."""
        with network_activity_tracker(tshark_path=TSHARK_PATH) as network_tracker:
            self.file, self.bytestream = read_hdf5_remfile(s3_url=s3_url)
        return network_tracker.asv_network_statistics

    @skip_benchmark_if(TSHARK_PATH is None)
    def track_network_activity_during_read_pynwb(self, s3_url: str):
        """Track network activity during reading NWB files with h5py and PyNWB using Remfile without caching."""
        with network_activity_tracker(tshark_path=TSHARK_PATH) as network_tracker:
            self.nwbfile, self.io, self.file, self.bytestream = read_hdf5_nwbfile_remfile(s3_url=s3_url)
        return network_tracker.asv_network_statistics


class RemfileWithCacheFileReadBenchmark(BaseBenchmark):
    """Benchmark reading a remote NWB file using Remfile with caching."""

    parameter_cases = parameter_cases

    def teardown(self, s3_url: str):
        """Clean up temporary directories."""
        if hasattr(self, "tmpdir"):
            self.tmpdir.cleanup()

    @skip_benchmark_if(TSHARK_PATH is None)
    def track_network_activity_during_read_h5py(self, s3_url: str):
        """Track network activity during reading HDF5 files with h5py using Remfile with caching."""
        with network_activity_tracker(tshark_path=TSHARK_PATH) as network_tracker:
            self.file, self.bytestream, self.tmpdir = read_hdf5_remfile_with_cache(s3_url=s3_url)
        return network_tracker.asv_network_statistics

    @skip_benchmark_if(TSHARK_PATH is None)
    def track_network_activity_during_read_pynwb(self, s3_url: str):
        """Track network activity during reading NWB files with h5py and PyNWB using Remfile with caching."""
        with network_activity_tracker(tshark_path=TSHARK_PATH) as network_tracker:
            self.nwbfile, self.io, self.file, self.bytestream, self.tmpdir = read_hdf5_nwbfile_remfile_with_cache(
                s3_url=s3_url
            )
        return network_tracker.asv_network_statistics


class Ros3FileReadBenchmark(BaseBenchmark):
    """Benchmark reading a remote NWB file using the HDF5 ROS3 driver."""

    parameter_cases = parameter_cases

    @skip_benchmark_if(TSHARK_PATH is None)
    def track_network_activity_during_read_h5py(self, s3_url: str):
        """Track network activity during reading HDF5 files with h5py using the ROS3 driver."""
        with network_activity_tracker(tshark_path=TSHARK_PATH) as network_tracker:
            self.file, retries = read_hdf5_ros3(s3_url=s3_url)
        network_tracker.asv_network_statistics.update(retries=retries)
        return network_tracker.asv_network_statistics

    @skip_benchmark_if(TSHARK_PATH is None)
    def track_network_activity_during_read_pynwb(self, s3_url: str):
        """Track network activity during reading NWB files with h5py and PyNWB using the ROS3 driver."""
        with network_activity_tracker(tshark_path=TSHARK_PATH) as network_tracker:
            self.nwbfile, self.io, retries = read_hdf5_nwbfile_ros3(s3_url=s3_url)
        network_tracker.asv_network_statistics.update(retries=retries)
        return network_tracker.asv_network_statistics


class LindiCreateLocalJsonFileBenchmark(BaseBenchmark):
    """Benchmark creating a local LINDI JSON file for a remote HDF5 file."""

    parameter_cases = lindi_hdf5_parameter_cases

    def setup(self, s3_url: str):
        self.lindi_file = os.path.basename(s3_url) + ".nwb.lindi.json"
        self.teardown(s3_url=s3_url)

    def teardown(self, s3_url: str):
        """Delete the LINDI JSON file if it exists"""
        if os.path.exists(self.lindi_file):
            os.remove(self.lindi_file)

    # TODO This benchmark takes a long time to index all of the chunks for these files! Do not run until ready
    @skip_benchmark_if(True)
    def track_network_activity_create_lindi_json_file(self, s3_url: str):
        """Track network activity during the creation of a LINDI JSON file."""
        with network_activity_tracker(tshark_path=TSHARK_PATH) as network_tracker:
            create_lindi_reference_file_system(s3_url=s3_url, outfile_path=self.lindi_file)
        return network_tracker.asv_network_statistics


class LindiReadLocalJsonFileBenchmark(BaseBenchmark):
    """Benchmark reading a remote NWB file using LINDI with a local LINDI JSON file."""

    parameter_cases = lindi_remote_rfs_parameter_cases

    def setup(self, s3_url: str):
        """Download the remote LINDI JSON file."""
        self.lindi_file = os.path.basename(s3_url)
        self.teardown(s3_url=s3_url)
        download_file(s3_url=s3_url, local_path=self.lindi_file)

    def teardown(self, s3_url: str):
        """Delete the local LINDI JSON file if it exists."""
        if os.path.exists(self.lindi_file):
            os.remove(self.lindi_file)

    @skip_benchmark_if(TSHARK_PATH is None)
    def track_network_activity_during_read_lindi(self, s3_url: str):
        """Track network activity during reading HDF5 files with LINDI using the local LINDI JSON file."""
        with network_activity_tracker(tshark_path=TSHARK_PATH) as network_tracker:
            self.client = read_hdf5_lindi(rfs=self.lindi_file)
        return network_tracker.asv_network_statistics

    @skip_benchmark_if(TSHARK_PATH is None)
    def track_network_activity_during_read_lindi_nwbfile(self, s3_url: str):
        """Track network activity during reading NWB files with LINDI and PyNWB using the local LINDI JSON file."""
        with network_activity_tracker(tshark_path=TSHARK_PATH) as network_tracker:
            self.nwbfile, self.io, self.client = read_hdf5_nwbfile_lindi(rfs=self.lindi_file)
        return network_tracker.asv_network_statistics


class LindiReadRemoteJsonFileBenchmark(BaseBenchmark):
    """Benchmark reading a remote NWB file using LINDI with a remote LINDI JSON file."""

    parameter_cases = lindi_remote_rfs_parameter_cases

    @skip_benchmark_if(TSHARK_PATH is None)
    def track_network_activity_during_read_lindi(self, s3_url: str):
        """Track network activity during reading HDF5 files with LINDI using the remote LINDI JSON file."""
        with network_activity_tracker(tshark_path=TSHARK_PATH) as network_tracker:
            self.client = read_hdf5_lindi(rfs=s3_url)
        return network_tracker.asv_network_statistics

    @skip_benchmark_if(TSHARK_PATH is None)
    def track_network_activity_during_read_lindi_nwbfile(self, s3_url: str):
        """Track network activity during reading NWB files with LINDI and PyNWB using the remote LINDI JSON file."""
        with network_activity_tracker(tshark_path=TSHARK_PATH) as network_tracker:
            self.nwbfile, self.io, self.client = read_hdf5_nwbfile_lindi(rfs=s3_url)
        return network_tracker.asv_network_statistics


class ZarrFileReadBenchmark(BaseBenchmark):
    """Benchmark reading a remote Zarr file."""

    parameter_cases = zarr_parameter_cases

    @skip_benchmark_if(TSHARK_PATH is None)
    def track_network_activity_during_read_zarr(self, s3_url: str):
        """Track network activity during reading Zarr files with Zarr-Python."""
        with network_activity_tracker(tshark_path=TSHARK_PATH) as network_tracker:
            self.zarr_file = read_zarr(s3_url=s3_url, open_without_consolidated_metadata=False)
        return network_tracker.asv_network_statistics

    @skip_benchmark_if(TSHARK_PATH is None)
    def track_network_activity_during_read_pynwb(self, s3_url: str):
        """Track network activity during reading NWB files with Zarr-Python and PyNWB."""
        with network_activity_tracker(tshark_path=TSHARK_PATH) as network_tracker:
            self.nwbfile, self.io = read_zarr_nwbfile(s3_url=s3_url, mode="r")
        return network_tracker.asv_network_statistics


class ZarrForceNoConsolidatedFileReadBenchmark(BaseBenchmark):
    """Benchmark reading a remote Zarr file without consolidated metadata."""

    parameter_cases = zarr_parameter_cases

    @skip_benchmark_if(TSHARK_PATH is None)
    def track_network_activity_during_read_zarr(self, s3_url: str):
        """Track network activity during reading Zarr files with Zarr-Python without consolidated metadata."""
        with network_activity_tracker(tshark_path=TSHARK_PATH) as network_tracker:
            self.zarr_file = read_zarr(s3_url=s3_url, open_without_consolidated_metadata=True)
        return network_tracker.asv_network_statistics

    @skip_benchmark_if(TSHARK_PATH is None)
    def track_network_activity_during_read_pynwb(self, s3_url: str):
        """Track network activity during reading NWB files with Zarr-Python and PyNWB without consolidated metadata."""
        with network_activity_tracker(tshark_path=TSHARK_PATH) as network_tracker:
            self.nwbfile, self.io = read_zarr_nwbfile(s3_url=s3_url, mode="r-")
        return network_tracker.asv_network_statistics
