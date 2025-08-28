"""Basic benchmarks for profiling network statistics for streaming access to NWB files and their contents."""

import os

from asv_runner.benchmarks.mark import skip_benchmark_if

from nwb_benchmarks import TSHARK_PATH
from nwb_benchmarks.core import (
    BaseBenchmark,
    create_lindi_reference_file_system,
    download_file,
    network_activity_tracker,
    read_hdf5_fsspec_https_no_cache,
    read_hdf5_fsspec_https_with_cache,
    read_hdf5_fsspec_s3_no_cache,
    read_hdf5_fsspec_s3_with_cache,
    read_hdf5_lindi,
    read_hdf5_nwbfile_fsspec_https_no_cache,
    read_hdf5_nwbfile_fsspec_https_with_cache,
    read_hdf5_nwbfile_fsspec_s3_no_cache,
    read_hdf5_nwbfile_fsspec_s3_with_cache,
    read_hdf5_nwbfile_lindi,
    read_hdf5_nwbfile_remfile,
    read_hdf5_nwbfile_remfile_with_cache,
    read_hdf5_nwbfile_ros3,
    read_hdf5_remfile,
    read_hdf5_remfile_with_cache,
    read_hdf5_ros3,
    read_zarr_https_protocol,
    read_zarr_nwbfile_https_protocol,
    read_zarr_nwbfile_s3_protocol,
    read_zarr_s3_protocol,
)

from .params_remote_file_reading import (
    lindi_hdf5_parameter_cases,
    lindi_remote_rfs_parameter_cases,
    parameter_cases,
    zarr_parameter_cases,
)


class FsspecHttpsNoCacheFileReadBenchmark(BaseBenchmark):
    """Benchmark reading a remote NWB file using fsspec & https protocol without caching."""

    parameter_cases = parameter_cases

    @skip_benchmark_if(TSHARK_PATH is None)
    def track_network_activity_during_read_h5py(self, https_url: str):
        """Track network activity during reading HDF5 files with h5py using fsspec & https without caching."""
        with network_activity_tracker(tshark_path=TSHARK_PATH) as network_tracker:
            self.file, self.bytestream = read_hdf5_fsspec_https_no_cache(https_url=https_url)
        return network_tracker.asv_network_statistics

    @skip_benchmark_if(TSHARK_PATH is None)
    def track_network_activity_during_read_pynwb(self, https_url: str):
        """Track network activity during reading NWB files with h5py and PyNWB using fsspec & https without caching."""
        with network_activity_tracker(tshark_path=TSHARK_PATH) as network_tracker:
            self.nwbfile, self.io, self.file, self.bytestream = read_hdf5_nwbfile_fsspec_https_no_cache(
                https_url=https_url
            )
        return network_tracker.asv_network_statistics


class FsspecS3NoCacheFileReadBenchmark(BaseBenchmark):
    """Benchmark reading a remote NWB file using fsspec & S3 protocol without caching."""

    parameter_cases = parameter_cases

    @skip_benchmark_if(TSHARK_PATH is None)
    def track_network_activity_during_read_h5py(self, https_url: str):
        """Track network activity during reading HDF5 files with h5py using fsspec & S3 without caching."""
        with network_activity_tracker(tshark_path=TSHARK_PATH) as network_tracker:
            self.file, self.bytestream = read_hdf5_fsspec_s3_no_cache(https_url=https_url)
        return network_tracker.asv_network_statistics

    @skip_benchmark_if(TSHARK_PATH is None)
    def track_network_activity_during_read_pynwb(self, https_url: str):
        """Track network activity during reading NWB files with h5py and PyNWB using fsspec & S3 without caching."""
        with network_activity_tracker(tshark_path=TSHARK_PATH) as network_tracker:
            self.nwbfile, self.io, self.file, self.bytestream = read_hdf5_nwbfile_fsspec_s3_no_cache(
                https_url=https_url
            )
        return network_tracker.asv_network_statistics


class FsspecHttpsWithCacheFileReadBenchmark(BaseBenchmark):
    """Benchmark reading a remote NWB file using fsspec & https protocol with caching."""

    parameter_cases = parameter_cases

    def teardown(self, https_url: str):
        """Clean up temporary directories."""
        if hasattr(self, "tmpdir"):
            self.tmpdir.cleanup()

    @skip_benchmark_if(TSHARK_PATH is None)
    def track_network_activity_during_read_h5py(self, https_url: str):
        """Track network activity during reading HDF5 files with h5py using fsspec & https with caching."""
        with network_activity_tracker(tshark_path=TSHARK_PATH) as network_tracker:
            self.file, self.bytestream, self.tmpdir = read_hdf5_fsspec_https_with_cache(https_url=https_url)
        return network_tracker.asv_network_statistics

    @skip_benchmark_if(TSHARK_PATH is None)
    def track_network_activity_during_read_pynwb(self, https_url: str):
        """Track network activity during reading NWB files with h5py and PyNWB using fsspec & https with caching."""
        with network_activity_tracker(tshark_path=TSHARK_PATH) as network_tracker:
            self.nwbfile, self.io, self.file, self.bytestream, self.tmpdir = read_hdf5_nwbfile_fsspec_https_with_cache(
                https_url=https_url
            )
        return network_tracker.asv_network_statistics


class FsspecS3WithCacheFileReadBenchmark(BaseBenchmark):
    """Benchmark reading a remote NWB file using fsspec & S3 protocol with caching."""

    parameter_cases = parameter_cases

    def teardown(self, https_url: str):
        """Clean up temporary directories."""
        if hasattr(self, "tmpdir"):
            self.tmpdir.cleanup()

    @skip_benchmark_if(TSHARK_PATH is None)
    def track_network_activity_during_read_h5py(self, https_url: str):
        """Track network activity during reading HDF5 files with h5py using fsspec & S3 with caching."""
        with network_activity_tracker(tshark_path=TSHARK_PATH) as network_tracker:
            self.file, self.bytestream, self.tmpdir = read_hdf5_fsspec_s3_with_cache(https_url=https_url)
        return network_tracker.asv_network_statistics

    @skip_benchmark_if(TSHARK_PATH is None)
    def track_network_activity_during_read_pynwb(self, https_url: str):
        """Track network activity during reading NWB files with h5py and PyNWB using fsspec & S3 with caching."""
        with network_activity_tracker(tshark_path=TSHARK_PATH) as network_tracker:
            self.nwbfile, self.io, self.file, self.bytestream, self.tmpdir = read_hdf5_nwbfile_fsspec_s3_with_cache(
                https_url=https_url
            )
        return network_tracker.asv_network_statistics


class RemfileNoCacheFileReadBenchmark(BaseBenchmark):
    """Benchmark reading a remote NWB file using Remfile without caching."""

    parameter_cases = parameter_cases

    @skip_benchmark_if(TSHARK_PATH is None)
    def track_network_activity_during_read_h5py(self, https_url: str):
        """Track network activity during reading HDF5 files with h5py using Remfile without caching."""
        with network_activity_tracker(tshark_path=TSHARK_PATH) as network_tracker:
            self.file, self.bytestream = read_hdf5_remfile(https_url=https_url)
        return network_tracker.asv_network_statistics

    @skip_benchmark_if(TSHARK_PATH is None)
    def track_network_activity_during_read_pynwb(self, https_url: str):
        """Track network activity during reading NWB files with h5py and PyNWB using Remfile without caching."""
        with network_activity_tracker(tshark_path=TSHARK_PATH) as network_tracker:
            self.nwbfile, self.io, self.file, self.bytestream = read_hdf5_nwbfile_remfile(https_url=https_url)
        return network_tracker.asv_network_statistics


class RemfileWithCacheFileReadBenchmark(BaseBenchmark):
    """Benchmark reading a remote NWB file using Remfile with caching."""

    parameter_cases = parameter_cases

    def teardown(self, https_url: str):
        """Clean up temporary directories."""
        if hasattr(self, "tmpdir"):
            self.tmpdir.cleanup()

    @skip_benchmark_if(TSHARK_PATH is None)
    def track_network_activity_during_read_h5py(self, https_url: str):
        """Track network activity during reading HDF5 files with h5py using Remfile with caching."""
        with network_activity_tracker(tshark_path=TSHARK_PATH) as network_tracker:
            self.file, self.bytestream, self.tmpdir = read_hdf5_remfile_with_cache(https_url=https_url)
        return network_tracker.asv_network_statistics

    @skip_benchmark_if(TSHARK_PATH is None)
    def track_network_activity_during_read_pynwb(self, https_url: str):
        """Track network activity during reading NWB files with h5py and PyNWB using Remfile with caching."""
        with network_activity_tracker(tshark_path=TSHARK_PATH) as network_tracker:
            self.nwbfile, self.io, self.file, self.bytestream, self.tmpdir = read_hdf5_nwbfile_remfile_with_cache(
                https_url=https_url
            )
        return network_tracker.asv_network_statistics


class Ros3FileReadBenchmark(BaseBenchmark):
    """Benchmark reading a remote NWB file using the HDF5 ROS3 driver."""

    parameter_cases = parameter_cases

    @skip_benchmark_if(TSHARK_PATH is None)
    def track_network_activity_during_read_h5py(self, https_url: str):
        """Track network activity during reading HDF5 files with h5py using the ROS3 driver."""
        with network_activity_tracker(tshark_path=TSHARK_PATH) as network_tracker:
            self.file, retries = read_hdf5_ros3(https_url=https_url)
        network_tracker.asv_network_statistics.update(retries=retries)
        return network_tracker.asv_network_statistics

    @skip_benchmark_if(TSHARK_PATH is None)
    def track_network_activity_during_read_pynwb(self, https_url: str):
        """Track network activity during reading NWB files with h5py and PyNWB using the ROS3 driver."""
        with network_activity_tracker(tshark_path=TSHARK_PATH) as network_tracker:
            self.nwbfile, self.io, retries = read_hdf5_nwbfile_ros3(https_url=https_url)
        network_tracker.asv_network_statistics.update(retries=retries)
        return network_tracker.asv_network_statistics


class LindiCreateLocalJsonFileBenchmark(BaseBenchmark):
    """Benchmark creating a local LINDI JSON file for a remote HDF5 file."""

    parameter_cases = lindi_hdf5_parameter_cases

    def setup(self, https_url: str):
        self.lindi_file = os.path.basename(https_url) + ".nwb.lindi.json"
        self.teardown(https_url=https_url)

    def teardown(self, https_url: str):
        """Delete the LINDI JSON file if it exists"""
        if os.path.exists(self.lindi_file):
            os.remove(self.lindi_file)

    # TODO This benchmark takes a long time to index all of the chunks for these files! Do not run until ready
    @skip_benchmark_if(True)
    def track_network_activity_create_lindi_json_file(self, https_url: str):
        """Track network activity during the creation of a LINDI JSON file."""
        with network_activity_tracker(tshark_path=TSHARK_PATH) as network_tracker:
            create_lindi_reference_file_system(https_url=https_url, outfile_path=self.lindi_file)
        return network_tracker.asv_network_statistics


class LindiReadLocalJsonFileBenchmark(BaseBenchmark):
    """Benchmark reading a remote NWB file using LINDI with a local LINDI JSON file."""

    parameter_cases = lindi_remote_rfs_parameter_cases

    def setup(self, https_url: str):
        """Download the remote LINDI JSON file."""
        self.lindi_file = os.path.basename(https_url)
        self.teardown(https_url=https_url)
        download_file(https_url=https_url, local_path=self.lindi_file)

    def teardown(self, https_url: str):
        """Delete the local LINDI JSON file if it exists."""
        if os.path.exists(self.lindi_file):
            os.remove(self.lindi_file)

    @skip_benchmark_if(TSHARK_PATH is None)
    def track_network_activity_during_read_lindi(self, https_url: str):
        """Track network activity during reading HDF5 files with LINDI using the local LINDI JSON file."""
        with network_activity_tracker(tshark_path=TSHARK_PATH) as network_tracker:
            self.client = read_hdf5_lindi(rfs=self.lindi_file)
        return network_tracker.asv_network_statistics

    @skip_benchmark_if(TSHARK_PATH is None)
    def track_network_activity_during_read_lindi_nwbfile(self, https_url: str):
        """Track network activity during reading NWB files with LINDI and PyNWB using the local LINDI JSON file."""
        with network_activity_tracker(tshark_path=TSHARK_PATH) as network_tracker:
            self.nwbfile, self.io, self.client = read_hdf5_nwbfile_lindi(rfs=self.lindi_file)
        return network_tracker.asv_network_statistics


class LindiReadRemoteJsonFileBenchmark(BaseBenchmark):
    """Benchmark reading a remote NWB file using LINDI with a remote LINDI JSON file."""

    parameter_cases = lindi_remote_rfs_parameter_cases

    @skip_benchmark_if(TSHARK_PATH is None)
    def track_network_activity_during_read_lindi(self, https_url: str):
        """Track network activity during reading HDF5 files with LINDI using the remote LINDI JSON file."""
        with network_activity_tracker(tshark_path=TSHARK_PATH) as network_tracker:
            self.client = read_hdf5_lindi(rfs=https_url)
        return network_tracker.asv_network_statistics

    @skip_benchmark_if(TSHARK_PATH is None)
    def track_network_activity_during_read_lindi_nwbfile(self, https_url: str):
        """Track network activity during reading NWB files with LINDI and PyNWB using the remote LINDI JSON file."""
        with network_activity_tracker(tshark_path=TSHARK_PATH) as network_tracker:
            self.nwbfile, self.io, self.client = read_hdf5_nwbfile_lindi(rfs=https_url)
        return network_tracker.asv_network_statistics


class ZarrS3ProtocolFileReadBenchmark(BaseBenchmark):
    """Benchmark reading a remote Zarr file."""

    parameter_cases = zarr_parameter_cases

    @skip_benchmark_if(TSHARK_PATH is None)
    def track_network_activity_during_read_zarr_s3(self, https_url: str):
        """Track network activity during reading Zarr files with Zarr-Python."""
        with network_activity_tracker(tshark_path=TSHARK_PATH) as network_tracker:
            self.zarr_file = read_zarr_s3_protocol(https_url=https_url, open_without_consolidated_metadata=True)
        return network_tracker.asv_network_statistics

    @skip_benchmark_if(TSHARK_PATH is None)
    def track_network_activity_during_read_zarr_https(self, https_url: str):
        """Track network activity during reading Zarr files with Zarr-Python."""
        with network_activity_tracker(tshark_path=TSHARK_PATH) as network_tracker:
            self.zarr_file = read_zarr_https_protocol(https_url=https_url, open_without_consolidated_metadata=True)
        return network_tracker.asv_network_statistics

    @skip_benchmark_if(TSHARK_PATH is None)
    def track_network_activity_during_read_pynwb_s3(self, https_url: str):
        """Track network activity during reading NWB files with Zarr-Python and PyNWB."""
        with network_activity_tracker(tshark_path=TSHARK_PATH) as network_tracker:
            self.nwbfile, self.io = read_zarr_nwbfile_s3_protocol(https_url=https_url, mode="r")
        return network_tracker.asv_network_statistics

    @skip_benchmark_if(TSHARK_PATH is None)
    def track_network_activity_during_read_pynwb_https(self, https_url: str):
        """Track network activity during reading NWB files with Zarr-Python and PyNWB."""
        with network_activity_tracker(tshark_path=TSHARK_PATH) as network_tracker:
            self.nwbfile, self.io = read_zarr_nwbfile_https_protocol(https_url=https_url, mode="r")
        return network_tracker.asv_network_statistics


class ZarrForceNoConsolidatedFileReadBenchmark(BaseBenchmark):
    """Benchmark reading a remote Zarr file without consolidated metadata."""

    parameter_cases = zarr_parameter_cases

    @skip_benchmark_if(TSHARK_PATH is None)
    def track_network_activity_during_read_zarr_s3(self, https_url: str):
        """Track network activity during reading Zarr files with Zarr-Python and S3 w/o consolidated metadata."""
        with network_activity_tracker(tshark_path=TSHARK_PATH) as network_tracker:
            self.zarr_file = read_zarr_s3_protocol(https_url=https_url, open_without_consolidated_metadata=True)
        return network_tracker.asv_network_statistics

    @skip_benchmark_if(TSHARK_PATH is None)
    def track_network_activity_during_read_zarr_https(self, https_url: str):
        """Track network activity during reading Zarr files with Zarr-Python and HTTPS w/o consolidated metadata."""
        with network_activity_tracker(tshark_path=TSHARK_PATH) as network_tracker:
            self.zarr_file = read_zarr_https_protocol(https_url=https_url, open_without_consolidated_metadata=True)
        return network_tracker.asv_network_statistics

    @skip_benchmark_if(TSHARK_PATH is None)
    def track_network_activity_during_read_pynwb_s3(self, https_url: str):
        """Track network activity during reading NWB files with Zarr-Python, PyNWB, and S3 w/o consolidated metadata."""
        with network_activity_tracker(tshark_path=TSHARK_PATH) as network_tracker:
            self.nwbfile, self.io = read_zarr_nwbfile_s3_protocol(https_url=https_url, mode="r-")
        return network_tracker.asv_network_statistics

    @skip_benchmark_if(TSHARK_PATH is None)
    def track_network_activity_during_read_pynwb_https(self, https_url: str):
        """Track network activity during reading NWB files with Zarr-Python, PyNWB, and HTTPS w/o consolidated metadata."""
        with network_activity_tracker(tshark_path=TSHARK_PATH) as network_tracker:
            self.nwbfile, self.io = read_zarr_nwbfile_https_protocol(https_url=https_url, mode="r-")
        return network_tracker.asv_network_statistics
