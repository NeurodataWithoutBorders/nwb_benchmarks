"""
Basic benchmarks for profiling network statistics for streaming access to NWB files and their contents.

The benchmarks should be consistent with the timing benchmarks - each function should be the same but wrapped in a
network activity tracker.
"""

import os

from asv_runner.benchmarks.mark import skip_benchmark, skip_benchmark_if

from nwb_benchmarks import TSHARK_PATH
from nwb_benchmarks.core import (
    BaseBenchmark,
    create_lindi_reference_file_system,
    download_file,
    network_activity_tracker,
    read_hdf5_h5py_fsspec_https_no_cache,
    read_hdf5_h5py_fsspec_https_with_cache,
    read_hdf5_h5py_fsspec_s3_no_cache,
    read_hdf5_h5py_fsspec_s3_with_cache,
    read_hdf5_h5py_lindi,
    read_hdf5_h5py_remfile_no_cache,
    read_hdf5_h5py_remfile_with_cache,
    read_hdf5_h5py_ros3,
    read_hdf5_pynwb_fsspec_https_no_cache,
    read_hdf5_pynwb_fsspec_https_with_cache,
    read_hdf5_pynwb_fsspec_s3_no_cache,
    read_hdf5_pynwb_fsspec_s3_with_cache,
    read_hdf5_pynwb_lindi,
    read_hdf5_pynwb_remfile_no_cache,
    read_hdf5_pynwb_remfile_with_cache,
    read_hdf5_pynwb_ros3,
    read_zarr_pynwb_https,
    read_zarr_pynwb_s3,
    read_zarr_zarrpython_https,
    read_zarr_zarrpython_s3,
)

from .params_remote_file_reading import (
    lindi_hdf5_parameter_cases,
    lindi_remote_rfs_parameter_cases,
    parameter_cases,
    zarr_parameter_cases,
)


class HDF5H5pyFileReadBenchmark(BaseBenchmark):
    """
    Track the network activity during read of remote HDF5 files with h5py using each streaming method.

    There is no formal parsing of the `pynwb.NWBFile` object.

    Note: in all cases, store the in-memory objects to be consistent with timing benchmarks.
    """

    parameter_cases = parameter_cases

    def teardown(self, https_url: str):
        # Not all tests in the class are using a temporary dir as cache. Clean up if it does.
        if hasattr(self, "tmpdir"):
            self.tmpdir.cleanup()

    @skip_benchmark_if(TSHARK_PATH is None)
    def track_network_read_hdf5_h5py_fsspec_https_no_cache(self, https_url: str):
        """Read remote HDF5 file using h5py and fsspec with HTTPS without cache."""
        with network_activity_tracker(tshark_path=TSHARK_PATH) as network_tracker:
            self.file, self.bytestream = read_hdf5_h5py_fsspec_https_no_cache(https_url=https_url)
        return network_tracker.asv_network_statistics

    @skip_benchmark_if(TSHARK_PATH is None)
    def track_network_read_hdf5_h5py_fsspec_https_with_cache(self, https_url: str):
        """Read remote HDF5 file using h5py and fsspec with HTTPS with cache."""
        with network_activity_tracker(tshark_path=TSHARK_PATH) as network_tracker:
            self.file, self.bytestream, self.tmpdir = read_hdf5_h5py_fsspec_https_with_cache(https_url=https_url)
        return network_tracker.asv_network_statistics

    @skip_benchmark_if(TSHARK_PATH is None)
    def track_network_read_hdf5_h5py_fsspec_s3_no_cache(self, https_url: str):
        """Read remote HDF5 file using h5py and fsspec with S3 without cache."""
        with network_activity_tracker(tshark_path=TSHARK_PATH) as network_tracker:
            self.file, self.bytestream = read_hdf5_h5py_fsspec_s3_no_cache(https_url=https_url)
        return network_tracker.asv_network_statistics

    @skip_benchmark_if(TSHARK_PATH is None)
    def track_network_read_hdf5_h5py_fsspec_s3_with_cache(self, https_url: str):
        """Read remote HDF5 file using h5py and fsspec with S3 with cache."""
        with network_activity_tracker(tshark_path=TSHARK_PATH) as network_tracker:
            self.file, self.bytestream, self.tmpdir = read_hdf5_h5py_fsspec_s3_with_cache(https_url=https_url)
        return network_tracker.asv_network_statistics

    @skip_benchmark_if(TSHARK_PATH is None)
    def track_network_read_hdf5_h5py_remfile_no_cache(self, https_url: str):
        """Read remote HDF5 file using h5py and remfile without cache."""
        with network_activity_tracker(tshark_path=TSHARK_PATH) as network_tracker:
            self.file, self.bytestream = read_hdf5_h5py_remfile_no_cache(https_url=https_url)
        return network_tracker.asv_network_statistics

    @skip_benchmark_if(TSHARK_PATH is None)
    def track_network_read_hdf5_h5py_remfile_with_cache(self, https_url: str):
        """Read remote HDF5 file using h5py and remfile with cache."""
        with network_activity_tracker(tshark_path=TSHARK_PATH) as network_tracker:
            self.file, self.bytestream, self.tmpdir = read_hdf5_h5py_remfile_with_cache(https_url=https_url)
        return network_tracker.asv_network_statistics

    @skip_benchmark_if(TSHARK_PATH is None)
    def track_network_read_hdf5_h5py_ros3(self, https_url: str):
        """Read remote HDF5 file using h5py and the ROS3 HDF5 driver."""
        with network_activity_tracker(tshark_path=TSHARK_PATH) as network_tracker:
            self.file, _ = read_hdf5_h5py_ros3(https_url=https_url)
        return network_tracker.asv_network_statistics


class HDF5PyNWBFileReadBenchmark(BaseBenchmark):
    """
    Track the network activity during read of remote HDF5 NWB files with pynwb using each streaming method.

    Note: in all cases, store the in-memory objects to be consistent with timing benchmarks.
    """

    parameter_cases = parameter_cases

    def teardown(self, https_url: str):
        # Not all tests in the class are using a temporary dir as cache. Clean up if it does.
        if hasattr(self, "tmpdir"):
            self.tmpdir.cleanup()

    @skip_benchmark_if(TSHARK_PATH is None)
    def track_network_read_hdf5_pynwb_fsspec_https_no_cache(self, https_url: str):
        """Read remote NWB file using pynwb and fsspec with HTTPS without cache."""
        with network_activity_tracker(tshark_path=TSHARK_PATH) as network_tracker:
            self.nwbfile, self.io, self.file, self.bytestream = read_hdf5_pynwb_fsspec_https_no_cache(
                https_url=https_url
            )
        return network_tracker.asv_network_statistics

    @skip_benchmark_if(TSHARK_PATH is None)
    def track_network_read_hdf5_pynwb_fsspec_https_with_cache(self, https_url: str):
        """Read remote NWB file using pynwb and fsspec with HTTPS with cache."""
        with network_activity_tracker(tshark_path=TSHARK_PATH) as network_tracker:
            self.nwbfile, self.io, self.file, self.bytestream, self.tmpdir = read_hdf5_pynwb_fsspec_https_with_cache(
                https_url=https_url
            )
        return network_tracker.asv_network_statistics

    @skip_benchmark_if(TSHARK_PATH is None)
    def track_network_read_hdf5_pynwb_fsspec_s3_no_cache(self, https_url: str):
        """Read remote NWB file using pynwb and fsspec with S3 without cache."""
        with network_activity_tracker(tshark_path=TSHARK_PATH) as network_tracker:
            self.nwbfile, self.io, self.file, self.bytestream = read_hdf5_pynwb_fsspec_s3_no_cache(https_url=https_url)
        return network_tracker.asv_network_statistics

    @skip_benchmark_if(TSHARK_PATH is None)
    def track_network_read_hdf5_pynwb_fsspec_s3_with_cache(self, https_url: str):
        """Read remote NWB file using pynwb and fsspec with S3 with cache."""
        with network_activity_tracker(tshark_path=TSHARK_PATH) as network_tracker:
            self.nwbfile, self.io, self.file, self.bytestream, self.tmpdir = read_hdf5_pynwb_fsspec_s3_with_cache(
                https_url=https_url
            )
        return network_tracker.asv_network_statistics

    @skip_benchmark_if(TSHARK_PATH is None)
    def track_network_read_hdf5_pynwb_remfile_no_cache(self, https_url: str):
        """Read remote NWB file using pynwb and remfile without cache."""
        with network_activity_tracker(tshark_path=TSHARK_PATH) as network_tracker:
            self.nwbfile, self.io, self.file, self.bytestream = read_hdf5_pynwb_remfile_no_cache(https_url=https_url)
        return network_tracker.asv_network_statistics

    @skip_benchmark_if(TSHARK_PATH is None)
    def track_network_read_hdf5_pynwb_remfile_with_cache(self, https_url: str):
        """Read remote NWB file using pynwb and remfile with cache."""
        with network_activity_tracker(tshark_path=TSHARK_PATH) as network_tracker:
            self.nwbfile, self.io, self.file, self.bytestream, self.tmpdir = read_hdf5_pynwb_remfile_with_cache(
                https_url=https_url
            )
        return network_tracker.asv_network_statistics

    @skip_benchmark_if(TSHARK_PATH is None)
    def track_network_read_hdf5_pynwb_ros3(self, https_url: str):
        """Read remote NWB file using pynwb and the ROS3 HDF5 driver."""
        with network_activity_tracker(tshark_path=TSHARK_PATH) as network_tracker:
            self.nwbfile, self.io, _ = read_hdf5_pynwb_ros3(https_url=https_url)
        return network_tracker.asv_network_statistics


class LindiCreateLocalJSONFileBenchmark(BaseBenchmark):
    """
    Track the network activity during read of remote HDF5 files and the creation of a LINDI JSON file using lindi.
    """

    parameter_cases = lindi_hdf5_parameter_cases

    def setup(self, https_url: str):
        self.lindi_file = os.path.basename(https_url) + ".nwb.lindi.json"
        self.teardown(https_url=https_url)

    def teardown(self, https_url: str):
        """Delete the LINDI JSON file if it exists"""
        if os.path.exists(self.lindi_file):
            os.remove(self.lindi_file)

    # TODO This benchmark takes a long time to index all of the chunks for these files! Do not run until ready
    @skip_benchmark
    def track_network_read_create_lindi_json(self, https_url: str):
        """Read a remote HDF5 file to create a LINDI JSON file."""
        with network_activity_tracker(tshark_path=TSHARK_PATH) as network_tracker:
            create_lindi_reference_file_system(https_url=https_url, outfile_path=self.lindi_file)
        return network_tracker.asv_network_statistics


class LindiLocalJSONFileReadBenchmark(BaseBenchmark):
    """
    Track the network activity during read of remote HDF5 files by reading the local LINDI JSON files with lindi and
    h5py or pynwb.

    This downloads the already created remote LINDI JSON files during setup.

    Note: in all cases, store the in-memory objects to be consistent with timing benchmarks.
    """

    parameter_cases = lindi_remote_rfs_parameter_cases

    def setup(self, https_url: str):
        """Download the LINDI JSON file."""
        self.lindi_file = os.path.basename(https_url) + ".lindi.json"
        self.teardown(https_url=https_url)
        download_file(https_url=https_url, local_path=self.lindi_file)

    def teardown(self, https_url: str):
        """Delete the LINDI JSON file if it exists."""
        if os.path.exists(self.lindi_file):
            os.remove(self.lindi_file)

    @skip_benchmark_if(TSHARK_PATH is None)
    def track_network_read_lindi_h5py(self, https_url: str):
        """Read a remote HDF5 file with h5py using lindi with the local LINDI JSON file."""
        with network_activity_tracker(tshark_path=TSHARK_PATH) as network_tracker:
            self.client = read_hdf5_h5py_lindi(rfs=self.lindi_file)
        return network_tracker.asv_network_statistics

    @skip_benchmark_if(TSHARK_PATH is None)
    def track_network_read_lindi_pynwb(self, https_url: str):
        """Read a remote HDF5 NWB file with pynwb using lindi with the local LINDI JSON file."""
        with network_activity_tracker(tshark_path=TSHARK_PATH) as network_tracker:
            self.nwbfile, self.io, self.client = read_hdf5_pynwb_lindi(rfs=self.lindi_file)
        return network_tracker.asv_network_statistics


class LindiRemoteJSONFileReadBenchmark(BaseBenchmark):
    """
    Track the network activity during read of remote HDF5 files by reading the remote LINDI JSON files with lindi and
    h5py or pynwb.

    Note: When LINDI is pointed to a remote JSON, it starts by downloading it, so the network activity should not be
    that different from the activity to download the remote JSON and load the local JSON.

    Note: in all cases, store the in-memory objects to be consistent with timing benchmarks.
    """

    parameter_cases = lindi_remote_rfs_parameter_cases

    @skip_benchmark_if(TSHARK_PATH is None)
    def track_network_read_lindi_h5py(self, https_url: str):
        """Read a remote HDF5 file with h5py using lindi with the remote LINDI JSON file."""
        with network_activity_tracker(tshark_path=TSHARK_PATH) as network_tracker:
            self.client = read_hdf5_h5py_lindi(rfs=https_url)
        return network_tracker.asv_network_statistics

    @skip_benchmark_if(TSHARK_PATH is None)
    def track_network_read_lindi_pynwb(self, https_url: str):
        """Read a remote HDF5 NWB file with pynwb using lindi with the remote LINDI JSON file."""
        with network_activity_tracker(tshark_path=TSHARK_PATH) as network_tracker:
            self.nwbfile, self.io, self.client = read_hdf5_pynwb_lindi(rfs=https_url)
        return network_tracker.asv_network_statistics


class ZarrZarrPythonFileReadBenchmark(BaseBenchmark):
    """
    Track the network activity during read of remote Zarr files with Zarr-Python only (not using PyNWB)

    Note: in all cases, store the in-memory objects to avoid timing garbage collection steps.
    """

    parameter_cases = zarr_parameter_cases

    @skip_benchmark_if(TSHARK_PATH is None)
    def track_network_read_zarr_https(self, https_url: str):
        """Read a Zarr file using Zarr-Python with HTTPS and consolidated metadata (if available)."""
        with network_activity_tracker(tshark_path=TSHARK_PATH) as network_tracker:
            self.zarr_file = read_zarr_zarrpython_https(https_url=https_url, open_without_consolidated_metadata=False)
        return network_tracker.asv_network_statistics

    @skip_benchmark_if(TSHARK_PATH is None)
    def track_network_read_zarr_https_force_no_consolidated(self, https_url: str):
        """Read a Zarr file using Zarr-Python with HTTPS and without using consolidated metadata."""
        with network_activity_tracker(tshark_path=TSHARK_PATH) as network_tracker:
            self.zarr_file = read_zarr_zarrpython_https(https_url=https_url, open_without_consolidated_metadata=True)
        return network_tracker.asv_network_statistics

    @skip_benchmark_if(TSHARK_PATH is None)
    def track_network_read_zarr_s3(self, https_url: str):
        """Read a Zarr file using Zarr-Python with S3 and consolidated metadata (if available)."""
        with network_activity_tracker(tshark_path=TSHARK_PATH) as network_tracker:
            self.zarr_file = read_zarr_zarrpython_s3(https_url=https_url, open_without_consolidated_metadata=False)
        return network_tracker.asv_network_statistics

    @skip_benchmark_if(TSHARK_PATH is None)
    def track_network_read_zarr_s3_force_no_consolidated(self, https_url: str):
        """Read a Zarr file using Zarr-Python with S3 and without using consolidated metadata."""
        with network_activity_tracker(tshark_path=TSHARK_PATH) as network_tracker:
            self.zarr_file = read_zarr_zarrpython_s3(https_url=https_url, open_without_consolidated_metadata=True)
        return network_tracker.asv_network_statistics


class ZarrPyNWBFileReadBenchmark(BaseBenchmark):
    """
    Track the network activity during read of remote Zarr NWB files with pynwb.

    Note: in all cases, store the in-memory objects to be consistent with timing benchmarks.
    """

    parameter_cases = zarr_parameter_cases

    @skip_benchmark_if(TSHARK_PATH is None)
    def track_network_read_zarr_pynwb_https(self, https_url: str):
        """Read a Zarr NWB file using pynwb with HTTPS and consolidated metadata (if available)."""
        with network_activity_tracker(tshark_path=TSHARK_PATH) as network_tracker:
            self.nwbfile, self.io = read_zarr_pynwb_https(https_url=https_url, mode="r")
        return network_tracker.asv_network_statistics

    @skip_benchmark_if(TSHARK_PATH is None)
    def track_network_read_zarr_pynwb_https_force_no_consolidated(self, https_url: str):
        """Read a Zarr NWB file using pynwb with HTTPS and without consolidated metadata."""
        with network_activity_tracker(tshark_path=TSHARK_PATH) as network_tracker:
            self.nwbfile, self.io = read_zarr_pynwb_https(https_url=https_url, mode="r-")
        return network_tracker.asv_network_statistics

    @skip_benchmark_if(TSHARK_PATH is None)
    def track_network_read_zarr_pynwb_s3(self, https_url: str):
        """Read a Zarr NWB file using pynwb with S3 and consolidated metadata (if available)."""
        with network_activity_tracker(tshark_path=TSHARK_PATH) as network_tracker:
            self.nwbfile, self.io = read_zarr_pynwb_s3(https_url=https_url, mode="r")
        return network_tracker.asv_network_statistics

    @skip_benchmark_if(TSHARK_PATH is None)
    def track_network_read_zarr_pynwb_s3_force_no_consolidated(self, https_url: str):
        """Read a Zarr NWB file using pynwb using S3 and without consolidated metadata."""
        with network_activity_tracker(tshark_path=TSHARK_PATH) as network_tracker:
            self.nwbfile, self.io = read_zarr_pynwb_s3(https_url=https_url, mode="r-")
        return network_tracker.asv_network_statistics
