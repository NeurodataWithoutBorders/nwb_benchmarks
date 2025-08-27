"""Basic benchmarks for profiling network statistics for streaming access to slices of data stored in NWB files."""

from abc import ABC, abstractmethod
from typing import Tuple

from asv_runner.benchmarks.mark import skip_benchmark_if

from nwb_benchmarks import TSHARK_PATH
from nwb_benchmarks.core import (
    BaseBenchmark,
    get_object_by_name,
    network_activity_tracker,
    read_hdf5_nwbfile_fsspec_https_no_cache,
    read_hdf5_nwbfile_fsspec_https_with_cache,
    read_hdf5_nwbfile_fsspec_s3_no_cache,
    read_hdf5_nwbfile_fsspec_s3_with_cache,
    read_hdf5_nwbfile_lindi,
    read_hdf5_nwbfile_remfile,
    read_hdf5_nwbfile_remfile_with_cache,
    read_hdf5_nwbfile_ros3,
    read_zarr_nwbfile_https_protocol,
    read_zarr_nwbfile_s3_protocol,
    robust_ros3_read,
)

from .params_remote_slicing import (
    lindi_remote_rfs_parameter_cases,
    parameter_cases,
    zarr_parameter_cases,
)


class ContinuousSliceBenchmark(BaseBenchmark, ABC):
    """Base class for benchmarking slice access to NWB data."""

    @abstractmethod
    def setup(self, s3_url: str, object_name: str, slice_range: Tuple[slice]):
        """Set up the benchmark by loading the NWB file and preparing data for slicing.

        This method must be implemented by subclasses to define how to:
        - Load the NWB file from the given s3_url
        - Get the neurodata object by name
        - Set self.data_to_slice to the data that will be sliced
        """
        pass

    @skip_benchmark_if(TSHARK_PATH is None)
    def track_network_activity_during_slice(self, s3_url: str, object_name: str, slice_range: Tuple[slice]):
        """Track network activity during slice access."""
        with network_activity_tracker(tshark_path=TSHARK_PATH) as network_tracker:
            self._temp = self.data_to_slice[slice_range]
        return network_tracker.asv_network_statistics


class FsspecHttpsNoCacheContinuousSliceBenchmark(ContinuousSliceBenchmark):
    """Benchmark streaming access to slices of NWB data using fsspec & https without caching."""

    parameter_cases = parameter_cases

    def setup(self, s3_url: str, object_name: str, slice_range: Tuple[slice]):
        self.nwbfile, self.io, self.file, self.bytestream = read_hdf5_nwbfile_fsspec_https_no_cache(s3_url=s3_url)
        self.neurodata_object = get_object_by_name(nwbfile=self.nwbfile, object_name=object_name)
        self.data_to_slice = self.neurodata_object.data


class FsspecS3NoCacheContinuousSliceBenchmark(ContinuousSliceBenchmark):
    """Benchmark streaming access to slices of NWB data using fsspec & S3 without caching."""

    parameter_cases = parameter_cases

    def setup(self, s3_url: str, object_name: str, slice_range: Tuple[slice]):
        self.nwbfile, self.io, self.file, self.bytestream = read_hdf5_nwbfile_fsspec_s3_no_cache(s3_url=s3_url)
        self.neurodata_object = get_object_by_name(nwbfile=self.nwbfile, object_name=object_name)
        self.data_to_slice = self.neurodata_object.data


class FsspecHttpsWithCacheContinuousSliceBenchmark(ContinuousSliceBenchmark):
    """Benchmark streaming access to slices of NWB data using fsspec & https with caching."""

    parameter_cases = parameter_cases

    def setup(self, s3_url: str, object_name: str, slice_range: Tuple[slice]):
        self.nwbfile, self.io, self.file, self.bytestream, self.tmpdir = read_hdf5_nwbfile_fsspec_https_with_cache(
            s3_url=s3_url
        )
        self.neurodata_object = get_object_by_name(nwbfile=self.nwbfile, object_name=object_name)
        self.data_to_slice = self.neurodata_object.data

    def teardown(self, s3_url: str, object_name: str, slice_range: Tuple[slice]):
        if hasattr(self, "tmpdir"):
            self.tmpdir.cleanup()


class FsspecS3WithCacheContinuousSliceBenchmark(ContinuousSliceBenchmark):
    """Benchmark streaming access to slices of NWB data using fsspec & S3 with caching."""

    parameter_cases = parameter_cases

    def setup(self, s3_url: str, object_name: str, slice_range: Tuple[slice]):
        self.nwbfile, self.io, self.file, self.bytestream, self.tmpdir = read_hdf5_nwbfile_fsspec_s3_with_cache(
            s3_url=s3_url
        )
        self.neurodata_object = get_object_by_name(nwbfile=self.nwbfile, object_name=object_name)
        self.data_to_slice = self.neurodata_object.data

    def teardown(self, s3_url: str, object_name: str, slice_range: Tuple[slice]):
        if hasattr(self, "tmpdir"):
            self.tmpdir.cleanup()


class RemfileNoCacheContinuousSliceBenchmark(ContinuousSliceBenchmark):
    """Benchmark streaming access to slices of NWB data using Remfile without caching."""

    parameter_cases = parameter_cases

    def setup(self, s3_url: str, object_name: str, slice_range: Tuple[slice]):
        self.nwbfile, self.io, self.file, self.bytestream = read_hdf5_nwbfile_remfile(s3_url=s3_url)
        self.neurodata_object = get_object_by_name(nwbfile=self.nwbfile, object_name=object_name)
        self.data_to_slice = self.neurodata_object.data


class RemfileWithCacheContinuousSliceBenchmark(ContinuousSliceBenchmark):
    """Benchmark streaming access to slices of NWB data using Remfile with caching."""

    parameter_cases = parameter_cases

    def setup(self, s3_url: str, object_name: str, slice_range: Tuple[slice]):
        self.nwbfile, self.io, self.file, self.bytestream, self.tmpdir = read_hdf5_nwbfile_remfile_with_cache(
            s3_url=s3_url
        )
        self.neurodata_object = get_object_by_name(nwbfile=self.nwbfile, object_name=object_name)
        self.data_to_slice = self.neurodata_object.data

    def teardown(self, s3_url: str, object_name: str, slice_range: Tuple[slice]):
        if hasattr(self, "tmpdir"):
            self.tmpdir.cleanup()


class Ros3ContinuousSliceBenchmark(ContinuousSliceBenchmark):
    """Benchmark streaming access to slices of NWB data using the HDF5 ROS3 driver."""

    parameter_cases = parameter_cases

    def setup(self, s3_url: str, object_name: str, slice_range: Tuple[slice]):
        self.nwbfile, self.io, _ = read_hdf5_nwbfile_ros3(s3_url=s3_url)
        self.neurodata_object = get_object_by_name(nwbfile=self.nwbfile, object_name=object_name)
        self.data_to_slice = self.neurodata_object.data

    @skip_benchmark_if(TSHARK_PATH is None)
    def track_network_activity_during_slice(self, s3_url: str, object_name: str, slice_range: Tuple[slice]):
        """Track network activity during slice access, retrying reads robustly."""
        # NOTE: This function overrides ContinuousSliceBenchmark.track_network_activity_during_slice
        with network_activity_tracker(tshark_path=TSHARK_PATH) as network_tracker:
            self._temp, self.retries = robust_ros3_read(
                command=self.data_to_slice.__getitem__, command_args=(slice_range,)
            )
        network_tracker.asv_network_statistics.update(retries=self.retries)
        return network_tracker.asv_network_statistics


class LindiRemoteJsonContinuousSliceBenchmark(ContinuousSliceBenchmark):
    """Benchmark streaming access to slices of NWB data using LINDI with a remote JSON file."""

    parameter_cases = lindi_remote_rfs_parameter_cases

    def setup(self, s3_url: str, object_name: str, slice_range: Tuple[slice]):
        self.nwbfile, self.io, self.client = read_hdf5_nwbfile_lindi(rfs=s3_url)
        self.neurodata_object = get_object_by_name(nwbfile=self.nwbfile, object_name=object_name)
        self.data_to_slice = self.neurodata_object.data


class ZarrS3ContinuousSliceBenchmark(ContinuousSliceBenchmark):
    """Benchmark streaming access to slices of NWB data using Zarr and S3 protocol."""

    parameter_cases = zarr_parameter_cases

    def setup(self, s3_url: str, object_name: str, slice_range: Tuple[slice]):
        self.nwbfile, self.io = read_zarr_nwbfile_s3_protocol(s3_url=s3_url, mode="r")
        self.neurodata_object = get_object_by_name(nwbfile=self.nwbfile, object_name=object_name)
        self.data_to_slice = self.neurodata_object.data


class ZarrHttpsContinuousSliceBenchmark(ContinuousSliceBenchmark):
    """Benchmark streaming access to slices of NWB data using Zarr and HTTPS protocol."""

    parameter_cases = zarr_parameter_cases

    def setup(self, s3_url: str, object_name: str, slice_range: Tuple[slice]):
        self.nwbfile, self.io = read_zarr_nwbfile_https_protocol(s3_url=s3_url, mode="r")
        self.neurodata_object = get_object_by_name(nwbfile=self.nwbfile, object_name=object_name)
        self.data_to_slice = self.neurodata_object.data


class ZarrS3ForceNoConsolidatedContinuousSliceBenchmark(ContinuousSliceBenchmark):
    """Benchmark streaming access to slices of NWB data using Zarr and S3 protocol without consolidated metadata."""

    parameter_cases = zarr_parameter_cases

    def setup(self, s3_url: str, object_name: str, slice_range: Tuple[slice]):
        self.nwbfile, self.io = read_zarr_nwbfile_s3_protocol(s3_url=s3_url, mode="r-")
        self.neurodata_object = get_object_by_name(nwbfile=self.nwbfile, object_name=object_name)
        self.data_to_slice = self.neurodata_object.data


class ZarrHttpsForceNoConsolidatedContinuousSliceBenchmark(ContinuousSliceBenchmark):
    """Benchmark streaming access to slices of NWB data using Zarr and HTTPS protocol without consolidated metadata."""

    parameter_cases = zarr_parameter_cases

    def setup(self, s3_url: str, object_name: str, slice_range: Tuple[slice]):
        self.nwbfile, self.io = read_zarr_nwbfile_https_protocol(s3_url=s3_url, mode="r-")
        self.neurodata_object = get_object_by_name(nwbfile=self.nwbfile, object_name=object_name)
        self.data_to_slice = self.neurodata_object.data
