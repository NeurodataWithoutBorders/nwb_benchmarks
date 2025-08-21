"""Basic benchmarks for profiling network statistics for streaming access to slices of data stored in NWB files."""

from typing import Tuple

from asv_runner.benchmarks.mark import skip_benchmark_if

from nwb_benchmarks import TSHARK_PATH
from nwb_benchmarks.core import (
    BaseBenchmark,
    get_object_by_name,
    network_activity_tracker,
    read_hdf5_nwbfile_fsspec_no_cache,
    read_hdf5_nwbfile_fsspec_with_cache,
    read_hdf5_nwbfile_lindi,
    read_hdf5_nwbfile_remfile,
    read_hdf5_nwbfile_remfile_with_cache,
    read_hdf5_nwbfile_ros3,
    read_zarr_nwbfile,
    robust_ros3_read,
)

from .params_remote_slicing import parameter_cases, lindi_remote_rfs_parameter_cases, zarr_parameter_cases


@skip_benchmark_if(TSHARK_PATH is None)
class FsspecNoCacheContinuousSliceBenchmark(BaseBenchmark):
    parameter_cases = parameter_cases

    def setup(self, s3_url: str, object_name: str, slice_range: Tuple[slice]):
        self.nwbfile, self.io, self.file, self.bytestream = read_hdf5_nwbfile_fsspec_no_cache(s3_url=s3_url)
        self.neurodata_object = get_object_by_name(nwbfile=self.nwbfile, object_name=object_name)
        self.data_to_slice = self.neurodata_object.data

    def track_network_activity_during_slice(self, s3_url: str, object_name: str, slice_range: Tuple[slice]):
        with network_activity_tracker(tshark_path=TSHARK_PATH) as network_tracker:
            self._temp = self.data_to_slice[slice_range]
        return network_tracker.asv_network_statistics


@skip_benchmark_if(TSHARK_PATH is None)
class FsspecWithCacheContinuousSliceBenchmark(BaseBenchmark):
    parameter_cases = parameter_cases

    def teardown(self, s3_url: str, object_name: str, slice_range: Tuple[slice]):
        self.tmpdir.cleanup()

    def setup(self, s3_url: str, object_name: str, slice_range: Tuple[slice]):
        self.nwbfile, self.io, self.file, self.bytestream, self.tmpdir = read_hdf5_nwbfile_fsspec_with_cache(
            s3_url=s3_url
        )
        self.neurodata_object = get_object_by_name(nwbfile=self.nwbfile, object_name=object_name)
        self.data_to_slice = self.neurodata_object.data

    def track_network_activity_during_slice(self, s3_url: str, object_name: str, slice_range: Tuple[slice]):
        with network_activity_tracker(tshark_path=TSHARK_PATH) as network_tracker:
            self._temp = self.data_to_slice[slice_range]
        return network_tracker.asv_network_statistics


@skip_benchmark_if(TSHARK_PATH is None)
class RemfileContinuousSliceBenchmark(BaseBenchmark):
    parameter_cases = parameter_cases

    def setup(self, s3_url: str, object_name: str, slice_range: Tuple[slice]):
        self.nwbfile, self.io, self.file, self.bytestream = read_hdf5_nwbfile_remfile(s3_url=s3_url)
        self.neurodata_object = get_object_by_name(nwbfile=self.nwbfile, object_name=object_name)
        self.data_to_slice = self.neurodata_object.data

    def track_network_activity_during_slice(self, s3_url: str, object_name: str, slice_range: Tuple[slice]):
        with network_activity_tracker(tshark_path=TSHARK_PATH) as network_tracker:
            self._temp = self.data_to_slice[slice_range]
        return network_tracker.asv_network_statistics


@skip_benchmark_if(TSHARK_PATH is None)
class RemfileContinuousSliceBenchmarkWithCache(BaseBenchmark):
    parameter_cases = parameter_cases

    def teardown(self, s3_url: str, object_name: str, slice_range: Tuple[slice]):
        self.tmpdir.cleanup()

    def setup(self, s3_url: str, object_name: str, slice_range: Tuple[slice]):
        self.nwbfile, self.io, self.file, self.bytestream, self.tmpdir = read_hdf5_nwbfile_remfile_with_cache(
            s3_url=s3_url
        )
        self.neurodata_object = get_object_by_name(nwbfile=self.nwbfile, object_name=object_name)
        self.data_to_slice = self.neurodata_object.data

    def track_network_activity_during_slice(self, s3_url: str, object_name: str, slice_range: Tuple[slice]):
        with network_activity_tracker(tshark_path=TSHARK_PATH) as network_tracker:
            self._temp = self.data_to_slice[slice_range]
        return network_tracker.asv_network_statistics


@skip_benchmark_if(TSHARK_PATH is None)
class Ros3ContinuousSliceBenchmark(BaseBenchmark):
    parameter_cases = parameter_cases

    def setup(self, s3_url: str, object_name: str, slice_range: Tuple[slice]):
        self.nwbfile, self.io, _ = read_hdf5_nwbfile_ros3(s3_url=s3_url)
        self.neurodata_object = get_object_by_name(nwbfile=self.nwbfile, object_name=object_name)
        self.data_to_slice = self.neurodata_object.data

    def track_network_activity_during_slice(self, s3_url: str, object_name: str, slice_range: Tuple[slice]):
        with network_activity_tracker(tshark_path=TSHARK_PATH) as network_tracker:
            self._temp, self.retries = robust_ros3_read(
                command=self.data_to_slice.__getitem__, command_args=(slice_range,)
            )
        network_tracker.asv_network_statistics.update(retries=self.retries)
        return network_tracker.asv_network_statistics


@skip_benchmark_if(TSHARK_PATH is None)
class LindiFileReadRemoteReferenceFileSystemContinuousSliceBenchmark(BaseBenchmark):
    """
    Time the read of a data slice from a remote NWB file with pynwb using lindi with a remote JSON reference
    filesystem available.
    """

    parameter_cases = lindi_remote_rfs_parameter_cases

    def setup(self, s3_url: str, object_name: str, slice_range: Tuple[slice]):
        self.nwbfile, self.io, self.client = read_hdf5_nwbfile_lindi(rfs=s3_url)
        self.neurodata_object = get_object_by_name(nwbfile=self.nwbfile, object_name=object_name)
        self.data_to_slice = self.neurodata_object.data

    def track_network_activity_during_slice(self, s3_url: str, object_name: str, slice_range: Tuple[slice]):
        with network_activity_tracker(tshark_path=TSHARK_PATH) as network_tracker:
            self._temp = self.data_to_slice[slice_range]
        return network_tracker.asv_network_statistics


@skip_benchmark_if(TSHARK_PATH is None)
class ZarrContinuousSliceBenchmark(BaseBenchmark):
    parameter_cases = zarr_parameter_cases

    def setup(self, s3_url: str, object_name: str, slice_range: Tuple[slice]):
        self.nwbfile, self.io = read_zarr_nwbfile(s3_url=s3_url, mode="r")
        self.neurodata_object = get_object_by_name(nwbfile=self.nwbfile, object_name=object_name)
        self.data_to_slice = self.neurodata_object.data

    def track_network_activity_during_slice(self, s3_url: str, object_name: str, slice_range: Tuple[slice]):
        with network_activity_tracker(tshark_path=TSHARK_PATH) as network_tracker:
            self._temp = self.data_to_slice[slice_range]
        return network_tracker.asv_network_statistics


@skip_benchmark_if(TSHARK_PATH is None)
class ZarrForceNoConsolidatedContinuousSliceBenchmark(BaseBenchmark):
    parameter_cases = zarr_parameter_cases

    def setup(self, s3_url: str, object_name: str, slice_range: Tuple[slice]):
        self.nwbfile, self.io = read_zarr_nwbfile(s3_url=s3_url, mode="r-")
        self.neurodata_object = get_object_by_name(nwbfile=self.nwbfile, object_name=object_name)
        self.data_to_slice = self.neurodata_object.data

    def track_network_activity_during_slice(self, s3_url: str, object_name: str, slice_range: Tuple[slice]):
        with network_activity_tracker(tshark_path=TSHARK_PATH) as network_tracker:
            self._temp = self.data_to_slice[slice_range]
        return network_tracker.asv_network_statistics
