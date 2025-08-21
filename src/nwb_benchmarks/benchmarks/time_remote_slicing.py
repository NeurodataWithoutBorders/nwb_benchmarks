"""Basic benchmarks for timing streaming access to slices of data stored in NWB files."""

from typing import Tuple

from nwb_benchmarks.core import (
    BaseBenchmark,
    get_object_by_name,
    read_hdf5_nwbfile_fsspec_no_cache,
    read_hdf5_nwbfile_fsspec_with_cache,
    read_hdf5_nwbfile_lindi,
    read_hdf5_nwbfile_remfile,
    read_hdf5_nwbfile_remfile_with_cache,
    read_hdf5_nwbfile_ros3,
    read_zarr_nwbfile,
)

from .params_remote_slicing import (
    lindi_remote_rfs_parameter_cases,
    parameter_cases,
    zarr_parameter_cases,
)


class FsspecNoCacheContinuousSliceBenchmark(BaseBenchmark):
    rounds = 1
    repeat = 3
    parameter_cases = parameter_cases

    def setup(self, s3_url: str, object_name: str, slice_range: Tuple[slice]):
        self.nwbfile, self.io, self.file, self.bytestream = read_hdf5_nwbfile_fsspec_no_cache(s3_url=s3_url)
        self.neurodata_object = get_object_by_name(nwbfile=self.nwbfile, object_name=object_name)
        self.data_to_slice = self.neurodata_object.data

    def time_slice(self, s3_url: str, object_name: str, slice_range: Tuple[slice]):
        """Track network activity for slicing into a h5py.Dataset with Fsspec"""
        self._temp = self.data_to_slice[slice_range]


class FsspecWithCacheContinuousSliceBenchmark(BaseBenchmark):
    rounds = 1
    repeat = 3
    parameter_cases = parameter_cases

    def teardown(self, s3_url: str, object_name: str, slice_range: Tuple[slice]):
        self.tmpdir.cleanup()

    def setup(self, s3_url: str, object_name: str, slice_range: Tuple[slice]):
        self.nwbfile, self.io, self.file, self.bytestream, self.tmpdir = read_hdf5_nwbfile_fsspec_with_cache(
            s3_url=s3_url
        )
        self.neurodata_object = get_object_by_name(nwbfile=self.nwbfile, object_name=object_name)
        self.data_to_slice = self.neurodata_object.data

    def time_slice(self, s3_url: str, object_name: str, slice_range: Tuple[slice]):
        """Track network activity for slicing into a `h5py.Dataset` with `fsspec`."""
        self._temp = self.data_to_slice[slice_range]


class RemfileNoCacheContinuousSliceBenchmark(BaseBenchmark):
    rounds = 1
    repeat = 3
    parameter_cases = parameter_cases

    def setup(self, s3_url: str, object_name: str, slice_range: Tuple[slice]):
        self.nwbfile, self.io, self.file, self.bytestream = read_hdf5_nwbfile_remfile(s3_url=s3_url)
        self.neurodata_object = get_object_by_name(nwbfile=self.nwbfile, object_name=object_name)
        self.data_to_slice = self.neurodata_object.data

    def time_slice(self, s3_url: str, object_name: str, slice_range: Tuple[slice]):
        """Track network activity for slicing into a h5py.Dataset with RemFile"""
        self._temp = self.data_to_slice[slice_range]


class RemfileWithCacheContinuousSliceBenchmark(BaseBenchmark):
    rounds = 1
    repeat = 3
    parameter_cases = parameter_cases

    def teardown(self, s3_url: str, object_name: str, slice_range: Tuple[slice]):
        self.tmpdir.cleanup()

    def setup(self, s3_url: str, object_name: str, slice_range: Tuple[slice]):
        self.nwbfile, self.io, self.file, self.bytestream, self.tmpdir = read_hdf5_nwbfile_remfile_with_cache(
            s3_url=s3_url
        )
        self.neurodata_object = get_object_by_name(nwbfile=self.nwbfile, object_name=object_name)
        self.data_to_slice = self.neurodata_object.data

    def time_slice(self, s3_url: str, object_name: str, slice_range: Tuple[slice]):
        """Track network activity for slicing into a h5py.Dataset with RemFile"""
        self._temp = self.data_to_slice[slice_range]


class Ros3ContinuousSliceBenchmark(BaseBenchmark):
    rounds = 1
    repeat = 3
    parameter_cases = parameter_cases

    def setup(self, s3_url: str, object_name: str, slice_range: Tuple[slice]):
        self.nwbfile, self.io, _ = read_hdf5_nwbfile_ros3(s3_url=s3_url)
        self.neurodata_object = get_object_by_name(nwbfile=self.nwbfile, object_name=object_name)
        self.data_to_slice = self.neurodata_object.data

    def time_slice(self, s3_url: str, object_name: str, slice_range: Tuple[slice]):
        """Track network activity for slicing into a h5py.Dataset with Ros3"""
        self._temp = self.data_to_slice[slice_range]


class LindiFileReadRemoteReferenceFileSystemContinuousSliceBenchmark(BaseBenchmark):
    """
    Time the read of a data slice from a remote NWB file with pynwb using lindi with a remote JSON reference
    filesystem available.
    """

    rounds = 1
    repeat = 3
    parameter_cases = lindi_remote_rfs_parameter_cases

    def setup(self, s3_url: str, object_name: str, slice_range: Tuple[slice]):
        self.nwbfile, self.io, self.client = read_hdf5_nwbfile_lindi(rfs=s3_url)
        self.neurodata_object = get_object_by_name(nwbfile=self.nwbfile, object_name=object_name)
        self.data_to_slice = self.neurodata_object.data

    def time_slice(self, s3_url: str, object_name: str, slice_range: Tuple[slice]):
        """Track network activity for slicing into a LindiDataset"""
        self._temp = self.data_to_slice[slice_range]


class ZarrContinuousSliceBenchmark(BaseBenchmark):
    """
    Benchmark network activity for slicing into a Zarr dataset using consolidated metadata (if available)
    """

    rounds = 1
    repeat = 3
    parameter_cases = zarr_parameter_cases

    def setup(self, s3_url: str, object_name: str, slice_range: Tuple[slice]):
        self.nwbfile, self.io = read_zarr_nwbfile(s3_url=s3_url, mode="r")
        self.neurodata_object = get_object_by_name(nwbfile=self.nwbfile, object_name=object_name)
        self.data_to_slice = self.neurodata_object.data

    def time_slice(self, s3_url: str, object_name: str, slice_range: Tuple[slice]):
        """Track network activity for slicing into a Zarr dataset"""
        self._temp = self.data_to_slice[slice_range]


class ZarrForceNoConsolidatedContinuousSliceBenchmark(BaseBenchmark):
    """
    Benchmark network activity for slicing into a Zarr dataset without using consolidated metadata
    """

    rounds = 1
    repeat = 3
    parameter_cases = zarr_parameter_cases

    def setup(self, s3_url: str, object_name: str, slice_range: Tuple[slice]):
        self.nwbfile, self.io = read_zarr_nwbfile(s3_url=s3_url, mode="r-")
        self.neurodata_object = get_object_by_name(nwbfile=self.nwbfile, object_name=object_name)
        self.data_to_slice = self.neurodata_object.data

    def time_slice(self, s3_url: str, object_name: str, slice_range: Tuple[slice]):
        """Track network activity for slicing into a Zarr dataset"""
        self._temp = self.data_to_slice[slice_range]
