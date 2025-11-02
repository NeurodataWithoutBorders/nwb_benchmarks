"""Basic benchmarks for timing streaming access to slices of data stored in NWB files."""

import shutil
from abc import ABC, abstractmethod
from typing import Tuple

from nwb_benchmarks.core import (
    BaseBenchmark,
    download_read_hdf5_pynwb_lindi,
    get_object_by_name,
    read_hdf5_pynwb_fsspec_https_no_cache,
    read_hdf5_pynwb_fsspec_https_with_cache,
    read_hdf5_pynwb_fsspec_s3_no_cache,
    read_hdf5_pynwb_fsspec_s3_with_cache,
    read_hdf5_pynwb_remfile_no_cache,
    read_hdf5_pynwb_remfile_with_cache,
    read_hdf5_pynwb_ros3,
    read_zarr_pynwb_s3,
)

from .params_remote_slicing import (
    hdf5_params,
    lindi_remote_rfs_params,
    zarr_params,
)


class ContinuousSliceBenchmark(BaseBenchmark, ABC):
    """
    Base class for benchmarking slice access to NWB data.

    Note: in all cases, store the in-memory objects to avoid timing garbage collection steps.
    """

    @abstractmethod
    def setup(self, params: dict[str, str | Tuple[slice]]):
        """Set up the benchmark by loading the NWB file and preparing data for slicing.

        This method must be implemented by subclasses to define how to:
        - Load the NWB file from the given https_url
        - Get the neurodata object by name
        - Set self.data_to_slice to the data that will be sliced
        """
        pass

    def teardown(self, params: dict[str, str | Tuple[slice]]):
        if hasattr(self, "io"):
            self.io.close()
        if hasattr(self, "file"):
            self.file.close()
        if hasattr(self, "bytestream"):
            self.bytestream.close()
        if hasattr(self, "client"):
            self.client.close()
        if hasattr(self, "tmpdir"):
            shutil.rmtree(path=self.tmpdir.name, ignore_errors=True)
            self.tmpdir.cleanup()

    def time_slice(self, params: dict[str, str | Tuple[slice]]):
        """Slice a range of a dataset in a remote NWB file."""
        slice_range = params["slice_range"]
        self._temp = self.data_to_slice[slice_range]


class HDF5PyNWBFsspecHttpsNoCacheContinuousSliceBenchmark(ContinuousSliceBenchmark):
    """
    Time the read of a continuous data slice from remote HDF5 NWB files using pynwb and fsspec with HTTPS without
    cache.
    """

    params = hdf5_params

    def setup(self, params: dict[str, str | Tuple[slice]]):
        https_url = params["https_url"]
        object_name = params["object_name"]

        self.nwbfile, self.io, self.file, self.bytestream = read_hdf5_pynwb_fsspec_https_no_cache(https_url=https_url)
        self.neurodata_object = get_object_by_name(nwbfile=self.nwbfile, object_name=object_name)
        self.data_to_slice = self.neurodata_object.data


class HDF5PyNWBFsspecHttpsWithCacheContinuousSliceBenchmark(ContinuousSliceBenchmark):
    """
    Time the read of a continuous data slice from remote HDF5 NWB files using pynwb and fsspec with HTTPS with cache.
    """

    params = hdf5_params

    def setup(self, params: dict[str, str | Tuple[slice]]):
        https_url = params["https_url"]
        object_name = params["object_name"]

        self.nwbfile, self.io, self.file, self.bytestream, self.tmpdir = read_hdf5_pynwb_fsspec_https_with_cache(
            https_url=https_url
        )
        self.neurodata_object = get_object_by_name(nwbfile=self.nwbfile, object_name=object_name)
        self.data_to_slice = self.neurodata_object.data


class HDF5PyNWBFsspecHttpsPreloadedNoCacheContinuousSliceBenchmark(ContinuousSliceBenchmark):
    """
    Time the read of a continuous data slice from remote HDF5 NWB files using pynwb and fsspec with HTTPS with preloaded
    data without cache.
    """

    params = hdf5_params

    def setup(self, params: dict[str, str | Tuple[slice]]):
        https_url = params["https_url"]
        object_name = params["object_name"]
        slice_range = params["slice_range"]

        self.nwbfile, self.io, self.file, self.bytestream = read_hdf5_pynwb_fsspec_https_no_cache(https_url=https_url)
        self.neurodata_object = get_object_by_name(nwbfile=self.nwbfile, object_name=object_name)
        self.data_to_slice = self.neurodata_object.data
        self._temp = self.data_to_slice[slice_range]


class HDF5PyNWBFsspecHttpsPreloadedWithCacheContinuousSliceBenchmark(ContinuousSliceBenchmark):
    """
    Time the read of a continuous data slice from remote HDF5 NWB files using pynwb and fsspec with HTTPS with preloaded
    cache.
    """

    params = hdf5_params

    def setup(self, params: dict[str, str | Tuple[slice]]):
        https_url = params["https_url"]
        object_name = params["object_name"]
        slice_range = params["slice_range"]

        self.nwbfile, self.io, self.file, self.bytestream, self.tmpdir = read_hdf5_pynwb_fsspec_https_with_cache(
            https_url=https_url
        )
        self.neurodata_object = get_object_by_name(nwbfile=self.nwbfile, object_name=object_name)
        self.data_to_slice = self.neurodata_object.data
        self._temp = self.data_to_slice[slice_range]


class HDF5PyNWBFsspecS3NoCacheContinuousSliceBenchmark(ContinuousSliceBenchmark):
    """
    Time the read of a continuous data slice from remote HDF5 NWB files using pynwb and fsspec with S3 without cache.
    """

    params = hdf5_params

    def setup(self, params: dict[str, str | Tuple[slice]]):
        https_url = params["https_url"]
        object_name = params["object_name"]

        self.nwbfile, self.io, self.file, self.bytestream = read_hdf5_pynwb_fsspec_s3_no_cache(https_url=https_url)
        self.neurodata_object = get_object_by_name(nwbfile=self.nwbfile, object_name=object_name)
        self.data_to_slice = self.neurodata_object.data


class HDF5PyNWBFsspecS3WithCacheContinuousSliceBenchmark(ContinuousSliceBenchmark):
    """
    Time the read of a continuous data slice from remote HDF5 NWB files using pynwb and fsspec with S3 with cache.
    """

    params = hdf5_params

    def setup(self, params: dict[str, str | Tuple[slice]]):
        https_url = params["https_url"]
        object_name = params["object_name"]

        self.nwbfile, self.io, self.file, self.bytestream, self.tmpdir = read_hdf5_pynwb_fsspec_s3_with_cache(
            https_url=https_url
        )
        self.neurodata_object = get_object_by_name(nwbfile=self.nwbfile, object_name=object_name)
        self.data_to_slice = self.neurodata_object.data


class HDF5PyNWBFsspecS3PreloadedNoCacheContinuousSliceBenchmark(ContinuousSliceBenchmark):
    """
    Time the read of a continuous data slice from remote HDF5 NWB files using pynwb and fsspec with S3 with preloaded
    data without cache.
    """

    params = hdf5_params

    def setup(self, params: dict[str, str | Tuple[slice]]):
        https_url = params["https_url"]
        object_name = params["object_name"]
        slice_range = params["slice_range"]

        self.nwbfile, self.io, self.file, self.bytestream = read_hdf5_pynwb_fsspec_s3_no_cache(https_url=https_url)
        self.neurodata_object = get_object_by_name(nwbfile=self.nwbfile, object_name=object_name)
        self.data_to_slice = self.neurodata_object.data
        self._temp = self.data_to_slice[slice_range]


class HDF5PyNWBFsspecS3PreloadedWithCacheContinuousSliceBenchmark(ContinuousSliceBenchmark):
    """
    Time the read of a continuous data slice from remote HDF5 NWB files using pynwb and fsspec with S3 with preloaded
    cache.
    """

    params = hdf5_params

    def setup(self, params: dict[str, str | Tuple[slice]]):
        https_url = params["https_url"]
        object_name = params["object_name"]
        slice_range = params["slice_range"]

        self.nwbfile, self.io, self.file, self.bytestream, self.tmpdir = read_hdf5_pynwb_fsspec_s3_with_cache(
            https_url=https_url
        )
        self.neurodata_object = get_object_by_name(nwbfile=self.nwbfile, object_name=object_name)
        self.data_to_slice = self.neurodata_object.data
        self._temp = self.data_to_slice[slice_range]


class HDF5PyNWBRemfileNoCacheContinuousSliceBenchmark(ContinuousSliceBenchmark):
    """
    Time the read of a continuous data slice from remote HDF5 NWB files using pynwb and remfile without cache.
    """

    params = hdf5_params

    def setup(self, params: dict[str, str | Tuple[slice]]):
        https_url = params["https_url"]
        object_name = params["object_name"]

        self.nwbfile, self.io, self.file, self.bytestream = read_hdf5_pynwb_remfile_no_cache(https_url=https_url)
        self.neurodata_object = get_object_by_name(nwbfile=self.nwbfile, object_name=object_name)
        self.data_to_slice = self.neurodata_object.data


class HDF5PyNWBRemfileWithCacheContinuousSliceBenchmark(ContinuousSliceBenchmark):
    """
    Time the read of a continuous data slice from remote HDF5 NWB files using pynwb and remfile with cache.
    """

    params = hdf5_params

    def setup(self, params: dict[str, str | Tuple[slice]]):
        https_url = params["https_url"]
        object_name = params["object_name"]

        self.nwbfile, self.io, self.file, self.bytestream, self.tmpdir = read_hdf5_pynwb_remfile_with_cache(
            https_url=https_url
        )
        self.neurodata_object = get_object_by_name(nwbfile=self.nwbfile, object_name=object_name)
        self.data_to_slice = self.neurodata_object.data


class HDF5PyNWBRemfilePreloadedNoCacheContinuousSliceBenchmark(ContinuousSliceBenchmark):
    """
    Time the read of a continuous data slice from remote HDF5 NWB files using pynwb and remfile with preloaded
    data without cache.
    """

    params = hdf5_params

    def setup(self, params: dict[str, str | Tuple[slice]]):
        https_url = params["https_url"]
        object_name = params["object_name"]
        slice_range = params["slice_range"]

        self.nwbfile, self.io, self.file, self.bytestream = read_hdf5_pynwb_remfile_no_cache(https_url=https_url)
        self.neurodata_object = get_object_by_name(nwbfile=self.nwbfile, object_name=object_name)
        self.data_to_slice = self.neurodata_object.data
        self._temp = self.data_to_slice[slice_range]


class HDF5PyNWBRemfilePreloadedWithCacheContinuousSliceBenchmark(ContinuousSliceBenchmark):
    """
    Time the read of a continuous data slice from remote HDF5 NWB files using pynwb and remfile with preloaded cache.
    """

    params = hdf5_params

    def setup(self, params: dict[str, str | Tuple[slice]]):
        https_url = params["https_url"]
        object_name = params["object_name"]
        slice_range = params["slice_range"]

        self.nwbfile, self.io, self.file, self.bytestream, self.tmpdir = read_hdf5_pynwb_remfile_with_cache(
            https_url=https_url
        )
        self.neurodata_object = get_object_by_name(nwbfile=self.nwbfile, object_name=object_name)
        self.data_to_slice = self.neurodata_object.data
        self._temp = self.data_to_slice[slice_range]


class HDF5PyNWBROS3ContinuousSliceBenchmark(ContinuousSliceBenchmark):
    """
    Time the read of a continuous data slice from remote HDF5 NWB files using pynwb and the ROS3 driver.
    """

    params = hdf5_params

    def setup(self, params: dict[str, str | Tuple[slice]]):
        https_url = params["https_url"]
        object_name = params["object_name"]

        self.nwbfile, self.io, _ = read_hdf5_pynwb_ros3(https_url=https_url)
        self.neurodata_object = get_object_by_name(nwbfile=self.nwbfile, object_name=object_name)
        self.data_to_slice = self.neurodata_object.data


class HDF5PyNWBROS3PreloadedContinuousSliceBenchmark(ContinuousSliceBenchmark):
    """
    Time the read of a continuous data slice from remote HDF5 NWB files using pynwb and the ROS3 driver with preloaded
    data.
    """

    params = hdf5_params

    def setup(self, params: dict[str, str | Tuple[slice]]):
        https_url = params["https_url"]
        object_name = params["object_name"]
        slice_range = params["slice_range"]

        self.nwbfile, self.io, _ = read_hdf5_pynwb_ros3(https_url=https_url)
        self.neurodata_object = get_object_by_name(nwbfile=self.nwbfile, object_name=object_name)
        self.data_to_slice = self.neurodata_object.data
        self._temp = self.data_to_slice[slice_range]


class LindiLocalJSONContinuousSliceBenchmark(ContinuousSliceBenchmark):
    """
    Time the read of a continuous data slice from remote HDF5 NWB files by reading the local LINDI JSON files with
    lindi and pynwb.

    This downloads the remote LINDI JSON file during setup if it does not already exist in the persistent download
    directory.
    """

    params = lindi_remote_rfs_params

    def setup(self, params: dict[str, str | Tuple[slice]]):
        https_url = params["https_url"]
        object_name = params["object_name"]

        self.nwbfile, self.io, self.client = download_read_hdf5_pynwb_lindi(https_url=https_url)
        self.neurodata_object = get_object_by_name(nwbfile=self.nwbfile, object_name=object_name)
        self.data_to_slice = self.neurodata_object.data


class LindiLocalJSONPreloadedContinuousSliceBenchmark(ContinuousSliceBenchmark):
    """
    Time the read of a continuous data slice from remote HDF5 NWB files by reading the local LINDI JSON files with
    lindi and pynwb after preloading the data into any caches.

    This downloads the remote LINDI JSON file during setup if it does not already exist in the persistent download
    directory.
    """

    params = lindi_remote_rfs_params

    def setup(self, params: dict[str, str | Tuple[slice]]):
        https_url = params["https_url"]
        object_name = params["object_name"]
        slice_range = params["slice_range"]

        self.nwbfile, self.io, self.client = download_read_hdf5_pynwb_lindi(https_url=https_url)
        self.neurodata_object = get_object_by_name(nwbfile=self.nwbfile, object_name=object_name)
        self.data_to_slice = self.neurodata_object.data
        self._temp = self.data_to_slice[slice_range]


class ZarrPyNWBS3ContinuousSliceBenchmark(ContinuousSliceBenchmark):
    """
    Time the read of a continuous data slice from remote Zarr NWB files using pynwb with S3.
    """

    params = zarr_params

    def setup(self, params: dict[str, str | Tuple[slice]]):
        https_url = params["https_url"]
        object_name = params["object_name"]

        self.nwbfile, self.io = read_zarr_pynwb_s3(https_url=https_url, mode="r")
        self.neurodata_object = get_object_by_name(nwbfile=self.nwbfile, object_name=object_name)
        self.data_to_slice = self.neurodata_object.data


class ZarrPyNWBS3PreloadedContinuousSliceBenchmark(ContinuousSliceBenchmark):
    """
    Time the read of a continuous data slice from remote Zarr NWB files using pynwb with S3 with preloaded data.
    """

    params = zarr_params

    def setup(self, params: dict[str, str | Tuple[slice]]):
        https_url = params["https_url"]
        object_name = params["object_name"]
        slice_range = params["slice_range"]

        self.nwbfile, self.io = read_zarr_pynwb_s3(https_url=https_url, mode="r")
        self.neurodata_object = get_object_by_name(nwbfile=self.nwbfile, object_name=object_name)
        self.data_to_slice = self.neurodata_object.data
        self._temp = self.data_to_slice[slice_range]


class ZarrPyNWBS3ForceNoConsolidatedContinuousSliceBenchmark(ContinuousSliceBenchmark):
    """
    Time the read of a continuous data slice from remote Zarr NWB files using pynwb with S3 and without using
    consolidated metadata.
    """

    params = zarr_params

    def setup(self, params: dict[str, str | Tuple[slice]]):
        https_url = params["https_url"]
        object_name = params["object_name"]

        self.nwbfile, self.io = read_zarr_pynwb_s3(https_url=https_url, mode="r-")
        self.neurodata_object = get_object_by_name(nwbfile=self.nwbfile, object_name=object_name)
        self.data_to_slice = self.neurodata_object.data


class ZarrPyNWBS3ForceNoConsolidatedPreloadedContinuousSliceBenchmark(ContinuousSliceBenchmark):
    """
    Time the read of a continuous data slice from remote Zarr NWB files using pynwb with S3 and without using
    consolidated metadata with preloaded data.
    """

    params = zarr_params

    def setup(self, params: dict[str, str | Tuple[slice]]):
        https_url = params["https_url"]
        object_name = params["object_name"]
        slice_range = params["slice_range"]

        self.nwbfile, self.io = read_zarr_pynwb_s3(https_url=https_url, mode="r-")
        self.neurodata_object = get_object_by_name(nwbfile=self.nwbfile, object_name=object_name)
        self.data_to_slice = self.neurodata_object.data
        self._temp = self.data_to_slice[slice_range]
