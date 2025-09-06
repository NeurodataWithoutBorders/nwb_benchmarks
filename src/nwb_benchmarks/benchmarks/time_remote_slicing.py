"""Basic benchmarks for timing streaming access to slices of data stored in NWB files."""

import pathlib
import shutil
from abc import ABC, abstractmethod
from typing import Tuple

from nwb_benchmarks.core import (
    BaseBenchmark,
    get_object_by_name,
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
)

from .params_remote_slicing import (
    lindi_remote_rfs_parameter_cases,
    parameter_cases,
    zarr_parameter_cases,
)


class ContinuousSliceBenchmark(BaseBenchmark, ABC):
    """
    Base class for benchmarking slice access to NWB data.

    Note: in all cases, store the in-memory objects to avoid timing garbage collection steps.
    """

    @abstractmethod
    def setup(self, https_url: str, object_name: str, slice_range: Tuple[slice]):
        """Set up the benchmark by loading the NWB file and preparing data for slicing.

        This method must be implemented by subclasses to define how to:
        - Load the NWB file from the given https_url
        - Get the neurodata object by name
        - Set self.data_to_slice to the data that will be sliced
        """
        pass

    def teardown(self, https_url: str, object_name: str, slice_range: Tuple[slice]):
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

    def time_slice(self, https_url: str, object_name: str, slice_range: Tuple[slice]):
        """Slice a range of a dataset in a remote NWB file."""
        self._temp = self.data_to_slice[slice_range]


class HDF5PyNWBFsspecHttpsNoCacheContinuousSliceBenchmark(ContinuousSliceBenchmark):
    """
    Time the read of a continuous data slice from remote HDF5 NWB files using pynwb and fsspec with HTTPS without
    cache.
    """

    parameter_cases = parameter_cases

    def setup(self, https_url: str, object_name: str, slice_range: Tuple[slice]):
        self.nwbfile, self.io, self.file, self.bytestream = read_hdf5_pynwb_fsspec_https_no_cache(https_url=https_url)
        self.neurodata_object = get_object_by_name(nwbfile=self.nwbfile, object_name=object_name)
        self.data_to_slice = self.neurodata_object.data


class HDF5PyNWBFsspecHttpsWithCacheContinuousSliceBenchmark(ContinuousSliceBenchmark):
    """
    Time the read of a continuous data slice from remote HDF5 NWB files using pynwb and fsspec with HTTPS with cache.
    """

    parameter_cases = parameter_cases

    def setup(self, https_url: str, object_name: str, slice_range: Tuple[slice]):
        self.nwbfile, self.io, self.file, self.bytestream, self.tmpdir = read_hdf5_pynwb_fsspec_https_with_cache(
            https_url=https_url
        )
        self.neurodata_object = get_object_by_name(nwbfile=self.nwbfile, object_name=object_name)
        self.data_to_slice = self.neurodata_object.data


class HDF5PyNWBFsspecS3NoCacheContinuousSliceBenchmark(ContinuousSliceBenchmark):
    """
    Time the read of a continuous data slice from remote HDF5 NWB files using pynwb and fsspec with S3 without cache.
    """

    parameter_cases = parameter_cases

    def setup(self, https_url: str, object_name: str, slice_range: Tuple[slice]):
        self.nwbfile, self.io, self.file, self.bytestream = read_hdf5_pynwb_fsspec_s3_no_cache(https_url=https_url)
        self.neurodata_object = get_object_by_name(nwbfile=self.nwbfile, object_name=object_name)
        self.data_to_slice = self.neurodata_object.data


class HDF5PyNWBFsspecS3WithCacheContinuousSliceBenchmark(ContinuousSliceBenchmark):
    """
    Time the read of a continuous data slice from remote HDF5 NWB files using pynwb and fsspec with S3 with cache.
    """

    parameter_cases = parameter_cases

    def setup(self, https_url: str, object_name: str, slice_range: Tuple[slice]):
        self.nwbfile, self.io, self.file, self.bytestream, self.tmpdir = read_hdf5_pynwb_fsspec_s3_with_cache(
            https_url=https_url
        )
        self.neurodata_object = get_object_by_name(nwbfile=self.nwbfile, object_name=object_name)
        self.data_to_slice = self.neurodata_object.data


class HDF5PyNWBRemfileNoCacheContinuousSliceBenchmark(ContinuousSliceBenchmark):
    """
    Time the read of a continuous data slice from remote HDF5 NWB files using pynwb and remfile without cache.
    """

    parameter_cases = parameter_cases

    def setup(self, https_url: str, object_name: str, slice_range: Tuple[slice]):
        self.nwbfile, self.io, self.file, self.bytestream = read_hdf5_pynwb_remfile_no_cache(https_url=https_url)
        self.neurodata_object = get_object_by_name(nwbfile=self.nwbfile, object_name=object_name)
        self.data_to_slice = self.neurodata_object.data


class HDF5PyNWBRemfileWithCacheContinuousSliceBenchmark(ContinuousSliceBenchmark):
    """
    Time the read of a continuous data slice from remote HDF5 NWB files using pynwb and remfile with cache.
    """

    parameter_cases = parameter_cases

    def setup(self, https_url: str, object_name: str, slice_range: Tuple[slice]):
        self.nwbfile, self.io, self.file, self.bytestream, self.tmpdir = read_hdf5_pynwb_remfile_with_cache(
            https_url=https_url
        )
        self.neurodata_object = get_object_by_name(nwbfile=self.nwbfile, object_name=object_name)
        self.data_to_slice = self.neurodata_object.data


class HDF5PyNWBROS3ContinuousSliceBenchmark(ContinuousSliceBenchmark):
    """
    Time the read of a continuous data slice from remote HDF5 NWB files using pynwb and the ROS3 driver.
    """

    parameter_cases = parameter_cases

    def setup(self, https_url: str, object_name: str, slice_range: Tuple[slice]):
        self.nwbfile, self.io, _ = read_hdf5_pynwb_ros3(https_url=https_url)
        self.neurodata_object = get_object_by_name(nwbfile=self.nwbfile, object_name=object_name)
        self.data_to_slice = self.neurodata_object.data


class LindiLocalJSONContinuousSliceBenchmark(ContinuousSliceBenchmark):
    """
    Time the read of a continuous data slice from remote HDF5 NWB files by reading the local LINDI JSON files with
    lindi and pynwb.

    This downloads the already created remote LINDI JSON files during setup.

    This should be about the same as reading a data slice from an NWB file that is instantiated with a remote LINDI JSON
    file because, in that case, the first thing that LINDI does is download the remote file to a temporary directory.
    """

    parameter_cases = lindi_remote_rfs_parameter_cases

    def setup(self, https_url: str, object_name: str, slice_range: Tuple[slice]):
        self.nwbfile, self.io, self.client = read_hdf5_pynwb_lindi(rfs=https_url)
        self.neurodata_object = get_object_by_name(nwbfile=self.nwbfile, object_name=object_name)
        self.data_to_slice = self.neurodata_object.data


class ZarrPyNWBS3ContinuousSliceBenchmark(ContinuousSliceBenchmark):
    """
    Time the read of a continuous data slice from remote Zarr NWB files using pynwb with S3.
    """

    parameter_cases = zarr_parameter_cases

    def setup(self, https_url: str, object_name: str, slice_range: Tuple[slice]):
        self.nwbfile, self.io = read_zarr_pynwb_s3(https_url=https_url, mode="r")
        self.neurodata_object = get_object_by_name(nwbfile=self.nwbfile, object_name=object_name)
        self.data_to_slice = self.neurodata_object.data


class ZarrPyNWBS3ForceNoConsolidatedContinuousSliceBenchmark(ContinuousSliceBenchmark):
    """
    Time the read of a continuous data slice from remote Zarr NWB files using pynwb with S3 and without using
    consolidated metadata.
    """

    parameter_cases = zarr_parameter_cases

    def setup(self, https_url: str, object_name: str, slice_range: Tuple[slice]):
        self.nwbfile, self.io = read_zarr_pynwb_s3(https_url=https_url, mode="r-")
        self.neurodata_object = get_object_by_name(nwbfile=self.nwbfile, object_name=object_name)
        self.data_to_slice = self.neurodata_object.data
