"""Basic benchmarks for timing streaming access to slices of data stored in NWB files."""

from abc import ABC, abstractmethod
from typing import Tuple

from asv_runner.benchmarks.mark import SkipNotImplemented
from pynwb import NWBHDF5IO


from nwb_benchmarks.core import (
    BaseBenchmark,
    get_asset_path_from_url,
    get_object_by_name,
    read_zarr_pynwb_https,
)
from nwb_benchmarks.setup import get_persistent_download_directory

from .params import (
    hdf5_no_redirect_read_slice_params,
    zarr_no_redirect_read_slice_params,
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
        - Load the local NWB file associated with the given https_url
        - Get the neurodata object by name
        - Set self.data_to_slice to the data that will be sliced
        """
        self.download_dir = get_persistent_download_directory()
        self.file_name = get_asset_path_from_url(https_url=params["https_url"])
        self.file_path = self.download_dir / self.file_name
        if not self.file_path.exists():
            raise SkipNotImplemented(f"Expected file {self.file_path} to exist for local file reading benchmark.")

    def teardown(self, params: dict[str, str | Tuple[slice]]):
        if hasattr(self, "io"):
            self.io.close()
        if hasattr(self, "file"):
            self.file.close()

    def time_slice(self, params: dict[str, str | Tuple[slice]]):
        """Slice a range of a dataset in a remote NWB file."""
        slice_range = params["slice_range"]
        self._temp = self.data_to_slice[slice_range]


class HDF5PyNWBLocalContinuousSliceBenchmark(ContinuousSliceBenchmark):
    """
    Time the read of a continuous data slice from local HDF5 NWB files using pynwb.
    """

    params = hdf5_no_redirect_read_slice_params

    def setup(self, params: dict[str, str | Tuple[slice]]):
        super().setup(params=params)
        object_name = params["object_name"]

        self.io = NWBHDF5IO(str(self.file_path), mode="r")
        self.nwbfile = self.io.read()
        self.neurodata_object = get_object_by_name(nwbfile=self.nwbfile, object_name=object_name)
        self.data_to_slice = self.neurodata_object.data


class HDF5PyNWBLocalPreloadedContinuousSliceBenchmark(ContinuousSliceBenchmark):
    """
    Time the read of a continuous data slice from local HDF5 NWB files using pynwb.
    """

    params = hdf5_no_redirect_read_slice_params

    def setup(self, params: dict[str, str | Tuple[slice]]):
        super().setup(params=params)
        object_name = params["object_name"]
        slice_range = params["slice_range"]

        self.io = NWBHDF5IO(str(self.file_path), mode="r")
        self.nwbfile = self.io.read()
        self.neurodata_object = get_object_by_name(nwbfile=self.nwbfile, object_name=object_name)
        self.data_to_slice = self.neurodata_object.data
        self._temp = self.data_to_slice[slice_range]


class ZarrPyNWBLocalContinuousSliceBenchmark(ContinuousSliceBenchmark):
    """
    Time the read of a continuous data slice from local Zarr NWB files using pynwb.
    """

    params = zarr_no_redirect_read_slice_params

    def setup(self, params: dict[str, str | Tuple[slice]]):
        super().setup(params=params)
        object_name = params["object_name"]

        self.nwbfile, self.io = read_zarr_pynwb_https(https_url=self.file_path, mode="r")
        self.neurodata_object = get_object_by_name(nwbfile=self.nwbfile, object_name=object_name)
        self.data_to_slice = self.neurodata_object.data


class ZarrPyNWBLocalPreloadedContinuousSliceBenchmark(ContinuousSliceBenchmark):
    """
    Time the read of a continuous data slice from local Zarr NWB files using pynwb.
    """

    params = zarr_no_redirect_read_slice_params

    def setup(self, params: dict[str, str | Tuple[slice]]):
        super().setup(params=params)
        object_name = params["object_name"]
        slice_range = params["slice_range"]

        self.nwbfile, self.io = read_zarr_pynwb_https(https_url=self.file_path, mode="r")
        self.neurodata_object = get_object_by_name(nwbfile=self.nwbfile, object_name=object_name)
        self.data_to_slice = self.neurodata_object.data
        self._temp = self.data_to_slice[slice_range]
