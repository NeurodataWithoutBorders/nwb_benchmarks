"""Basic benchmarks for timing reading of local NWB files and their contents."""

import h5py
from asv_runner.benchmarks.mark import SkipNotImplemented
from pynwb import NWBHDF5IO

from nwb_benchmarks.core import (
    BaseBenchmark,
    get_asset_path_from_url,
    read_zarr_pynwb_https,
    read_zarr_zarrpython_https,
)
from nwb_benchmarks.setup import get_persistent_download_directory

from .params import (
    hdf5_no_redirect_download_params,
    zarr_no_redirect_download_params,
)


class HDF5FileReadBenchmark(BaseBenchmark):
    """
    Time the read of local HDF5 NWB files using h5py/pynwb. The HDF5 files should be downloaded into the persistent
    download directory before running this benchmark, ideally through the time_download benchmark.

    If the file does not exist in the persistent download directory, the benchmark is skipped.

    Note: in all cases, store the in-memory objects to avoid timing garbage collection steps.
    """

    params = hdf5_no_redirect_download_params

    def setup(self, params: dict[str, str]):
        self.download_dir = get_persistent_download_directory()
        self.file_name = get_asset_path_from_url(https_url=params["https_url"])
        self.file_path = self.download_dir / self.file_name
        if not self.file_path.exists():
            raise SkipNotImplemented(f"Expected file {self.file_path} to exist for local file reading benchmark.")

    def teardown(self, params: dict[str, str]):
        if hasattr(self, "file"):
            self.file.close()
        if hasattr(self, "io"):
            self.io.close()

    def time_read_local_h5py(self, params: dict[str, str]):
        """Read local NWB file using h5py."""
        self.file = h5py.File(str(self.file_path), mode="r")

    def time_read_local_pynwb(self, params: dict[str, str]):
        """Read local NWB file using pynwb."""
        self.io = NWBHDF5IO(str(self.file_path), mode="r")
        self.nwbfile = self.io.read()


class ZarrFileReadBenchmark(BaseBenchmark):
    """
    Time the read of remote Zarr files with Zarr-Python only or PyNWB. The Zarr files should be downloaded into the
    persistent download directory before running this benchmark, ideally through the time_download benchmark.

    If the file does not exist in the persistent download directory, the benchmark is skipped.

    Note: in all cases, store the in-memory objects to avoid timing garbage collection steps.
    """

    params = zarr_no_redirect_download_params

    def setup(self, params: dict[str, str]):
        self.download_dir = get_persistent_download_directory()
        self.file_name = get_asset_path_from_url(https_url=params["https_url"])
        self.file_path = self.download_dir / self.file_name
        if not self.file_path.exists():
            raise SkipNotImplemented(f"Expected file {self.file_path} to exist for local file reading benchmark.")

    def teardown(self, params: dict[str, str]):
        if hasattr(self, "io"):
            self.io.close()

    def time_read_local_zarrpython(self, params: dict[str, str]):
        """Read a Zarr file using Zarr-Python with HTTPS and consolidated metadata (if available)."""
        self.zarr_file = read_zarr_zarrpython_https(https_url=self.file_path, open_without_consolidated_metadata=False)

    def time_read_local_zarrpython_force_no_consolidated(self, params: dict[str, str]):
        """Read a Zarr file using Zarr-Python with HTTPS and without using consolidated metadata."""
        self.zarr_file = read_zarr_zarrpython_https(https_url=self.file_path, open_without_consolidated_metadata=True)

    def time_read_local_pynwb(self, params: dict[str, str]):
        """Read a Zarr NWB file using pynwb with HTTPS and consolidated metadata (if available)."""
        self.nwbfile, self.io = read_zarr_pynwb_https(https_url=self.file_path, mode="r")

    def time_read_local_pynwb_force_no_consolidated(self, params: dict[str, str]):
        """Read a Zarr NWB file using pynwb with HTTPS and without using consolidated metadata."""
        self.nwbfile, self.io = read_zarr_pynwb_https(https_url=self.file_path, mode="r-")
