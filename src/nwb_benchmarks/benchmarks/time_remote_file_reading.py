"""Basic benchmarks for timing streaming access of NWB files and their contents."""

import os
import pathlib
import shutil

from asv_runner.benchmarks.mark import skip_benchmark

from nwb_benchmarks.core import (
    BaseBenchmark,
    create_lindi_reference_file_system,
    download_file,
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
    Time the read of remote HDF5 files using h5py with each streaming method.

    There is no formal parsing of the `pynwb.NWBFile` object.

    Note: in all cases, store the in-memory objects to avoid timing garbage collection steps.
    """

    parameter_cases = parameter_cases

    def teardown(self, https_url: str):
        # Not all tests in the class are using a temporary dir as cache. Clean up if it does.
        if hasattr(self, "tmpdir"):
            shutil.rmtree(path=self.tmpdir.name, ignore_errors=True)
            self.tmpdir.cleanup()

    def time_read_hdf5_h5py_fsspec_https_no_cache(self, https_url: str):
        """Read a remote HDF5 file using h5py and fsspec with HTTPS without cache."""
        self.file, self.bytestream = read_hdf5_h5py_fsspec_https_no_cache(https_url=https_url)

    def time_read_hdf5_h5py_fsspec_https_with_cache(self, https_url: str):
        """Read a remote HDF5 file using h5py and fsspec with HTTPS with cache."""
        self.file, self.bytestream, self.tmpdir = read_hdf5_h5py_fsspec_https_with_cache(https_url=https_url)

    def time_read_hdf5_h5py_fsspec_s3_no_cache(self, https_url: str):
        """Read a remote HDF5 file using h5py and fsspec with S3 without cache."""
        self.file, self.bytestream = read_hdf5_h5py_fsspec_s3_no_cache(https_url=https_url)

    def time_read_hdf5_h5py_fsspec_s3_with_cache(self, https_url: str):
        """Read a remote HDF5 file using h5py and fsspec with S3 with cache."""
        self.file, self.bytestream, self.tmpdir = read_hdf5_h5py_fsspec_s3_with_cache(https_url=https_url)

    def time_read_hdf5_h5py_remfile_no_cache(self, https_url: str):
        """Read a remote HDF5 file using h5py and remfile without cache."""
        self.file, self.bytestream = read_hdf5_h5py_remfile_no_cache(https_url=https_url)

    def time_read_hdf5_h5py_remfile_with_cache(self, https_url: str):
        """Read a remote HDF5 file using h5py and remfile with cache."""
        self.file, self.bytestream, self.tmpdir = read_hdf5_h5py_remfile_with_cache(https_url=https_url)

    def time_read_hdf5_h5py_ros3(self, https_url: str):
        """Read a remote HDF5 file using h5py and the ROS3 HDF5 driver."""
        self.file, _ = read_hdf5_h5py_ros3(https_url=https_url)


class HDF5PyNWBFileReadBenchmark(BaseBenchmark):
    """
    Time the read of remote HDF5 NWB files using pynwb with each streaming method.

    Note: in all cases, store the in-memory objects to avoid timing garbage collection steps.
    """

    parameter_cases = parameter_cases

    def teardown(self, https_url: str):
        # Not all tests in the class are using a temporary dir as cache. Clean up if it does.
        if hasattr(self, "tmpdir"):
            shutil.rmtree(path=self.tmpdir.name, ignore_errors=True)
            self.tmpdir.cleanup()

    def time_read_hdf5_pynwb_fsspec_https_no_cache(self, https_url: str):
        """Read a remote HDF5 NWB file using pynwb and fsspec with HTTPS without cache."""
        self.nwbfile, self.io, self.file, self.bytestream = read_hdf5_pynwb_fsspec_https_no_cache(https_url=https_url)

    def time_read_hdf5_pynwb_fsspec_https_with_cache(self, https_url: str):
        """Read a remote HDF5 NWB file using pynwb and fsspec with HTTPS with cache."""
        self.nwbfile, self.io, self.file, self.bytestream, self.tmpdir = read_hdf5_pynwb_fsspec_https_with_cache(
            https_url=https_url
        )

    def time_read_hdf5_pynwb_fsspec_s3_no_cache(self, https_url: str):
        """Read a remote HDF5 NWB file using pynwb and fsspec with S3 without cache."""
        self.nwbfile, self.io, self.file, self.bytestream = read_hdf5_pynwb_fsspec_s3_no_cache(https_url=https_url)

    def time_read_hdf5_pynwb_fsspec_s3_with_cache(self, https_url: str):
        """Read a remote HDF5 NWB file using pynwb and fsspec with S3 with cache."""
        self.nwbfile, self.io, self.file, self.bytestream, self.tmpdir = read_hdf5_pynwb_fsspec_s3_with_cache(
            https_url=https_url
        )

    def time_read_hdf5_pynwb_remfile_no_cache(self, https_url: str):
        """Read a remote HDF5 NWB file using pynwb and remfile without cache."""
        self.nwbfile, self.io, self.file, self.bytestream = read_hdf5_pynwb_remfile_no_cache(https_url=https_url)

    def time_read_hdf5_pynwb_remfile_with_cache(self, https_url: str):
        """Read a remote HDF5 NWB file using pynwb and remfile with cache."""
        self.nwbfile, self.io, self.file, self.bytestream, self.tmpdir = read_hdf5_pynwb_remfile_with_cache(
            https_url=https_url
        )

    def time_read_hdf5_pynwb_ros3(self, https_url: str):
        """Read a remote HDF5 NWB file using pynwb and the ROS3 HDF5 driver."""
        self.nwbfile, self.io, _ = read_hdf5_pynwb_ros3(https_url=https_url)


class LindiCreateLocalJSONFileBenchmark(BaseBenchmark):
    """
    Time the read of remote HDF5 files and the creation of a LINDI JSON file using lindi.
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
    def time_read_create_lindi_json(self, https_url: str):
        """Read a remote HDF5 file to create a LINDI JSON file."""
        create_lindi_reference_file_system(https_url=https_url, outfile_path=self.lindi_file)


class LindiLocalJSONFileReadBenchmark(BaseBenchmark):
    """
    Time the read of remote HDF5 NWB files by reading the local LINDI JSON files using lindi and h5py or pynwb.

    This downloads the already created remote LINDI JSON files during setup.

    Note: in all cases, store the in-memory objects to avoid timing garbage collection steps.
    """

    parameter_cases = lindi_remote_rfs_parameter_cases

    def setup(self, https_url: str):
        """Download the LINDI JSON file."""
        self.lindi_file = os.path.basename(https_url) + ".lindi.json"
        self.teardown(https_url=https_url)
        download_file(url=https_url, local_path=self.lindi_file)

    def teardown(self, https_url: str):
        """Delete the LINDI JSON file if it exists."""
        if os.path.exists(self.lindi_file):
            os.remove(self.lindi_file)

    def time_read_lindi_h5py(self, https_url: str):
        """Read a remote HDF5 file with h5py using lindi with the local LINDI JSON file."""
        self.client = read_hdf5_h5py_lindi(rfs=self.lindi_file)

    def time_read_lindi_pynwb(self, https_url: str):
        """Read a remote HDF5 NWB file with pynwb using lindi with the local LINDI JSON file."""
        self.nwbfile, self.io, self.client = read_hdf5_pynwb_lindi(rfs=self.lindi_file)


class ZarrZarrPythonFileReadBenchmark(BaseBenchmark):
    """
    Time the read of remote Zarr files with Zarr-Python only (not using PyNWB)

    Note: in all cases, store the in-memory objects to avoid timing garbage collection steps.
    """

    parameter_cases = zarr_parameter_cases

    def time_read_zarr_https(self, https_url: str):
        """Read a Zarr file using Zarr-Python with HTTPS and consolidated metadata (if available)."""
        self.zarr_file = read_zarr_zarrpython_https(https_url=https_url, open_without_consolidated_metadata=False)

    def time_read_zarr_https_force_no_consolidated(self, https_url: str):
        """Read a Zarr file using Zarr-Python with HTTPS and without using consolidated metadata."""
        self.zarr_file = read_zarr_zarrpython_https(https_url=https_url, open_without_consolidated_metadata=True)

    def time_read_zarr_s3(self, https_url: str):
        """Read a Zarr file using Zarr-Python with S3 and consolidated metadata (if available)."""
        self.zarr_file = read_zarr_zarrpython_s3(https_url=https_url, open_without_consolidated_metadata=False)

    def time_read_zarr_s3_force_no_consolidated(self, https_url: str):
        """Read a Zarr file using Zarr-Python with S3 and without using consolidated metadata."""
        self.zarr_file = read_zarr_zarrpython_s3(https_url=https_url, open_without_consolidated_metadata=True)


class ZarrPyNWBFileReadBenchmark(BaseBenchmark):
    """
    Time the read of remote Zarr NWB files with pynwb.

    Note: in all cases, store the in-memory objects to avoid timing garbage collection steps.
    """

    parameter_cases = zarr_parameter_cases

    def time_read_zarr_pynwb_https(self, https_url: str):
        """Read a Zarr NWB file using pynwb with HTTPS and consolidated metadata (if available)."""
        self.nwbfile, self.io = read_zarr_pynwb_https(https_url=https_url, mode="r")

    def time_read_zarr_pynwb_https_force_no_consolidated(self, https_url: str):
        """Read a Zarr NWB file using pynwb with HTTPS and without consolidated metadata."""
        self.nwbfile, self.io = read_zarr_pynwb_https(https_url=https_url, mode="r-")

    def time_read_zarr_pynwb_s3(self, https_url: str):
        """Read a Zarr NWB file using pynwb with S3 and consolidated metadata (if available)."""
        self.nwbfile, self.io = read_zarr_pynwb_s3(https_url=https_url, mode="r")

    def time_read_zarr_pynwb_s3_force_no_consolidated(self, https_url: str):
        """Read a Zarr NWB file using pynwb using S3 and without consolidated metadata."""
        self.nwbfile, self.io = read_zarr_pynwb_s3(https_url=https_url, mode="r-")
