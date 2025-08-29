"""Basic benchmarks for timing streaming access of NWB files and their contents."""

import os

from asv_runner.benchmarks.mark import skip_benchmark

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


class DirectFileReadBenchmark(BaseBenchmark):
    """
    Time the direct read of the HDF5-backend files with `h5py` using each streaming method.

    There is no formal parsing of the `pynwb.NWBFile` object.

    Note: in all cases, store the in-memory objects to avoid timing garbage collection steps.
    """

    rounds = 1
    repeat = 3
    parameter_cases = parameter_cases

    def teardown(self, https_url: str):
        # Not all tests in the class are using a temporary dir as cache. Clean up if it does.
        if hasattr(self, "tmpdir"):
            self.tmpdir.cleanup()

    def time_read_hdf5_fsspec_no_cache(self, https_url: str):
        self.file, self.bytestream = read_hdf5_fsspec_https_no_cache(https_url=https_url)

    def time_read_hdf5_fsspec_with_cache(self, https_url: str):
        self.file, self.bytestream, self.tmpdir = read_hdf5_fsspec_https_with_cache(https_url=https_url)

    def time_read_hdf5_remfile(self, https_url: str):
        self.file, self.bytestream = read_hdf5_remfile(https_url=https_url)

    def time_read_hdf5_remfile_with_cache(self, https_url: str):
        self.file, self.bytestream, self.tmpdir = read_hdf5_remfile_with_cache(https_url=https_url)

    def time_read_hdf5_ros3(self, https_url: str):
        self.file, _ = read_hdf5_ros3(https_url=https_url, retry=False)


class NWBFileReadBenchmark(BaseBenchmark):
    """
    Time the read of the HDF5-backend files with `pynwb` using each streaming method.

    Note: in all cases, store the in-memory objects to avoid timing garbage collection steps.
    """

    rounds = 1
    repeat = 3
    parameter_cases = parameter_cases

    def teardown(self, https_url: str):
        # Not all tests in the class are using a temporary dir as cache. Clean up if it does.
        if hasattr(self, "tmpdir"):
            self.tmpdir.cleanup()

    def time_read_hdf5_nwbfile_fsspec_no_cache(self, https_url: str):
        self.nwbfile, self.io, self.file, self.bytestream = read_hdf5_nwbfile_fsspec_https_no_cache(https_url=https_url)

    def time_read_hdf5_nwbfile_fsspec_with_cache(self, https_url: str):
        self.nwbfile, self.io, self.file, self.bytestream, self.tmpdir = read_hdf5_nwbfile_fsspec_https_with_cache(
            https_url=https_url
        )

    def time_read_hdf5_nwbfile_remfile(self, https_url: str):
        self.nwbfile, self.io, self.file, self.bytestream = read_hdf5_nwbfile_remfile(https_url=https_url)

    def time_read_hdf5_nwbfile_remfile_with_cache(self, https_url: str):
        self.nwbfile, self.io, self.file, self.bytestream, self.tmpdir = read_hdf5_nwbfile_remfile_with_cache(
            https_url=https_url
        )

    def time_read_hdf5_nwbfile_ros3(self, https_url: str):
        self.nwbfile, self.io, _ = read_hdf5_nwbfile_ros3(https_url=https_url, retry=False)


class LindiFileReadLocalReferenceFileSystemBenchmark(BaseBenchmark):
    """
    Time the read of the Lindi HDF5 files with `pynwb` assuming that a local copy of the lindi
    filesystem is available locally.
    """

    rounds = 1
    repeat = 3
    parameter_cases = lindi_remote_rfs_parameter_cases

    def setup(self, https_url: str):
        """Create the local JSON LINDI reference filesystem if it does not exist"""
        self.lindi_file = os.path.basename(https_url) + ".lindi.json"
        self.teardown(https_url=https_url)
        download_file(https_url=https_url, local_path=self.lindi_file)

    def teardown(self, https_url: str):
        """Delete the local LINDI JSON file if it exists."""
        if os.path.exists(self.lindi_file):
            os.remove(self.lindi_file)

    def time_read_lindi_nwbfile(self, https_url: str):
        """Read the NWB file with pynwb using LINDI with the local reference filesystem JSON"""
        self.nwbfile, self.io, self.client = read_hdf5_nwbfile_lindi(rfs=self.lindi_file)

    def time_read_lindi_jsonrfs(self, https_url: str):
        """Read the NWB file with LINDI directly using the local reference filesystem JSON"""
        self.client = read_hdf5_lindi(rfs=self.lindi_file)


class NWBLindiFileCreateLocalReferenceFileSystemBenchmark(BaseBenchmark):
    """
    Time the creation of a local Lindi JSON reference filesystem for a remote NWB file
    as well as reading the NWB file with PyNWB when the local reference filesystem does not
    yet exist.
    """

    rounds = 1
    repeat = 3
    parameter_cases = lindi_hdf5_parameter_cases

    def setup(self, https_url: str):
        """Clear the LINDI JSON if it still exists"""
        self.lindi_file = os.path.basename(https_url) + ".lindi.json"
        self.teardown(https_url=https_url)

    def teardown(self, https_url: str):
        """Clear the LINDI JSON if it still exists"""
        if os.path.exists(self.lindi_file):
            os.remove(self.lindi_file)

    # TODO This benchmark takes a long time to index all of the chunks for these files! Do not run until ready
    @skip_benchmark
    def time_create_lindi_reference_file_system(self, https_url: str):
        """Create a local Lindi JSON reference filesystem from a remote HDF5 file"""
        create_lindi_reference_file_system(https_url=https_url, outfile_path=self.lindi_file)

    # TODO This benchmark takes a long time to index all of the chunks for these files! Do not run until ready
    @skip_benchmark
    def time_create_lindi_reference_file_system_and_read_nwbfile(self, https_url: str):
        """
        Create a local Lindi JSON reference filesystem from a remote HDF5 file
        and then read the NWB file with PyNWB using LINDI with the local JSON
        """
        create_lindi_reference_file_system(https_url=https_url, outfile_path=self.lindi_file)
        self.nwbfile, self.io, self.client = read_hdf5_nwbfile_lindi(rfs=self.lindi_file)

    # TODO This benchmark takes a long time to index all of the chunks for these files! Do not run until ready
    @skip_benchmark
    def time_create_lindi_reference_file_system_and_read_jsonrfs(self, https_url: str):
        """
        Create a local Lindi JSON reference filesystem  from a remote HDF5 file and
        then read the HDF5 file with LINDI using the local JSON.
        """
        create_lindi_reference_file_system(https_url=https_url, outfile_path=self.lindi_file)
        self.client = read_hdf5_lindi(rfs=self.lindi_file)


class NWBLindiFileReadRemoteReferenceFileSystemBenchmark(BaseBenchmark):
    """
    Time the read of a remote NWB file with pynwb using lindi with a remote JSON reference
    filesystem available.
    """

    rounds = 1
    repeat = 3
    parameter_cases = lindi_remote_rfs_parameter_cases

    def time_read_lindi_nwbfile(self, https_url: str):
        """Read a remote NWB file with PyNWB using the remote LINDI JSON reference filesystem"""
        self.nwbfile, self.io, self.client = read_hdf5_nwbfile_lindi(rfs=https_url)

    def time_read_lindi_jsonrfs(self, https_url: str):
        """Read a remote HDF5 file with LINDI using the remote LINDI JSON reference filesystem"""
        self.client = read_hdf5_lindi(rfs=https_url)


class DirectZarrFileReadBenchmark(BaseBenchmark):
    """
    Time the read of with Zarr only (not using PyNWB)

    Note: in all cases, store the in-memory objects to avoid timing garbage collection steps.
    """

    rounds = 1
    repeat = 3
    parameter_cases = zarr_parameter_cases

    def time_read_zarr(self, https_url: str):
        """Read a Zarr file using consolidated metadata (if available)"""
        self.zarr_file = read_zarr_https_protocol(https_url=https_url, open_without_consolidated_metadata=False)

    def time_read_zarr_force_no_consolidated(self, https_url: str):
        """Read a Zarr file without using consolidated metadata"""
        self.zarr_file = read_zarr_https_protocol(https_url=https_url, open_without_consolidated_metadata=True)


class NWBZarrFileReadBenchmark(BaseBenchmark):
    """
    Time the read of the Zarr-backend files with `pynwb` using each streaming method.

    Note: in all cases, store the in-memory objects to avoid timing garbage collection steps.
    """

    rounds = 1
    repeat = 3
    parameter_cases = zarr_parameter_cases

    def time_read_zarr_nwbfile(self, https_url: str):
        """Read NWB file with PyNWB using Zarr with consolidated metadata. (if available)"""
        self.nwbfile, self.io = read_zarr_nwbfile_https_protocol(https_url=https_url, mode="r")

    def time_read_zarr_nwbfile_force_no_consolidated(self, https_url: str):
        """Read NWB file with PyNWB using Zarr without consolidated metadata."""
        self.nwbfile, self.io = read_zarr_nwbfile_https_protocol(https_url=https_url, mode="r-")
