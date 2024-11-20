"""Basic benchmarks for timing streaming access of NWB files and their contents."""

import os

from nwb_benchmarks.core import (
    BaseBenchmark,
    create_lindi_reference_file_system,
    get_s3_url,
    read_hdf5_fsspec_no_cache,
    read_hdf5_fsspec_with_cache,
    read_hdf5_lindi,
    read_hdf5_nwbfile_fsspec_no_cache,
    read_hdf5_nwbfile_fsspec_with_cache,
    read_hdf5_nwbfile_lindi,
    read_hdf5_nwbfile_remfile,
    read_hdf5_nwbfile_remfile_with_cache,
    read_hdf5_nwbfile_ros3,
    read_hdf5_remfile,
    read_hdf5_remfile_with_cache,
    read_hdf5_ros3,
    read_zarr,
    read_zarr_nwbfile,
)

parameter_cases = dict(
    ICEphysTestCase=dict(
        s3_url=get_s3_url(
            dandiset_id="000717",
            dandi_path="sub-1214579789_ses-1214621812_icephys/sub-1214579789_ses-1214621812_icephys.nwb",
        ),
    ),
    EPhysTestCase=dict(
        s3_url=get_s3_url(
            dandiset_id="000717",
            dandi_path="sub-npI3_ses-20190421_behavior+ecephys/sub-npI3_ses-20190421_behavior+ecephys.nwb",
        ),
    ),
    OPhysTestCase=dict(
        s3_url=get_s3_url(
            dandiset_id="000717",
            dandi_path="sub-R6_ses-20200206T210000_behavior+ophys/sub-R6_ses-20200206T210000_behavior+ophys.nwb",
        ),
    ),
)


# Parameters for LINDI when HDF5 files are remote without using an existing LINDI JSON reference file system on
# the remote server (i.e., we create the LINDI JSON file for these in these tests)
lindi_hdf5_parameter_cases = parameter_cases

# Parameters for LINDI pointing to a remote LINDI reference file system JSON file. I.e., here we do not
# to create the JSON but can load it directly from the remote store
lindi_remote_rfs_parameter_cases = dict(
    EcephysTestCase=dict(
        s3_url=get_s3_url(
            dandiset_id="213889",
            dandi_path="sub-ecephys/c493119b-4b99-4b14-bc03-65bb28cfbd29.lindi.json",
        ),
    ),
    OphysTestCase=dict(
        s3_url=get_s3_url(
            dandiset_id="213889",
            dandi_path="sub-R6_ses-20200206T210000_behavior+ophys/sub-R6_ses-20200206T210000_behavior+ophys.lindi.json",
        ),
    ),
    IcephysTestCase=dict(
        s3_url=get_s3_url(
            dandiset_id="213889",
            dandi_path="sub-1214579789_ses-1214621812_icephys/sub-1214579789_ses-1214621812_icephys.lindi.json",
        ),
    ),
)

zarr_parameter_cases = dict(
    ZarrICEphysTestCase=dict(s3_url="s3://dandiarchive/zarr/2e8d0cb4-c5d4-4abc-88d8-2581c3cf7f5a/"),
    ZarrOPhysTestCase=dict(s3_url="s3://dandiarchive/zarr/c8c6b848-fbc6-4f58-85ff-e3f2618ee983/"),
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

    def teardown(self, s3_url: str):
        # Not all tests in the class are using a temporary dir as cache. Clean up if it does.
        if hasattr(self, "tmpdir"):
            self.tmpdir.cleanup()

    def time_read_hdf5_fsspec_no_cache(self, s3_url: str):
        self.file, self.bytestream = read_hdf5_fsspec_no_cache(s3_url=s3_url)

    def time_read_hdf5_fsspec_with_cache(self, s3_url: str):
        self.file, self.bytestream, self.tmpdir = read_hdf5_fsspec_with_cache(s3_url=s3_url)

    def time_read_hdf5_remfile(self, s3_url: str):
        self.file, self.bytestream = read_hdf5_remfile(s3_url=s3_url)

    def time_read_hdf5_remfile_with_cache(self, s3_url: str):
        self.file, self.bytestream, self.tmpdir = read_hdf5_remfile_with_cache(s3_url=s3_url)

    def time_read_hdf5_ros3(self, s3_url: str):
        self.file, _ = read_hdf5_ros3(s3_url=s3_url, retry=False)


class NWBFileReadBenchmark(BaseBenchmark):
    """
    Time the read of the HDF5-backend files with `pynwb` using each streaming method.

    Note: in all cases, store the in-memory objects to avoid timing garbage collection steps.
    """

    rounds = 1
    repeat = 3
    parameter_cases = parameter_cases

    def teardown(self, s3_url: str):
        # Not all tests in the class are using a temporary dir as cache. Clean up if it does.
        if hasattr(self, "tmpdir"):
            self.tmpdir.cleanup()

    def time_read_hdf5_nwbfile_fsspec_no_cache(self, s3_url: str):
        self.nwbfile, self.io, self.file, self.bytestream = read_hdf5_nwbfile_fsspec_no_cache(s3_url=s3_url)

    def time_read_hdf5_nwbfile_fsspec_with_cache(self, s3_url: str):
        self.nwbfile, self.io, self.file, self.bytestream, self.tmpdir = read_hdf5_nwbfile_fsspec_with_cache(
            s3_url=s3_url
        )

    def time_read_hdf5_nwbfile_remfile(self, s3_url: str):
        self.nwbfile, self.io, self.file, self.bytestream = read_hdf5_nwbfile_remfile(s3_url=s3_url)

    def time_read_hdf5_nwbfile_remfile_with_cache(self, s3_url: str):
        self.nwbfile, self.io, self.file, self.bytestream, self.tmpdir = read_hdf5_nwbfile_remfile_with_cache(
            s3_url=s3_url
        )

    def time_read_hdf5_nwbfile_ros3(self, s3_url: str):
        self.nwbfile, self.io, _ = read_hdf5_nwbfile_ros3(s3_url=s3_url, retry=False)


class LindiFileReadLocalReferenceFileSystemBenchmark(BaseBenchmark):
    """
    Time the read of the Lindi HDF5 files with `pynwb` assuming that a local copy of the lindi
    filesystem is available locally.
    """

    rounds = 1
    repeat = 3
    parameter_cases = lindi_hdf5_parameter_cases

    def setup(self, s3_url: str):
        """Create the local JSON LINDI reference filesystem if it does not exist"""
        self.lindi_file = os.path.basename(s3_url) + ".lindi.json"
        if not os.path.exists(self.lindi_file):
            create_lindi_reference_file_system(s3_url=s3_url, outfile_path=self.lindi_file)

    def time_read_lindi_nwbfile(self, s3_url: str):
        """Read the NWB file with pynwb using LINDI with the local reference filesystem JSON"""
        self.nwbfile, self.io, self.client = read_hdf5_nwbfile_lindi(rfs=self.lindi_file)

    def time_read_lindi_jsonrfs(self, s3_url: str):
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

    def setup(self, s3_url: str):
        """Clear the LINDI JSON if it still exists"""
        self.lindi_file = os.path.basename(s3_url) + ".lindi.json"
        self.teardown(s3_url=s3_url)

    def teardown(self, s3_url: str):
        """Clear the LINDI JSON if it still exists"""
        if os.path.exists(self.lindi_file):
            os.remove(self.lindi_file)

    def time_create_lindi_referernce_file_system(self, s3_url: str):
        """Create a local Lindi JSON reference filesystem from a remote HDF5 file"""
        create_lindi_reference_file_system(s3_url=s3_url, outfile_path=self.lindi_file)

    def time_create_lindi_referernce_file_system_and_read_nwbfile(self, s3_url: str):
        """
        Create a local Lindi JSON reference filesystem from a remote HDF5 file
        and then read the NWB file with PyNWB using LINDI with the local JSON
        """
        create_lindi_reference_file_system(s3_url=s3_url, outfile_path=self.lindi_file)
        self.nwbfile, self.io, self.client = read_hdf5_nwbfile_lindi(rfs=self.lindi_file)

    def time_create_lindi_referernce_file_system_and_read_jsonrfs(self, s3_url: str):
        """
        Create a local Lindi JSON reference filesystem  from a remote HDF5 file and
        then read the HDF5 file with LINDI using the local JSON.
        """
        create_lindi_reference_file_system(s3_url=s3_url, outfile_path=self.lindi_file)
        self.client = read_hdf5_lindi(rfs=self.lindi_file)


class NWBLindiFileReadRemoteReferenceFileSystemBenchmark(BaseBenchmark):
    """
    Time the read of a remote NWB file with pynwb using lindi with a remote JSON reference
    filesystem available.
    """

    rounds = 1
    repeat = 3
    parameter_cases = lindi_remote_rfs_parameter_cases

    def time_read_lindi_nwbfile(self, s3_url: str):
        """Read a remote NWB file with PyNWB using the remote LINDI JSON reference filesystem"""
        self.nwbfile, self.io, self.client = read_hdf5_nwbfile_lindi(rfs=s3_url)

    def time_read_lindi_jsonrfs(self, s3_url: str):
        """Read a remote HDF5 file with LINDI using the remote LINDI JSON reference filesystem"""
        self.client = read_hdf5_lindi(rfs=s3_url)


class DirectZarrFileReadBenchmark(BaseBenchmark):
    """
    Time the read of with Zarr only (not using PyNWB)

    Note: in all cases, store the in-memory objects to avoid timing garbage collection steps.
    """

    rounds = 1
    repeat = 3
    parameter_cases = zarr_parameter_cases

    def time_read_zarr(self, s3_url: str):
        """Read a Zarr file using consolidated metadata (if available)"""
        self.zarr_file = read_zarr(s3_url=s3_url, open_without_consolidated_metadata=False)

    def time_read_zarr_force_no_consolidated(self, s3_url: str):
        """Read a Zarr file without using consolidated metadata"""
        self.zarr_file = read_zarr(s3_url=s3_url, open_without_consolidated_metadata=True)


class NWBZarrFileReadBenchmark(BaseBenchmark):
    """
    Time the read of the Zarr-backend files with `pynwb` using each streaming method.

    Note: in all cases, store the in-memory objects to avoid timing garbage collection steps.
    """

    rounds = 1
    repeat = 3
    parameter_cases = zarr_parameter_cases

    def time_read_zarr_nwbfile(self, s3_url: str):
        """Read NWB file with PyNWB using Zarr with consolidated metadata. (if available)"""
        self.nwbfile, self.io = read_zarr_nwbfile(s3_url=s3_url, mode="r")

    def time_read_zarr_nwbfile_force_no_consolidated(self, s3_url: str):
        """Read NWB file with PyNWB using Zarr without consolidated metadata."""
        self.nwbfile, self.io = read_zarr_nwbfile(s3_url=s3_url, mode="r-")
