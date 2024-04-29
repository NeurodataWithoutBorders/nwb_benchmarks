"""Basic benchmarks for profiling network statistics for streaming access to NWB files and their contents."""

import os

from nwb_benchmarks import TSHARK_PATH
from nwb_benchmarks.core import (
    BaseBenchmark,
    create_lindi_reference_file_system,
    get_s3_url,
    network_activity_tracker,
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
    IBLTestCase1=dict(
        s3_url=get_s3_url(dandiset_id="000717", dandi_path="sub-mock/sub-mock_ses-ecephys1.nwb"),
    ),
    # IBLTestCase2 is not the best example for testing a theory about file read; should probably replace with simpler
    IBLTestCase2=dict(
        s3_url=get_s3_url(
            dandiset_id="000717",
            dandi_path="sub-IBL-ecephys/sub-IBL-ecephys_ses-3e7ae7c0_desc-18000000-frames-13653-by-384-chunking.nwb",
        ),
    ),
    ClassicRos3TestCase=dict(s3_url="https://dandiarchive.s3.amazonaws.com/ros3test.nwb"),
)


# Parameters for LINDI when HDF5 files are remote without using an existing LINDI JSON reference file system on
# the remote server (i.e., we create the LINDI JSON file for these in these tests)
lindi_hdf5_parameter_cases = parameter_cases

# Parameters for LINDI pointing to a remote LINDI reference file system JSON file. I.e., here we do not
# to create the JSON but can load it directly from the remote store
lindi_remote_rfs_parameter_cases = dict(
    # TODO: Just an example case for testing. Replace with real test case
    BaseExample=dict(
        s3_url="https://kerchunk.neurosift.org/dandi/dandisets/000939/assets/11f512ba-5bcf-4230-a8cb-dc8d36db38cb/zarr.json",
    ),
)


zarr_parameter_cases = dict(
    AIBSTestCase=dict(
        s3_url=(
            "s3://aind-open-data/ecephys_625749_2022-08-03_15-15-06_nwb_2023-05-16_16-34-55/"
            "ecephys_625749_2022-08-03_15-15-06_nwb/"
            "ecephys_625749_2022-08-03_15-15-06_experiment1_recording1.nwb.zarr/"
        ),
    ),
)


class FsspecNoCacheDirectFileReadBenchmark(BaseBenchmark):
    parameter_cases = parameter_cases

    def track_network_activity_during_read(self, s3_url: str):
        with network_activity_tracker(tshark_path=TSHARK_PATH) as network_tracker:
            self.file, self.bytestream = read_hdf5_fsspec_no_cache(s3_url=s3_url)
        return network_tracker.asv_network_statistics


class FsspecWithCacheDirectFileReadBenchmark(BaseBenchmark):
    parameter_cases = parameter_cases

    def teardown(self, s3_url: str):
        self.tmpdir.cleanup()

    def track_network_activity_during_read(self, s3_url: str):
        with network_activity_tracker(tshark_path=TSHARK_PATH) as network_tracker:
            self.file, self.bytestream, self.tmpdir = read_hdf5_fsspec_with_cache(s3_url=s3_url)
        return network_tracker.asv_network_statistics


class RemfileDirectFileReadBenchmark(BaseBenchmark):
    parameter_cases = parameter_cases

    def track_network_activity_during_read(self, s3_url: str):
        with network_activity_tracker(tshark_path=TSHARK_PATH) as network_tracker:
            self.file, self.bytestream = read_hdf5_remfile(s3_url=s3_url)
        return network_tracker.asv_network_statistics


class RemfileDirectFileReadBenchmarkWithCache(BaseBenchmark):
    parameter_cases = parameter_cases

    def teardown(self, s3_url: str):
        self.tmpdir.cleanup()

    def track_network_activity_during_read(self, s3_url: str):
        with network_activity_tracker(tshark_path=TSHARK_PATH) as network_tracker:
            self.file, self.bytestream, self.tmpdir = read_hdf5_remfile_with_cache(s3_url=s3_url)
        return network_tracker.asv_network_statistics


class Ros3DirectFileReadBenchmark(BaseBenchmark):
    parameter_cases = parameter_cases

    def track_network_activity_during_read(self, s3_url: str):
        with network_activity_tracker(tshark_path=TSHARK_PATH) as network_tracker:
            self.file, retries = read_hdf5_ros3(s3_url=s3_url)
        network_tracker.asv_network_statistics.update(retries=retries)
        return network_tracker.asv_network_statistics


class FsspecNoCacheNWBFileReadBenchmark(BaseBenchmark):
    parameter_cases = parameter_cases

    def track_network_activity_during_read(self, s3_url: str):
        with network_activity_tracker(tshark_path=TSHARK_PATH) as network_tracker:
            self.nwbfile, self.io, self.file, self.bytestream = read_hdf5_nwbfile_fsspec_no_cache(s3_url=s3_url)
        return network_tracker.asv_network_statistics


class FsspecWithCacheNWBFileReadBenchmark(BaseBenchmark):
    parameter_cases = parameter_cases

    def teardown(self, s3_url: str):
        self.tmpdir.cleanup()

    def track_network_activity_during_read(self, s3_url: str):
        with network_activity_tracker(tshark_path=TSHARK_PATH) as network_tracker:
            self.nwbfile, self.io, self.file, self.bytestream, self.tmpdir = read_hdf5_nwbfile_fsspec_with_cache(
                s3_url=s3_url
            )
        return network_tracker.asv_network_statistics


class RemfileNWBFileReadBenchmark(BaseBenchmark):
    parameter_cases = parameter_cases

    def track_network_activity_during_read(self, s3_url: str):
        with network_activity_tracker(tshark_path=TSHARK_PATH) as network_tracker:
            self.nwbfile, self.io, self.file, self.bytestream = read_hdf5_nwbfile_remfile(s3_url=s3_url)
        return network_tracker.asv_network_statistics


class RemfileNWBFileReadBenchmarkWithCache(BaseBenchmark):
    parameter_cases = parameter_cases

    def teardown(self, s3_url: str):
        self.tmpdir.cleanup()

    def track_network_activity_during_read(self, s3_url: str):
        with network_activity_tracker(tshark_path=TSHARK_PATH) as network_tracker:
            self.nwbfile, self.io, self.file, self.bytestream, self.tmpdir = read_hdf5_nwbfile_remfile_with_cache(
                s3_url=s3_url
            )
        return network_tracker.asv_network_statistics


class Ros3NWBFileReadBenchmark(BaseBenchmark):
    parameter_cases = parameter_cases

    def track_network_activity_during_read(self, s3_url: str):
        with network_activity_tracker(tshark_path=TSHARK_PATH) as network_tracker:
            self.nwbfile, self.io, retries = read_hdf5_nwbfile_ros3(s3_url=s3_url)
        network_tracker.asv_network_statistics.update(retries=retries)
        return network_tracker.asv_network_statistics


class LindiFileReadLocalReferenceFileSystemBenchmark(BaseBenchmark):
    """
    Time the read of the Lindi HDF5 files with and without `pynwb` assuming that a local
    copy of the lindi filesystem is available locally.
    """

    rounds = 1
    repeat = 3
    parameter_cases = lindi_hdf5_parameter_cases

    def setup(self, s3_url: str):
        """Create the local JSON LINDI reference filesystem if it does not exist"""
        self.lindi_file = os.path.basename(s3_url) + ".lindi.json"
        if not os.path.exists(self.lindi_file):
            create_lindi_reference_file_system(s3_url=s3_url, outfile_path=self.lindi_file)

    def track_network_activity_during_read_lindi_nwbfile(self, s3_url: str):
        """Read the NWB file with pynwb using LINDI with the local reference filesystem JSON"""
        with network_activity_tracker(tshark_path=TSHARK_PATH) as network_tracker:
            self.nwbfile, self.io, self.client = read_hdf5_nwbfile_lindi(rfs=self.lindi_file)
        return network_tracker.asv_network_statistics

    def track_network_activity_during_read_lindi_jsonrfs(self, s3_url: str):
        """Read the NWB file with LINDI directly using the local reference filesystem JSON"""
        with network_activity_tracker(tshark_path=TSHARK_PATH) as network_tracker:
            self.client = read_hdf5_lindi(rfs=self.lindi_file)
        return network_tracker.asv_network_statistics


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
        self.teardown(s3_url=s3_url)

    def teardown(self, s3_url: str):
        """Clear the LINDI JSON if it still exists"""
        if os.path.exists(self.lindi_file):
            os.remove(self.lindi_file)

    def track_network_activity_create_lindi_referernce_file_system(self, s3_url: str):
        """Create a local Lindi JSON reference filesystem from a remote HDF5 file"""
        with network_activity_tracker(tshark_path=TSHARK_PATH) as network_tracker:
            self.lindi_file = os.path.basename(s3_url) + ".lindi.json"
            create_lindi_reference_file_system(s3_url=s3_url, outfile_path=self.lindi_file)
        return network_tracker.asv_network_statistics

    def track_network_activity_create_lindi_referernce_file_system_and_read_nwbfile(self, s3_url: str):
        """
        Create a local Lindi JSON reference filesystem from a remote HDF5 file
        and then read the NWB file with PyNWB using LINDI with the local JSON
        """
        with network_activity_tracker(tshark_path=TSHARK_PATH) as network_tracker:
            self.lindi_file = os.path.basename(s3_url) + ".lindi.json"
            create_lindi_reference_file_system(s3_url=s3_url, outfile_path=self.lindi_file)
            self.nwbfile, self.io, self.client = read_hdf5_nwbfile_lindi(rfs=self.lindi_file)
        return network_tracker.asv_network_statistics


class NWBLindiFileReadRemoteReferenceFileSystemBenchmark(BaseBenchmark):
    """
    Time the read of the Lindi HDF5 files with `pynwb` assuming that a local copy of the lindi
    filesystem is available locally.
    """

    rounds = 1
    repeat = 3
    parameter_cases = lindi_remote_rfs_parameter_cases

    def track_network_activity_time_read_lindi_nwbfile(self, s3_url: str):
        """Read a remote NWB file with PyNWB using the remote LINDI JSON reference filesystem"""
        with network_activity_tracker(tshark_path=TSHARK_PATH) as network_tracker:
            self.nwbfile, self.io, self.client = read_hdf5_nwbfile_lindi(rfs=s3_url)
        return network_tracker.asv_network_statistics

    def track_network_activity_time_read_lindi_jsonrfs(self, s3_url: str):
        """Read a remote HDF5 file with LINDI using the remote LINDI JSON reference filesystem"""
        with network_activity_tracker(tshark_path=TSHARK_PATH) as network_tracker:
            self.client = read_hdf5_lindi(rfs=self.lindi_file)
        return network_tracker.asv_network_statistics


class ZarrDirectFileReadBenchmark(BaseBenchmark):
    parameter_cases = zarr_parameter_cases

    def track_network_activity_during_read(self, s3_url: str):
        with network_activity_tracker(tshark_path=TSHARK_PATH) as network_tracker:
            self.zarr_file = read_zarr(s3_url=s3_url, open_without_consolidated_metadata=False)
        return network_tracker.asv_network_statistics


class ZarrForceNoConsolidatedDirectFileReadBenchmark(BaseBenchmark):
    parameter_cases = zarr_parameter_cases

    def track_network_activity_during_read(self, s3_url: str):
        with network_activity_tracker(tshark_path=TSHARK_PATH) as network_tracker:
            self.zarr_file = read_zarr(s3_url=s3_url, open_without_consolidated_metadata=True)
        return network_tracker.asv_network_statistics


class ZarrNWBFileReadBenchmark(BaseBenchmark):
    parameter_cases = zarr_parameter_cases

    def track_network_activity_during_read(self, s3_url: str):
        with network_activity_tracker(tshark_path=TSHARK_PATH) as network_tracker:
            self.nwbfile, self.io = read_zarr_nwbfile(s3_url=s3_url, mode="r")
        return network_tracker.asv_network_statistics


class ZarrForceNoConsolidatedNWBFileReadBenchmark(BaseBenchmark):
    parameter_cases = zarr_parameter_cases

    def track_network_activity_during_read(self, s3_url: str):
        with network_activity_tracker(tshark_path=TSHARK_PATH) as network_tracker:
            self.nwbfile, self.io = read_zarr_nwbfile(s3_url=s3_url, mode="r-")
        return network_tracker.asv_network_statistics
