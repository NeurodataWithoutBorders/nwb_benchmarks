"""Basic benchmarks for profiling network statistics for streaming access to NWB files and their contents."""

import os

from asv_runner.benchmarks.mark import skip_benchmark_if

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


@skip_benchmark_if(TSHARK_PATH is None)
class FsspecNoCacheDirectFileReadBenchmark(BaseBenchmark):
    parameter_cases = parameter_cases

    def track_network_activity_during_read(self, s3_url: str):
        with network_activity_tracker(tshark_path=TSHARK_PATH) as network_tracker:
            self.file, self.bytestream = read_hdf5_fsspec_no_cache(s3_url=s3_url)
        return network_tracker.asv_network_statistics


@skip_benchmark_if(TSHARK_PATH is None)
class FsspecWithCacheDirectFileReadBenchmark(BaseBenchmark):
    parameter_cases = parameter_cases

    def teardown(self, s3_url: str):
        self.tmpdir.cleanup()

    def track_network_activity_during_read(self, s3_url: str):
        with network_activity_tracker(tshark_path=TSHARK_PATH) as network_tracker:
            self.file, self.bytestream, self.tmpdir = read_hdf5_fsspec_with_cache(s3_url=s3_url)
        return network_tracker.asv_network_statistics


@skip_benchmark_if(TSHARK_PATH is None)
class RemfileDirectFileReadBenchmark(BaseBenchmark):
    parameter_cases = parameter_cases

    def track_network_activity_during_read(self, s3_url: str):
        with network_activity_tracker(tshark_path=TSHARK_PATH) as network_tracker:
            self.file, self.bytestream = read_hdf5_remfile(s3_url=s3_url)
        return network_tracker.asv_network_statistics


@skip_benchmark_if(TSHARK_PATH is None)
class RemfileDirectFileReadBenchmarkWithCache(BaseBenchmark):
    parameter_cases = parameter_cases

    def teardown(self, s3_url: str):
        self.tmpdir.cleanup()

    def track_network_activity_during_read(self, s3_url: str):
        with network_activity_tracker(tshark_path=TSHARK_PATH) as network_tracker:
            self.file, self.bytestream, self.tmpdir = read_hdf5_remfile_with_cache(s3_url=s3_url)
        return network_tracker.asv_network_statistics


@skip_benchmark_if(TSHARK_PATH is None)
class Ros3DirectFileReadBenchmark(BaseBenchmark):
    parameter_cases = parameter_cases

    def track_network_activity_during_read(self, s3_url: str):
        with network_activity_tracker(tshark_path=TSHARK_PATH) as network_tracker:
            self.file, retries = read_hdf5_ros3(s3_url=s3_url)
        network_tracker.asv_network_statistics.update(retries=retries)
        return network_tracker.asv_network_statistics


@skip_benchmark_if(TSHARK_PATH is None)
class FsspecNoCacheNWBFileReadBenchmark(BaseBenchmark):
    parameter_cases = parameter_cases

    def track_network_activity_during_read(self, s3_url: str):
        with network_activity_tracker(tshark_path=TSHARK_PATH) as network_tracker:
            self.nwbfile, self.io, self.file, self.bytestream = read_hdf5_nwbfile_fsspec_no_cache(s3_url=s3_url)
        return network_tracker.asv_network_statistics


@skip_benchmark_if(TSHARK_PATH is None)
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


@skip_benchmark_if(TSHARK_PATH is None)
class RemfileNWBFileReadBenchmark(BaseBenchmark):
    parameter_cases = parameter_cases

    def track_network_activity_during_read(self, s3_url: str):
        with network_activity_tracker(tshark_path=TSHARK_PATH) as network_tracker:
            self.nwbfile, self.io, self.file, self.bytestream = read_hdf5_nwbfile_remfile(s3_url=s3_url)
        return network_tracker.asv_network_statistics


@skip_benchmark_if(TSHARK_PATH is None)
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


@skip_benchmark_if(TSHARK_PATH is None)
class Ros3NWBFileReadBenchmark(BaseBenchmark):
    parameter_cases = parameter_cases

    def track_network_activity_during_read(self, s3_url: str):
        with network_activity_tracker(tshark_path=TSHARK_PATH) as network_tracker:
            self.nwbfile, self.io, retries = read_hdf5_nwbfile_ros3(s3_url=s3_url)
        network_tracker.asv_network_statistics.update(retries=retries)
        return network_tracker.asv_network_statistics


@skip_benchmark_if(TSHARK_PATH is None)
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


# @skip_benchmark_if(TSHARK_PATH is None)
@skip_benchmark_if(True)
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

    def track_network_activity_create_lindi_referernce_file_system(self, s3_url: str):
        """Create a local Lindi JSON reference filesystem from a remote HDF5 file"""
        with network_activity_tracker(tshark_path=TSHARK_PATH) as network_tracker:
            create_lindi_reference_file_system(s3_url=s3_url, outfile_path=self.lindi_file)
        return network_tracker.asv_network_statistics

    def track_network_activity_create_lindi_referernce_file_system_and_read_nwbfile(self, s3_url: str):
        """
        Create a local Lindi JSON reference filesystem from a remote HDF5 file
        and then read the NWB file with PyNWB using LINDI with the local JSON
        """
        with network_activity_tracker(tshark_path=TSHARK_PATH) as network_tracker:
            create_lindi_reference_file_system(s3_url=s3_url, outfile_path=self.lindi_file)
            self.nwbfile, self.io, self.client = read_hdf5_nwbfile_lindi(rfs=self.lindi_file)
        return network_tracker.asv_network_statistics


@skip_benchmark_if(TSHARK_PATH is None)
class NWBLindiFileReadRemoteReferenceFileSystemBenchmark(BaseBenchmark):
    """
    Time the read of a remote NWB file with pynwb using lindi with a remote JSON reference
    filesystem available.
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
            self.client = read_hdf5_lindi(rfs=s3_url)
        return network_tracker.asv_network_statistics


@skip_benchmark_if(TSHARK_PATH is None)
class ZarrDirectFileReadBenchmark(BaseBenchmark):
    parameter_cases = zarr_parameter_cases

    def track_network_activity_during_read(self, s3_url: str):
        with network_activity_tracker(tshark_path=TSHARK_PATH) as network_tracker:
            self.zarr_file = read_zarr(s3_url=s3_url, open_without_consolidated_metadata=False)
        return network_tracker.asv_network_statistics


@skip_benchmark_if(TSHARK_PATH is None)
class ZarrForceNoConsolidatedDirectFileReadBenchmark(BaseBenchmark):
    parameter_cases = zarr_parameter_cases

    def track_network_activity_during_read(self, s3_url: str):
        with network_activity_tracker(tshark_path=TSHARK_PATH) as network_tracker:
            self.zarr_file = read_zarr(s3_url=s3_url, open_without_consolidated_metadata=True)
        return network_tracker.asv_network_statistics


@skip_benchmark_if(TSHARK_PATH is None)
class ZarrNWBFileReadBenchmark(BaseBenchmark):
    parameter_cases = zarr_parameter_cases

    def track_network_activity_during_read(self, s3_url: str):
        with network_activity_tracker(tshark_path=TSHARK_PATH) as network_tracker:
            self.nwbfile, self.io = read_zarr_nwbfile(s3_url=s3_url, mode="r")
        return network_tracker.asv_network_statistics


@skip_benchmark_if(TSHARK_PATH is None)
class ZarrForceNoConsolidatedNWBFileReadBenchmark(BaseBenchmark):
    parameter_cases = zarr_parameter_cases

    def track_network_activity_during_read(self, s3_url: str):
        with network_activity_tracker(tshark_path=TSHARK_PATH) as network_tracker:
            self.nwbfile, self.io = read_zarr_nwbfile(s3_url=s3_url, mode="r-")
        return network_tracker.asv_network_statistics
