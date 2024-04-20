"""Basic benchmarks for profiling network statistics for streaming access to NWB files and their contents."""

import os

from nwb_benchmarks import TSHARK_PATH
from nwb_benchmarks.core import (
    BaseBenchmark,
    get_s3_url,
    network_activity_tracker,
    read_hdf5_fsspec_no_cache,
    read_hdf5_nwbfile_fsspec_no_cache,
    read_hdf5_nwbfile_remfile,
    read_hdf5_nwbfile_ros3,
    read_hdf5_remfile,
    read_hdf5_ros3,
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


class FsspecNoCacheDirectFileReadBenchmark(BaseBenchmark):
    parameter_cases = parameter_cases

    def track_network_activity_during_read(self, s3_url: str):
        with network_activity_tracker(tshark_path=TSHARK_PATH) as network_tracker:
            self.file, self.bytestream = read_hdf5_fsspec_no_cache(s3_url=s3_url)
        return network_tracker.asv_network_statistics


class RemfileDirectFileReadBenchmark(BaseBenchmark):
    parameter_cases = parameter_cases

    def track_network_activity_during_read(self, s3_url: str):
        with network_activity_tracker(tshark_path=TSHARK_PATH) as network_tracker:
            self.file, self.bytestream = read_hdf5_remfile(s3_url=s3_url)
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


class RemfileNWBFileReadBenchmark(BaseBenchmark):
    parameter_cases = parameter_cases

    def track_network_activity_during_read(self, s3_url: str):
        with network_activity_tracker(tshark_path=TSHARK_PATH) as network_tracker:
            self.nwbfile, self.io, self.file, self.bytestream = read_hdf5_nwbfile_remfile(s3_url=s3_url)
        return network_tracker.asv_network_statistics


class Ros3NWBFileReadBenchmark(BaseBenchmark):
    parameter_cases = parameter_cases

    def track_network_activity_during_read(self, s3_url: str):
        with network_activity_tracker(tshark_path=TSHARK_PATH) as network_tracker:
            self.nwbfile, self.io, retries = read_hdf5_nwbfile_ros3(s3_url=s3_url)
        network_tracker.asv_network_statistics.update(retries=retries)
        return network_tracker.asv_network_statistics
