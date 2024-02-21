"""Basic benchmarks for profiling network statistics for streaming access to NWB files and their contents."""

import os

from nwb_benchmarks.core import (
    get_s3_url,
    network_activity_tracker,
    read_hdf5_fsspec_no_cache,
    read_hdf5_nwbfile_fsspec_no_cache,
    read_hdf5_nwbfile_remfile,
    read_hdf5_nwbfile_ros3,
    read_hdf5_remfile,
    read_hdf5_ros3,
)

TSHARK_PATH = os.environ.get("TSHARK_PATH", None)

param_names = ["s3_url"]
params = [
    get_s3_url(dandiset_id="000717", dandi_path="sub-mock/sub-mock_ses-ecephys1.nwb"),
    get_s3_url(
        dandiset_id="000717",
        dandi_path="sub-IBL-ecephys/sub-IBL-ecephys_ses-3e7ae7c0_desc-18000000-frames-13653-by-384-chunking.nwb",
    ),  # Not the best example for testing a theory about file read; should probably replace with something simpler
    "https://dandiarchive.s3.amazonaws.com/ros3test.nwb",  # The original small test NWB file
]


class FsspecNoCacheDirectFileReadBenchmark:
    param_names = param_names
    params = params

    def track_network_activity_during_read(self, s3_url: str):
        with network_activity_tracker(tshark_path=TSHARK_PATH) as network_activity:
            self.file, self.bytestream = read_hdf5_fsspec_no_cache(s3_url=s3_url)
        return network_activity.asv_network_statistics


class RemfileDirectFileReadBenchmark:
    param_names = param_names
    params = params

    def track_network_activity_during_read(self, s3_url: str):
        with network_activity_tracker(tshark_path=TSHARK_PATH) as network_activity:
            self.file, self.bytestream = read_hdf5_remfile(s3_url=s3_url)
        return network_activity.asv_network_statistics


class Ros3DirectFileReadBenchmark:
    param_names = param_names
    params = params

    def track_network_activity_during_read(self, s3_url: str):
        with network_activity_tracker(tshark_path=TSHARK_PATH) as network_activity:
            self.file = read_hdf5_ros3(s3_url=s3_url)
        return network_activity.asv_network_statistics


class FsspecNoCacheNWBFileReadBenchmark:
    param_names = param_names
    params = params

    def track_network_activity_during_read(self, s3_url: str):
        with network_activity_tracker(tshark_path=TSHARK_PATH) as network_activity:
            self.nwbfile, self.io, self.file, self.bytestream = read_hdf5_nwbfile_fsspec_no_cache(s3_url=s3_url)
        return network_activity.asv_network_statistics


class RemfileNWBFileReadBenchmark:
    param_names = param_names
    params = params

    def track_network_activity_during_read(self, s3_url: str):
        with network_activity_tracker(tshark_path=TSHARK_PATH) as network_activity:
            self.nwbfile, self.io, self.file, self.bytestream = read_hdf5_nwbfile_remfile(s3_url=s3_url)
        return network_activity.asv_network_statistics


class Ros3NWBFileReadBenchmark:
    param_names = param_names
    params = params

    def track_network_activity_during_read(self, s3_url: str):
        with network_activity_tracker(tshark_path=TSHARK_PATH) as network_activity:
            self.nwbfile, self.io = read_hdf5_nwbfile_ros3(s3_url=s3_url)
        return network_activity.asv_network_statistics
