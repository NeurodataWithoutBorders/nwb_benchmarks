"""Basic benchmarks for profiling network statistics for streaming access to NWB files and their contents."""

from nwb_benchmarks import TSHARK_PATH
from nwb_benchmarks.core import (
    get_s3_url,
    network_activity_tracker,
    read_hdf5_fsspec_no_cache,
    read_hdf5_fsspec_with_cache,
    read_hdf5_nwbfile_fsspec_no_cache,
    read_hdf5_nwbfile_fsspec_with_cache,
    read_hdf5_nwbfile_remfile,
    read_hdf5_nwbfile_remfile_with_cache,
    read_hdf5_nwbfile_ros3,
    read_hdf5_remfile,
    read_hdf5_remfile_with_cache,
    read_hdf5_ros3,
)

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
        with network_activity_tracker(tshark_path=TSHARK_PATH) as network_tracker:
            self.file, self.bytestream = read_hdf5_fsspec_no_cache(s3_url=s3_url)
        return network_tracker.asv_network_statistics


class FsspecWithCacheDirectFileReadBenchmark:
    param_names = param_names
    params = params

    def teardown(self, s3_url: str):
        self.tmpdir.cleanup()

    def track_network_activity_during_read(self, s3_url: str):
        with network_activity_tracker(tshark_path=TSHARK_PATH) as network_tracker:
            self.file, self.bytestream, self.tmpdir = read_hdf5_fsspec_with_cache(s3_url=s3_url)
        return network_tracker.asv_network_statistics


class RemfileDirectFileReadBenchmark:
    param_names = param_names
    params = params

    def track_network_activity_during_read(self, s3_url: str):
        with network_activity_tracker(tshark_path=TSHARK_PATH) as network_tracker:
            self.file, self.bytestream = read_hdf5_remfile(s3_url=s3_url)
        return network_tracker.asv_network_statistics


class RemfileDirectFileReadBenchmarkWithCache:
    param_names = param_names
    params = params

    def teardown(self, s3_url: str):
        self.tmpdir.cleanup()

    def track_network_activity_during_read(self, s3_url: str):
        with network_activity_tracker(tshark_path=TSHARK_PATH) as network_tracker:
            self.file, self.bytestream, self.tmpdir = read_hdf5_remfile_with_cache(s3_url=s3_url)
        return network_tracker.asv_network_statistics


class Ros3DirectFileReadBenchmark:
    param_names = param_names
    params = params

    def track_network_activity_during_read(self, s3_url: str):
        with network_activity_tracker(tshark_path=TSHARK_PATH) as network_tracker:
            self.file, retries = read_hdf5_ros3(s3_url=s3_url)
        network_tracker.asv_network_statistics.update(retries=retries)
        return network_tracker.asv_network_statistics


class FsspecNoCacheNWBFileReadBenchmark:
    param_names = param_names
    params = params

    def track_network_activity_during_read(self, s3_url: str):
        with network_activity_tracker(tshark_path=TSHARK_PATH) as network_tracker:
            self.nwbfile, self.io, self.file, self.bytestream = read_hdf5_nwbfile_fsspec_no_cache(s3_url=s3_url)
        return network_tracker.asv_network_statistics


class FsspecWithCacheNWBFileReadBenchmark:
    param_names = param_names
    params = params

    def teardown(self, s3_url: str):
        self.tmpdir.cleanup()

    def track_network_activity_during_read(self, s3_url: str):
        with network_activity_tracker(tshark_path=TSHARK_PATH) as network_tracker:
            self.nwbfile, self.io, self.file, self.bytestream, self.tmpdir = read_hdf5_nwbfile_fsspec_with_cache(
                s3_url=s3_url
            )
        return network_tracker.asv_network_statistics


class RemfileNWBFileReadBenchmark:
    param_names = param_names
    params = params

    def track_network_activity_during_read(self, s3_url: str):
        with network_activity_tracker(tshark_path=TSHARK_PATH) as network_tracker:
            self.nwbfile, self.io, self.file, self.bytestream = read_hdf5_nwbfile_remfile(s3_url=s3_url)
        return network_tracker.asv_network_statistics


class RemfileNWBFileReadBenchmarkWithCache:
    param_names = param_names
    params = params

    def teardown(self, s3_url: str):
        self.tmpdir.cleanup()

    def track_network_activity_during_read(self, s3_url: str):
        with network_activity_tracker(tshark_path=TSHARK_PATH) as network_tracker:
            self.nwbfile, self.io, self.file, self.bytestream, self.tmpdir = read_hdf5_nwbfile_remfile_with_cache(
                s3_url=s3_url
            )
        return network_tracker.asv_network_statistics


class Ros3NWBFileReadBenchmark:
    param_names = param_names
    params = params

    def track_network_activity_during_read(self, s3_url: str):
        with network_activity_tracker(tshark_path=TSHARK_PATH) as network_tracker:
            self.nwbfile, self.io, retries = read_hdf5_nwbfile_ros3(s3_url=s3_url)
        network_tracker.asv_network_statistics.update(retries=retries)
        return network_tracker.asv_network_statistics
