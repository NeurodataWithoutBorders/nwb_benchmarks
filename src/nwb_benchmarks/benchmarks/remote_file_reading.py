"""Basic benchmarks for stream NWB files and their contents."""

from nwb_benchmarks.core import (
    BaseNetworkBenchmark,
    get_s3_url,
    read_hdf5_fsspec_no_cache,
    read_hdf5_nwbfile_fsspec_no_cache,
    read_hdf5_nwbfile_remfile,
    read_hdf5_nwbfile_ros3,
    read_hdf5_remfile,
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


class FsspecNoCacheDirectFileReadBenchmark(BaseNetworkBenchmark):
    repeat = 1
    param_names = param_names
    params = params

    def time_file_read(self, s3_url: str):
        self.file, self.bytestream = read_hdf5_fsspec_no_cache(s3_url=s3_url)

    def operation_to_track_network_activity_of(self, s3_url: str):
        self.file, self.bytestream = read_hdf5_fsspec_no_cache(s3_url=s3_url)


class RemfileDirectFileReadBenchmark(BaseNetworkBenchmark):
    repeat = 1
    param_names = param_names
    params = params

    def time_file_read(self, s3_url: str):
        self.file, self.bytestream = read_hdf5_remfile(s3_url=s3_url)

    def operation_to_track_network_activity_of(self, s3_url: str):
        self.file, self.bytestream = read_hdf5_remfile(s3_url=s3_url)


class Ros3DirectFileReadBenchmark(BaseNetworkBenchmark):
    repeat = 1
    param_names = param_names
    params = params

    def time_file_read(self, s3_url: str):
        self.file = read_hdf5_ros3(s3_url=s3_url)

    def operation_to_track_network_activity_of(self, s3_url: str):
        self.file = read_hdf5_ros3(s3_url=s3_url)


class FsspecNoCacheNWBFileReadBenchmark(BaseNetworkBenchmark):
    repeat = 1
    param_names = param_names
    params = params

    def time_file_read(self, s3_url: str):
        self.nwbfile, self.io, self.file, self.bytestream = read_hdf5_nwbfile_fsspec_no_cache(s3_url=s3_url)

    def operation_to_track_network_activity_of(self, s3_url: str):
        self.nwbfile, self.io, self.file, self.bytestream = read_hdf5_nwbfile_fsspec_no_cache(s3_url=s3_url)


class RemfileNWBFileReadBenchmark(BaseNetworkBenchmark):
    repeat = 1
    param_names = param_names
    params = params

    def time_file_read(self, s3_url: str):
        self.nwbfile, self.io, self.file, self.bytestream = read_hdf5_nwbfile_remfile(s3_url=s3_url)

    def operation_to_track_network_activity_of(self, s3_url: str):
        self.nwbfile, self.io, self.file, self.bytestream = read_hdf5_nwbfile_remfile(s3_url=s3_url)


class Ros3NWBFileReadBenchmark(BaseNetworkBenchmark):
    repeat = 1
    param_names = param_names
    params = params

    def time_file_read(self, s3_url: str):
        self.nwbfile, self.io = read_hdf5_nwbfile_ros3(s3_url=s3_url)

    def operation_to_track_network_activity_of(self, s3_url: str):
        self.nwbfile, self.io = read_hdf5_nwbfile_ros3(s3_url=s3_url)
