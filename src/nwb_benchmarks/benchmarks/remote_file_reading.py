"""Basic benchmarks for stream NWB files and their contents."""

import parameterized

from nwb_benchmarks.core import (
    get_s3_url,
    read_hdf5_fsspec_no_cache,
    read_hdf5_nwbfile_fsspec_no_cache,
    read_hdf5_nwbfile_remfile,
    read_hdf5_nwbfile_ros3,
    read_hdf5_remfile,
    read_hdf5_ros3,
)

remote_read_test_files = [
    dict(s3_url=get_s3_url(dandiset_id="000717", dandi_path="sub-mock/sub-mock_ses-ecephys1.nwb")),
    dict(
        s3_url=get_s3_url(
            dandiset_id="000717",
            dandi_path="sub-IBL-ecephys/sub-IBL-ecephys_ses-3e7ae7c0_desc-18000000-frames-13653-by-384-chunking.nwb",
        )  # Not the best example for testing a theory about file read; should probably replace with something simpler
    ),
]


@parameterized.parameterized_class(remote_read_test_files)
class FsspecNoCacheDirectFileReadBenchmark:
    repeat = 1

    def time_file_read(self):
        self.nwbfile, self.io, self.file, self.bytestream = read_hdf5_fsspec_no_cache(s3_url=self.s3_url)


@parameterized.parameterized_class(remote_read_test_files)
class FsspecNoCacheNWBFileReadBenchmark:
    repeat = 1

    def time_file_read(self):
        self.nwbfile, self.io, self.file, self.bytestream = read_hdf5_nwbfile_fsspec_no_cache(s3_url=self.s3_url)


@parameterized.parameterized_class(remote_read_test_files)
class RemfileDirectFileReadBenchmark:
    repeat = 1

    def time_file_read(self):
        self.nwbfile, self.io, self.file, self.bytestream = read_hdf5_remfile(s3_url=self.s3_url)


@parameterized.parameterized_class(remote_read_test_files)
class RemfileNWBFileReadBenchmark:
    repeat = 1

    def time_file_read(self):
        self.nwbfile, self.io, self.file, self.bytestream = read_hdf5_nwbfile_remfile(s3_url=self.s3_url)


@parameterized.parameterized_class(remote_read_test_files)
class Ros3DirectFileReadBenchmark:
    repeat = 1

    def time_file_read(self):
        self.nwbfile, self.io = read_hdf5_ros3(s3_url=self.s3_url)


@parameterized.parameterized_class(remote_read_test_files)
class Ros3NWBFileReadBenchmark:
    repeat = 1

    def time_file_read(self):
        self.nwbfile, self.io = read_hdf5_nwbfile_ros3(s3_url=self.s3_url)
