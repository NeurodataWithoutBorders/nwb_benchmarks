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

ibl_chunk_experiments = [
    dict(
        dandiset_id="000717",
        dandi_path="sub-IBL-ecephys/sub-IBL-ecephys_ses-3e7ae7c0_desc-18000000-frames-13653-by-384-chunking.nwb",
    )
]


@parameterized.parameterized_class(
    [
        dict(
            dandiset_id="000717",
            dandi_path="sub-IBL-ecephys/sub-IBL-ecephys_ses-3e7ae7c0_desc-18000000-frames-13653-by-384-chunking.nwb",
        )
    ]
)
class FsspecNoCacheHDF5FileReadBenchmark:
    repeat = 1

    def setup(self):
        self.s3_url = get_s3_url(dandiset_id=self.dand, dandi_path=self.dandi_path)

    def time_file_read(self):
        self.nwbfile, self.io, self.file, self.bytestream = read_hdf5_fsspec_no_cache(s3_url=self.s3_url)


class FsspecNoCacheNWBFileReadBenchmark:
    repeat = 1
    s3_url = "https://dandiarchive.s3.amazonaws.com/blobs/8c5/65f/8c565f28-e5fc-43fe-8fb7-318ad2081319"

    def time_file_read(self):
        self.nwbfile, self.io, self.file, self.bytestream = read_hdf5_nwbfile_fsspec_no_cache(s3_url=self.s3_url)


class RemfileFileReadBenchmark:
    repeat = 1
    s3_url = "https://dandiarchive.s3.amazonaws.com/blobs/8c5/65f/8c565f28-e5fc-43fe-8fb7-318ad2081319"

    def time_file_read(self):
        self.nwbfile, self.io, self.file, self.bytestream = read_nwbfile_remfile(s3_url=self.s3_url)


class Ros3FileReadBenchmark:
    repeat = 1
    s3_url = "https://dandiarchive.s3.amazonaws.com/blobs/8c5/65f/8c565f28-e5fc-43fe-8fb7-318ad2081319"

    def time_file_read(self):
        self.nwbfile, self.io = read_nwbfile_ros3(s3_url=self.s3_url)


class FsspecNoCacheSliceBenchmark:
    repeat = 1
    s3_url = "https://dandiarchive.s3.amazonaws.com/blobs/8c5/65f/8c565f28-e5fc-43fe-8fb7-318ad2081319"
    params = [()]
    slice_range = (slice(0, 30_000), slice(0, 384))  # ~23 MB

    def setup(self):
        self.nwbfile, self.io, self.file, self.bytestream = read_nwbfile_fsspec(s3_url=self.s3_url)

    def time_slice(self):
        """Note: store as self._temp to avoid tracking garbage collection as well."""
        self._temp = self.nwbfile.acquisition["ElectricalSeriesAp"].data[self.slice_range]


class Ros3SliceBenchmark:
    repeat = 1
    s3_url = "s3://dandiarchive/blobs/8c5/65f/8c565f28-e5fc-43fe-8fb7-318ad2081319"
    slice_range = (slice(0, 30_000), slice(0, 384))  # ~23 MB

    def setup(self):
        self.nwbfile, self.io = read_nwbfile_ros3(s3_url=self.s3_url)

    def time_slice(self):
        self._temp = self.nwbfile.acquisition["ElectricalSeriesAp"].data[self.slice_range]


class RemfileSliceBenchmark:
    repeat = 1
    s3_url = "https://dandiarchive.s3.amazonaws.com/blobs/8c5/65f/8c565f28-e5fc-43fe-8fb7-318ad2081319"
    slice_range = (slice(0, 30_000), slice(0, 384))  # ~23 MB

    def setup(self):
        self.nwbfile, self.io, self.file, self.bytestream = read_nwbfile_remfile(s3_url=self.s3_url)

    def time_slice(self):
        self._temp = self.nwbfile.acquisition["ElectricalSeriesAp"].data[self.slice_range]
