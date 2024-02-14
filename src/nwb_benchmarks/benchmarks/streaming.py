"""Basic benchmarks for stream NWB files and their contents."""
from ..core.common import read_nwbfile_fsspec, read_nwbfile_remfile, read_nwbfile_ros3


def slice_dataset(nwbfile, acquisition_path, slice_range):
    return nwbfile.acquisition[self.acquisition_path].data[self.slice_range]

class FsspecNoCacheFileReadBenchmark:
    repeat = 1
    s3_url = "https://dandiarchive.s3.amazonaws.com/blobs/8c5/65f/8c565f28-e5fc-43fe-8fb7-318ad2081319"

    def time_file_read(self):
        self.nwbfile, self.io, self.file, self.bytestream = read_nwbfile_fsspec(s3_url=self.s3_url)


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
    acquisition_path = "ElectricalSeriesAp"
    slice_range = (slice(0, 30_000), slice(0, 384))  # ~23 MB

    def setup(self):
        self.nwbfile, self.io, self.file, self.bytestream = read_nwbfile_fsspec(s3_url=self.s3_url)

    def time_slice(self):
        # Store as self._temp to avoid tracking garbage collection as well
        self._temp = slice_dataset(nwbfile=self.nwbfile, ...)


class Ros3SliceBenchmark:
    repeat = 1
    s3_url = "s3://dandiarchive/blobs/8c5/65f/8c565f28-e5fc-43fe-8fb7-318ad2081319"
    acquisition_path = "ElectricalSeriesAp"
    slice_range = (slice(0, 30_000), slice(0, 384))  # ~23 MB

    def setup(self):
        self.nwbfile, self.io = read_nwbfile_ros3(s3_url=self.s3_url)

    def time_slice(self):
        # Store as self._temp to avoid tracking garbage collection as well
        self._temp = slice_dataset(nwbfile=self.nwbfile, ...)


class RemfileSliceBenchmark:
    repeat = 1
    s3_url = "https://dandiarchive.s3.amazonaws.com/blobs/8c5/65f/8c565f28-e5fc-43fe-8fb7-318ad2081319"
    acquisition_path = "ElectricalSeriesAp"
    slice_range = (slice(0, 30_000), slice(0, 384))  # ~23 MB

    def setup(self):
        self.nwbfile, self.io, self.file, self.bytestream = read_nwbfile_remfile(s3_url=self.s3_url)

    def time_slice(self):
        # Store as self._temp to avoid tracking garbage collection as well
        self._temp = slice_dataset(nwbfile=self.nwbfile, ...)
