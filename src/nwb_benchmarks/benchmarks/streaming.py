"""Basic benchmarks for stream NWB files and their contents."""
from ..core.base_benchmarks import AcquisitionTimeSeriesSliceBenchmark
from ..core.common import read_nwbfile_fsspec, read_nwbfile_remfile, read_nwbfile_ros3


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


class FsspecNoCacheSliceBenchmark(AcquisitionTimeSeriesSliceBenchmark):
    repeat = 1
    s3_url = "https://dandiarchive.s3.amazonaws.com/blobs/8c5/65f/8c565f28-e5fc-43fe-8fb7-318ad2081319"
    acquisition_path = "ElectricalSeriesAp"
    slice_range = (slice(0, 30_000), slice(0, 384))  # ~23 MB

    def setup(self):
        self.nwbfile, self.io, self.file, self.bytestream = read_nwbfile_fsspec(s3_url=self.s3_url)


class Ros3SliceBenchmark(AcquisitionTimeSeriesSliceBenchmark):
    repeat = 1
    s3_url = "s3://dandiarchive/blobs/8c5/65f/8c565f28-e5fc-43fe-8fb7-318ad2081319"
    acquisition_path = "ElectricalSeriesAp"
    slice_range = (slice(0, 30_000), slice(0, 384))  # ~23 MB

    def setup(self):
        self.nwbfile, self.io = read_nwbfile_ros3(s3_url=self.s3_url)


class RemfileSliceBenchmark(AcquisitionTimeSeriesSliceBenchmark):
    repeat = 1
    s3_url = "https://dandiarchive.s3.amazonaws.com/blobs/8c5/65f/8c565f28-e5fc-43fe-8fb7-318ad2081319"
    acquisition_path = "ElectricalSeriesAp"
    slice_range = (slice(0, 30_000), slice(0, 384))  # ~23 MB

    def setup(self):
        self.nwbfile, self.io, self.file, self.bytestream = read_nwbfile_remfile(s3_url=self.s3_url)
