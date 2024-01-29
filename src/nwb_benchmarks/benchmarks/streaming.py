"""Basic benchmarks for stream NWB files and their contents."""
from ..core import (
    AcquisitionTimeSeriesSlice,
    FsspecNwbFileRead,
    RemfileNwbFileRead,
    RemoteNwbFileReadBenchmark,
    Ros3NwbFileRead,
    TimeSliceRequestBenchmark,
)


class FsspecFileReadBenchmark(RemoteNwbFileReadBenchmark, FsspecNwbFileRead):
    repeat = 1
    s3_url = "https://dandiarchive.s3.amazonaws.com/blobs/8c5/65f/8c565f28-e5fc-43fe-8fb7-318ad2081319"


class Ros3FileReadBenchmark(RemoteNwbFileReadBenchmark, Ros3NwbFileRead):
    repeat = 1
    s3_url = "https://dandiarchive.s3.amazonaws.com/blobs/8c5/65f/8c565f28-e5fc-43fe-8fb7-318ad2081319"


class RemfileFileReadBenchmark(RemoteNwbFileReadBenchmark, RemfileNwbFileRead):
    repeat = 1
    s3_url = "https://dandiarchive.s3.amazonaws.com/blobs/8c5/65f/8c565f28-e5fc-43fe-8fb7-318ad2081319"


class ElectricalSeriesStreamingROS3(TimeSliceRequestBenchmark, Ros3NwbFileRead, AcquisitionTimeSeriesSlice):
    repeat = 1
    s3_url = "s3://dandiarchive/blobs/8c5/65f/8c565f28-e5fc-43fe-8fb7-318ad2081319"
    acquisition_path = "ElectricalSeriesAp"
    slice_range = (slice(0, 30_000), slice(0, 384))  # ~23 MB


class ElectricalSeriesStreamingFsspec(TimeSliceRequestBenchmark, FsspecNwbFileRead, AcquisitionTimeSeriesSlice):
    repeat = 1
    s3_url = "https://dandiarchive.s3.amazonaws.com/blobs/8c5/65f/8c565f28-e5fc-43fe-8fb7-318ad2081319"
    acquisition_path = "ElectricalSeriesAp"
    slice_range = (slice(0, 30_000), slice(0, 384))  # ~23 MB


class ElectricalSeriesStreamingRemfile(TimeSliceRequestBenchmark, RemfileNwbFileRead, AcquisitionTimeSeriesSlice):
    repeat = 1
    s3_url = "https://dandiarchive.s3.amazonaws.com/blobs/8c5/65f/8c565f28-e5fc-43fe-8fb7-318ad2081319"
    acquisition_path = "ElectricalSeriesAp"
    slice_range = (slice(0, 30_000), slice(0, 384))  # ~23 MB
