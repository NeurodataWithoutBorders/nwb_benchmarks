"""Basic benchmarks for NWB."""
from .streaming_base import (FileReadStreamingBase,
                             ElectricalSeriesStreamingROS3Base,
                             ElectricalSeriesStreamingFsspecBase,
                             ElectricalSeriesStreamingRemfileBase)
import warnings

# Useful if running in verbose mode
warnings.filterwarnings(action="ignore", message="No cached namespaces found in .*")
warnings.filterwarnings(action="ignore", message="Ignoring cached namespace .*")


class FileReadStreaming(FileReadStreamingBase):
    """A basic benchmark for streaming an NWB file from the DANDI archive."""

    repeat = 1
    s3_url = "https://dandiarchive.s3.amazonaws.com/blobs/8c5/65f/8c565f28-e5fc-43fe-8fb7-318ad2081319"

    def time_fsspec_no_cache(self):
        self.fsspec_no_cache()

    def time_ros3(self):
        self.ros3()

    def time_remfile(self):
        self.remfile()


class ElectricalSeriesStreamingROS3(ElectricalSeriesStreamingROS3Base):
    """
    A basic benchmark for streaming raw ecephys data.

    Needs separate setup per class to only time slicing operation.
    """
    repeat = 1
    s3_url = "s3://dandiarchive/blobs/8c5/65f/8c565f28-e5fc-43fe-8fb7-318ad2081319"

    def time_ros3(self):
        self.ros3()


class ElectricalSeriesStreamingFsspec(ElectricalSeriesStreamingFsspecBase):
    """
    A basic benchmark for streaming raw ecephys data.

    Needs separate setup per class to only time slicing operation.
    """

    repeat = 1
    s3_url = "https://dandiarchive.s3.amazonaws.com/blobs/8c5/65f/8c565f28-e5fc-43fe-8fb7-318ad2081319"

    def time_fsspec_no_cache(self):
        self.fsspec_no_cache()


class ElectricalSeriesStreamingRemfile(ElectricalSeriesStreamingRemfileBase):
    """
    A basic benchmark for streaming raw ecephys data.

    Needs separate setup per class to only time slicing operation.
    """
    repeat = 1
    s3_url = "https://dandiarchive.s3.amazonaws.com/blobs/8c5/65f/8c565f28-e5fc-43fe-8fb7-318ad2081319"

    def time_remfile(self):
        self.remfile()
