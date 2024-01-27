"""Base template classes for basic benchmarks for NWB streaming read."""
import fsspec
import h5py
import pynwb
import remfile
from fsspec.asyn import reset_lock


class FileReadStreamingBase:
    """Base class for basic benchmarks for streaming an NWB file from the DANDI archive."""
    def setup(self):
        # Random IBL raw data file; not that many groups
        self.s3_url = "https://dandiarchive.s3.amazonaws.com/blobs/8c5/65f/8c565f28-e5fc-43fe-8fb7-318ad2081319"

    def fsspec_no_cache(self):
        reset_lock()
        fsspec.get_filesystem_class("https").clear_instance_cache()
        filesystem = fsspec.filesystem("https")

        with filesystem.open(path=self.s3_url, mode="rb") as byte_stream:
            with h5py.File(name=byte_stream) as file:
                with pynwb.NWBHDF5IO(file=file, load_namespaces=True) as io:
                    nwbfile = io.read()

        # Must be done at this level since teardown occurs outside of repetitions
        # reset_lock()
        # fsspec.get_filesystem_class("https").clear_instance_cache()

    def ros3(self):
        ros3_form = self.s3_url.replace("https://dandiarchive.s3.amazonaws.com", "s3://dandiarchive")
        with pynwb.NWBHDF5IO(path=ros3_form, mode="r", load_namespaces=True, driver="ros3") as io:
            nwbfile = io.read()

    def remfile(self):
        byte_stream = remfile.File(url=self.s3_url)
        with h5py.File(name=byte_stream) as file:
            with pynwb.NWBHDF5IO(file=file, load_namespaces=True) as io:
                nwbfile = io.read()


class ElectricalSeriesStreamingROS3Base:
    """
    "Base class for basic benchmark for streaming raw ecephys data.

    Needs separate setup per class to only time slicing operation.
    """
    def setup(self):
        self.s3_url = "s3://dandiarchive/blobs/8c5/65f/8c565f28-e5fc-43fe-8fb7-318ad2081319"
        self.acquisition_path = "ElectricalSeriesAp"
        self.slice_range = (slice(0, 30_000), slice(0, 384))  # ~23 MB
        self.io = pynwb.NWBHDF5IO(path=self.s3_url, mode="r", load_namespaces=True, driver="ros3")
        self.nwbfile = self.io.read()

    def ros3(self):
        self.nwbfile.acquisition[self.acquisition_path].data[self.slice_range]


class ElectricalSeriesStreamingFsspecBase:
    """
    "Base class for basic benchmarks for streaming raw ecephys data.

    Needs separate setup per class to only time slicing operation.
    """

    def setup(self):
        self.s3_url = "https://dandiarchive.s3.amazonaws.com/blobs/8c5/65f/8c565f28-e5fc-43fe-8fb7-318ad2081319"
        self.acquisition_path = "ElectricalSeriesAp"
        self.slice_range = (slice(0, 30_000), slice(0, 384))  # ~23 MB

        reset_lock()
        fsspec.get_filesystem_class("https").clear_instance_cache()

        self.filesystem = fsspec.filesystem("https")
        self.byte_stream = self.filesystem.open(path=self.s3_url, mode="rb")
        self.file = h5py.File(name=self.byte_stream)
        self.io = pynwb.NWBHDF5IO(file=self.file, mode="r", load_namespaces=True)
        self.nwbfile = self.io.read()

    def fsspec_no_cache(self):
        self.nwbfile.acquisition[self.acquisition_path].data[self.slice_range]



class ElectricalSeriesStreamingRemfileBase:
    """
    "Base class for basic benchmarks for streaming raw ecephys data.

    Needs separate setup per class to only time slicing operation.
    """

    def setup(self):
        self.s3_url = "https://dandiarchive.s3.amazonaws.com/blobs/8c5/65f/8c565f28-e5fc-43fe-8fb7-318ad2081319"
        self.acquisition_path = "ElectricalSeriesAp"
        self.slice_range = (slice(0, 30_000), slice(0, 384))  # ~23 MB
        self.byte_stream = remfile.File(url=self.s3_url)
        self.file = h5py.File(name=self.byte_stream)
        self.io = pynwb.NWBHDF5IO(file=self.file, mode="r", load_namespaces=True)
        self.nwbfile = self.io.read()

    def remfile(self):
        self.nwbfile.acquisition[self.acquisition_path].data[self.slice_range]
