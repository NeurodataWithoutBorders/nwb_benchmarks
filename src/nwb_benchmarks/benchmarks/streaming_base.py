"""Base template classes for basic benchmarks for NWB streaming read."""
import fsspec
import h5py
import pynwb
import remfile
from fsspec.asyn import reset_lock


class FileReadStreamingBase:
    """
    Base class for basic benchmarks for opening  an NWB file on S3 for streaming read.

    Child classes must set:
    - set the s3_url on the class to an S3 asset
    - specify the performance metrics to use for the test cases by specifying benchmark functions
    """

    s3_url: str = None   # S3 URL of the NWB asset

    def setup(self):
        assert self.s3_url is not None, "Test must set s3_url class variable"

    def fsspec_no_cache(self):
        reset_lock()
        fsspec.get_filesystem_class("https").clear_instance_cache()
        filesystem = fsspec.filesystem("https")

        with filesystem.open(path=self.s3_url, mode="rb") as byte_stream:
            with h5py.File(name=byte_stream) as file:
                with pynwb.NWBHDF5IO(file=file, load_namespaces=True) as io:
                    nwbfile = io.read()

    def ros3(self):
        ros3_form = self.s3_url.replace("https://dandiarchive.s3.amazonaws.com", "s3://dandiarchive")
        with pynwb.NWBHDF5IO(path=ros3_form, mode="r", load_namespaces=True, driver="ros3") as io:
            nwbfile = io.read()

    def remfile(self):
        byte_stream = remfile.File(url=self.s3_url)
        with h5py.File(name=byte_stream) as file:
            with pynwb.NWBHDF5IO(file=file, load_namespaces=True) as io:
                nwbfile = io.read()


class ElectricalSeriesStreamingSliceTestMixin:
    """
    Define test case for slicing an ElectricalSeries.

    Child classes must set:
    - self.nwb_file : NWBFile object to use. Usually set in the setup function
    - acquisition_path: class variable with name of object in nwbfile.acquisition
    - slice_range: class variable with data selection to apply

    Child classes must specify the performance metrics to use for the test cases by
    specifying the corresponding test. E.g.:

    .. code-block:: python

        def time_slice_request(self):
            self.slice_request()
    """

    acquisition_path: str  # name of object in nwbfile.acquisition
    slice_range: tuple[slice, int]  # data selection to apply

    def slice_request(self):
        """Test case for slicing the ElectricalSeries"""
        self.nwbfile.acquisition[self.acquisition_path].data[self.slice_range]


class ElectricalSeriesStreamingROS3Base(ElectricalSeriesStreamingSliceTestMixin):
    """
    "Base class for basic benchmark for streaming raw ecephys data.

    Needs separate setup per class to only time slicing operation.

    Child classes must set the following class variables:
    - s3_url: URL of the S3 asset
    - See ElectricalSeriesStreamingSliceTestMixin for additional requirements
    """

    s3_url: str = None  # S3 URL of the NWB asset

    def setup(self):
        assert self.s3_url is not None, "Test must set s3_url class variable"
        assert self.acquisition_path is not None, "Test must set the acquisition_path class variable."
        assert self.slice_range is not None, "Test must set the slice_range class variable."
        self.io = pynwb.NWBHDF5IO(path=self.s3_url, mode="r", load_namespaces=True, driver="ros3")
        self.nwbfile = self.io.read()


class ElectricalSeriesStreamingFsspecBase(ElectricalSeriesStreamingSliceTestMixin):
    """
    "Base class for basic benchmarks for streaming raw ecephys data.

    Needs separate setup per class to only time slicing operation.

    Child classes must set the following class variables:
    - s3_url: URL of the S3 asset
    - See ElectricalSeriesStreamingSliceTestMixin for additional requirements
    """

    s3_url: str = None   # S3 URL of the NWB asset

    def setup(self):
        assert self.s3_url is not None, "Test must set s3_url class variable"
        assert self.acquisition_path is not None, "Test must set the acquisition_path class variable."
        assert self.slice_range is not None, "Test must set the slice_range class variable."

        reset_lock()
        fsspec.get_filesystem_class("https").clear_instance_cache()

        self.filesystem = fsspec.filesystem("https")
        self.byte_stream = self.filesystem.open(path=self.s3_url, mode="rb")
        self.file = h5py.File(name=self.byte_stream)
        self.io = pynwb.NWBHDF5IO(file=self.file, mode="r", load_namespaces=True)
        self.nwbfile = self.io.read()


class ElectricalSeriesStreamingRemfileBase(ElectricalSeriesStreamingSliceTestMixin):
    """
    "Base class for basic benchmarks for streaming raw ecephys data.

    Needs separate setup per class to only time slicing operation.

    Child classes must set the following class variables:
    - s3_url: URL of the S3 asset
    - See ElectricalSeriesStreamingSliceTestMixin for additional requirements
    """

    s3_url: str = None  # S3 URL of the NWB asset

    def setup(self):
        assert self.s3_url is not None, "Test must set s3_url class variable"
        assert self.acquisition_path is not None, "Test must set the acquisition_path class variable."
        assert self.slice_range is not None, "Test must set the slice_range class variable."

        self.byte_stream = remfile.File(url=self.s3_url)
        self.file = h5py.File(name=self.byte_stream)
        self.io = pynwb.NWBHDF5IO(file=self.file, mode="r", load_namespaces=True)
        self.nwbfile = self.io.read()

