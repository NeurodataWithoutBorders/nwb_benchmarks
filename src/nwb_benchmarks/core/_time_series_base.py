import abc
import typing


class SliceRequestBase(abc.ABC):
    slice_range: typing.Tuple[slice, int]  # data selection to apply

    @abc.abstractmethod
    def slice_request(self):
        raise NotImplementedError


class AcquisitionTimeSeriesSlice(SliceRequestBase):
    """
    Define test case for slicing a TimeSeries located in the acquisition module.

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

    def slice_request(self):
        """
        Definition of how to perform the slicing operation.

        Output is stored in _temp attribute to avoid any benchmarks also including the garbage collection.
        """
        self._temp = self.nwbfile.acquisition[self.acquisition_path].data[self.slice_range]

    # TODO: add more tracking functions here
