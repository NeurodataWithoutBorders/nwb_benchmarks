class TimeSliceRequestBenchmark:
    """
    Benchmark for testing the time it takes to execute the `self.slice_request` method on an NWB file.

    Required mixins:
        - NwbFileReadBase
        - SliceRequestBase
    """

    def setup(self):
        self.read_nwbfile()

    def time_slice_request(self):
        """Time the slice request."""
        self.slice_request()

    # TODO: add more tracking functions here
