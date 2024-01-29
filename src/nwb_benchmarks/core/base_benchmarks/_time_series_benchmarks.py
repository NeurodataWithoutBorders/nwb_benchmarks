class AcquisitionTimeSeriesSliceBenchmark:
    s3_url: str
    acquisition_path: str
    slice_range: tuple  # TODO: enhance annotation

    def time_slice(self):
        # Store as self._temp to avoid tracking garbage collection as well
        self._temp = self.nwbfile.acquisition[self.acquisition_path].data[self.slice_range]

    # TODO: add additional trackers here
