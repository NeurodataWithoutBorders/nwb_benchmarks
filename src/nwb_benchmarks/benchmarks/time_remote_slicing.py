"""Basic benchmarks for timing streaming access to slices of data stored in NWB files."""

import time
from typing import Tuple

from nwb_benchmarks.core import (
    get_object_by_name,
    get_s3_url,
    read_hdf5_nwbfile_fsspec_no_cache,
    read_hdf5_nwbfile_remfile,
    read_hdf5_nwbfile_ros3,
)

# TODO: add the others
# TODO: could also expand a range of slice styles relative to chunking pattern
param_names = ["s3_url", "object_name", "slice_range"]
params = (
    [
        get_s3_url(
            dandiset_id="000717",
            dandi_path="sub-IBL-ecephys/sub-IBL-ecephys_ses-3e7ae7c0_desc-18000000-frames-13653-by-384-chunking.nwb",
        )
    ],
    ["ElectricalSeriesAp"],
    [(slice(0, 30_000), slice(0, 384))],  # ~23 MB
)


class FsspecNoCacheContinuousSliceBenchmark:
    rounds = 1
    repeat = 3
    param_names = param_names
    params = params

    def setup(self, s3_url: str, object_name: str, slice_range: Tuple[slice]):
        self.nwbfile, self.io, self.file, self.bytestream = read_hdf5_nwbfile_fsspec_no_cache(s3_url=s3_url)
        self.neurodata_object = get_object_by_name(nwbfile=self.nwbfile, object_name=object_name)
        self.data_to_slice = self.neurodata_object.data

    def time_slice(self, s3_url: str, object_name: str, slice_range: Tuple[slice]):
        """Note: store as self._temp to avoid tracking garbage collection as well."""
        self._temp = self.data_to_slice[slice_range]


class RemfileContinuousSliceBenchmark:
    rounds = 1
    repeat = 3
    param_names = param_names
    params = params

    def setup(self, s3_url: str, object_name: str, slice_range: Tuple[slice]):
        self.nwbfile, self.io, self.file, self.bytestream = read_hdf5_nwbfile_remfile(s3_url=s3_url)
        self.neurodata_object = get_object_by_name(nwbfile=self.nwbfile, object_name="ElectricalSeriesAp")
        self.data_to_slice = self.neurodata_object.data

    def time_slice(self, s3_url: str, object_name: str, slice_range: Tuple[slice]):
        """Note: store as self._temp to avoid tracking garbage collection as well."""
        self._temp = self.data_to_slice[slice_range]


class Ros3ContinuousSliceBenchmark:
    rounds = 1
    repeat = 3
    param_names = param_names
    params = params

    def setup(self, s3_url: str, object_name: str, slice_range: Tuple[slice]):
        self.nwbfile, self.io, _ = read_hdf5_nwbfile_ros3(s3_url=s3_url)
        self.neurodata_object = get_object_by_name(nwbfile=self.nwbfile, object_name="ElectricalSeriesAp")
        self.data_to_slice = self.neurodata_object.data

    def time_slice(self, s3_url: str, object_name: str, slice_range: Tuple[slice]):
        """Note: store as self._temp to avoid tracking garbage collection as well."""
        self.data_to_slice[slice_range]
