"""Basic benchmarks for streaming access to NWB files and their contents."""

import os
from typing import Tuple

from asv.runner import BenchmarkResult

from nwb_benchmarks.core import (
    get_object_by_name,
    get_s3_url,
    network_activity_tracker,
    read_hdf5_nwbfile_fsspec_no_cache,
    read_hdf5_nwbfile_remfile,
    read_hdf5_nwbfile_ros3,
    robust_ros3_read,
)

TSHARK_PATH = os.environ.get("TSHARK_PATH", None)

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
    param_names = param_names
    params = params

    def setup(self, s3_url: str, object_name: str, slice_range: Tuple[slice]):
        self.nwbfile, self.io, self.file, self.bytestream = read_hdf5_nwbfile_fsspec_no_cache(s3_url=s3_url)
        self.neurodata_object = get_object_by_name(nwbfile=self.nwbfile, object_name=object_name)
        self.data_to_slice = self.neurodata_object.data

    def track_network_activity_during_slice(self, s3_url: str, object_name: str, slice_range: Tuple[slice]):
        with network_activity_tracker(tshark_path=TSHARK_PATH) as network_tracker:
            self._temp = self.data_to_slice[slice_range]
        return network_tracker.asv_network_statistics


class RemfileContinuousSliceBenchmark:
    param_names = param_names
    params = params

    def setup(self, s3_url: str, object_name: str, slice_range: Tuple[slice]):
        self.nwbfile, self.io, self.file, self.bytestream = read_hdf5_nwbfile_remfile(s3_url=s3_url)
        self.neurodata_object = get_object_by_name(nwbfile=self.nwbfile, object_name=object_name)
        self.data_to_slice = self.neurodata_object.data

    def track_network_activity_during_slice(self, s3_url: str, object_name: str, slice_range: Tuple[slice]):
        with network_activity_tracker(tshark_path=TSHARK_PATH) as network_tracker:
            self._temp = self.data_to_slice[slice_range]
        return network_tracker.asv_network_statistics


class Ros3ContinuousSliceBenchmark:
    param_names = param_names
    params = params

    def setup(self, s3_url: str, object_name: str, slice_range: Tuple[slice]):
        self.nwbfile, self.io = read_hdf5_nwbfile_ros3(s3_url=s3_url)
        self.neurodata_object = get_object_by_name(nwbfile=self.nwbfile, object_name=object_name)
        self.data_to_slice = self.neurodata_object.data

    def track_network_activity_during_slice(self, s3_url: str, object_name: str, slice_range: Tuple[slice]):
        with network_activity_tracker(tshark_path=TSHARK_PATH) as network_tracker:
            self._temp = robust_ros3_read(command=self.data_to_slice.__getitem__, command_args=(slice_range,))
        return network_tracker.asv_network_statistics
