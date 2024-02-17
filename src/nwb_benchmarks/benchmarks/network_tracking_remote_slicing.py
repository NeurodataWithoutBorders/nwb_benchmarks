"""Basic benchmarks for stream NWB files and their contents."""

from typing import Tuple

from nwb_benchmarks.core import (
    BaseNetworkBenchmark,
    get_object_by_name,
    get_s3_url,
    read_hdf5_nwbfile_fsspec_no_cache,
    read_hdf5_nwbfile_remfile,
    read_hdf5_nwbfile_ros3,
    robust_ros3_read,
)

# Network base does not yet support params
# Might have more luck using parameterized.paramterize_class
# param_names = ["s3_url", "object_name", "slice_range"]
# params = (
#     [
#         get_s3_url(
#             dandiset_id="000717",
#             dandi_path="sub-IBL-ecephys/sub-IBL-ecephys_ses-3e7ae7c0_desc-18000000-frames-13653-by-384-chunking.nwb",
#         )
#     ],
#     ["ElectricalSeriesAp"],
#     [(slice(0, 30_000), slice(0, 384))],  # ~23 MB
# )
s3_url = get_s3_url(
    dandiset_id="000717",
    dandi_path="sub-IBL-ecephys/sub-IBL-ecephys_ses-3e7ae7c0_desc-18000000-frames-13653-by-384-chunking.nwb",
)
object_name = "ElectricalSeriesAp"
slice_range = (slice(0, 30_000), slice(0, 384))


class FsspecNoCacheContinuousSliceBenchmark(BaseNetworkBenchmark):
    s3_url = s3_url
    object_name = object_name
    slice_range = slice_range

    def setup_cache(self, s3_url: str, object_name: str, slice_range: Tuple[slice]):
        super().setup_cache(s3_url=self.s3_url, object_name=self.object_name, slice_range=self.slice_range)

    def setup(self, s3_url: str, object_name: str, slice_range: Tuple[slice]):
        self.nwbfile, self.io, self.file, self.bytestream = read_hdf5_nwbfile_fsspec_no_cache(s3_url=s3_url)
        self.neurodata_object = get_object_by_name(nwbfile=self.nwbfile, object_name=object_name)
        self.data_to_slice = self.neurodata_object.data

    def operation_to_track_network_activity_of(self, s3_url: str, object_name: str, slice_range: Tuple[slice]):
        self._temp = self.data_to_slice[slice_range]


class RemfileContinuousSliceBenchmark(BaseNetworkBenchmark):
    s3_url = s3_url
    object_name = object_name
    slice_range = slice_range

    def setup_cache(self, s3_url: str, object_name: str, slice_range: Tuple[slice]):
        super().setup_cache(s3_url=self.s3_url, object_name=self.object_name, slice_range=self.slice_range)

    def setup(self, s3_url: str, object_name: str, slice_range: Tuple[slice]):
        self.nwbfile, self.io, self.file, self.bytestream = read_hdf5_nwbfile_remfile(s3_url=s3_url)
        self.neurodata_object = get_object_by_name(nwbfile=self.nwbfile, object_name=object_name)
        self.data_to_slice = self.neurodata_object.data

    def operation_to_track_network_activity_of(self, s3_url: str, object_name: str, slice_range: Tuple[slice]):
        self._temp = self.data_to_slice[slice_range]


class Ros3ContinuousSliceBenchmark(BaseNetworkBenchmark):
    s3_url = s3_url
    object_name = object_name
    slice_range = slice_range

    def setup_cache(self, s3_url: str, object_name: str, slice_range: Tuple[slice]):
        super().setup_cache(s3_url=self.s3_url, object_name=self.object_name, slice_range=self.slice_range)

    def setup(self, s3_url: str, object_name: str, slice_range: Tuple[slice]):
        self.nwbfile, self.io = read_hdf5_nwbfile_ros3(s3_url=s3_url)
        self.neurodata_object = get_object_by_name(nwbfile=self.nwbfile, object_name=object_name)
        self.data_to_slice = self.neurodata_object.data

    def operation_to_track_network_activity_of(self, s3_url: str, object_name: str, slice_range: Tuple[slice]):
        self._temp = robust_ros3_read(command=self.data_to_slice.__getitem__, command_args=(slice_range,))
