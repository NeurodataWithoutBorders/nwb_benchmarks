"""Basic benchmarks for stream NWB files and their contents."""

import parameterized

from nwb_benchmarks.core import (
    get_object_by_name,
    get_s3_url,
    read_hdf5_nwbfile_fsspec_no_cache,
    read_hdf5_nwbfile_remfile,
    read_hdf5_nwbfile_ros3,
    robust_ros3_read,
)

# TODO: add the others
# TODO: could also expand a range of slice styles relative to chunking pattern
remote_slicing_test_files = [
    dict(
        s3_url=get_s3_url(
            dandiset_id="000717",
            dandi_path="sub-IBL-ecephys/sub-IBL-ecephys_ses-3e7ae7c0_desc-18000000-frames-13653-by-384-chunking.nwb",
        ),
        slice_range=(slice(0, 30_000), slice(0, 384)),  # ~23 MB
    ),
]


@parameterized.parameterized_class(remote_slicing_test_files)
class FsspecNoCacheContinuousSliceBenchmark:
    repeat = 1

    def setup(self):
        self.nwbfile, self.io, self.file, self.bytestream = read_hdf5_nwbfile_fsspec_no_cache(s3_url=self.s3_url)
        self.neurodata_object = get_object_by_name(nwbfile=self.nwbfile, object_name="ElectricalSeriesAp")
        self.data_to_slice = self.neurodata_object.data

    def time_slice(self):
        """Note: store as self._temp to avoid tracking garbage collection as well."""
        self._temp = self.data[self.slice_range]


@parameterized.parameterized_class(remote_slicing_test_files)
class RemfileContinuousSliceBenchmark:
    repeat = 1

    def setup(self):
        self.nwbfile, self.io, self.file, self.bytestream = read_hdf5_nwbfile_remfile(s3_url=self.s3_url)
        self.neurodata_object = get_object_by_name(nwbfile=self.nwbfile, object_name="ElectricalSeriesAp")
        self.data_to_slice = self.neurodata_object.data

    def time_slice(self):
        """Note: store as self._temp to avoid tracking garbage collection as well."""
        self._temp = self.data[self.slice_range]


@parameterized.parameterized_class(remote_slicing_test_files)
class Ros3ContinuousSliceBenchmark:
    repeat = 1

    def setup(self):
        self.nwbfile, self.io = read_hdf5_nwbfile_ros3(s3_url=self.s3_url)
        self.neurodata_object = get_object_by_name(nwbfile=self.nwbfile, object_name="ElectricalSeriesAp")
        self.data_to_slice = self.neurodata_object.data

    def time_slice(self):
        """Note: store as self._temp to avoid tracking garbage collection as well."""
        self._temp = robust_ros3_read(command=self.data.__getitem__, command_args=(self.slice_range,))
