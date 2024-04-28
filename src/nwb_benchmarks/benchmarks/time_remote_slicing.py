"""Basic benchmarks for timing streaming access to slices of data stored in NWB files."""

import time
from typing import Tuple

from nwb_benchmarks.core import (
    BaseBenchmark,
    get_object_by_name,
    get_s3_url,
    read_hdf5_nwbfile_fsspec_no_cache,
    read_hdf5_nwbfile_fsspec_with_cache,
    read_hdf5_nwbfile_lindi,
    read_hdf5_nwbfile_remfile,
    read_hdf5_nwbfile_remfile_with_cache,
    read_hdf5_nwbfile_ros3,
)

parameter_cases = dict(
    IBLTestCase1=dict(
        s3_url=get_s3_url(
            dandiset_id="000717",
            dandi_path="sub-IBL-ecephys/sub-IBL-ecephys_ses-3e7ae7c0_desc-18000000-frames-13653-by-384-chunking.nwb",
        ),
        object_name="ElectricalSeriesAp",
        slice_range=(slice(0, 30_000), slice(0, 384)),  #  ~23 MB
    )
)


# Parameters for LINDI pointing to a remote LINDI reference file system JSON file
lindi_remote_rfs_param_names = param_names
lindi_remote_rfs_params = [
    ["https://kerchunk.neurosift.org/dandi/dandisets/000939/assets/11f512ba-5bcf-4230-a8cb-dc8d36db38cb/zarr.json"],
    ["accelerometer"],
    [(slice(0, 30_000), slice(0, 3))],
]


class FsspecNoCacheContinuousSliceBenchmark(BaseBenchmark):
    rounds = 1
    repeat = 3
    parameter_cases = parameter_cases

    def setup(self, s3_url: str, object_name: str, slice_range: Tuple[slice]):
        self.nwbfile, self.io, self.file, self.bytestream = read_hdf5_nwbfile_fsspec_no_cache(s3_url=s3_url)
        self.neurodata_object = get_object_by_name(nwbfile=self.nwbfile, object_name=object_name)
        self.data_to_slice = self.neurodata_object.data

    def time_slice(self, s3_url: str, object_name: str, slice_range: Tuple[slice]):
        """Note: store as self._temp to avoid tracking garbage collection as well."""
        self._temp = self.data_to_slice[slice_range]


class FsspecWithCacheContinuousSliceBenchmark(BaseBenchmark):
    rounds = 1
    repeat = 3
    parameter_cases = parameter_cases

    def teardown(self, s3_url: str, object_name: str, slice_range: Tuple[slice]):
        self.tmpdir.cleanup()

    def setup(self, s3_url: str, object_name: str, slice_range: Tuple[slice]):
        self.nwbfile, self.io, self.file, self.bytestream, self.tmpdir = read_hdf5_nwbfile_fsspec_with_cache(
            s3_url=s3_url
        )
        self.neurodata_object = get_object_by_name(nwbfile=self.nwbfile, object_name=object_name)
        self.data_to_slice = self.neurodata_object.data

    def time_slice(self, s3_url: str, object_name: str, slice_range: Tuple[slice]):
        """Note: store as self._temp to avoid tracking garbage collection as well."""
        self._temp = self.data_to_slice[slice_range]


class RemfileNoCacheContinuousSliceBenchmark(BaseBenchmark):
    rounds = 1
    repeat = 3
    parameter_cases = parameter_cases

    def setup(self, s3_url: str, object_name: str, slice_range: Tuple[slice]):
        self.nwbfile, self.io, self.file, self.bytestream = read_hdf5_nwbfile_remfile(s3_url=s3_url)
        self.neurodata_object = get_object_by_name(nwbfile=self.nwbfile, object_name=object_name)
        self.data_to_slice = self.neurodata_object.data

    def time_slice(self, s3_url: str, object_name: str, slice_range: Tuple[slice]):
        """Note: store as self._temp to avoid tracking garbage collection as well."""
        self._temp = self.data_to_slice[slice_range]


class RemfileWithCacheContinuousSliceBenchmark(BaseBenchmark):
    rounds = 1
    repeat = 3
    parameter_cases = parameter_cases

    def teardown(self, s3_url: str, object_name: str, slice_range: Tuple[slice]):
        self.tmpdir.cleanup()

    def setup(self, s3_url: str, object_name: str, slice_range: Tuple[slice]):
        self.nwbfile, self.io, self.file, self.bytestream, self.tmpdir = read_hdf5_nwbfile_remfile_with_cache(
            s3_url=s3_url
        )
        self.neurodata_object = get_object_by_name(nwbfile=self.nwbfile, object_name=object_name)
        self.data_to_slice = self.neurodata_object.data

    def time_slice(self, s3_url: str, object_name: str, slice_range: Tuple[slice]):
        """Note: store as self._temp to avoid tracking garbage collection as well."""
        self._temp = self.data_to_slice[slice_range]


class Ros3ContinuousSliceBenchmark(BaseBenchmark):
    rounds = 1
    repeat = 3
    parameter_cases = parameter_cases

    def setup(self, s3_url: str, object_name: str, slice_range: Tuple[slice]):
        self.nwbfile, self.io, _ = read_hdf5_nwbfile_ros3(s3_url=s3_url)
        self.neurodata_object = get_object_by_name(nwbfile=self.nwbfile, object_name=object_name)
        self.data_to_slice = self.neurodata_object.data

    def time_slice(self, s3_url: str, object_name: str, slice_range: Tuple[slice]):
        """Note: store as self._temp to avoid tracking garbage collection as well."""
        self.data_to_slice[slice_range]


class NWBLindiFileReadRemoteReferenceFileSystemBenchmark:
    """
    Time the read of a data slice from a remote NWB file with pynwb using lindi with a remote JSON reference
    filesystem available.
    """

    rounds = 1
    repeat = 3
    param_names = lindi_remote_rfs_param_names
    params = lindi_remote_rfs_params

    def setup(self, s3_url: str, object_name: str, slice_range: Tuple[slice]):
        self.nwbfile, self.io, self.client = read_hdf5_nwbfile_lindi(rfs=s3_url)
        self.neurodata_object = get_object_by_name(nwbfile=self.nwbfile, object_name=object_name)
        self.data_to_slice = self.neurodata_object.data

    def time_slice(self, s3_url: str, object_name: str, slice_range: Tuple[slice]):
        """Note: store as self._temp to avoid tracking garbage collection as well."""
        self.data_to_slice[slice_range]
