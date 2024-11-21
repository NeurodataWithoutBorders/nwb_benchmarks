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
    read_zarr_nwbfile,
)

parameter_cases = dict(
    EcephysTestCase=dict(
        s3_url=get_s3_url(
            dandiset_id="000717",
            dandi_path="sub-npI3_ses-20190421_behavior+ecephys/sub-npI3_ses-20190421_behavior+ecephys.nwb",
        ),
        object_name="ElectricalSeries",  # TODO
        slice_range=(slice(0, 30_000), slice(0, 384)),
    ),
    OphysTestCase=dict(
        s3_url=get_s3_url(
            dandiset_id="000717",
            dandi_path="sub-R6_ses-20200206T210000_behavior+ophys/sub-R6_ses-20200206T210000_behavior+ophys.nwb",
        ),
        object_name="TwoPhotonSeries",
        slice_range=(slice(0, 3), slice(0, 796), slice(0, 512)),
    ),
    IcephysTestCase=dict(
        s3_url=get_s3_url(
            dandiset_id="000717",
            dandi_path="sub-1214579789_ses-1214621812_icephys/sub-1214579789_ses-1214621812_icephys.nwb",
        ),
        object_name="data_00002_AD0",
        slice_range=(slice(0, 30_000),),
    ),
)

zarr_parameter_cases = dict(
    ZarrICEphysTestCase=dict(
        s3_url="s3://dandiarchive/zarr/2e8d0cb4-c5d4-4abc-88d8-2581c3cf7f5a/",
        object_name="data_00002_AD0",
        slice_range=(slice(0, 30_000),),
    ),
    ZarrOPhysTestCase=dict(
        s3_url="s3://dandiarchive/zarr/c8c6b848-fbc6-4f58-85ff-e3f2618ee983/",
        object_name="TwoPhotonSeries",
        slice_range=(slice(0, 3), slice(0, 796), slice(0, 512)),
    ),
)


# Parameters for LINDI pointing to a remote LINDI reference file system JSON file
lindi_remote_rfs_parameter_cases = dict(
    EcephysTestCase=dict(
        s3_url=get_s3_url(
            dandiset_id="213889",
            dandi_path="sub-ecephys/c493119b-4b99-4b14-bc03-65bb28cfbd29.lindi.json",
        ),
        object_name="ElectricalSeries",
        slice_range=(slice(0, 30_000), slice(0, 384)),
    ),
    OphysTestCase=dict(
        s3_url=get_s3_url(
            dandiset_id="213889",
            dandi_path="sub-R6_ses-20200206T210000_behavior+ophys/sub-R6_ses-20200206T210000_behavior+ophys.lindi.json",
        ),
        object_name="TwoPhotonSeries",
        slice_range=(slice(0, 3), slice(0, 796), slice(0, 512)),
    ),
    IcephysTestCase=dict(
        s3_url=get_s3_url(
            dandiset_id="213889",
            dandi_path="sub-1214579789_ses-1214621812_icephys/sub-1214579789_ses-1214621812_icephys.lindi.json",
        ),
        object_name="data_00002_AD0",
        slice_range=(slice(0, 30_000),),
    ),
)


class FsspecNoCacheContinuousSliceBenchmark(BaseBenchmark):
    rounds = 1
    repeat = 3
    parameter_cases = parameter_cases

    def setup(self, s3_url: str, object_name: str, slice_range: Tuple[slice]):
        self.nwbfile, self.io, self.file, self.bytestream = read_hdf5_nwbfile_fsspec_no_cache(s3_url=s3_url)
        self.neurodata_object = get_object_by_name(nwbfile=self.nwbfile, object_name=object_name)
        self.data_to_slice = self.neurodata_object.data

    def time_slice(self, s3_url: str, object_name: str, slice_range: Tuple[slice]):
        """Track network activity for slicing into a h5py.Dataset with Fsspec"""
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
        """Track network activity for slicing into a h5py.Dataset with Fsspec"""
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
        """Track network activity for slicing into a h5py.Dataset with RemFile"""
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
        """Track network activity for slicing into a h5py.Dataset with RemFile"""
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
        """Track network activity for slicing into a h5py.Dataset with Ros3"""
        self._temp = self.data_to_slice[slice_range]


class LindiFileReadRemoteReferenceFileSystemContinuousSliceBenchmark(BaseBenchmark):
    """
    Time the read of a data slice from a remote NWB file with pynwb using lindi with a remote JSON reference
    filesystem available.
    """

    rounds = 1
    repeat = 3
    parameter_cases = lindi_remote_rfs_parameter_cases

    def setup(self, s3_url: str, object_name: str, slice_range: Tuple[slice]):
        self.nwbfile, self.io, self.client = read_hdf5_nwbfile_lindi(rfs=s3_url)
        self.neurodata_object = get_object_by_name(nwbfile=self.nwbfile, object_name=object_name)
        self.data_to_slice = self.neurodata_object.data

    def time_slice(self, s3_url: str, object_name: str, slice_range: Tuple[slice]):
        """Track network activity for slicing into a LindiDataset"""
        self._temp = self.data_to_slice[slice_range]


class ZarrContinuousSliceBenchmark(BaseBenchmark):
    """
    Benchmark network activity for slicing into a Zarr dataset using consolidated metadata (if available)
    """

    rounds = 1
    repeat = 3
    parameter_cases = zarr_parameter_cases

    def setup(self, s3_url: str, object_name: str, slice_range: Tuple[slice]):
        self.nwbfile, self.io = read_zarr_nwbfile(s3_url=s3_url, mode="r")
        self.neurodata_object = get_object_by_name(nwbfile=self.nwbfile, object_name=object_name)
        self.data_to_slice = self.neurodata_object.data

    def time_slice(self, s3_url: str, object_name: str, slice_range: Tuple[slice]):
        """Track network activity for slicing into a Zarr dataset"""
        self._temp = self.data_to_slice[slice_range]


class ZarrForceNoConsolidatedContinuousSliceBenchmark(BaseBenchmark):
    """
    Benchmark network activity for slicing into a Zarr dataset without using consolidated metadata
    """

    rounds = 1
    repeat = 3
    parameter_cases = zarr_parameter_cases

    def setup(self, s3_url: str, object_name: str, slice_range: Tuple[slice]):
        self.nwbfile, self.io = read_zarr_nwbfile(s3_url=s3_url, mode="r-")
        self.neurodata_object = get_object_by_name(nwbfile=self.nwbfile, object_name=object_name)
        self.data_to_slice = self.neurodata_object.data

    def time_slice(self, s3_url: str, object_name: str, slice_range: Tuple[slice]):
        """Track network activity for slicing into a Zarr dataset"""
        self._temp = self.data_to_slice[slice_range]
