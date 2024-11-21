"""Basic benchmarks for profiling network statistics for streaming access to slices of data stored in NWB files."""

from typing import Tuple

from asv_runner.benchmarks.mark import skip_benchmark_if

from nwb_benchmarks import TSHARK_PATH
from nwb_benchmarks.core import (
    BaseBenchmark,
    get_object_by_name,
    get_s3_url,
    network_activity_tracker,
    read_hdf5_nwbfile_fsspec_no_cache,
    read_hdf5_nwbfile_fsspec_with_cache,
    read_hdf5_nwbfile_lindi,
    read_hdf5_nwbfile_remfile,
    read_hdf5_nwbfile_remfile_with_cache,
    read_hdf5_nwbfile_ros3,
    read_zarr_nwbfile,
    robust_ros3_read,
)

parameter_cases = dict(
    ICEphysTestCase=dict(
        s3_url=get_s3_url(
            dandiset_id="000717",
            dandi_path="sub-1214579789_ses-1214621812_icephys/sub-1214579789_ses-1214621812_icephys.nwb",
        ),
    ),
    EPhysTestCase=dict(
        s3_url=get_s3_url(
            dandiset_id="000717",
            dandi_path="sub-npI3_ses-20190421_behavior+ecephys/sub-npI3_ses-20190421_behavior+ecephys.nwb",
        ),
    ),
    OPhysTestCase=dict(
        s3_url=get_s3_url(
            dandiset_id="000717",
            dandi_path="sub-R6_ses-20200206T210000_behavior+ophys/sub-R6_ses-20200206T210000_behavior+ophys.nwb",
        ),
    ),
)

# Parameters for LINDI pointing to a remote LINDI reference file system JSON file. I.e., here we do not
# to create the JSON but can load it directly from the remote store
# Parameters for LINDI pointing to a remote LINDI reference file system JSON file
lindi_remote_rfs_parameter_cases = dict(
    EcephysTestCase=dict(
        s3_url=get_s3_url(
            dandiset_id="213889",
            dandi_path="sub-ecephys/c493119b-4b99-4b14-bc03-65bb28cfbd29.lindi.json",
        ),
    ),
    OphysTestCase=dict(
        s3_url=get_s3_url(
            dandiset_id="213889",
            dandi_path="sub-R6_ses-20200206T210000_behavior+ophys/sub-R6_ses-20200206T210000_behavior+ophys.lindi.json",
        ),
    ),
    IcephysTestCase=dict(
        s3_url=get_s3_url(
            dandiset_id="213889",
            dandi_path="sub-1214579789_ses-1214621812_icephys/sub-1214579789_ses-1214621812_icephys.lindi.json",
        ),
    ),
)


zarr_parameter_cases = dict(
    ZarrICEphysTestCase=dict(s3_url="s3://dandiarchive/zarr/2e8d0cb4-c5d4-4abc-88d8-2581c3cf7f5a/"),
    ZarrOPhysTestCase=dict(s3_url="s3://dandiarchive/zarr/c8c6b848-fbc6-4f58-85ff-e3f2618ee983/"),
)


@skip_benchmark_if(TSHARK_PATH is None)
class FsspecNoCacheContinuousSliceBenchmark(BaseBenchmark):
    parameter_cases = parameter_cases

    def setup(self, s3_url: str, object_name: str, slice_range: Tuple[slice]):
        self.nwbfile, self.io, self.file, self.bytestream = read_hdf5_nwbfile_fsspec_no_cache(s3_url=s3_url)
        self.neurodata_object = get_object_by_name(nwbfile=self.nwbfile, object_name=object_name)
        self.data_to_slice = self.neurodata_object.data

    def track_network_activity_during_slice(self, s3_url: str, object_name: str, slice_range: Tuple[slice]):
        with network_activity_tracker(tshark_path=TSHARK_PATH) as network_tracker:
            self._temp = self.data_to_slice[slice_range]
        return network_tracker.asv_network_statistics


@skip_benchmark_if(TSHARK_PATH is None)
class FsspecWithCacheContinuousSliceBenchmark(BaseBenchmark):
    parameter_cases = parameter_cases

    def teardown(self, s3_url: str, object_name: str, slice_range: Tuple[slice]):
        self.tmpdir.cleanup()

    def setup(self, s3_url: str, object_name: str, slice_range: Tuple[slice]):
        self.nwbfile, self.io, self.file, self.bytestream, self.tmpdir = read_hdf5_nwbfile_fsspec_with_cache(
            s3_url=s3_url
        )
        self.neurodata_object = get_object_by_name(nwbfile=self.nwbfile, object_name=object_name)
        self.data_to_slice = self.neurodata_object.data

    def track_network_activity_during_slice(self, s3_url: str, object_name: str, slice_range: Tuple[slice]):
        with network_activity_tracker(tshark_path=TSHARK_PATH) as network_tracker:
            self._temp = self.data_to_slice[slice_range]
        return network_tracker.asv_network_statistics


@skip_benchmark_if(TSHARK_PATH is None)
class RemfileContinuousSliceBenchmark(BaseBenchmark):
    parameter_cases = parameter_cases

    def setup(self, s3_url: str, object_name: str, slice_range: Tuple[slice]):
        self.nwbfile, self.io, self.file, self.bytestream = read_hdf5_nwbfile_remfile(s3_url=s3_url)
        self.neurodata_object = get_object_by_name(nwbfile=self.nwbfile, object_name=object_name)
        self.data_to_slice = self.neurodata_object.data

    def track_network_activity_during_slice(self, s3_url: str, object_name: str, slice_range: Tuple[slice]):
        with network_activity_tracker(tshark_path=TSHARK_PATH) as network_tracker:
            self._temp = self.data_to_slice[slice_range]
        return network_tracker.asv_network_statistics


@skip_benchmark_if(TSHARK_PATH is None)
class RemfileContinuousSliceBenchmarkWithCache(BaseBenchmark):
    parameter_cases = parameter_cases

    def teardown(self, s3_url: str, object_name: str, slice_range: Tuple[slice]):
        self.tmpdir.cleanup()

    def setup(self, s3_url: str, object_name: str, slice_range: Tuple[slice]):
        self.nwbfile, self.io, self.file, self.bytestream, self.tmpdir = read_hdf5_nwbfile_remfile_with_cache(
            s3_url=s3_url
        )
        self.neurodata_object = get_object_by_name(nwbfile=self.nwbfile, object_name=object_name)
        self.data_to_slice = self.neurodata_object.data

    def track_network_activity_during_slice(self, s3_url: str, object_name: str, slice_range: Tuple[slice]):
        with network_activity_tracker(tshark_path=TSHARK_PATH) as network_tracker:
            self._temp = self.data_to_slice[slice_range]
        return network_tracker.asv_network_statistics


@skip_benchmark_if(TSHARK_PATH is None)
class Ros3ContinuousSliceBenchmark(BaseBenchmark):
    parameter_cases = parameter_cases

    def setup(self, s3_url: str, object_name: str, slice_range: Tuple[slice]):
        self.nwbfile, self.io, _ = read_hdf5_nwbfile_ros3(s3_url=s3_url)
        self.neurodata_object = get_object_by_name(nwbfile=self.nwbfile, object_name=object_name)
        self.data_to_slice = self.neurodata_object.data

    def track_network_activity_during_slice(self, s3_url: str, object_name: str, slice_range: Tuple[slice]):
        with network_activity_tracker(tshark_path=TSHARK_PATH) as network_tracker:
            self._temp, self.retries = robust_ros3_read(
                command=self.data_to_slice.__getitem__, command_args=(slice_range,)
            )
        network_tracker.asv_network_statistics.update(retries=self.retries)
        return network_tracker.asv_network_statistics


@skip_benchmark_if(TSHARK_PATH is None)
class LindiFileReadRemoteReferenceFileSystemContinuousSliceBenchmark(BaseBenchmark):
    """
    Time the read of a data slice from a remote NWB file with pynwb using lindi with a remote JSON reference
    filesystem available.
    """

    parameter_cases = lindi_remote_rfs_parameter_cases

    def setup(self, s3_url: str, object_name: str, slice_range: Tuple[slice]):
        self.nwbfile, self.io, self.client = read_hdf5_nwbfile_lindi(rfs=s3_url)
        self.neurodata_object = get_object_by_name(nwbfile=self.nwbfile, object_name=object_name)
        self.data_to_slice = self.neurodata_object.data

    def track_network_activity_during_slice(self, s3_url: str, object_name: str, slice_range: Tuple[slice]):
        with network_activity_tracker(tshark_path=TSHARK_PATH) as network_tracker:
            self._temp = self.data_to_slice[slice_range]
        return network_tracker.asv_network_statistics


@skip_benchmark_if(TSHARK_PATH is None)
class ZarrContinuousSliceBenchmark(BaseBenchmark):
    parameter_cases = zarr_parameter_cases

    def setup(self, s3_url: str, object_name: str, slice_range: Tuple[slice]):
        self.nwbfile, self.io = read_zarr_nwbfile(s3_url=s3_url, mode="r")
        self.neurodata_object = get_object_by_name(nwbfile=self.nwbfile, object_name=object_name)
        self.data_to_slice = self.neurodata_object.data

    def track_network_activity_during_slice(self, s3_url: str, object_name: str, slice_range: Tuple[slice]):
        with network_activity_tracker(tshark_path=TSHARK_PATH) as network_tracker:
            self._temp = self.data_to_slice[slice_range]
        return network_tracker.asv_network_statistics


@skip_benchmark_if(TSHARK_PATH is None)
class ZarrForceNoConsolidatedContinuousSliceBenchmark(BaseBenchmark):
    parameter_cases = zarr_parameter_cases

    def setup(self, s3_url: str, object_name: str, slice_range: Tuple[slice]):
        self.nwbfile, self.io = read_zarr_nwbfile(s3_url=s3_url, mode="r-")
        self.neurodata_object = get_object_by_name(nwbfile=self.nwbfile, object_name=object_name)
        self.data_to_slice = self.neurodata_object.data

    def track_network_activity_during_slice(self, s3_url: str, object_name: str, slice_range: Tuple[slice]):
        with network_activity_tracker(tshark_path=TSHARK_PATH) as network_tracker:
            self._temp = self.data_to_slice[slice_range]
        return network_tracker.asv_network_statistics
