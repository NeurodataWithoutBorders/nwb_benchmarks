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
    IBLTestCase1=dict(
        s3_url=get_s3_url(
            dandiset_id="000717",
            dandi_path="sub-IBL-ecephys/sub-IBL-ecephys_ses-3e7ae7c0_desc-18000000-frames-13653-by-384-chunking.nwb",
        ),
        object_name="ElectricalSeriesAp",
        slice_range=(slice(0, 30_000), slice(0, 384)),  #  ~23 MB
    )
)

# Parameters for LINDI pointing to a remote LINDI reference file system JSON file. I.e., here we do not
# to create the JSON but can load it directly from the remote store
# Parameters for LINDI pointing to a remote LINDI reference file system JSON file
lindi_remote_rfs_parameter_cases = dict(
    EcephysTestCase=dict(
        s3_url=get_s3_url(
            is_staging=True,
            dandiset_id="213889",
            dandi_path="sub-IBL-ecephys/sub-IBL-ecephys_ses-3e7ae7c0_desc-18000000-frames-13653-by-384-chunking.lindi.json",
        ),
        object_name="ElectricalSeriesAp",
        slice_range=(slice(0, 30_000), slice(0, 384)),
    ),
    OphysTestCase=dict(
        s3_url=get_s3_url(
            is_staging=True,
            dandiset_id="213889",
            dandi_path="sub-R6_ses-20200206T210000_behavior+ophys/sub-R6_ses-20200206T210000_behavior+ophys.lindi.json",
        ),
        object_name="TwoPhotonSeries",
        slice_range=(slice(0, 3), slice(0, 796), slice(0, 512)),
    ),
    IcephysTestCase=dict(
        s3_url=get_s3_url(
            is_staging=True,
            dandiset_id="213889",
            dandi_path="sub-1214579789_ses-1214621812_icephys/sub-1214579789_ses-1214621812_icephys.lindi.json",
        ),
        object_name="data_00002_AD0",
        slice_range=(slice(0, 30_000), ),
    ),
    # # TODO: Just an example case for testing. Replace with real test case
    # BaseExample=dict(
    #     s3_url="https://kerchunk.neurosift.org/dandi/dandisets/000939/assets/11f512ba-5bcf-4230-a8cb-dc8d36db38cb/zarr.json",
    #     object_name="accelerometer",
    #     slice_range=(slice(0, 30_000), slice(0, 3)),
    # ),
)


zarr_parameter_cases = dict(
    AIBSTestCase=dict(
        s3_url=(
            "s3://aind-open-data/ecephys_625749_2022-08-03_15-15-06_nwb_2023-05-16_16-34-55/"
            "ecephys_625749_2022-08-03_15-15-06_nwb/"
            "ecephys_625749_2022-08-03_15-15-06_experiment1_recording1.nwb.zarr/"
        ),
        object_name="ElectricalSeriesProbe A-LFP",
        slice_range=(slice(0, 30_000), slice(0, 384)),
    )
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
