"""Basic benchmarks for timing streaming access of NWB files and their contents."""

from nwb_benchmarks.core import (
    BaseBenchmark,
    get_s3_url,
    read_hdf5_fsspec_no_cache,
    read_hdf5_fsspec_with_cache,
    read_hdf5_nwbfile_fsspec_no_cache,
    read_hdf5_nwbfile_fsspec_with_cache,
    read_hdf5_nwbfile_remfile,
    read_hdf5_nwbfile_remfile_with_cache,
    read_hdf5_nwbfile_ros3,
    read_hdf5_remfile,
    read_hdf5_remfile_with_cache,
    read_hdf5_ros3,
)

parameter_cases = dict(
    IBLTestCase1=dict(
        s3_url=get_s3_url(dandiset_id="000717", dandi_path="sub-mock/sub-mock_ses-ecephys1.nwb"),
    ),
    # IBLTestCase2 is not the best example for testing a theory about file read; should probably replace with simpler
    IBLTestCase2=dict(
        s3_url=get_s3_url(
            dandiset_id="000717",
            dandi_path="sub-IBL-ecephys/sub-IBL-ecephys_ses-3e7ae7c0_desc-18000000-frames-13653-by-384-chunking.nwb",
        ),
    ),
    ClassicRos3TestCase=dict(s3_url="https://dandiarchive.s3.amazonaws.com/ros3test.nwb"),
)


class DirectFileReadBenchmark(BaseBenchmark):
    """
    Time the direct read of the HDF5-backend files with `h5py` using each streaming method.

    There is no formal parsing of the `pynwb.NWBFile` object.

    Note: in all cases, store the in-memory objects to avoid timing garbage collection steps.
    """

    rounds = 1
    repeat = 3
    parameter_cases = parameter_cases

    def teardown(self, s3_url: str):
        # Not all tests in the class are using a temporary dir as cache. Clean up if it does.
        if hasattr(self, "tmpdir"):
            self.tmpdir.cleanup()

    def time_read_hdf5_fsspec_no_cache(self, s3_url: str):
        self.file, self.bytestream = read_hdf5_fsspec_no_cache(s3_url=s3_url)

    def time_read_hdf5_fsspec_with_cache(self, s3_url: str):
        self.file, self.bytestream, self.tmpdir = read_hdf5_fsspec_with_cache(s3_url=s3_url)

    def time_read_hdf5_remfile(self, s3_url: str):
        self.file, self.bytestream = read_hdf5_remfile(s3_url=s3_url)

    def time_read_hdf5_remfile_with_cache(self, s3_url: str):
        self.file, self.bytestream, self.tmpdir = read_hdf5_remfile_with_cache(s3_url=s3_url)

    def time_read_hdf5_ros3(self, s3_url: str):
        self.file, _ = read_hdf5_ros3(s3_url=s3_url, retry=False)


class NWBFileReadBenchmark(BaseBenchmark):
    """
    Time the read of the HDF5-backend files with `pynwb` using each streaming method.

    Note: in all cases, store the in-memory objects to avoid timing garbage collection steps.
    """

    rounds = 1
    repeat = 3
    parameter_cases = parameter_cases

    def teardown(self, s3_url: str):
        # Not all tests in the class are using a temporary dir as cache. Clean up if it does.
        if hasattr(self, "tmpdir"):
            self.tmpdir.cleanup()

    def time_read_hdf5_nwbfile_fsspec_no_cache(self, s3_url: str):
        self.nwbfile, self.io, self.file, self.bytestream = read_hdf5_nwbfile_fsspec_no_cache(s3_url=s3_url)

    def time_read_hdf5_nwbfile_fsspec_with_cache(self, s3_url: str):
        self.nwbfile, self.io, self.file, self.bytestream, self.tmpdir = read_hdf5_nwbfile_fsspec_with_cache(
            s3_url=s3_url
        )

    def time_read_hdf5_nwbfile_remfile(self, s3_url: str):
        self.nwbfile, self.io, self.file, self.bytestream = read_hdf5_nwbfile_remfile(s3_url=s3_url)

    def time_read_hdf5_nwbfile_remfile_with_cache(self, s3_url: str):
        self.nwbfile, self.io, self.file, self.bytestream, self.tmpdir = read_hdf5_nwbfile_remfile_with_cache(
            s3_url=s3_url
        )

    def time_read_hdf5_nwbfile_ros3(self, s3_url: str):
        self.nwbfile, self.io, _ = read_hdf5_nwbfile_ros3(s3_url=s3_url, retry=False)
