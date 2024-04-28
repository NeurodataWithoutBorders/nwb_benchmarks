"""Basic benchmarks for timing streaming access of NWB files and their contents."""

from nwb_benchmarks.core import (
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
    read_zarr,
    read_zarr_nwbfile,
)

param_names = ["s3_url"]
params = [
    get_s3_url(dandiset_id="000717", dandi_path="sub-mock/sub-mock_ses-ecephys1.nwb"),
    get_s3_url(
        dandiset_id="000717",
        dandi_path="sub-IBL-ecephys/sub-IBL-ecephys_ses-3e7ae7c0_desc-18000000-frames-13653-by-384-chunking.nwb",
    ),  # Not the best example for testing a theory about file read; should probably replace with something simpler
    "https://dandiarchive.s3.amazonaws.com/ros3test.nwb",  # The original small test NWB file
]

zarr_param_names = ["s3_url"]
zarr_params = [
    (
        "s3://aind-open-data/ecephys_625749_2022-08-03_15-15-06_nwb_2023-05-16_16-34-55/"
        "ecephys_625749_2022-08-03_15-15-06_nwb/"
        "ecephys_625749_2022-08-03_15-15-06_experiment1_recording1.nwb.zarr/"
    ),
]


class DirectFileReadBenchmark:
    """
    Time the direct read of the HDF5-backend files with `h5py` using each streaming method.

    There is no formal parsing of the `pynwb.NWBFile` object.

    Note: in all cases, store the in-memory objects to avoid timing garbage collection steps.
    """

    rounds = 1
    repeat = 3
    param_names = param_names
    params = params

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


class NWBFileReadBenchmark:
    """
    Time the read of the HDF5-backend files with `pynwb` using each streaming method.

    Note: in all cases, store the in-memory objects to avoid timing garbage collection steps.
    """

    rounds = 1
    repeat = 3
    param_names = param_names
    params = params

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


class DirectZarrFileReadBenchmark:
    """
    Time the read of the Zarr-backend files with `pynwb` using each streaming method.

    Note: in all cases, store the in-memory objects to avoid timing garbage collection steps.
    """

    rounds = 1
    repeat = 3
    param_names = zarr_param_names
    params = zarr_params

    def time_read_zarr_nwbfile(self, s3_url: str):
        self.zarr_file = read_zarr(s3_url=s3_url, open_without_consolidated_metadata=False)

    def time_read_zarr_nwbfile_force_no_consolidated(self, s3_url: str):
        self.zarr_file = read_zarr(s3_url=s3_url, open_without_consolidated_metadata=True)


class NWBZarrFileReadBenchmark:
    """
    Time the read of the Zarr-backend files with `pynwb` using each streaming method.

    Note: in all cases, store the in-memory objects to avoid timing garbage collection steps.
    """

    rounds = 1
    repeat = 3
    param_names = zarr_param_names
    params = zarr_params

    def time_read_zarr_nwbfile(self, s3_url: str):
        self.nwbfile, self.io = read_zarr_nwbfile(s3_url=s3_url, open_without_consolidated_metadata=False)

    def time_read_zarr_nwbfile_force_no_consolidated(self, s3_url: str):
        self.nwbfile, self.io = read_zarr_nwbfile(s3_url=s3_url, open_without_consolidated_metadata=True)
