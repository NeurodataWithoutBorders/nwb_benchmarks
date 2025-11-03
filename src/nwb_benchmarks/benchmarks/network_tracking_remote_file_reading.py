"""
Basic benchmarks for profiling network statistics for streaming access to NWB files and their contents.

The benchmarks should be consistent with the timing benchmarks - each function should be the same but wrapped in a
network activity tracker.
"""

import shutil

from asv_runner.benchmarks.mark import skip_benchmark_if

from nwb_benchmarks import TSHARK_PATH
from nwb_benchmarks.core import (
    BaseBenchmark,
    download_asset_if_not_exists,
    network_activity_tracker,
    read_hdf5_h5py_fsspec_https_no_cache,
    read_hdf5_h5py_fsspec_https_with_cache,
    read_hdf5_h5py_fsspec_s3_no_cache,
    read_hdf5_h5py_fsspec_s3_with_cache,
    read_hdf5_h5py_lindi,
    read_hdf5_h5py_remfile_no_cache,
    read_hdf5_h5py_remfile_with_cache,
    read_hdf5_h5py_ros3,
    read_hdf5_pynwb_fsspec_https_no_cache,
    read_hdf5_pynwb_fsspec_https_with_cache,
    read_hdf5_pynwb_fsspec_s3_no_cache,
    read_hdf5_pynwb_fsspec_s3_with_cache,
    read_hdf5_pynwb_lindi,
    read_hdf5_pynwb_remfile_no_cache,
    read_hdf5_pynwb_remfile_with_cache,
    read_hdf5_pynwb_ros3,
    read_zarr_pynwb_s3,
    read_zarr_zarrpython_https,
    read_zarr_zarrpython_s3,
)

from .params import (
    hdf5_redirected_read_params,
    lindi_no_redirect_download_params,
    zarr_direct_read_params,
)


class HDF5H5pyFileReadBenchmark(BaseBenchmark):
    """
    Track the network activity during read of remote HDF5 files with h5py using each streaming method.

    There is no formal parsing of the `pynwb.NWBFile` object.

    Note: in all cases, store the in-memory objects to be consistent with timing benchmarks.
    """

    params = hdf5_redirected_read_params

    def teardown(self, params: dict[str, str]):
        if hasattr(self, "file"):
            self.file.close()
        if hasattr(self, "bytestream"):
            self.bytestream.close()
        if hasattr(self, "tmpdir"):
            shutil.rmtree(path=self.tmpdir.name, ignore_errors=True)
            self.tmpdir.cleanup()

    @skip_benchmark_if(TSHARK_PATH is None)
    def track_network_read_hdf5_h5py_fsspec_https_no_cache(self, params: dict[str, str]):
        """Read remote HDF5 file using h5py and fsspec with HTTPS without cache."""
        https_url = params["https_url"]
        with network_activity_tracker(tshark_path=TSHARK_PATH) as network_tracker:
            self.file, self.bytestream = read_hdf5_h5py_fsspec_https_no_cache(https_url=https_url)
        return network_tracker.asv_network_statistics

    @skip_benchmark_if(TSHARK_PATH is None)
    def track_network_read_hdf5_h5py_fsspec_https_with_cache(self, params: dict[str, str]):
        """Read remote HDF5 file using h5py and fsspec with HTTPS with cache."""
        https_url = params["https_url"]
        with network_activity_tracker(tshark_path=TSHARK_PATH) as network_tracker:
            self.file, self.bytestream, self.tmpdir = read_hdf5_h5py_fsspec_https_with_cache(https_url=https_url)
        return network_tracker.asv_network_statistics

    @skip_benchmark_if(TSHARK_PATH is None)
    def track_network_read_hdf5_h5py_fsspec_s3_no_cache(self, params: dict[str, str]):
        """Read remote HDF5 file using h5py and fsspec with S3 without cache."""
        https_url = params["https_url"]
        with network_activity_tracker(tshark_path=TSHARK_PATH) as network_tracker:
            self.file, self.bytestream = read_hdf5_h5py_fsspec_s3_no_cache(https_url=https_url)
        return network_tracker.asv_network_statistics

    @skip_benchmark_if(TSHARK_PATH is None)
    def track_network_read_hdf5_h5py_fsspec_s3_with_cache(self, params: dict[str, str]):
        """Read remote HDF5 file using h5py and fsspec with S3 with cache."""
        https_url = params["https_url"]
        with network_activity_tracker(tshark_path=TSHARK_PATH) as network_tracker:
            self.file, self.bytestream, self.tmpdir = read_hdf5_h5py_fsspec_s3_with_cache(https_url=https_url)
        return network_tracker.asv_network_statistics

    @skip_benchmark_if(TSHARK_PATH is None)
    def track_network_read_hdf5_h5py_remfile_no_cache(self, params: dict[str, str]):
        """Read remote HDF5 file using h5py and remfile without cache."""
        https_url = params["https_url"]
        with network_activity_tracker(tshark_path=TSHARK_PATH) as network_tracker:
            self.file, self.bytestream = read_hdf5_h5py_remfile_no_cache(https_url=https_url)
        return network_tracker.asv_network_statistics

    @skip_benchmark_if(TSHARK_PATH is None)
    def track_network_read_hdf5_h5py_remfile_with_cache(self, params: dict[str, str]):
        """Read remote HDF5 file using h5py and remfile with cache."""
        https_url = params["https_url"]
        with network_activity_tracker(tshark_path=TSHARK_PATH) as network_tracker:
            self.file, self.bytestream, self.tmpdir = read_hdf5_h5py_remfile_with_cache(https_url=https_url)
        return network_tracker.asv_network_statistics

    @skip_benchmark_if(TSHARK_PATH is None)
    def track_network_read_hdf5_h5py_ros3(self, params: dict[str, str]):
        """Read remote HDF5 file using h5py and the ROS3 HDF5 driver."""
        https_url = params["https_url"]
        with network_activity_tracker(tshark_path=TSHARK_PATH) as network_tracker:
            self.file, _ = read_hdf5_h5py_ros3(https_url=https_url)
        return network_tracker.asv_network_statistics


class HDF5PyNWBFileReadBenchmark(BaseBenchmark):
    """
    Track the network activity during read of remote HDF5 NWB files with pynwb using each streaming method.

    Note: in all cases, store the in-memory objects to be consistent with timing benchmarks.
    """

    params = hdf5_redirected_read_params

    def teardown(self, params: dict[str, str]):
        if hasattr(self, "file"):
            self.file.close()
        if hasattr(self, "bytestream"):
            self.bytestream.close()
        if hasattr(self, "tmpdir"):
            shutil.rmtree(path=self.tmpdir.name, ignore_errors=True)
            self.tmpdir.cleanup()

    @skip_benchmark_if(TSHARK_PATH is None)
    def track_network_read_hdf5_pynwb_fsspec_https_no_cache(self, params: dict[str, str]):
        """Read remote NWB file using pynwb and fsspec with HTTPS without cache."""
        https_url = params["https_url"]
        with network_activity_tracker(tshark_path=TSHARK_PATH) as network_tracker:
            self.nwbfile, self.io, self.file, self.bytestream = read_hdf5_pynwb_fsspec_https_no_cache(
                https_url=https_url
            )
        return network_tracker.asv_network_statistics

    @skip_benchmark_if(TSHARK_PATH is None)
    def track_network_read_hdf5_pynwb_fsspec_https_with_cache(self, params: dict[str, str]):
        """Read remote NWB file using pynwb and fsspec with HTTPS with cache."""
        https_url = params["https_url"]
        with network_activity_tracker(tshark_path=TSHARK_PATH) as network_tracker:
            self.nwbfile, self.io, self.file, self.bytestream, self.tmpdir = read_hdf5_pynwb_fsspec_https_with_cache(
                https_url=https_url
            )
        return network_tracker.asv_network_statistics

    @skip_benchmark_if(TSHARK_PATH is None)
    def track_network_read_hdf5_pynwb_fsspec_s3_no_cache(self, params: dict[str, str]):
        """Read remote NWB file using pynwb and fsspec with S3 without cache."""
        https_url = params["https_url"]
        with network_activity_tracker(tshark_path=TSHARK_PATH) as network_tracker:
            self.nwbfile, self.io, self.file, self.bytestream = read_hdf5_pynwb_fsspec_s3_no_cache(https_url=https_url)
        return network_tracker.asv_network_statistics

    @skip_benchmark_if(TSHARK_PATH is None)
    def track_network_read_hdf5_pynwb_fsspec_s3_with_cache(self, params: dict[str, str]):
        """Read remote NWB file using pynwb and fsspec with S3 with cache."""
        https_url = params["https_url"]
        with network_activity_tracker(tshark_path=TSHARK_PATH) as network_tracker:
            self.nwbfile, self.io, self.file, self.bytestream, self.tmpdir = read_hdf5_pynwb_fsspec_s3_with_cache(
                https_url=https_url
            )
        return network_tracker.asv_network_statistics

    @skip_benchmark_if(TSHARK_PATH is None)
    def track_network_read_hdf5_pynwb_remfile_no_cache(self, params: dict[str, str]):
        """Read remote NWB file using pynwb and remfile without cache."""
        https_url = params["https_url"]
        with network_activity_tracker(tshark_path=TSHARK_PATH) as network_tracker:
            self.nwbfile, self.io, self.file, self.bytestream = read_hdf5_pynwb_remfile_no_cache(https_url=https_url)
        return network_tracker.asv_network_statistics

    @skip_benchmark_if(TSHARK_PATH is None)
    def track_network_read_hdf5_pynwb_remfile_with_cache(self, params: dict[str, str]):
        """Read remote NWB file using pynwb and remfile with cache."""
        https_url = params["https_url"]
        with network_activity_tracker(tshark_path=TSHARK_PATH) as network_tracker:
            self.nwbfile, self.io, self.file, self.bytestream, self.tmpdir = read_hdf5_pynwb_remfile_with_cache(
                https_url=https_url
            )
        return network_tracker.asv_network_statistics

    @skip_benchmark_if(TSHARK_PATH is None)
    def track_network_read_hdf5_pynwb_ros3(self, params: dict[str, str]):
        """Read remote NWB file using pynwb and the ROS3 HDF5 driver."""
        https_url = params["https_url"]
        with network_activity_tracker(tshark_path=TSHARK_PATH) as network_tracker:
            self.nwbfile, self.io, _ = read_hdf5_pynwb_ros3(https_url=https_url)
        return network_tracker.asv_network_statistics


class HDF5PyNWBFsspecHttpsPreloadedNoCacheFileReadBenchmark(BaseBenchmark):
    """
    Time the read of remote HDF5 NWB files using pynwb and fsspec with HTTPS with preloaded data without cache.
    """

    params = hdf5_redirected_read_params

    def setup(self, params: dict[str, str]):
        https_url = params["https_url"]
        self.nwbfile, self.io, self.file, self.bytestream = read_hdf5_pynwb_fsspec_https_no_cache(https_url=https_url)

    @skip_benchmark_if(TSHARK_PATH is None)
    def track_network_read_hdf5_pynwb_fsspec_https_preloaded_no_cache(self, params: dict[str, str]):
        """Read remote NWB file using pynwb and fsspec with HTTPS with preloaded data without cache."""
        https_url = params["https_url"]
        with network_activity_tracker(tshark_path=TSHARK_PATH) as network_tracker:
            self.nwbfile, self.io, self.file, self.bytestream = read_hdf5_pynwb_fsspec_https_no_cache(
                https_url=https_url
            )
        return network_tracker.asv_network_statistics


class HDF5PyNWBFsspecHttpsPreloadedWithCacheFileReadBenchmark(BaseBenchmark):
    """
    Time the read of remote HDF5 NWB files using pynwb and fsspec with HTTPS with preloaded cache.
    """

    params = hdf5_redirected_read_params

    def setup(self, params: dict[str, str]):
        https_url = params["https_url"]
        self.nwbfile, self.io, self.file, self.bytestream, self.tmpdir = read_hdf5_pynwb_fsspec_https_with_cache(
            https_url=https_url
        )

    @skip_benchmark_if(TSHARK_PATH is None)
    def track_network_read_hdf5_pynwb_fsspec_https_preloaded_with_cache(self, params: dict[str, str]):
        """Read remote NWB file using pynwb and fsspec with HTTPS with preloaded cache."""
        https_url = params["https_url"]
        with network_activity_tracker(tshark_path=TSHARK_PATH) as network_tracker:
            self.nwbfile, self.io, self.file, self.bytestream, self.tmpdir = read_hdf5_pynwb_fsspec_https_with_cache(
                https_url=https_url
            )
        return network_tracker.asv_network_statistics


class HDF5PyNWBFsspecS3PreloadedNoCacheFileReadBenchmark(BaseBenchmark):
    """
    Time the read of remote HDF5 NWB files using pynwb and fsspec with S3 with preloaded data without cache.
    """

    params = hdf5_redirected_read_params

    def setup(self, params: dict[str, str]):
        https_url = params["https_url"]
        self.nwbfile, self.io, self.file, self.bytestream = read_hdf5_pynwb_fsspec_s3_no_cache(https_url=https_url)

    @skip_benchmark_if(TSHARK_PATH is None)
    def track_network_read_hdf5_pynwb_fsspec_s3_preloaded_no_cache(self, params: dict[str, str]):
        """Read remote NWB file using pynwb and fsspec with S3 with preloaded data without cache."""
        https_url = params["https_url"]
        with network_activity_tracker(tshark_path=TSHARK_PATH) as network_tracker:
            self.nwbfile, self.io, self.file, self.bytestream = read_hdf5_pynwb_fsspec_s3_no_cache(https_url=https_url)
        return network_tracker.asv_network_statistics


class HDF5PyNWBFsspecS3PreloadedWithCacheFileReadBenchmark(BaseBenchmark):
    """
    Time the read of remote HDF5 NWB files using pynwb and fsspec with S3 with preloaded cache.
    """

    params = hdf5_redirected_read_params

    def setup(self, params: dict[str, str]):
        https_url = params["https_url"]
        self.nwbfile, self.io, self.file, self.bytestream, self.tmpdir = read_hdf5_pynwb_fsspec_s3_with_cache(
            https_url=https_url
        )

    @skip_benchmark_if(TSHARK_PATH is None)
    def track_network_read_hdf5_pynwb_fsspec_s3_preloaded_with_cache(self, params: dict[str, str]):
        """Read remote NWB file using pynwb and fsspec with S3 with preloaded cache."""
        https_url = params["https_url"]
        with network_activity_tracker(tshark_path=TSHARK_PATH) as network_tracker:
            self.nwbfile, self.io, self.file, self.bytestream, self.tmpdir = read_hdf5_pynwb_fsspec_s3_with_cache(
                https_url=https_url
            )
        return network_tracker.asv_network_statistics


class HDF5PyNWBRemfilePreloadedNoCacheFileReadBenchmark(BaseBenchmark):
    """
    Time the read of remote HDF5 NWB files using pynwb and remfile with preloaded data without cache.
    """

    params = hdf5_redirected_read_params

    def setup(self, params: dict[str, str]):
        https_url = params["https_url"]
        self.nwbfile, self.io, self.file, self.bytestream = read_hdf5_pynwb_remfile_no_cache(https_url=https_url)

    @skip_benchmark_if(TSHARK_PATH is None)
    def track_network_read_hdf5_pynwb_remfile_preloaded_no_cache(self, params: dict[str, str]):
        """Read remote NWB file using pynwb and remfile with preloaded data without cache."""
        https_url = params["https_url"]
        with network_activity_tracker(tshark_path=TSHARK_PATH) as network_tracker:
            self.nwbfile, self.io, self.file, self.bytestream = read_hdf5_pynwb_remfile_no_cache(https_url=https_url)
        return network_tracker.asv_network_statistics


class HDF5PyNWBRemfilePreloadedWithCacheFileReadBenchmark(BaseBenchmark):
    """
    Time the read of remote HDF5 NWB files using pynwb and remfile with preloaded cache.
    """

    params = hdf5_redirected_read_params

    def setup(self, params: dict[str, str]):
        https_url = params["https_url"]
        self.nwbfile, self.io, self.file, self.bytestream, self.tmpdir = read_hdf5_pynwb_remfile_with_cache(
            https_url=https_url
        )

    @skip_benchmark_if(TSHARK_PATH is None)
    def track_network_read_hdf5_pynwb_remfile_preloaded_with_cache(self, params: dict[str, str]):
        """Read remote NWB file using pynwb and remfile with preloaded cache."""
        https_url = params["https_url"]
        with network_activity_tracker(tshark_path=TSHARK_PATH) as network_tracker:
            self.nwbfile, self.io, self.file, self.bytestream, self.tmpdir = read_hdf5_pynwb_remfile_with_cache(
                https_url=https_url
            )
        return network_tracker.asv_network_statistics


class LindiLocalJSONFileReadBenchmark(BaseBenchmark):
    """
    Track the network activity during read of remote HDF5 files by reading the local LINDI JSON files with lindi and
    h5py or pynwb.

    This downloads the remote LINDI JSON file during setup if it does not already exist in the persistent download
    directory.

    Note: in all cases, store the in-memory objects to be consistent with timing benchmarks.
    """

    params = lindi_no_redirect_download_params

    def setup(self, params: dict[str, str]):
        """Download the LINDI JSON file."""
        https_url = params["https_url"]
        self.lindi_file = download_asset_if_not_exists(https_url=https_url)

    def teardown(self, params: dict[str, str]):
        if hasattr(self, "io"):
            self.io.close()
        if hasattr(self, "client"):
            self.client.close()

    @skip_benchmark_if(TSHARK_PATH is None)
    def track_network_read_lindi_h5py(self, params: dict[str, str]):
        """Read a remote HDF5 file with h5py using lindi with the local LINDI JSON file."""
        with network_activity_tracker(tshark_path=TSHARK_PATH) as network_tracker:
            self.client = read_hdf5_h5py_lindi(rfs=self.lindi_file)
        return network_tracker.asv_network_statistics

    @skip_benchmark_if(TSHARK_PATH is None)
    def track_network_read_lindi_pynwb(self, params: dict[str, str]):
        """Read a remote HDF5 NWB file with pynwb using lindi with the local LINDI JSON file."""
        with network_activity_tracker(tshark_path=TSHARK_PATH) as network_tracker:
            self.nwbfile, self.io, self.client = read_hdf5_pynwb_lindi(rfs=self.lindi_file)
        return network_tracker.asv_network_statistics


class ZarrZarrPythonFileReadBenchmark(BaseBenchmark):
    """
    Track the network activity during read of remote Zarr files with Zarr-Python only (not using PyNWB)

    Note: in all cases, store the in-memory objects to avoid timing garbage collection steps.
    """

    params = zarr_direct_read_params

    @skip_benchmark_if(TSHARK_PATH is None)
    def track_network_read_zarr_https(self, params: dict[str, str]):
        """Read a Zarr file using Zarr-Python with HTTPS and consolidated metadata (if available)."""
        https_url = params["https_url"]
        with network_activity_tracker(tshark_path=TSHARK_PATH) as network_tracker:
            self.zarr_file = read_zarr_zarrpython_https(https_url=https_url, open_without_consolidated_metadata=False)
        return network_tracker.asv_network_statistics

    @skip_benchmark_if(TSHARK_PATH is None)
    def track_network_read_zarr_https_force_no_consolidated(self, params: dict[str, str]):
        """Read a Zarr file using Zarr-Python with HTTPS and without using consolidated metadata."""
        https_url = params["https_url"]
        with network_activity_tracker(tshark_path=TSHARK_PATH) as network_tracker:
            self.zarr_file = read_zarr_zarrpython_https(https_url=https_url, open_without_consolidated_metadata=True)
        return network_tracker.asv_network_statistics

    @skip_benchmark_if(TSHARK_PATH is None)
    def track_network_read_zarr_s3(self, params: dict[str, str]):
        """Read a Zarr file using Zarr-Python with S3 and consolidated metadata (if available)."""
        https_url = params["https_url"]
        with network_activity_tracker(tshark_path=TSHARK_PATH) as network_tracker:
            self.zarr_file = read_zarr_zarrpython_s3(https_url=https_url, open_without_consolidated_metadata=False)
        return network_tracker.asv_network_statistics

    @skip_benchmark_if(TSHARK_PATH is None)
    def track_network_read_zarr_s3_force_no_consolidated(self, params: dict[str, str]):
        """Read a Zarr file using Zarr-Python with S3 and without using consolidated metadata."""
        https_url = params["https_url"]
        with network_activity_tracker(tshark_path=TSHARK_PATH) as network_tracker:
            self.zarr_file = read_zarr_zarrpython_s3(https_url=https_url, open_without_consolidated_metadata=True)
        return network_tracker.asv_network_statistics


class ZarrPyNWBFileReadBenchmark(BaseBenchmark):
    """
    Track the network activity during read of remote Zarr NWB files with pynwb.

    Note: in all cases, store the in-memory objects to be consistent with timing benchmarks.
    """

    params = zarr_direct_read_params

    def teardown(self, params: dict[str, str]):
        if hasattr(self, "io"):
            self.io.close()

    @skip_benchmark_if(TSHARK_PATH is None)
    def track_network_read_zarr_pynwb_s3(self, params: dict[str, str]):
        """Read a Zarr NWB file using pynwb with S3 and consolidated metadata (if available)."""
        https_url = params["https_url"]
        with network_activity_tracker(tshark_path=TSHARK_PATH) as network_tracker:
            self.nwbfile, self.io = read_zarr_pynwb_s3(https_url=https_url, mode="r")
        return network_tracker.asv_network_statistics

    @skip_benchmark_if(TSHARK_PATH is None)
    def track_network_read_zarr_pynwb_s3_force_no_consolidated(self, params: dict[str, str]):
        """Read a Zarr NWB file using pynwb using S3 and without consolidated metadata."""
        https_url = params["https_url"]
        with network_activity_tracker(tshark_path=TSHARK_PATH) as network_tracker:
            self.nwbfile, self.io = read_zarr_pynwb_s3(https_url=https_url, mode="r-")
        return network_tracker.asv_network_statistics
