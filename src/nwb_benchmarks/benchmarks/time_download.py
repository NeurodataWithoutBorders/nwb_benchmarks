"""Basic benchmarks for timing download of remote NWB files using different methods."""
from asv_runner.benchmarks.mark import skip_benchmark_if
from dandi.download import download, DownloadExisting

from nwb_benchmarks import RUN_DOWNLOAD_BENCHMARKS
from nwb_benchmarks.core import BaseBenchmark
from nwb_benchmarks.setup import get_persistent_download_directory

from .params_remote_download import hdf5_params, lindi_remote_rfs_params, zarr_params


class BaseDandiAPIDownloadBenchmark(BaseBenchmark):
    """
    Base class for timing the download of remote NWB files using the DANDI API.
    """
    # Download each file only once per timing run even if download time < asv sample_time setting
    number = 1

    # 12 hours max per download benchmark
    timeout = 60 * 60 * 12

    def setup(self, params: dict[str, str]):
        self.download_dir = get_persistent_download_directory()


class HDF5DandiAPIDownloadBenchmark(BaseDandiAPIDownloadBenchmark):
    """
    Time the download of remote HDF5 NWB files using the DANDI API.

    If the file already exists in the download directory, it will be overwritten.
    """

    params = hdf5_params

    # NOTE - these benchmarks download the full file which can take a long time.
    # Only run explicitly using RUN_DOWNLOAD_BENCHMARKS=true when needed.
    @skip_benchmark_if(not RUN_DOWNLOAD_BENCHMARKS)
    def time_download_hdf5_dandi_api(self, params: dict[str, str]):
        """Download a remote HDF5 NWB file using the DANDI API."""
        download(urls=params["https_url"], output_dir=self.download_dir, existing=DownloadExisting.OVERWRITE)


class ZarrDandiAPIDownloadBenchmark(BaseDandiAPIDownloadBenchmark):
    """
    Time the download of remote Zarr NWB files using the DANDI API.

    If the file already exists in the download directory, it will be overwritten.
    """

    params = zarr_params

    # NOTE - these benchmarks download the full file which can take a long time.
    # Only run explicitly using RUN_DOWNLOAD_BENCHMARKS=true when needed.
    @skip_benchmark_if(not RUN_DOWNLOAD_BENCHMARKS)
    def time_download_zarr_dandi_api(self, params: dict[str, str]):
        """Download a remote Zarr NWB directory using the DANDI API."""
        download(urls=params["https_url"], output_dir=self.download_dir, existing=DownloadExisting.OVERWRITE)


class LindiDandiAPIDownloadBenchmark(BaseDandiAPIDownloadBenchmark):
    """
    Time the download of a remote LINDI JSON file.

    If the file already exists in the download directory, it will be overwritten.
    """

    params = lindi_remote_rfs_params

    def time_download_lindi_dandi_api(self, params: dict[str, str]):
        """Download a remote Lindi file using the DANDI API."""
        download(urls=params["https_url"], output_dir=self.download_dir, existing=DownloadExisting.OVERWRITE)
