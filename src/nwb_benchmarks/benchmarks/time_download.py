"""Basic benchmarks for timing download of remote NWB files using different methods."""

import os
import shutil

from asv_runner.benchmarks.mark import skip_benchmark_if
from dandi.download import download

from nwb_benchmarks import RUN_DOWNLOAD_BENCHMARKS
from nwb_benchmarks.core import BaseBenchmark, download_file
from nwb_benchmarks.setup import get_temporary_directory

from .params_remote_download import hdf5_params, lindi_remote_rfs_params, zarr_params


class BaseDownloadDandiAPIBenchmark(BaseBenchmark):
    """
    Base class for timing the download of remote NWB files using the DANDI API.
    """

    def setup(self, params: dict[str, str]):
        self.tmpdir = get_temporary_directory()
        self.teardown(params)

    def teardown(self, params: dict[str, str]):
        if hasattr(self, "tmpdir"):
            shutil.rmtree(path=self.tmpdir.name, ignore_errors=True)
            self.tmpdir.cleanup()


class HDF5DownloadDandiAPIBenchmark(BaseDownloadDandiAPIBenchmark):
    """
    Time the download of remote HDF5 NWB files using the DANDI API.
    """

    params = hdf5_params

    # NOTE - these benchmarks download the full file which can take a long time. Only run explicitly using RUN_DOWNLOAD_BENCHMARKS=true when needed.
    @skip_benchmark_if(not RUN_DOWNLOAD_BENCHMARKS)
    def time_download_hdf5_dandi_api(self, params: dict[str, str]):
        """Download a remote HDF5 NWB file using the DANDI API."""
        download(urls=params["https_url"], output_dir=self.tmpdir.name)


class ZarrDownloadDandiAPIBenchmark(BaseDownloadDandiAPIBenchmark):
    """
    Time the download of remote Zarr NWB files using the DANDI API.
    """

    params = zarr_params

    # NOTE - these benchmarks download the full file which can take a long time. Only run explicitly using RUN_DOWNLOAD_BENCHMARKS=true when needed.
    @skip_benchmark_if(not RUN_DOWNLOAD_BENCHMARKS)
    def time_download_zarr_dandi_api(self, params: dict[str, str]):
        """Download a remote Zarr NWB directory using the DANDI API."""
        download(urls=params["https_url"], output_dir=self.tmpdir.name)


class LindiDownloadFsspecBenchmark(BaseBenchmark):
    """
    Time the download of a remote LINDI JSON file.
    """

    params = lindi_remote_rfs_params

    def setup(self, params: dict[str, str]):
        https_url = params["https_url"]
        self.lindi_file = os.path.basename(https_url) + ".lindi.json"
        self.teardown(params)

    def teardown(self, params: dict[str, str]):
        if os.path.exists(self.lindi_file):
            os.remove(self.lindi_file)

    def time_download_lindi_fsspec(self, params: dict[str, str]):
        https_url = params["https_url"]
        download_file(url=https_url, local_path=self.lindi_file)
