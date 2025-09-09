"""Basic benchmarks for timing download of a remote LINDI JSON file."""

import os

from nwb_benchmarks.core import BaseBenchmark, download_file

from .params_remote_file_reading import lindi_remote_rfs_params


class LindiDownloadBenchmark(BaseBenchmark):
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

    def time_download(self, params: dict[str, str]):
        https_url = params["https_url"]
        download_file(url=https_url, local_path=self.lindi_file)
