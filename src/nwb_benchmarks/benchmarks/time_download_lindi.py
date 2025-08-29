"""Basic benchmarks for timing download of a remote LINDI JSON file."""

import os

from nwb_benchmarks.core import BaseBenchmark, download_file

from .params_remote_file_reading import lindi_remote_rfs_parameter_cases


class LindiDownloadBenchmark(BaseBenchmark):
    """
    Time the download of a remote LINDI JSON file.
    """

    parameter_cases = lindi_remote_rfs_parameter_cases

    def setup(self, https_url: str):
        self.lindi_file = os.path.basename(https_url) + ".lindi.json"
        self.teardown(https_url=https_url)

    def teardown(self, https_url: str):
        if os.path.exists(self.lindi_file):
            os.remove(self.lindi_file)

    def time_download(self, https_url: str):
        download_file(url=https_url, local_path=self.lindi_file)
