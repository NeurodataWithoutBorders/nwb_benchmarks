import os

from asv_runner.benchmarks.mark import skip_benchmark

from nwb_benchmarks import TSHARK_PATH
from nwb_benchmarks.core import (
    BaseBenchmark,
    create_lindi_reference_file_system,
    network_activity_tracker,
)

from .params_remote_file_reading import hdf5_params


class LindiCreateJSONFromRemoteFileBenchmark(BaseBenchmark):
    """
    Track the network activity during the creation of a LINDI JSON file for a remote NWB HDF5 file using lindi.
    """

    params = hdf5_params

    def setup(self, params: dict[str, str]):
        https_url = params["https_url"]
        self.lindi_file = os.path.basename(https_url) + ".nwb.lindi.json"
        self.teardown(params)

    def teardown(self, params: dict[str, str]):
        if os.path.exists(self.lindi_file):
            os.remove(self.lindi_file)

    # TODO This benchmark takes a long time to index all of the chunks for these files! Do not run until ready
    @skip_benchmark
    def track_network_create_lindi_json(self, params: dict[str, str]):
        """Read a remote HDF5 file to create a LINDI JSON file."""
        https_url = params["https_url"]
        with network_activity_tracker(tshark_path=TSHARK_PATH) as network_tracker:
            create_lindi_reference_file_system(https_url=https_url, outfile_path=self.lindi_file)
        return network_tracker.asv_network_statistics
