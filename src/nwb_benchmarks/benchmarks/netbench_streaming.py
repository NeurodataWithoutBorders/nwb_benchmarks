"""Basic benchmarks for network performance metrics for streaming read of NWB files."""
import pynwb
import remfile
import h5py
import fsspec
from fsspec.asyn import reset_lock
from .netperf.benchmarks import NetworkBenchmarkBase

# Useful if running in verbose mode
import warnings
warnings.filterwarnings(action="ignore", message="No cached namespaces found in .*")
warnings.filterwarnings(action="ignore", message="Ignoring cached namespace .*")


class NetworkBenchmarkRos3Read(NetworkBenchmarkBase):

    s3_url = "https://dandiarchive.s3.amazonaws.com/ros3test.nwb"
    repeat = 1
    unit = "Bytes"

    def setup_cache(self):
        return self.compute_test_case_metrics()

    def test_case(self):

        with pynwb.NWBHDF5IO(self.s3_url, mode="r", driver="ros3") as io:
            nwbfile = io.read()
            # test_data = nwbfile.acquisition["ts_name"].data[:]

    def track_bytes_downloaded(self, cache):
        return cache["bytes_downloaded"]

    def track_bytes_uploaded(self, cache):
        return cache["bytes_uploaded"]

    def track_bytes_total(self, cache):
        return cache["bytes_total"]

    def track_num_packets(self, cache):
        return cache["num_packets"]

    def track_num_packets_downloaded(self, cache):
        return cache["num_packets_downloaded"]

    def track_num_packets_uploaded(self, cache):
        return cache["num_packets_uploaded"]


class NetworkBenchmarkRemFileRead(NetworkBenchmarkBase):

    s3_url = "https://dandiarchive.s3.amazonaws.com/ros3test.nwb"
    repeat = 1
    unit = "Bytes"

    def setup_cache(self):
        return self.compute_test_case_metrics()

    def test_case(self):
        byte_stream = remfile.File(url=self.s3_url)
        with h5py.File(name=byte_stream) as file:
            with pynwb.NWBHDF5IO(file=file, load_namespaces=True) as io:
                nwbfile = io.read()
                # test_data = nwbfile.acquisition["ts_name"].data[:]

    def track_bytes_downloaded(self, cache):
        return cache["bytes_downloaded"]

    def track_bytes_uploaded(self, cache):
        return cache["bytes_uploaded"]

    def track_bytes_total(self, cache):
        return cache["bytes_total"]

    def track_num_packets(self, cache):
        return cache["num_packets"]

    def track_num_packets_downloaded(self, cache):
        return cache["num_packets_downloaded"]

    def track_num_packets_uploaded(self, cache):
        return cache["num_packets_uploaded"]


class NetworkBenchmarkFsspecFileRead(NetworkBenchmarkBase):

    s3_url = "https://dandiarchive.s3.amazonaws.com/ros3test.nwb"
    repeat = 1
    unit = "Bytes"

    def setup_cache(self):
        return self.compute_test_case_metrics()

    def test_case(self):
        reset_lock()
        fsspec.get_filesystem_class("https").clear_instance_cache()
        filesystem = fsspec.filesystem("https")

        with filesystem.open(path=self.s3_url, mode="rb") as byte_stream:
            with h5py.File(name=byte_stream) as file:
                with pynwb.NWBHDF5IO(file=file, load_namespaces=True) as io:
                    nwbfile = io.read()
                    # test_data = nwbfile.acquisition["ts_name"].data[:]

    def track_bytes_downloaded(self, cache):
        return cache["bytes_downloaded"]

    def track_bytes_uploaded(self, cache):
        return cache["bytes_uploaded"]

    def track_bytes_total(self, cache):
        return cache["bytes_total"]

    def track_num_packets(self, cache):
        return cache["num_packets"]

    def track_num_packets_downloaded(self, cache):
        return cache["num_packets_downloaded"]

    def track_num_packets_uploaded(self, cache):
        return cache["num_packets_uploaded"]
