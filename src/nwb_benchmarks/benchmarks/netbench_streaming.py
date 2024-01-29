"""Basic benchmarks for network performance metrics for streaming read of NWB files."""
import tempfile
import warnings

import fsspec
import h5py
import pynwb
import remfile
from fsspec.asyn import reset_lock
from fsspec.implementations.cached import CachingFileSystem

from .netperf.benchmarks import NetworkBenchmarkBase

# Useful if running in verbose mode
warnings.filterwarnings(action="ignore", message="No cached namespaces found in .*")
warnings.filterwarnings(action="ignore", message="Ignoring cached namespace .*")

S3_NWB = "https://dandiarchive.s3.amazonaws.com/ros3test.nwb"  # Small test NWB file
# S3_NWB = "https://dandiarchive.s3.amazonaws.com/blobs/c49/311/c493119b-4b99-4b14-bc03-65bb28cfbd29"  # ECephy+Behavior
# S3_NWB = "https://dandiarchive.s3.amazonaws.com/blobs/38c/c24/38cc240b-77c5-418a-9040-a7f582ff6541"  # Ophys
# S3_NWB = "https://dandiarchive.s3.amazonaws.com/blobs/c98/3a4/c983a4e1-097a-402c-bda8-e6a41cb7e24a"   # ICEphys


class NetworkBenchmarkRos3Read(NetworkBenchmarkBase):
    """Benchmark NWB file read with Ros3"""

    s3_url = S3_NWB
    repeat = 1
    unit = "Bytes"
    timeout = 1200

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

    def track_total_transfer_time(self, cache):
        return cache["total_transfer_time"]

    def track_total_time(self, cache):
        return cache["total_time"]


class NetworkBenchmarkRemFileRead(NetworkBenchmarkBase):
    """Benchmark NWB file read with RemFile"""

    s3_url = S3_NWB
    repeat = 1
    unit = "Bytes"
    timeout = 1200

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

    def track_total_transfer_time(self, cache):
        return cache["total_transfer_time"]

    def track_total_time(self, cache):
        return cache["total_time"]


class NetworkBenchmarkRemFileWithCacheRead(NetworkBenchmarkBase):
    """Benchmark NWB file read with RemFile using a remfile.DiskCache as a temporary cache"""

    s3_url = S3_NWB
    repeat = 1
    unit = "Bytes"
    timeout = 1200

    def setup_cache(self):
        return self.compute_test_case_metrics()

    def test_case(self):
        byte_stream = remfile.File(url=self.s3_url)
        with tempfile.TemporaryDirectory() as tmpdirname:
            disk_cache = remfile.DiskCache(tmpdirname)
            with h5py.File(name=byte_stream, disk_cache=disk_cache) as file:
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

    def track_total_transfer_time(self, cache):
        return cache["total_transfer_time"]

    def track_total_time(self, cache):
        return cache["total_time"]


class NetworkBenchmarkFsspecWithCacheFileRead(NetworkBenchmarkBase):
    """Benchmark NWB file read with fsspec using CachingFileSystem"""

    s3_url = S3_NWB
    repeat = 1
    unit = "Bytes"
    timeout = 1200

    def setup_cache(self):
        return self.compute_test_case_metrics()

    def test_case(self):
        reset_lock()
        fsspec.get_filesystem_class("https").clear_instance_cache()
        filesystem = fsspec.filesystem("https")
        # create a cache to save downloaded data to disk (optional)
        cached_filesystem = CachingFileSystem(fs=filesystem)  # Use temporary storage that will be cleaned up

        with cached_filesystem.open(path=self.s3_url, mode="rb") as byte_stream:
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

    def track_total_transfer_time(self, cache):
        return cache["total_transfer_time"]

    def track_total_time(self, cache):
        return cache["total_time"]


class NetworkBenchmarkFsspecFileRead(NetworkBenchmarkBase):
    """Benchmark NWB file read with fsspec (no extra cache)"""

    s3_url = S3_NWB
    repeat = 1
    unit = "Bytes"
    timeout = 1200

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

    def track_total_transfer_time(self, cache):
        return cache["total_transfer_time"]

    def track_total_time(self, cache):
        return cache["total_time"]
