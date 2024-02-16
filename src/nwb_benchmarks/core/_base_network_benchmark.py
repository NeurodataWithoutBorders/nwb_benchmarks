"""Base class for implementing benchmarks for evaluating network performance metrics for streaming read of NWB files."""

import os
import time

from asv_runner.benchmarks.mark import SkipNotImplemented

from ._capture_connections import CaptureConnections
from ._network_profiler import NetworkProfiler
from ._network_statistics import NetworkStatistics


class BaseNetworkBenchmark:
    """
    Base class for ASV Benchmark network performance metrics for streaming data access.

    Child classes must implement the `operation_to_track_network_activity_of`,
    which replaces the usual time_ or mem_ operation being tracked.
    """

    def operation_to_track_network_activity_of(self):
        raise SkipNotImplemented()

    def setup_cache(self, **keyword_arguments):
        return self.compute_test_case_metrics(**keyword_arguments)

    def compute_test_case_metrics(self, **keyword_arguments):
        """Strategy essentially mimics a single benchmark execution (setup + run), but only once (ignores `repeat`)."""
        self.start_net_capture()
        t0 = time.time()
        self.setup(**keyword_arguments)
        self.operation_to_track_network_activity_of(**keyword_arguments)
        t1 = time.time()
        total_time = t1 - t0
        self.stop_netcapture()
        self.net_stats["network_total_time"] = total_time
        return self.net_stats

    def start_net_capture(self):
        # start the capture_connections() function to update the current connections of this machine
        self.connections_thread = CaptureConnections()
        self.connections_thread.start()
        time.sleep(0.2)  # not sure if this is needed but just to be safe

        # start capturing the raw packets by running the tshark commandline tool in a subprocess
        self.netprofiler = NetworkProfiler()
        self.netprofiler.start_capture()

    def stop_netcapture(self):
        # Stop capturing packets and connections
        self.netprofiler.stop_capture()
        self.connections_thread.stop()

        # get the connections for the PID of this process
        self.pid_connections = self.connections_thread.get_connections_for_pid(os.getpid())
        # Parse packets and filter out all the packets for this process pid by matching with the pid_connections
        self.pid_packets = self.netprofiler.get_packets_for_connections(self.pid_connections)
        # Compute all the network statistics
        self.net_stats = NetworkStatistics.get_stats(packets=self.pid_packets)

    def track_bytes_downloaded(self, cache):
        return cache["bytes_downloaded"]

    track_bytes_downloaded.unit = "bytes"

    def track_bytes_uploaded(self, cache):
        return cache["bytes_uploaded"]

    track_bytes_uploaded.unit = "bytes"

    def track_bytes_total(self, cache):
        return cache["bytes_total"]

    track_bytes_total.unit = "bytes"

    def track_num_packets(self, cache):
        return cache["num_packets"]

    track_num_packets.unit = "count"

    def track_num_packets_downloaded(self, cache):
        return cache["num_packets_downloaded"]

    track_num_packets_downloaded.unit = "count"

    def track_num_packets_uploaded(self, cache):
        return cache["num_packets_uploaded"]

    track_num_packets_uploaded.unit = "count"

    def track_total_transfer_time(self, cache):
        return cache["total_transfer_time"]

    track_total_transfer_time.unit = "seconds"

    def track_network_total_time(self, cache):
        """Technically different from the official time_ approach."""
        return cache["network_total_time"]

    track_network_total_time.unit = "seconds"
