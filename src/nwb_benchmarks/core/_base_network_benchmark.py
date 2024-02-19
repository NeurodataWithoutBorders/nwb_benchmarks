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

    Child classes must implement:
        1) The `operation_to_track_network_activity_of`, which replaces the usual time_ or mem_ operation being tracked.
        2) The `setup_cache` method to explicitly recieve the necessary keyword arguments to be passed to the operation.
    """

    def setup_network_test(self, **keyword_arguments):
        raise SkipNotImplemented()

    def operation_to_track_network_activity_of(self, **keyword_arguments):
        raise SkipNotImplemented()

    def setup_cache(self, **keyword_arguments) -> dict:
        """
        Strategy essentially mimics a single benchmark execution (setup + run).

        Only supports single operations (ignores `repeat` and prohibits `params`).
        """
        self.setup_network_test(**keyword_arguments)
        self.start_net_capture()
        t0 = time.time()
        self.operation_to_track_network_activity_of(**keyword_arguments)
        t1 = time.time()
        total_time = t1 - t0
        self.stop_net_capture()
        self.network_statistics["network_total_time"] = total_time
        return self.network_statistics

    def start_net_capture(self):
        # start the capture_connections() function to update the current connections of this machine
        self.connections_thread = CaptureConnections()
        self.connections_thread.start()
        time.sleep(0.2)  # not sure if this is needed but just to be safe

        # start capturing the raw packets by running the tshark commandline tool in a subprocess
        self.network_profiler = NetworkProfiler()
        self.network_profiler.start_capture()

    def stop_net_capture(self):
        # Stop capturing packets and connections
        self.network_profiler.stop_capture()
        self.connections_thread.stop()

        # get the connections for the PID of this process
        self.pid_connections = self.connections_thread.get_connections_for_pid(os.getpid())
        # Parse packets and filter out all the packets for this process pid by matching with the pid_connections
        self.pid_packets = self.network_profiler.get_packets_for_connections(self.pid_connections)
        # Compute all the network statistics
        self.network_statistics = NetworkStatistics.get_statistics(packets=self.pid_packets)

    def track_bytes_downloaded(self, net_stats: dict):
        return net_stats["bytes_downloaded"]

    track_bytes_downloaded.unit = "bytes"

    def track_bytes_uploaded(self, net_stats: dict):
        return net_stats["bytes_uploaded"]

    track_bytes_uploaded.unit = "bytes"

    def track_bytes_total(self, net_stats: dict):
        return net_stats["bytes_total"]

    track_bytes_total.unit = "bytes"

    def track_number_of_packets(self, net_stats: dict):
        return net_stats["number_of_packets"]

    track_number_of_packets.unit = "count"

    def track_number_of_packets_downloaded(self, net_stats: dict):
        return net_stats["number_of_packets_downloaded"]

    track_number_of_packets_downloaded.unit = "count"

    def track_number_of_packets_uploaded(self, net_stats: dict):
        return net_stats["number_of_packets_uploaded"]

    track_number_of_packets_uploaded.unit = "count"

    def track_total_transfer_time(self, net_stats: dict):
        return net_stats["total_transfer_time"]

    track_total_transfer_time.unit = "seconds"

    def track_network_total_time(self, net_stats: dict):
        """Technically different from the official time_ approach."""
        return net_stats["network_total_time"]

    track_network_total_time.unit = "seconds"
