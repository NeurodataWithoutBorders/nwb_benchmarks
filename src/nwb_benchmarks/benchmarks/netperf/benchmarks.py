"""
Base classes for implementing benchmarks for evaluating
network performance metrics for streaming read of NWB files.
"""
import os
import time

from asv_runner.benchmarks.mark import SkipNotImplemented

from .profile import CaptureConnections, NetProfiler, NetStats


class NetworkBenchmarkBase:
    """
    Base class for network performance metrics for streaming data access.

    Child classes must implement:
    - test_case : implementing the test case to be run
    - setup_cache: when implemented here in the base class, asv will run the setup_cache function
                   only once for all child classes. As such, each child class must implement its
                   own setup_cache, which should typically just consists of a call to
                   ``return self.compute_test_case_metrics()``.
    - track_...: methods defining the metrics to be tracked. These functions usually just retrieve
                 the corresponding metric from the cache and return the result. For example:

    .. code-block:: python

        def track_bytes_downloaded(self, cache): # unit is Bytes
            return cache["bytes_downloaded"]

        def track_bytes_uploaded(self, cache): # unit is Bytes
            return cache["bytes_uploaded"]

        def track_bytes_total(self, cache): # unit is Bytes
            return cache["bytes_total"]

        def track_num_packets(self, cache): # unit is Count
            return cache["num_packets"]

        def track_num_packets_downloaded(self, cache): # unit is Count
            return cache["num_packets_downloaded"]

        def track_num_packets_uploaded(self, cache):  # unit is Count
            return cache["num_packets_uploaded"]

    """

    s3_url: str = None

    def test_case(self):
        raise SkipNotImplemented()

    def compute_test_case_metrics(self):
        self.start_net_capture()
        t0 = time.time()
        self.test_case()
        t1 = time.time()
        total_time = t1 - t0
        self.stop_netcapture()
        self.net_stats["total_time"] = total_time
        return self.net_stats

    def start_net_capture(self):
        # start the capture_connections() function to update the current connections of this machine
        self.connections_thread = CaptureConnections()
        self.connections_thread.start()
        time.sleep(0.2)  # not sure if this is needed but just to be safe

        # start capturing the raw packets by running the tshark commandline tool in a subprocess
        self.netprofiler = NetProfiler()
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
        self.net_stats = NetStats.get_stats(packets=self.pid_packets)
