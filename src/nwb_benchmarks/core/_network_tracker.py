"""
Network tracker for capturing network traffic and performance metrics during the execution of particular
methods or code snippets.
"""

import contextlib
import os
import pathlib
import time
from typing import Union

from ._capture_connections import CaptureConnections
from ._network_profiler import NetworkProfiler
from ._network_statistics import NetworkStatistics


@contextlib.contextmanager
def network_activity_tracker(tshark_path: Union[pathlib.Path, None] = None):
    network_tracker = NetworkTracker()

    try:
        network_tracker.start_network_capture(tshark_path=tshark_path)
        time.sleep(0.3)

        t0 = time.time()
        yield network_tracker
    finally:
        network_tracker.stop_network_capture()

        t1 = time.time()
        network_total_time = t1 - t0
        network_tracker.network_statistics["network_total_time"] = network_total_time


class NetworkTracker:
    def start_network_capture(self, tshark_path: Union[pathlib.Path, None] = None):
        # start the capture_connections() function to update the current connections of this machine
        self.connections_thread = CaptureConnections()
        self.connections_thread.start()
        time.sleep(0.2)  # not sure if this is needed but just to be safe

        # start capturing the raw packets by running the tshark commandline tool in a subprocess
        self.network_profiler = NetworkProfiler()
        self.network_profiler.start_capture(tshark_path=tshark_path)

    def stop_network_capture(self):
        # Stop capturing packets and connections
        self.network_profiler.stop_capture()
        self.connections_thread.stop()

        # get the connections for the PID of this process
        self.pid_connections = self.connections_thread.get_connections_for_pid(os.getpid())
        # Parse packets and filter out all the packets for this process pid by matching with the pid_connections
        self.pid_packets = self.network_profiler.get_packets_for_connections(self.pid_connections)
        # Compute all the network statistics
        self.network_statistics = NetworkStatistics.get_statistics(packets=self.pid_packets)

        # Very special structure required by ASV
        self.asv_network_statistics = dict(samples=self.network_statistics, number=None)
