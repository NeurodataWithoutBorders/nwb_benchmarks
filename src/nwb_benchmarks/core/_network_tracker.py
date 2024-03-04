"""Network tracker for capturing traffic and performance metrics during the execution of methods or code snippets."""

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
    """Context manager for tracking network activity and statistics for the code executed in the context"""
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
        network_tracker.network_statistics["network_total_time_in_seconds"] = network_total_time


class NetworkTracker:
    """
    Track network traffic and compute network statistics for operations performed by the current
    Python process during the active period of the tracker, which is started by `start_network_capture`
    and ended by `stop_network_capture`.

    :ivar connections_thread: Instance of `CaptureConnections` used to relate network connections to process IDs
    :ivar network_profiler: Instance of `NetworkProfiler` used to capture network traffic with TShark
    :ivar pid_connections: connections for the PID of this process
    :ivar self.pid_packets: Network packets associated with this PID (i.e., `os.getpid()`)
    :ivar self.network_statistics: Network statistics calculated via `NetworkStatistics.get_statistics`
    :ivar self.asv_network_statistics: The network statistics wrapped in a dict for compliance with ASV

    """

    def __init__(self):
        self.connections_thread = None
        self.network_profiler = None
        self.pid_connections = None
        self.pid_packets = None
        self.network_statistics = None
        self.asv_network_statistics = None

    def start_network_capture(self, tshark_path: Union[pathlib.Path, None] = None):
        """
        Start capturing the connections on this machine as well as all network packets

        Side effects: This functions sets the following instance variables:
        * self.connections_thread
        * self.network_profile
        """
        self.connections_thread = CaptureConnections()
        self.connections_thread.start()
        time.sleep(0.2)  # not sure if this is needed but just to be safe

        # start capturing the raw packets by running the tshark commandline tool in a subprocess
        self.network_profiler = NetworkProfiler()
        self.network_profiler.start_capture(tshark_path=tshark_path)

    def stop_network_capture(self):
        """
        Stop capturing network packets and connections.

        Note: This function will fail if `start_network_capture` was not called first.

        Side effects: This functions sets the following instance variables:
        * self.pid_connections
        * self.pid_packets
        * self.network_statistics
        * self.asv_network_statistics
        """
        self.network_profiler.stop_capture()
        self.connections_thread.stop()

        # get the connections for the PID of this process
        self.pid_connections = self.connections_thread.get_connections_for_pid(os.getpid())
        # Parse packets and filter out all the packets for this process pid by matching with the pid_connections
        self.pid_packets = self.network_profiler.get_packets_for_connections(self.pid_connections)
        # Compute all the network statistics
        self.network_statistics = NetworkStatistics.get_statistics(packets=self.pid_packets)

        # Very special structure required by ASV
        # 'samples' is the value tracked in our results
        # 'number' is simply required, but needs to be None for custom track_ functions
        self.asv_network_statistics = dict(samples=self.network_statistics, number=None)
