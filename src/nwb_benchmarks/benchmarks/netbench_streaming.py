"""Basic benchmarks for network performance metrics for streaming read of NWB files."""
import warnings
import time
import os

import pynwb

from .netperf.profile import (CaptureConnections,
                              NetProfiler,
                              NetStats)

# Useful if running in verbose mode
warnings.filterwarnings(action="ignore", message="No cached namespaces found in .*")
warnings.filterwarnings(action="ignore", message="Ignoring cached namespace .*")


class FileReadStreamingNetworkBenchmark:

    repeat = 1
    unit = 'kB'

    def setup(self):
        pass
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

    def track_network_bytes_downloaded_ros3(self):
        self.stop_netcapture()
        s3_path = 'https://dandiarchive.s3.amazonaws.com/ros3test.nwb'
        with pynwb.NWBHDF5IO(s3_path, mode='r', driver='ros3') as io:
            nwbfile = io.read()
            test_data = nwbfile.acquisition['ts_name'].data[:]
        self.stop_netcapture()
        return self.net_stats['bytes_downloaded']
