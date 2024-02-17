"""Demo of network profiling tools."""

import os
import pathlib
import time

import pynwb

from nwb_benchmarks.core import CaptureConnections, NetworkProfiler, NetworkStatistics

###
# This would be part of the setUp() of a test
##
# start the capture_connections() function to update the current connections of this machine
connections_thread = CaptureConnections()
connections_thread.start()
time.sleep(0.2)  # not sure if this is needed but just to be safe

# start capturing the raw packets by running the tshark commandline tool in a subprocess
netprofiler = NetworkProfiler()

# If `tshark` is not available as a global command, specify the pathlib.Path pointing to the .exe
# Otherwise, set to None or do not pass into `.start_capture`

# tshark_exe_path=pathlib.Path("D:/Wireshark/tshark.exe")
tshark_exe_path = None

netprofiler.start_capture(tshark_exe_path=pathlib.Path("D:/Wireshark/tshark.exe"))

###
# This would be the unit test
###
# Read the NWB data file from DANDI
t0 = time.time()
s3_path = "https://dandiarchive.s3.amazonaws.com/ros3test.nwb"
with pynwb.NWBHDF5IO(s3_path, mode="r", driver="ros3") as io:
    nwbfile = io.read()
    test_data = nwbfile.acquisition["ts_name"].data[:]
t1 = time.time()
total_time = t1 - t0

###
# This part would go into tearDown to stop the capture and compute statistics
###
# Stop capturing packets and connections
netprofiler.stop_capture()
connections_thread.stop()

# get the connections for the PID of this process
pid_connections = connections_thread.get_connections_for_pid(os.getpid())

# Parse packets and filter out all the packets for this process pid by matching with the pid_connections
pid_packets = netprofiler.get_packets_for_connections(pid_connections)

# Print basic connection statistics
print("num_connections:", int(len(pid_connections) / 2.0))
NetworkStatistics.print_stats(packets=pid_packets)
print("total_time:", total_time)
