"""Demo of network profiling tools."""

import json

import pynwb

from nwb_benchmarks.core import track_network_activity

# If `tshark` is not available as a global command, specify the pathlib.Path pointing to the .exe
# Otherwise, set to None or do not pass into `.start_capture`

# tshark_exe_path = pathlib.Path("D:/Wireshark/tshark.exe")
tshark_exe_path = None

# Read the NWB data file from DANDI
s3_path = "https://dandiarchive.s3.amazonaws.com/ros3test.nwb"
with track_network_activity(tshark_exe_path=tshark_exe_path) as network_tracker:
    with pynwb.NWBHDF5IO(s3_path, mode="r", driver="ros3") as io:
        nwbfile = io.read()
        test_data = nwbfile.acquisition["ts_name"].data[:]

# Print basic connection statistics
print(json.dumps(obj=network_tracker.network_statistics, indent=4))
