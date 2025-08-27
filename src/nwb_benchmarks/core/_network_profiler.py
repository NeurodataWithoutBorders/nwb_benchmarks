"""
Class definition for capturing network traffic when remotely reading data from an NWB file.

NOTE: This requires sudo/root access on  macOS and AIX.
"""

import os
import pathlib
import subprocess
import tempfile
import time
import warnings
from typing import Union

import pyshark


class NetworkProfiler:
    """Use TShark command line interface in a subprocess to capture network traffic packets in the background."""

    def __init__(self, capture_file_path: Union[pathlib.Path, None] = None):
        self.__tshark_process = None
        self.__packets = None

        self.capture_file_path = capture_file_path
        if self.capture_file_path is None:
            self.capture_file = tempfile.NamedTemporaryFile(mode="w", delete=False)
            self.capture_file_path = pathlib.Path(self.capture_file.name)

    def __del__(self):
        """Stop capture and cleanup temporary file."""
        self.stop_capture()
        try:
            self.capture_file_path.unlink(missing_ok=True)
        except PermissionError:
            warnings.warn("Unable to clean temporary network profiling files! Please clean temp directory manually.")

    @property
    def packets(self):
        """List of all packets captured."""
        if self.__packets is None:
            try:
                cap = pyshark.FileCapture(self.capture_file_path)
                self.__packets = [packet for packet in cap]
                del cap
            except Exception:
                pass
        return self.__packets

    def start_capture(self, tshark_path: Union[pathlib.Path, None] = None):
        """Start the capture with tshark in a subprocess."""
        tshark_path = tshark_path or "tshark"
        tsharkCall = [str(tshark_path), "-w", str(self.capture_file_path)]
        networkInterface = os.environ.get("NWB_BENCHMARKS_NETWORK_INTERFACE")
        if networkInterface:
            tsharkCall.extend(["-i", networkInterface])
        self.__tshark_process = subprocess.Popen(tsharkCall, stderr=subprocess.DEVNULL)
        time.sleep(0.2)  # not sure if this is needed but just to be safe

    def stop_capture(self):
        """Stop the capture with tshark in a subprocess."""
        if self.__tshark_process is not None:
            self.__tshark_process.terminate()
            self.__tshark_process.kill()
            del self.__tshark_process
            self.__tshark_process = None
        # if hasattr(self, "capture_file"):
        #     del self.capture_file

    def get_packets_for_connections(self, pid_connections: list):
        """
        Get packets for all connections in the given pid_connections list.

        To get the local connection we can use CaptureConntections to
        simultaneously capture all connections with psutils and then use
        `connections_thread.get_connections_for_pid(os.getpid())` to get
        the local connections.
        """
        pid_packets = []
        try:
            for packet in self.packets:
                if hasattr(packet, "tcp"):
                    ports = int(str(packet.tcp.srcport)), int(str(packet.tcp.dstport))
                    if ports in pid_connections:
                        pid_packets.append(packet)
        except Exception:  # pyshark.capture.capture.TSharkCrashException:
            pass
        return pid_packets
