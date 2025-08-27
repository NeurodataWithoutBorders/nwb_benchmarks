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

        print("Using capture file:", self.capture_file_path)

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
                print("Getting packets...")
                start_time = time.time()
                cap = pyshark.FileCapture(self.capture_file_path, use_json=True)
                self.__packets = [packet for packet in cap]
                end_time = time.time()
                print("Packets captured:", len(self.__packets))
                print(f"Time taken to read packets: {end_time - start_time:.1f} seconds")
                del cap
            except Exception as e:
                print("Error getting packets:", e)
        return self.__packets

    def start_capture(self, tshark_path: Union[pathlib.Path, None] = None):
        """Start the capture with tshark in a subprocess."""
        tshark_path = tshark_path or "tshark"
        tsharkCall = [str(tshark_path), "-w", str(self.capture_file_path)]
        networkInterface = os.environ.get("NWB_BENCHMARKS_NETWORK_INTERFACE")
        if networkInterface:
            tsharkCall.extend(["-i", networkInterface])
        self.__tshark_process = subprocess.Popen(tsharkCall, stderr=subprocess.DEVNULL)
        time.sleep(1.0)  # Give TShark a moment to start

    def stop_capture(self):
        """Stop the capture with tshark in a subprocess."""
        if self.__tshark_process is not None:
            try:
                # First try to terminate gracefully
                print("Attempting to terminate TShark...")
                self.__tshark_process.terminate()
                try:
                    # Wait a short time for graceful termination
                    self.__tshark_process.wait(timeout=2.0)
                except subprocess.TimeoutExpired:
                    # If it doesn't terminate gracefully, force kill it
                    warnings.warn("TShark did not terminate gracefully, force killing it.")
                    self.__tshark_process.kill()
                    self.__tshark_process.wait(timeout=2.0)
            except Exception:
                # If anything goes wrong, force kill it
                warnings.warn("Error terminating TShark process, force killing it.")
                self.__tshark_process.kill()
                self.__tshark_process.wait(timeout=2.0)
            finally:
                # Check to see if the process is still running
                if self.__tshark_process.poll() is None:
                    warnings.warn(
                        f"TShark process (PID: {self.__tshark_process.pid}) is still running "
                        "after termination attempts. Please check and terminate it manually if needed."
                    )
                else:
                    print("TShark process terminated successfully!")
                self.__tshark_process = None

        # Give tshark a moment to flush its output to the file
        time.sleep(0.1)

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
        except Exception as e:  # pyshark.capture.capture.TSharkCrashException:
            print("Error filtering packets:", e)
        return pid_packets
