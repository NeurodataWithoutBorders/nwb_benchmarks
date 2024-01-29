"""
Example script to test capturing network traffic for remote data reads for NWB

NOTE:  This requires sudo/root access on  macOS and AIX
"""
import os
import subprocess
import tempfile
import time
from threading import Thread

import numpy as np
import psutil
import pyshark
from pynwb import NWBHDF5IO


class CaptureConnections(Thread):
    """
    Thread class used to  listen for connections on this machine
    and collecting the mapping of connections to process pid's
    """

    def __init__(self):
        super(CaptureConnections, self).__init__()
        self.__connection_to_pid = {}  # map each pair of connection ports to the corresponding process ID (PID)
        self.__run_capture_connections = False  # Used to control the capture_connections thread

    @property
    def connection_to_pid(self):
        """
        Dict mapping of connections to process pid's.
        The keys are tuples of 2 ports, and the values are the process id
        """
        return self.__connection_to_pid

    def get_connections_for_pid(self, pid):
        """
        Get list of all the connection for a given pid from `self.connection_to_pid`
        """
        return [k for k, v in self.connection_to_pid.items() if v == pid]

    def start(self):
        """Start the capture thread"""
        self.__run_capture_connections = True
        super(CaptureConnections, self).start()

    def stop(self):
        """Stop the capture thread"""
        self.__run_capture_connections = False

    def run(self):
        """Run the capture thread"""
        while self.__run_capture_connections:
            # using psutil, we can grab each connection's source and destination ports
            # and their process ID
            for connection in psutil.net_connections():  # NOTE: This requires sudo/root access on  macOS and AIX
                if connection.laddr and connection.raddr and connection.pid:
                    # if local address, remote address and PID are in the connection
                    # add them to our global dictionary
                    self.connection_to_pid[(connection.laddr.port, connection.raddr.port)] = connection.pid
                    self.connection_to_pid[(connection.raddr.port, connection.laddr.port)] = connection.pid
            # check how much sleep-time we should use
            time.sleep(0.2)

    @staticmethod
    def get_local_addresses():
        """Get list of all local addresses"""
        addresses = [
            psutil.net_if_addrs()[interface][0].address  # get the address of the interface
            for interface in psutil.net_if_addrs()  # for all network interfaces
            if psutil.net_if_addrs()[interface][0].address  # if the interface has a valid address
        ]
        return addresses


class NetProfiler:
    """
    Use TShark command line interface in a subprocess to capture network traffic packets
    in the background.
    """

    def __init__(self, capture_filename=None):
        self.__capture_filename = capture_filename
        self.__tshark_process = None
        self.__packets = None
        if self.__capture_filename is None:
            self.__capture_filename = tempfile.NamedTemporaryFile(mode="w", delete=False)

    def __del__(self):
        self.stop_capture()
        if os.path.exists(self.capture_filename):
            os.remove(self.capture_filename)

    @property
    def packets(self):
        """List of all packets captured"""
        if self.__packets is None:
            try:
                cap = pyshark.FileCapture(self.capture_filename)
                self.__packets = [packet for packet in cap]
                del cap
            except:
                pass
        return self.__packets

    @property
    def capture_filename(self):
        """Filename of the capture file"""
        if isinstance(self.__capture_filename, str):
            return self.__capture_filename
        else:
            return self.__capture_filename.name

    def start_capture(self):
        """Start the capture with tshark in a subprocess"""
        tsharkCall = ["tshark", "-w", self.capture_filename]
        self.__tshark_process = subprocess.Popen(tsharkCall, stderr=subprocess.DEVNULL)  # ,
        time.sleep(0.2)  # not sure if this is needed but just to be safe

    def stop_capture(self):
        """Stop the capture with tshark in a subprocess"""
        if self.__tshark_process is not None:
            self.__tshark_process.terminate()
            self.__tshark_process.kill()
            del self.__tshark_process
            self.__tshark_process = None

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


class NetStats:
    """Compute basic statistics about network packets captures with tshark/pyshark"""
    @staticmethod
    def num_packets(packets: list):
        """Total number of packets"""
        return len(packets)

    @staticmethod
    def get_web_traffic_packets(packets):
        """
        Get all HTTP and HTTPS packets from the packets captured.

        :param packet: raw packet from a pcap file.
        :return: specific packet details
        """
        web_packets = [
            packet
            for packet in packets
            if hasattr(packet, "tcp") and packet[packet.transport_layer].dstport in ["80", "443"]
        ]
        return web_packets

    @staticmethod
    def transfer_time(packets):
        """Sum of all time_delta's between packets"""
        return float(np.sum([float(p.tcp.time_delta) for p in packets]))

    @staticmethod
    def num_packets_downloaded(packets: list, local_addresses: list = None):
        """Total number of packets downloaded"""
        if local_addresses is None:
            local_addresses = CaptureConnections.get_local_addresses()
        downloaded = [
            packet
            for packet in packets  # check all packets
            if packet.ip.src not in local_addresses  # the source address is not ours so it's download
        ]
        return int(len(downloaded))

    @staticmethod
    def num_packets_uploaded(packets: list, local_addresses: list = None):
        """Total number of packets uploaded (e.g., HTTP requests)"""
        if local_addresses is None:
            local_addresses = CaptureConnections.get_local_addresses()
        uploaded = [
            packet
            for packet in packets  # check all packets
            if packet.ip.src in local_addresses  # the source address is ours so it's upload
        ]
        return int(len(uploaded))

    @staticmethod
    def total_bytes(packets: list):
        """Total number of bytes in the packets"""
        total_bytes = np.sum([len(packet) for packet in packets])
        return int(total_bytes)

    @staticmethod
    def bytes_downloaded(packets: list, local_addresses: list = None):
        """Total number of bytes from downloaded packets"""
        if local_addresses is None:
            local_addresses = CaptureConnections.get_local_addresses()
        bytes_downloaded = np.sum(
            [
                len(packet)  # size of packet in bytes
                for packet in packets  # check all packets
                if packet.ip.src not in local_addresses  # the source address is not ours so it's download
            ]
        )
        return int(bytes_downloaded)

    @staticmethod
    def bytes_uploaded(packets: list, local_addresses: list = None):
        """Total number of bytes from uploaded packets"""
        if local_addresses is None:
            local_addresses = CaptureConnections.get_local_addresses()
        bytes_uploaded = np.sum(
            [
                len(packet)  # size of packet in bytes
                for packet in packets  # check all packets
                if packet.ip.src in local_addresses  # the source address is ours so it's upload
            ]
        )
        return int(bytes_uploaded)

    @staticmethod
    def bytes_to_str(bytes: int) -> str:
        """Format the size in bytes as a human-readable string"""
        for unit in ["", "K", "M", "G", "T", "P"]:
            if bytes < 1024:
                return f"{bytes:.2f}{unit}B"
            bytes /= 1024

    @classmethod
    def get_stats(cls, packets: list, local_addresses: list = None):
        """
        Calculate all the statistics and return them as a dictionary
        """
        if local_addresses is None:
            local_addresses = CaptureConnections.get_local_addresses()
        stats = {
            "bytes_downloaded": cls.bytes_downloaded(packets=packets, local_addresses=local_addresses),
            "bytes_uploaded": cls.bytes_uploaded(packets=packets, local_addresses=local_addresses),
            "bytes_total": cls.total_bytes(packets=packets),
            "num_packets": cls.num_packets(packets=packets),
            "num_packets_downloaded": cls.num_packets_downloaded(packets=packets, local_addresses=local_addresses),
            "num_packets_uploaded": cls.num_packets_uploaded(packets=packets, local_addresses=local_addresses),
            "num_web_packets": len(cls.get_web_traffic_packets(packets)),
            "total_transfer_time": cls.transfer_time(packets=packets),
        }
        return stats

    @classmethod
    def print_stats(cls, packets: list, local_addresses: list = None):
        """Print all the statistics"""
        stats = cls.get_stats(packets=packets, local_addresses=local_addresses)
        for k, v in stats.items():
            print(f"{k}: {v}")


if __name__ == "__main__":
    ###
    # This would be part of the setUp() of a test
    ##
    # start the capture_connections() function to update the current connections of this machine
    connections_thread = CaptureConnections()
    connections_thread.start()
    time.sleep(0.2)  # not sure if this is needed but just to be safe

    # start capturing the raw packets by running the tshark commandline tool in a subprocess
    netprofiler = NetProfiler()
    netprofiler.start_capture()

    ###
    # This would be the unit test
    ###
    # Read the NWB data file from DANDI
    t0 = time.time()
    s3_path = "https://dandiarchive.s3.amazonaws.com/ros3test.nwb"
    with NWBHDF5IO(s3_path, mode="r", driver="ros3") as io:
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
    NetStats.print_stats(packets=pid_packets)
    print("total_time:", total_time)
