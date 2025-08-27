"""
Class definition for capturing network traffic when remotely reading data from an NWB file.

NOTE: This requires sudo/root access on  macOS and AIX.
"""

import time
from threading import Thread

import psutil


class CaptureConnections(Thread):
    """
    Thread class used to listen to connections on this machine.

    Collects a mapping of connections to process PIDs.
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

    def get_connections_for_pid(self, pid: int):
        """Get list of all the connection for a given pid from `self.connection_to_pid`."""
        return [k for k, v in self.connection_to_pid.items() if v == pid]

    def start(self):
        """Start the capture thread."""
        self.__run_capture_connections = True
        super(CaptureConnections, self).start()

    def stop(self):
        """Stop the capture thread."""
        self.__run_capture_connections = False

    def run(self):
        """Run the capture thread."""
        while self.__run_capture_connections:
            # using psutil, we can grab each connection's source and destination ports
            # and their process ID
            for connection in psutil.net_connections():  # NOTE: This requires sudo/root access on macOS and Linux
                if connection.laddr and connection.raddr and connection.pid:
                    # if local address, remote address and PID are in the connection
                    # add them to our global dictionary
                    self.connection_to_pid[(connection.laddr.port, connection.raddr.port)] = connection.pid
                    self.connection_to_pid[(connection.raddr.port, connection.laddr.port)] = connection.pid
            # check how much sleep-time we should use
            time.sleep(0.2)

    @staticmethod
    def get_local_addresses() -> list:
        """Get list of all local addresses."""
        addresses = [
            psutil.net_if_addrs()[interface][0].address  # get the address of the interface
            for interface in psutil.net_if_addrs()  # for all network interfaces
            if psutil.net_if_addrs()[interface][0].address  # if the interface has a valid address
        ]
        return addresses
