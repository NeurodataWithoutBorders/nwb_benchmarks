"""Class for summary and display of basic network statistics."""

import numpy as np

from ._capture_connections import CaptureConnections


class NetworkStatistics:
    """Compute basic statistics about network packets captures with tshark/pyshark."""

    @staticmethod
    def num_packets(packets: list):
        """Total number of packets."""
        return len(packets)

    @staticmethod
    def get_web_traffic_packets(packets: list) -> list:
        """
        Get all HTTP and HTTPS packets from the packets captured.

        Parameters
        ----------
        packet :
            Raw packet from a pcap file.

        Returns
        -------
        Specific packet details.
        """
        web_packets = [
            packet
            for packet in packets
            if hasattr(packet, "tcp") and packet[packet.transport_layer].dstport in ["80", "443"]
        ]
        return web_packets

    @staticmethod
    def transfer_time(packets: list) -> float:
        """Sum of all time_delta's between packets."""
        return float(np.sum([float(p.tcp.time_delta) for p in packets]))

    @staticmethod
    def num_packets_downloaded(packets: list, local_addresses: list = None):
        """Total number of packets downloaded."""
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
        """Total number of packets uploaded (e.g., HTTP requests)."""
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
        """Total number of bytes in the packets."""
        total_bytes = np.sum([len(packet) for packet in packets])
        return int(total_bytes)

    @staticmethod
    def bytes_downloaded(packets: list, local_addresses: list = None):
        """Total number of bytes from downloaded packets."""
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
        """Total number of bytes from uploaded packets."""
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
    def bytes_to_str(size_in_bytes: int) -> str:
        """Format the size in bytes as a human-readable string."""
        for unit in ["", "K", "M", "G", "T", "P"]:
            if size_in_bytes < 1024:
                return f"{bytes:.2f}{unit}B"
            size_in_bytes /= 1024

    @classmethod
    def get_stats(cls, packets: list, local_addresses: list = None):
        """Calculate all the statistics and return them as a dictionary."""
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
        """Print all the statistics."""
        stats = cls.get_stats(packets=packets, local_addresses=local_addresses)
        for k, v in stats.items():
            print(f"{k}: {v}")
