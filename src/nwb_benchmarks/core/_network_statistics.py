"""Class for summary and display of basic network statistics."""

from typing import Dict, Literal, Union

import numpy as np

from ._capture_connections import CaptureConnections


class NetworkStatistics:
    """Compute basic statistics about network packets captures with tshark/pyshark."""

    @staticmethod
    def total_transfer_in_number_of_packets(packets: list) -> int:
        """Total number of packets."""
        return len(packets)

    @staticmethod
    def total_traffic_in_number_of_web_packets(packets: list) -> list:
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
    def total_transfer_time_in_seconds(packets: list) -> float:
        """Sum of all time_delta's between packets."""
        return float(np.sum([float(packet.tcp.Timestamps.time_delta) for packet in packets]))

    @staticmethod
    def amount_downloaded_in_number_of_packets(packets: list, local_addresses: list = None) -> int:
        """Total number of packets downloaded."""
        if local_addresses is None:
            local_addresses = CaptureConnections.get_local_addresses()
        downloaded = [
            packet
            for packet in packets  # check all packets
            if packet.ip.src not in local_addresses  # the source address is not ours so it's download
        ]
        return len(downloaded)

    @staticmethod
    def amount_uploaded_in_number_of_packets(packets: list, local_addresses: list = None) -> int:
        """Total number of packets uploaded (e.g., HTTP requests)."""
        if local_addresses is None:
            local_addresses = CaptureConnections.get_local_addresses()
        uploaded = [
            packet
            for packet in packets  # check all packets
            if packet.ip.src in local_addresses  # the source address is ours so it's upload
        ]
        return len(uploaded)

    @staticmethod
    def total_transfer_in_bytes(packets: list) -> int:
        """Total number of bytes in the packets."""
        total_transfer_in_bytes = np.sum([len(packet) for packet in packets])
        return int(total_transfer_in_bytes)

    @staticmethod
    def amount_downloaded_in_bytes(packets: list, local_addresses: list = None) -> int:
        """Total number of bytes from downloaded packets."""
        if local_addresses is None:
            local_addresses = CaptureConnections.get_local_addresses()
        amount_downloaded_in_bytes = np.sum(
            [
                len(packet)  # size of packet in bytes
                for packet in packets  # check all packets
                if packet.ip.src not in local_addresses  # the source address is not ours so it's download
            ]
        )
        return int(amount_downloaded_in_bytes)

    @staticmethod
    def amount_uploaded_in_bytes(packets: list, local_addresses: list = None) -> int:
        """Total number of bytes from uploaded packets."""
        if local_addresses is None:
            local_addresses = CaptureConnections.get_local_addresses()
        amount_uploaded_in_bytes = np.sum(
            [
                len(packet)  # size of packet in bytes
                for packet in packets  # check all packets
                if packet.ip.src in local_addresses  # the source address is ours so it's upload
            ]
        )
        return int(amount_uploaded_in_bytes)

    @staticmethod
    def bytes_to_str(size_in_bytes: int, convention: Literal["B", "iB"] = "iB") -> str:
        """Format the size in bytes as a human-readable string."""
        factor = 1024 if convention == "iB" else 1000
        for unit in ["", "K", "M", "G", "T", "P"]:
            if size_in_bytes < factor:
                return f"{bytes:.2f}{unit}{convention}"
            size_in_bytes /= factor

    @classmethod
    def get_statistics(cls, packets: list, local_addresses: list = None) -> Dict[str, Union[int, float]]:
        """Calculate all the statistics and return them as a dictionary."""
        if local_addresses is None:
            local_addresses = CaptureConnections.get_local_addresses()
        statistics = {
            "amount_downloaded_in_bytes": cls.amount_downloaded_in_bytes(
                packets=packets, local_addresses=local_addresses
            ),
            "amount_uploaded_in_bytes": cls.amount_uploaded_in_bytes(packets=packets, local_addresses=local_addresses),
            "total_transfer_in_bytes": cls.total_transfer_in_bytes(packets=packets),
            "amount_downloaded_in_number_of_packets": cls.amount_downloaded_in_number_of_packets(
                packets=packets, local_addresses=local_addresses
            ),
            "amount_uploaded_in_number_of_packets": cls.amount_uploaded_in_number_of_packets(
                packets=packets, local_addresses=local_addresses
            ),
            "total_transfer_in_number_of_packets": cls.total_transfer_in_number_of_packets(packets=packets),
            "total_traffic_in_number_of_web_packets": len(cls.total_traffic_in_number_of_web_packets(packets)),
            "total_transfer_time_in_seconds": cls.total_transfer_time_in_seconds(packets=packets),
        }
        return statistics

    @classmethod
    def print_statistics(cls, packets: list, local_addresses: list = None):
        """Print all the statistics."""
        statistics = cls.get_statistics(packets=packets, local_addresses=local_addresses)
        for key, value in statistics.items():
            print(f"{key}: {value}")
