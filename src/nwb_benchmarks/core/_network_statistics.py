"""Class for summary and display of basic network statistics."""

import time
from typing import Dict, Union

import pyshark

from ._capture_connections import CaptureConnections


class NetworkStatistics:
    """Compute basic statistics about network packets captures with tshark/pyshark."""

    @staticmethod
    def compute_statistics(capture_file_path, pid_connections: list) -> Dict[str, Union[int, float]]:
        """
        Compute network statistics by streaming through packets without storing them all in memory.

        Parameters
        ----------
        capture_file_path : pathlib.Path
            Path to the capture file to process
        pid_connections : list
            List of connection tuples (src_port, dst_port) to filter packets for

        Returns
        -------
        Dict[str, Union[int, float]]
            Dictionary containing all computed statistics
        """

        local_addresses = CaptureConnections.get_local_addresses()

        # Initialize counters
        stats = {
            "total_transfer_in_number_of_packets": 0,
            "total_traffic_in_number_of_web_packets": 0,
            "amount_downloaded_in_number_of_packets": 0,
            "amount_uploaded_in_number_of_packets": 0,
            "total_transfer_in_bytes": 0,
            "amount_downloaded_in_bytes": 0,
            "amount_uploaded_in_bytes": 0,
            "total_transfer_time_in_seconds": 0.0,
        }

        try:
            capture_file_size_mb = capture_file_path.stat().st_size / (1024 * 1024)
            print(f"Processing packets from capture file ({capture_file_size_mb:.0f} MB) with streaming...")
            start_time = time.time()

            cap = pyshark.FileCapture(capture_file_path, use_json=True)

            # Process packets one by one
            packet_count = 0
            for packet in cap:
                packet_count += 1

                # Filter for PID connections if specified
                is_pid_packet = False
                if hasattr(packet, "tcp"):
                    ports = int(str(packet.tcp.srcport)), int(str(packet.tcp.dstport))
                    if ports in pid_connections:
                        is_pid_packet = True

                if not is_pid_packet:
                    continue

                # Update packet count
                stats["total_transfer_in_number_of_packets"] += 1

                # Check if it's a web packet (HTTP/HTTPS)
                if hasattr(packet, "tcp") and packet[packet.transport_layer].dstport in ["80", "443"]:
                    stats["total_traffic_in_number_of_web_packets"] += 1

                # Calculate bytes
                packet_size = len(packet)
                stats["total_transfer_in_bytes"] += packet_size

                # Determine if upload or download
                if hasattr(packet, "ip"):
                    if packet.ip.src not in local_addresses:  # Download
                        stats["amount_downloaded_in_number_of_packets"] += 1
                        stats["amount_downloaded_in_bytes"] += packet_size
                    else:  # Upload
                        stats["amount_uploaded_in_number_of_packets"] += 1
                        stats["amount_uploaded_in_bytes"] += packet_size

                # Add time delta if available
                if (
                    hasattr(packet, "tcp")
                    and hasattr(packet.tcp, "Timestamps")
                    and hasattr(packet.tcp.Timestamps, "time_delta")
                ):
                    stats["total_transfer_time_in_seconds"] += float(packet.tcp.Timestamps.time_delta)

                # Print progress every 100000 packets
                if packet_count % 100000 == 0:
                    print(
                        f"Processed {packet_count} packets, {stats['total_transfer_in_number_of_packets']} "
                        "relevant packets found..."
                    )

            cap.close()
            del cap

            end_time = time.time()
            print(f"Packets processed: {packet_count}")
            print(f"Relevant packets found: {stats['total_transfer_in_number_of_packets']}")
            print(f"Time taken to process packets: {end_time - start_time:.1f} seconds")

        except Exception as e:
            print("Error processing packets:", e)

        return stats
