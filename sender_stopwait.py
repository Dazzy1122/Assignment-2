
"""CP372 Assignment 2 - Stop-and-Wait UDP Sender.

Implements reliable file transfer over UDP using Stop-and-Wait.
The sender transmits one packet at a time and waits for its ACK
before continuing to the next packet.

Usage:
    python sender_stopwait.py input.txt
"""

import argparse
import socket
import struct
import time
from pathlib import Path

# Network settings and transmission parameters
SERVER_IP = "127.0.0.1"  # Localhost
SERVER_PORT = 5001
TIMEOUT = 2.0            # Seconds before retransmission
CHUNK_SIZE = 1024        # Maximum payload size: 1024 bytes

# Packet types used by both sender and receiver
TYPE_START, TYPE_DATA, TYPE_ACK, TYPE_COMPLETE = 0, 1, 2, 3

# Packet header: sequence number, ACK number, and packet type
# Each field is a 4-byte unsigned integer (12 bytes total)
HEADER_FORMAT = "!III"
HEADER_SIZE = struct.calcsize(HEADER_FORMAT)


def packet_construction(seq_num, ack_num, pkt_type, payload=b""):
    """Construct a packet by combining its header and payload."""
    return struct.pack(HEADER_FORMAT, seq_num, ack_num, pkt_type) + payload


def parse_packet(packet):
    """Extract sequence number, ACK number, type, and payload."""
    if len(packet) < HEADER_SIZE:
        raise ValueError("Packet too short")

    seq, ack, kind = struct.unpack(
        HEADER_FORMAT, packet[:HEADER_SIZE]
    )

    return seq, ack, kind, packet[HEADER_SIZE:]


def send_and_wait(sock, address, packet, expected_ack, timeout):
    """Send a packet and wait for its matching ACK.

    If the ACK does not arrive before the timeout,
    retransmit the same packet until it is acknowledged.
    """
    retries = 0

    while True:
        # Send one packet over UDP
        sock.sendto(packet, address)

        # Start the timer for this transmission attempt
        deadline = time.monotonic() + timeout

        while True:
            remaining = deadline - time.monotonic()

            if remaining <= 0:
                break

            sock.settimeout(remaining)

            try:
                ack_data, source = sock.recvfrom(2048)
            except socket.timeout:
                break

            # Ignore packets from unexpected senders
            if source != address:
                continue

            try:
                _, ack_num, kind, _ = parse_packet(ack_data)
            except ValueError:
                continue

            # Only advance after receiving the correct ACK
            if kind == TYPE_ACK and ack_num == expected_ack:
                print(f"[SENDER] ACK {ack_num} received")
                return retries

            # Ignore incorrect or duplicate ACKs
            print(f"[SENDER] Ignored unexpected ACK {ack_num}")

        # No matching ACK received before timeout
        # Retransmit the same packet without advancing
        retries += 1
        print(f"[SENDER] Timeout; retransmitting seq={expected_ack}")


def main():
    # Read command-line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("filename")
    parser.add_argument("--host", default=SERVER_IP)
    parser.add_argument("--port", type=int, default=SERVER_PORT)
    parser.add_argument("--timeout", type=float, default=TIMEOUT)

    args = parser.parse_args()

    if args.timeout <= 0:
        parser.error("--timeout must be positive")

    # Check whether the input file exists
    path = Path(args.filename)

    if not path.is_file():
        parser.error(f"Input file not found: {path}")

    address = (socket.gethostbyname(args.host), args.port)

    # Initialize performance measurements
    total_retries = 0
    start_time = time.perf_counter()

    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:

        # Step 1: Notify the receiver that a transfer is starting
        # START uses sequence number 0 and includes the filename
        total_retries += send_and_wait(
            sock, address,
            packet_construction(
                0, 0, TYPE_START, path.name.encode("utf-8")
            ),
            0, args.timeout
        )

        # Step 2: Divide the file into 1024-byte data packets
        seq = 1
        total_bytes = 0

        with path.open("rb") as stream:
            while chunk := stream.read(CHUNK_SIZE):

                # Send one DATA packet and wait for its ACK
                total_retries += send_and_wait(
                    sock, address,
                    packet_construction(seq, 0, TYPE_DATA, chunk),
                    seq, args.timeout
                )

                # Update counters only after successful ACK
                total_bytes += len(chunk)
                seq += 1

        # Step 3: Notify the receiver that transfer is complete
        # COMPLETE also requires an acknowledgment
        total_retries += send_and_wait(
            sock, address,
            packet_construction(seq, 0, TYPE_COMPLETE),
            seq, args.timeout
        )

    # Step 4: Calculate transfer performance metrics
    elapsed = time.perf_counter() - start_time

    # Prevent division by zero for extremely short transfers
    throughput = total_bytes / elapsed if elapsed > 0 else 0.0

    print("[SENDER] Transfer completed successfully!")
    print(f"[SENDER] Total bytes: {total_bytes}")
    print(f"[SENDER] Total time: {elapsed:.6f} seconds")
    print(f"[SENDER] Retransmissions: {total_retries}")
    print(f"[SENDER] Throughput: {throughput:.2f} bytes/s")


if __name__ == "__main__":
    main()
