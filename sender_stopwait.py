"""CP372 Assignment 2: Stop-and-Wait UDP file sender.

Usage:
    python sender_stopwait.py input.txt --host 127.0.0.1 --port 12000

Uses only Python's standard library. Coordinate packet format with teammates.
"""

import argparse
import os
import socket
import struct
import time
from pathlib import Path

# Packet: type (1 byte), sequence (4 bytes), acknowledgment (4 bytes),
# payload length (2 bytes), payload (0..1024 bytes).
HEADER = struct.Struct("!BIIH")
TYPE_START = 1
TYPE_DATA = 2
TYPE_END = 3
TYPE_ACK = 4
CHUNK_SIZE = 1024
MAX_DATAGRAM = HEADER.size + CHUNK_SIZE


def make_packet(packet_type, seq=0, ack=0, payload=b""):
    if len(payload) > CHUNK_SIZE:
        raise ValueError("Payload too large")
    return HEADER.pack(packet_type, seq, ack, len(payload)) + payload


def parse_packet(raw):
    if len(raw) < HEADER.size:
        return None
    packet_type, seq, ack, length = HEADER.unpack(raw[:HEADER.size])
    if length != len(raw) - HEADER.size or length > CHUNK_SIZE:
        return None
    return packet_type, seq, ack, raw[HEADER.size:]


def send_and_wait(sock, destination, packet, seq, timeout, max_retries):
    """Wait for an ACK of this exact sequence number; retransmit on timeout."""
    retransmissions = 0
    for attempt in range(max_retries + 1):
        sock.sendto(packet, destination)
        if attempt:
            retransmissions += 1
            print(f"  Timeout; retransmitting seq={seq} (retry {attempt})")

        # Fresh deadline for this transmission. Other ACKs do not restart it.
        deadline = time.monotonic() + timeout
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            sock.settimeout(remaining)
            try:
                raw, address = sock.recvfrom(MAX_DATAGRAM)
            except socket.timeout:
                break
            if address != destination:
                continue
            parsed = parse_packet(raw)
            if parsed is None:
                continue
            kind, _, ack, _ = parsed
            if kind == TYPE_ACK and ack == seq:
                return retransmissions
    raise TimeoutError(f"No ACK for sequence {seq} after {max_retries} retries")


def main():
    parser = argparse.ArgumentParser(description="Reliable Stop-and-Wait UDP sender")
    parser.add_argument("file", type=Path, help="File to transfer")
    parser.add_argument("--host", default="127.0.0.1", help="Receiver IPv4 address")
    parser.add_argument("--port", type=int, default=12000, help="Receiver UDP port")
    parser.add_argument("--timeout", type=float, default=0.5, help="ACK timeout (seconds)")
    parser.add_argument("--retries", type=int, default=30, help="Maximum retries per packet")
    args = parser.parse_args()

    if args.timeout <= 0 or args.retries < 0:
        parser.error("--timeout must be positive and --retries cannot be negative")
    if not args.file.is_file():
        parser.error(f"File not found: {args.file}")

    # START carries file name as UTF-8. If exceptionally long, reject it.
    name = args.file.name.encode("utf-8")
    if not name or len(name) > CHUNK_SIZE:
        parser.error("File name is too long to fit in the START packet")

    destination = (socket.gethostbyname(args.host), args.port)
    file_size = args.file.stat().st_size
    retransmissions = 0
    data_packets = 0
    start_time = time.monotonic()

    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
        with args.file.open("rb") as source:
            # START is seq 0; every packet (including control messages) is ACKed.
            retransmissions += send_and_wait(
                sock, destination, make_packet(TYPE_START, seq=0, payload=name),
                seq=0, timeout=args.timeout, max_retries=args.retries
            )
            seq = 1
            while True:
                chunk = source.read(CHUNK_SIZE)
                if not chunk:
                    break
                retransmissions += send_and_wait(
                    sock, destination, make_packet(TYPE_DATA, seq=seq, payload=chunk),
                    seq=seq, timeout=args.timeout, max_retries=args.retries
                )
                data_packets += 1
                if data_packets % 1000 == 0:
                    print(f"Acknowledged {data_packets} data packets")
                seq += 1

            retransmissions += send_and_wait(
                sock, destination, make_packet(TYPE_END, seq=seq),
                seq=seq, timeout=args.timeout, max_retries=args.retries
            )

    elapsed = time.monotonic() - start_time
    print("\nTransfer complete")
    print(f"File: {args.file.name} ({file_size} bytes)")
    print(f"Data packets: {data_packets}")
    print(f"Retransmissions: {retransmissions}")
    print(f"Total transfer time: {elapsed:.3f} seconds")
    print(f"Throughput: {file_size / elapsed:.2f} bytes/second")


if __name__ == "__main__":
    main()
