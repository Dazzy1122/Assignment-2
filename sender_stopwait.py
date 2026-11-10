"""CP372 Assignment 2 - Stop-and-Wait UDP sender.

Compatible with receiver_stopwait.py in this folder.
Usage: python sender_stopwait.py input.txt
"""

import argparse
import socket
import struct
import time
from pathlib import Path

SERVER_IP = "127.0.0.1"
SERVER_PORT = 5001
TIMEOUT = 2.0
CHUNK_SIZE = 1024
TYPE_START, TYPE_DATA, TYPE_ACK, TYPE_COMPLETE = 0, 1, 2, 3
HEADER_FORMAT = "!III"
HEADER_SIZE = struct.calcsize(HEADER_FORMAT)


def packet_construction(seq_num, ack_num, pkt_type, payload=b""):
    return struct.pack(HEADER_FORMAT, seq_num, ack_num, pkt_type) + payload


def parse_packet(packet):
    if len(packet) < HEADER_SIZE:
        raise ValueError("Packet too short")
    seq, ack, kind = struct.unpack(HEADER_FORMAT, packet[:HEADER_SIZE])
    return seq, ack, kind, packet[HEADER_SIZE:]


def send_and_wait(sock, address, packet, expected_ack, timeout):
    retries = 0
    while True:
        sock.sendto(packet, address)
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
            if source != address:
                continue
            try:
                _, ack_num, kind, _ = parse_packet(ack_data)
            except ValueError:
                continue
            if kind == TYPE_ACK and ack_num == expected_ack:
                print(f"[SENDER] ACK {ack_num} received")
                return retries
            print(f"[SENDER] Ignored unexpected ACK {ack_num}")
        retries += 1
        print(f"[SENDER] Timeout; retransmitting seq={expected_ack}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("filename")
    parser.add_argument("--host", default=SERVER_IP)
    parser.add_argument("--port", type=int, default=SERVER_PORT)
    parser.add_argument("--timeout", type=float, default=TIMEOUT)
    args = parser.parse_args()
    if args.timeout <= 0:
        parser.error("--timeout must be positive")
    path = Path(args.filename)
    if not path.is_file():
        parser.error(f"Input file not found: {path}")
    address = (socket.gethostbyname(args.host), args.port)
    total_retries = 0
    start_time = time.monotonic()
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
        total_retries += send_and_wait(sock, address,
            packet_construction(0, 0, TYPE_START, path.name.encode("utf-8")), 0, args.timeout)
        seq = 1
        total_bytes = 0
        with path.open("rb") as stream:
            while chunk := stream.read(CHUNK_SIZE):
                total_retries += send_and_wait(sock, address,
                    packet_construction(seq, 0, TYPE_DATA, chunk), seq, args.timeout)
                total_bytes += len(chunk)
                seq += 1
        total_retries += send_and_wait(sock, address,
            packet_construction(seq, 0, TYPE_COMPLETE), seq, args.timeout)
    elapsed = time.monotonic() - start_time
    print(f"[SENDER] Completed: {total_bytes} bytes, {elapsed:.3f} seconds, "
          f"{total_retries} retransmissions, {total_bytes / elapsed:.1f} bytes/s")


if __name__ == "__main__":
    main()
