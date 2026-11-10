"""CP372 Assignment 2: Stop-and-Wait UDP file receiver.

Usage:
    python receiver_stopwait.py --port 12000 --loss 0.2 --output-dir received

Loss rate: 0.0 (0%), 0.1 (10%), 0.2 (20%), 0.3 (30%).
Uses only Python's standard library. Coordinate packet format with teammates.
"""

import argparse
import random
import socket
import struct
import time
from pathlib import Path

# MUST match sender_stopwait.py (duplicated to keep each required file standalone).
HEADER = struct.Struct("!BIIH")
TYPE_START = 1
TYPE_DATA = 2
TYPE_END = 3
TYPE_ACK = 4
CHUNK_SIZE = 1024
MAX_DATAGRAM = HEADER.size + CHUNK_SIZE


def make_ack(seq):
    return HEADER.pack(TYPE_ACK, 0, seq, 0)


def parse_packet(raw):
    if len(raw) < HEADER.size:
        return None
    kind, seq, ack, length = HEADER.unpack(raw[:HEADER.size])
    if length != len(raw) - HEADER.size or length > CHUNK_SIZE:
        return None
    return kind, seq, ack, raw[HEADER.size:]


def main():
    parser = argparse.ArgumentParser(description="Reliable Stop-and-Wait UDP receiver")
    parser.add_argument("--host", default="0.0.0.0", help="Address to listen on")
    parser.add_argument("--port", type=int, default=12000, help="UDP port")
    parser.add_argument("--loss", type=float, default=0.0, help="Simulated packet loss from 0.0 to 1.0")
    parser.add_argument("--output-dir", type=Path, default=Path("received"))
    parser.add_argument("--linger", type=float, default=3.0,
                        help="Seconds to acknowledge duplicate END packets after completion")
    args = parser.parse_args()
    if not 0 <= args.loss <= 1 or args.linger <= 0:
        parser.error("--loss must be between 0 and 1 and --linger must be positive")
    args.output_dir.mkdir(parents=True, exist_ok=True)

    expected_seq = 0
    last_acked = None
    peer = None
    output = None
    output_path = None
    bytes_written = 0
    end_time = None

    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
        sock.bind((args.host, args.port))
        print(f"Listening on UDP {args.host}:{args.port}, simulated loss: {args.loss:.0%}")
        try:
            while True:
                if end_time is not None:
                    remaining = end_time + args.linger - time.monotonic()
                    if remaining <= 0:
                        break
                    sock.settimeout(remaining)
                else:
                    sock.settimeout(None)

                try:
                    raw, address = sock.recvfrom(MAX_DATAGRAM)
                except socket.timeout:
                    break

                # One transfer per run. Ignore packets from unrelated senders.
                if peer is not None and address != peer:
                    continue

                if random.random() < args.loss:
                    print(f"Simulated packet drop from {address}")
                    continue

                packet = parse_packet(raw)
                if packet is None:
                    continue
                kind, seq, _, payload = packet
                if kind not in (TYPE_START, TYPE_DATA, TYPE_END):
                    continue

                if seq != expected_seq:
                    # Duplicate packet: never write payload again. Re-ACK last valid.
                    # Out-of-order packet: also respond with the last accepted ACK.
                    if last_acked is not None:
                        sock.sendto(make_ack(last_acked), address)
                    continue

                if kind == TYPE_START and expected_seq == 0:
                    # Only use a basename; never allow sender-supplied paths.
                    filename = Path(payload.decode("utf-8", errors="replace").replace("\\", "/")).name
                    if not filename or filename in (".", ".."):
                        filename = "received_file.bin"
                    output_path = args.output_dir / filename
                    output = output_path.open("wb")
                    peer = address
                    print(f"Receiving file: {output_path}")
                elif kind == TYPE_DATA and output is not None and end_time is None:
                    output.write(payload)
                    bytes_written += len(payload)
                elif kind == TYPE_END and output is not None and end_time is None:
                    output.close()
                    output = None
                    end_time = time.monotonic()
                    print(f"File complete: {output_path} ({bytes_written} bytes)")
                else:
                    # Unexpected packet type for current stage: do not ACK it.
                    continue

                sock.sendto(make_ack(seq), address)
                last_acked = seq
                expected_seq += 1
        finally:
            if output is not None:
                output.close()

    if end_time is not None:
        print("Receiver finished successfully")
    else:
        print("Receiver stopped before transfer completed")


if __name__ == "__main__":
    main()
