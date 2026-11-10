"""CP372 Assignment 2 - Stop-and-Wait UDP receiver.

Packet header: network-order unsigned 32-bit integers (seq, ack, type).
Types: START=0, DATA=1, ACK=2, COMPLETE=3.

Run: python receiver_stopwait.py --loss 0.2
The receiver listens continuously for new transfers. The sender must use
sequence 0 for START, sequence 1 for first DATA, and next seq for COMPLETE.
"""

import argparse
import random
import socket
import struct
from pathlib import Path

BIND_IP = "127.0.0.1"
BIND_PORT = 5001
OUTPUT_DIR = Path("received")
HEADER_FORMAT = "!III"
HEADER_SIZE = struct.calcsize(HEADER_FORMAT)
TYPE_START = 0
TYPE_DATA = 1
TYPE_ACK = 2
TYPE_COMPLETE = 3
BUFFER_SIZE = 65535


def packet_construction(seq_num, ack_num, pkt_type, payload=b""):
    return struct.pack(HEADER_FORMAT, seq_num, ack_num, pkt_type) + payload


def parse_packet(packet):
    if len(packet) < HEADER_SIZE:
        raise ValueError("Packet too short to contain a valid header")
    seq_num, ack_num, pkt_type = struct.unpack(HEADER_FORMAT, packet[:HEADER_SIZE])
    return seq_num, ack_num, pkt_type, packet[HEADER_SIZE:]


def safe_filename(payload):
    """Prevent remotely supplied names from escaping the output directory."""
    name = payload.decode("utf-8", errors="replace") if payload else "received_file.txt"
    name = name.replace("\\", "/").split("/")[-1]
    return name if name not in ("", ".", "..") else "received_file.txt"


def main():
    parser = argparse.ArgumentParser(description="Stop-and-Wait UDP file receiver")
    parser.add_argument("--port", type=int, default=BIND_PORT)
    parser.add_argument("--loss", type=float, default=0.0,
                        help="Simulated inbound packet loss probability between 0 and 1")
    args = parser.parse_args()
    if not 0.0 <= args.loss <= 1.0:
        parser.error("--loss must be between 0 and 1")

    OUTPUT_DIR.mkdir(exist_ok=True)
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind((BIND_IP, args.port))
    print(f"[RECEIVER] Listening on {BIND_IP}:{args.port}; packet loss={args.loss:.0%}")

    file_handle = None
    sender_addr = None
    expected_seq = 1
    last_ack = None
    finished = False

    def send_ack(number, destination):
        sock.sendto(packet_construction(0, number, TYPE_ACK), destination)
        print(f"[RECEIVER] Sent ACK {number}")

    try:
        while True:
            packet, addr = sock.recvfrom(BUFFER_SIZE)
            try:
                seq_num, _, pkt_type, payload = parse_packet(packet)
            except ValueError:
                print("[RECEIVER] Ignored malformed packet")
                continue

            # Simulate incoming packet loss, including START / COMPLETE.
            if random.random() < args.loss:
                print(f"[RECEIVER] Simulated loss: dropped type={pkt_type} seq={seq_num}")
                continue

            if pkt_type == TYPE_START:
                if sender_addr == addr and not finished and file_handle is not None:
                    # Lost START ACK: never reopen/truncate an in-progress file.
                    send_ack(0, addr)
                    continue
                if sender_addr is not None and not finished and sender_addr != addr:
                    print("[RECEIVER] Busy with a different sender; ignoring START")
                    continue
                if file_handle is not None:
                    file_handle.close()
                filename = safe_filename(payload)
                output_path = OUTPUT_DIR / filename
                file_handle = output_path.open("wb")
                sender_addr = addr
                expected_seq = 1
                last_ack = 0
                finished = False
                print(f"[RECEIVER] START: saving to {output_path}")
                send_ack(0, addr)
                continue

            if addr != sender_addr:
                continue

            if pkt_type == TYPE_DATA:
                if file_handle is not None and not finished and seq_num == expected_seq:
                    file_handle.write(payload)
                    last_ack = seq_num
                    expected_seq += 1
                    print(f"[RECEIVER] Wrote DATA seq={seq_num} ({len(payload)} bytes)")
                    send_ack(seq_num, addr)
                elif last_ack is not None:
                    print(f"[RECEIVER] Ignored duplicate/out-of-order DATA seq={seq_num}; expected={expected_seq}")
                    send_ack(last_ack, addr)

            elif pkt_type == TYPE_COMPLETE:
                if not finished and file_handle is not None and seq_num == expected_seq:
                    file_handle.close()
                    file_handle = None
                    finished = True
                    last_ack = seq_num
                    print("[RECEIVER] Transfer complete; file closed")
                    send_ack(seq_num, addr)
                elif finished and seq_num == last_ack:
                    # Lost COMPLETE ACK: re-ACK without reopening the file.
                    send_ack(seq_num, addr)
                elif last_ack is not None:
                    print(f"[RECEIVER] Ignored premature COMPLETE seq={seq_num}; expected={expected_seq}")
                    send_ack(last_ack, addr)
    except KeyboardInterrupt:
        print("\n[RECEIVER] Stopped by user")
    finally:
        if file_handle is not None:
            file_handle.close()
        sock.close()


if __name__ == "__main__":
    main()
