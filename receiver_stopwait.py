
"""CP372 Assignment 2 - Stop-and-Wait UDP Receiver.

Implements reliable file reception over UDP using Stop-and-Wait.
The receiver accepts packets in sequence, saves their payloads,
and sends acknowledgments (ACKs) to the sender.

Duplicate or out-of-order DATA packets are discarded and the
last valid ACK is resent.

Packet header: sequence number, ACK number, and packet type.
Types: START=0, DATA=1, ACK=2, COMPLETE=3.

Usage:
    python receiver_stopwait.py --loss 0.2
"""

import argparse
import random
import socket
import struct
from pathlib import Path

# Network settings and receiver configuration
BIND_IP = "127.0.0.1"       # Localhost
BIND_PORT = 5001
OUTPUT_DIR = Path("received")  # Directory for received files

# Packet header: sequence number, ACK number, and packet type
# Each field is a 4-byte unsigned integer (12 bytes total)
HEADER_FORMAT = "!III"
HEADER_SIZE = struct.calcsize(HEADER_FORMAT)

# Packet types shared with the sender
TYPE_START = 0
TYPE_DATA = 1
TYPE_ACK = 2
TYPE_COMPLETE = 3

# Maximum size of an incoming UDP datagram
BUFFER_SIZE = 65535


def packet_construction(seq_num, ack_num, pkt_type, payload=b""):
    """Construct a packet by combining its header and payload."""
    return struct.pack(HEADER_FORMAT, seq_num, ack_num, pkt_type) + payload


def parse_packet(packet):
    """Extract the sequence number, ACK number, type, and payload."""
    if len(packet) < HEADER_SIZE:
        raise ValueError("Packet too short to contain a valid header")

    seq_num, ack_num, pkt_type = struct.unpack(
        HEADER_FORMAT, packet[:HEADER_SIZE]
    )

    return seq_num, ack_num, pkt_type, packet[HEADER_SIZE:]


def safe_filename(payload):
    """Prevent remotely supplied names from escaping the output directory."""
    name = payload.decode("utf-8", errors="replace") if payload else "received_file.txt"
    name = name.replace("\\", "/").split("/")[-1]
    return name if name not in ("", ".", "..") else "received_file.txt"


def main():
    # Step 1: Read command-line arguments
    # The loss rate can be changed for different experiments
    parser = argparse.ArgumentParser(description="Stop-and-Wait UDP file receiver")
    parser.add_argument("--port", type=int, default=BIND_PORT)
    parser.add_argument("--loss", type=float, default=0.0,
                        help="Simulated inbound packet loss probability between 0 and 1")

    args = parser.parse_args()

    # Ensure the packet loss rate is within the valid range
    if not 0.0 <= args.loss <= 1.0:
        parser.error("--loss must be between 0 and 1")

    # Step 2: Create output directory and initialize UDP socket
    OUTPUT_DIR.mkdir(exist_ok=True)

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind((BIND_IP, args.port))

    print(f"[RECEIVER] Listening on {BIND_IP}:{args.port}; packet loss={args.loss:.0%}")

    # Track the current transfer and the next expected sequence number
    file_handle = None
    sender_addr = None
    expected_seq = 1  # START uses seq 0; first DATA uses seq 1
    last_ack = None
    finished = False

    def send_ack(number, destination):
        """Send an ACK for a successfully received packet."""
        sock.sendto(packet_construction(0, number, TYPE_ACK), destination)
        print(f"[RECEIVER] Sent ACK {number}")

    try:
        # Step 3: Continuously listen for incoming UDP packets
        while True:
            packet, addr = sock.recvfrom(BUFFER_SIZE)

            # Extract packet information from the header
            try:
                seq_num, _, pkt_type, payload = parse_packet(packet)
            except ValueError:
                print("[RECEIVER] Ignored malformed packet")
                continue

            # Step 4: Simulate random packet loss
            # Dropped packets are ignored without sending ACKs
            # This includes START, DATA, and COMPLETE packets
            if random.random() < args.loss:
                print(f"[RECEIVER] Simulated loss: dropped type={pkt_type} seq={seq_num}")
                continue

            # Step 5: Handle START notification
            # Create a new file and initialize sequence tracking
            if pkt_type == TYPE_START:

                # If START is retransmitted during an active transfer,
                # resend ACK 0 without reopening or overwriting the file
                if sender_addr == addr and not finished and file_handle is not None:
                    send_ack(0, addr)
                    continue

                # Only process one active sender at a time
                if sender_addr is not None and not finished and sender_addr != addr:
                    print("[RECEIVER] Busy with a different sender; ignoring START")
                    continue

                if file_handle is not None:
                    file_handle.close()

                # Extract filename and open the destination file
                filename = safe_filename(payload)
                output_path = OUTPUT_DIR / filename
                file_handle = output_path.open("wb")

                # Initialize the new file transfer
                sender_addr = addr
                expected_seq = 1
                last_ack = 0
                finished = False

                print(f"[RECEIVER] START: saving to {output_path}")
                send_ack(0, addr)
                continue

            # Ignore packets that do not belong to the active sender
            if addr != sender_addr:
                continue

            # Step 6: Handle incoming DATA packets
            if pkt_type == TYPE_DATA:

                # Accept only the next expected packet
                if file_handle is not None and not finished and seq_num == expected_seq:

                    # Save the payload and update sequence tracking
                    file_handle.write(payload)
                    last_ack = seq_num
                    expected_seq += 1

                    print(f"[RECEIVER] Wrote DATA seq={seq_num} ({len(payload)} bytes)")

                    # Send ACK to confirm successful reception
                    send_ack(seq_num, addr)

                elif last_ack is not None:
                    # Discard duplicate or out-of-order packets
                    # Resend the last valid ACK to help the sender recover
                    print(f"[RECEIVER] Ignored duplicate/out-of-order DATA seq={seq_num}; expected={expected_seq}")
                    send_ack(last_ack, addr)

            # Step 7: Handle file transfer completion
            elif pkt_type == TYPE_COMPLETE:

                # Accept COMPLETE only when all DATA packets have arrived
                if not finished and file_handle is not None and seq_num == expected_seq:

                    # Close the file and mark transfer as finished
                    file_handle.close()
                    file_handle = None
                    finished = True
                    last_ack = seq_num

                    print("[RECEIVER] Transfer complete; file closed")
                    send_ack(seq_num, addr)

                elif finished and seq_num == last_ack:
                    # If the COMPLETE ACK was lost, acknowledge again
                    # without reopening the completed file
                    send_ack(seq_num, addr)

                elif last_ack is not None:
                    # Reject a premature COMPLETE packet
                    # and acknowledge the last valid packet instead
                    print(f"[RECEIVER] Ignored premature COMPLETE seq={seq_num}; expected={expected_seq}")
                    send_ack(last_ack, addr)

    except KeyboardInterrupt:
        # Allow the receiver to be stopped manually in PyCharm
        print("\n[RECEIVER] Stopped by user")

    finally:
        # Close any remaining open file and release the socket
        if file_handle is not None:
            file_handle.close()
        sock.close()


if __name__ == "__main__":
    main()
