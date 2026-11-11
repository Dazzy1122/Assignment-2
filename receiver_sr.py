"""
receiver_gbn.py
CP372 - Computer Networks A02 - Part C

Implements a reliable data transfer receiver over UDP sockets using the
Go-Back-N (GBN) protocol. It accepts only the next in-order packet, discards
out-of-order packets, and replies with cumulative ACKs.

Cumulative ACK convention (GBN):
    The ack_num field carries the sequence number of the LAST correctly
    received in-order packet. An ACK with ack_num = k therefore tells the
    sender that every packet 0..k has been received correctly (matches the
    spec example: packets 0 1 2 -> ACK0 ACK1 ACK2; expected 3, receive 4 ->
    discard and Send ACK2).

Sequence-number stream (same window covers every packet):
    seq 0        TYPE 0 (START)     Open a new file write-stream
    seq 1..N     TYPE 1 (DATA)      File content chunks
    seq N+1      TYPE 3 (COMPLETE)  Close the file stream

Packet loss (Part C testing): incoming DATA packets are randomly dropped at
LOSS_RATE before processing, so retransmissions can be observed. Change
LOSS_RATE to test 0%, 10%, 20%, 30%.

Usage:
    python receiver_gbn.py
    (Binds to local port 5001 and remains idle until a sender initiates)
"""

import socket, struct, random, sys

# Configuration constants
BIND_IP = "127.0.0.1"
BIND_PORT = 5001

LOSS_RATE = 0.2     # Default loss rate; override on the command line:
if len(sys.argv) > 1:        #   python receiver_gbn.py 0.3   -> 30% loss
    LOSS_RATE = float(sys.argv[1])
print(f"[RECEIVER] Packet loss rate: {LOSS_RATE * 100:.0f}%")

# Packet types
TYPE_START = 0
TYPE_DATA = 1
TYPE_ACK = 2
TYPE_COMPLETE = 3

HEADER_FORMAT = "!III"

def packet_construction(seq_num, ack_num, pkt_type, payload=b""):
    header = struct.pack(HEADER_FORMAT, seq_num, ack_num, pkt_type)
    return header + payload

def parse_packet(packet):
    header_size = struct.calcsize(HEADER_FORMAT)
    header = packet[:header_size]
    payload = packet[header_size:]
    seq_num, ack_num, pkt_type = struct.unpack(HEADER_FORMAT, header)
    return seq_num, ack_num, pkt_type, payload

def main():
    # Initialize UDP Socket and bind to port
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind((BIND_IP, BIND_PORT))
    print(f"[RECEIVER] Listening on {BIND_IP}:{BIND_PORT}...")

    file_handle = None
    output_filename = "received_file_sr"
    base = 0         # The next in-order packet we MUST deliver to the file
    buffer = {}      # Buffer for out-of-order packets: seq_num -> (pkt_type, payload)

    while True:
        packet, addr = sock.recvfrom(2048)
        seq_num, _, pkt_type, payload = parse_packet(packet)

        # Simulate packet loss (drop DATA only)
        if pkt_type == TYPE_DATA and random.random() < LOSS_RATE:
            print(f"[RECEIVER] Simulated loss: dropped DATA packet {seq_num}")
            continue

        # In Selective Repeat, we ALWAYS send an individual ACK for the received packet.
        # Even if it's old (seq_num < base), the sender's ACK might have been lost.
        ack_packet = packet_construction(seq_num=0, ack_num=seq_num, pkt_type=TYPE_ACK)
        sock.sendto(ack_packet, addr)

        # If the packet is inside our current receiving window
        if seq_num >= base:
            # Buffer the packet if we don't already have it
            if seq_num not in buffer:
                buffer[seq_num] = (pkt_type, payload)
                print(f"[RECEIVER] Buffered Seq {seq_num}")

            # If this packet fills the gap at 'base', process all consecutive buffered packets
            while base in buffer:
                b_type, b_payload = buffer[base]
                
                if b_type == TYPE_START:
                    name = b_payload.decode("utf-8", "ignore") or "file"
                    output_filename = "received_" + name
                    print(f"[RECEIVER] Processing Seq {base} (START). Opening '{output_filename}'.")
                    file_handle = open(output_filename, "wb")

                elif b_type == TYPE_DATA:
                    print(f"[RECEIVER] Processing Seq {base} (DATA). Saving data.")
                    if file_handle:
                        file_handle.write(b_payload)

                elif b_type == TYPE_COMPLETE:
                    print(f"[RECEIVER] Processing Seq {base} (COMPLETE). Closing file.")
                    if file_handle:
                        file_handle.close()
                        file_handle = None

                # Remove from buffer and slide the receiver window forward
                del buffer[base]
                base += 1

                if b_type == TYPE_COMPLETE:
                    print("[RECEIVER] Transfer complete. Ready for next file.")
                    base = 0
                    buffer.clear()
        else:
            print(f"[RECEIVER] Received old Seq {seq_num} (base is {base}). Re-ACKed.")

if __name__ == "__main__":
    main()