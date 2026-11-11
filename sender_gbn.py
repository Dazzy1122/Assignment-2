#!/usr/bin/env python3
"""
sender_gbn.py - Go-Back-N sender over UDP
CP372 Assignment 2, Part C

Behaviour:
  * Notifies the receiver with a START packet (payload = filename).
  * Splits the file into DATA packets and sends up to WINDOW_SIZE packets
    without waiting (sliding window: base / nextseqnum / window_size).
  * One timer, always for the oldest unacknowledged packet (base).
  * Cumulative ACKs: ACK n means every packet <= n was received.
  * On timeout, retransmits every packet in the window [base, nextseqnum).
  * Finishes with an END packet; transfer is complete when END is ACKed.

Prints one machine-readable line at the end, e.g.
  RESULT protocol=GBN file_bytes=10240 packets=12 time=0.0123 throughput=832520.3 retransmissions=0

Usage:
  python sender_gbn.py --file test.txt --window 4
  python sender_gbn.py --size 102400 --window 8 --timeout 0.01
"""

import argparse
import os
import socket
import struct
import time

# ---------------------------------------------------------------------------
# Packet format (shared with receiver_gbn.py)
#
#   +--------+----------------+----------------+----------+-----------------+
#   |  TYPE  |   SEQ NUMBER   |   ACK NUMBER   |  LENGTH  |  PAYLOAD ...    |
#   | 1 byte |    4 bytes     |    4 bytes     | 2 bytes  |  LENGTH bytes   |
#   +--------+----------------+----------------+----------+-----------------+
# ---------------------------------------------------------------------------
HEADER = struct.Struct("!BIIH")
START, DATA, ACK, END = 1, 2, 3, 4
TYPE_NAMES = {START: "START", DATA: "DATA", ACK: "ACK", END: "END"}


def make_packet(ptype, seq=0, ack=0, payload=b""):
    return HEADER.pack(ptype, seq, ack, len(payload)) + payload


def parse_packet(raw):
    if len(raw) < HEADER.size:
        return None
    ptype, seq, ack, length = HEADER.unpack_from(raw)
    payload = raw[HEADER.size:HEADER.size + length]
    if len(payload) != length or ptype not in TYPE_NAMES:
        return None
    return ptype, seq, ack, payload


def generate_content(size):
    """Program-generated text content of exactly `size` bytes."""
    line = b"CP372 Go-Back-N test data: the quick brown fox jumps over the lazy dog.\n"
    reps = size // len(line) + 1
    return (line * reps)[:size]


def main():
    ap = argparse.ArgumentParser(description="Go-Back-N UDP sender")
    ap.add_argument("--host", default="127.0.0.1", help="receiver address")
    ap.add_argument("--port", type=int, default=5001, help="receiver UDP port")
    src = ap.add_mutually_exclusive_group(required=True)
    src.add_argument("--file", help="text file to send")
    src.add_argument("--size", type=int, help="generate this many bytes instead of reading a file")
    ap.add_argument("--window", type=int, default=4, help="window size N (default 4)")
    ap.add_argument("--timeout", type=float, default=0.01, help="retransmission timeout in seconds")
    ap.add_argument("--payload", type=int, default=1024, help="payload bytes per DATA packet")
    ap.add_argument("--max-timeouts", type=int, default=200,
                    help="give up after this many consecutive timeouts with no progress")
    ap.add_argument("-v", "--verbose", action="store_true", help="print every packet event")
    args = ap.parse_args()

    # ---- 1. read / generate the content -----------------------------------
    if args.file:
        with open(args.file, "rb") as f:
            data = f.read()
        name = os.path.basename(args.file)
    else:
        data = generate_content(args.size)
        name = f"generated_{args.size}.txt"

    # ---- 2. split into packets: START, DATA..., END -----------------------
    chunks = [data[i:i + args.payload] for i in range(0, len(data), args.payload)]
    packets = [make_packet(START, seq=0, payload=name.encode())]
    for i, chunk in enumerate(chunks, start=1):
        packets.append(make_packet(DATA, seq=i, payload=chunk))
    packets.append(make_packet(END, seq=len(packets)))
    total = len(packets)

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4 * 1024 * 1024)
    dest = (args.host, args.port)

    print(f"[GBN sender] sending '{name}' ({len(data)} bytes, {total} packets) "
          f"to {dest[0]}:{dest[1]}  N={args.window} timeout={args.timeout}s")

    # ---- 3. Go-Back-N state ----------------------------------------------
    base = 0               # oldest unacknowledged packet
    nextseqnum = 0         # next packet to send for the first time
    window_size = args.window
    timer_start = None     # single timer for packet `base`; None = stopped
    retransmissions = 0
    consecutive_timeouts = 0

    start_time = time.perf_counter()

    while base < total:
        # -- send new packets while the window has room --
        while nextseqnum < base + window_size and nextseqnum < total:
            sock.sendto(packets[nextseqnum], dest)
            if args.verbose:
                print(f"  send seq={nextseqnum}")
            if base == nextseqnum:              # first packet in window -> start timer
                timer_start = time.perf_counter()
            nextseqnum += 1

        # -- wait for an ACK until the timer expires --
        # -- wait for an ACK until the timer expires --
        if timer_start is not None:
            remaining = args.timeout - (time.perf_counter() - timer_start)
        else:
            remaining = args.timeout

        timed_out = remaining <= 0
        
        if not timed_out:
            sock.settimeout(remaining)
            try:
                raw, _ = sock.recvfrom(65535)
                pkt = parse_packet(raw)
                if pkt and pkt[0] == ACK:
                    acknum = pkt[2]
                    if base <= acknum < nextseqnum:      # new cumulative ACK
                        if args.verbose:
                            print(f"  recv ACK{acknum} -> base {base} -> {acknum + 1}")
                        base = acknum + 1
                        consecutive_timeouts = 0
                        # restart timer if packets are still outstanding, else stop it
                        timer_start = time.perf_counter() if base < nextseqnum else None
                    elif args.verbose:
                        print(f"  recv duplicate ACK{acknum} (ignored)")
            except socket.timeout:
                timed_out = True
            except ConnectionResetError:          # Windows: receiver not up yet
                pass

        # -- timeout: go back N, resend the whole window --
        if timed_out and base < nextseqnum:
            consecutive_timeouts += 1
            if consecutive_timeouts > args.max_timeouts:
                print("[GBN sender] ERROR: receiver not responding, giving up")
                sock.close()
                raise SystemExit(1)
            if args.verbose:
                print(f"  TIMEOUT -> resend {base}..{nextseqnum - 1}")
            for seq in range(base, nextseqnum):
                sock.sendto(packets[seq], dest)
            retransmissions += nextseqnum - base
            timer_start = time.perf_counter()

    elapsed = time.perf_counter() - start_time
    sock.close()

    throughput = len(data) / elapsed if elapsed > 0 else 0.0
    print(f"[GBN sender] done in {elapsed:.4f}s, throughput {throughput:,.0f} B/s, "
          f"retransmissions {retransmissions}")
    print(f"RESULT protocol=GBN file_bytes={len(data)} packets={total} "
          f"time={elapsed:.6f} throughput={throughput:.2f} retransmissions={retransmissions}")


if __name__ == "__main__":
    main()