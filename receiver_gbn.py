#!/usr/bin/env python3
"""
receiver_gbn.py - Go-Back-N receiver over UDP
CP372 Assignment 2, Part C

Behaviour:
  * Accepts ONLY the packet whose sequence number == expected.
  * Discards out-of-order / duplicate packets.
  * Always replies with a cumulative ACK for the last in-order packet.
  * Simulates packet loss: each incoming packet is dropped with probability LOSS_RATE.
  * START packet -> create the output file
    DATA packet  -> write payload to the file
    END packet   -> close the file (receiver then lingers briefly to re-ACK
                    a retransmitted END in case the final ACK was lost)

Usage:
  python receiver_gbn.py --port 5001 --loss 0.1
"""

import argparse
import os
import random
import socket
import struct
import time

# ---------------------------------------------------------------------------
# Packet format (shared with sender_gbn.py)
#
#   0        1                5                9          11
#   +--------+----------------+----------------+----------+-----------------+
#   |  TYPE  |   SEQ NUMBER   |   ACK NUMBER   |  LENGTH  |  PAYLOAD ...    |
#   | 1 byte |    4 bytes     |    4 bytes     | 2 bytes  |  LENGTH bytes   |
#   +--------+----------------+----------------+----------+-----------------+
#   All header fields are unsigned, network byte order (big-endian).
# ---------------------------------------------------------------------------
HEADER = struct.Struct("!BIIH")
START, DATA, ACK, END = 1, 2, 3, 4
TYPE_NAMES = {START: "START", DATA: "DATA", ACK: "ACK", END: "END"}


def make_packet(ptype, seq=0, ack=0, payload=b""):
    return HEADER.pack(ptype, seq, ack, len(payload)) + payload


def parse_packet(raw):
    """Return (type, seq, ack, payload) or None if the packet is malformed."""
    if len(raw) < HEADER.size:
        return None
    ptype, seq, ack, length = HEADER.unpack_from(raw)
    payload = raw[HEADER.size:HEADER.size + length]
    if len(payload) != length or ptype not in TYPE_NAMES:
        return None
    return ptype, seq, ack, payload


def main():
    ap = argparse.ArgumentParser(description="Go-Back-N UDP receiver")
    ap.add_argument("--host", default="0.0.0.0", help="address to bind (default 0.0.0.0)")
    ap.add_argument("--port", type=int, default=5001, help="UDP port to listen on")
    ap.add_argument("--loss", type=float, default=0.0, help="packet loss rate, 0.0-1.0")
    ap.add_argument("--outdir", default="received", help="folder for received files")
    ap.add_argument("--linger", type=float, default=1.0,
                    help="seconds to stay alive after END to re-ACK duplicates")
    ap.add_argument("--seed", type=int, default=None, help="random seed (for repeatable tests)")
    ap.add_argument("-v", "--verbose", action="store_true", help="print every packet event")
    args = ap.parse_args()

    if args.seed is not None:
        random.seed(args.seed)
    os.makedirs(args.outdir, exist_ok=True)

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4 * 1024 * 1024)
    sock.bind((args.host, args.port))
    print(f"[GBN receiver] listening on {args.host}:{args.port}  loss={args.loss:.0%}")

    expected = 0          # next in-order sequence number we will accept
    out_file = None
    out_path = None
    done = False
    received = dropped = discarded = acks_sent = 0
    bytes_written = 0

    while True:
        if done:
            sock.settimeout(args.linger)   # after END, only wait briefly
        try:
            raw, addr = sock.recvfrom(65535)
        except socket.timeout:
            break
        except ConnectionResetError:       # Windows: ICMP port-unreachable noise
            continue

        received += 1

        # ---- simulated packet loss --------------------------------------
        if random.random() < args.loss:
            dropped += 1
            if args.verbose:
                print(f"  [LOSS] dropped incoming packet")
            continue

        pkt = parse_packet(raw)
        if pkt is None:
            continue
        ptype, seq, _, payload = pkt

        if seq == expected:
            # ---- in-order packet: deliver it -----------------------------
            if ptype == START:
                name = os.path.basename(payload.decode("utf-8", errors="replace")) or "file.bin"
                out_path = os.path.join(args.outdir, "received_" + name)
                out_file = open(out_path, "wb")
                print(f"[GBN receiver] START: receiving '{name}' -> {out_path}")
            elif ptype == DATA:
                if out_file:
                    out_file.write(payload)
                    bytes_written += len(payload)
            elif ptype == END:
                if out_file:
                    out_file.close()
                    out_file = None
                done = True
                print(f"[GBN receiver] END: file complete ({bytes_written} bytes)")

            sock.sendto(make_packet(ACK, ack=expected), addr)
            acks_sent += 1
            if args.verbose:
                print(f"  recv {TYPE_NAMES[ptype]} seq={seq} (expected) -> ACK{expected}")
            expected += 1
        else:
            # ---- out-of-order or duplicate: discard, re-send last ACK -----
            discarded += 1
            if expected > 0:
                sock.sendto(make_packet(ACK, ack=expected - 1), addr)
                acks_sent += 1
            if args.verbose:
                print(f"  recv seq={seq}, expected={expected} -> discard, ACK{expected - 1}")

    if out_file:
        out_file.close()
    sock.close()
    print(f"[GBN receiver] packets received={received} dropped={dropped} "
          f"discarded={discarded} acks_sent={acks_sent} bytes={bytes_written}")


if __name__ == "__main__":
    main()
