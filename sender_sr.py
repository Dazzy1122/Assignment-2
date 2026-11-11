"""
sender_gbn.py
CP372 - Computer Networks A02 - Part C

Implements a reliable data transfer sender over UDP sockets using the
Go-Back-N (GBN) protocol. It pipelines packets inside a sliding window
without waiting for individual ACKs, runs a single timer for the oldest
unacknowledged packet, processes cumulative ACKs, and retransmits the whole
outstanding window when the timer expires.

Sliding-window state:
    base          oldest unacknowledged sequence number
    nextseqnum    next sequence number to send
    window_size   maximum number of unacknowledged packets in flight

Sequence-number stream (the window covers every packet uniformly):
    seq 0        TYPE 0 (START)     Notify the receiver a file is incoming
    seq 1..N     TYPE 1 (DATA)      File content chunks
    seq N+1      TYPE 3 (COMPLETE)  Notify completion of the file transfer

A background thread receives cumulative ACKs (ack_num = last in-order packet
received) and slides the window forward to ack_num + 1.

Usage:
    python sender_gbn.py                       # built-in demo content
    python sender_gbn.py --file input.txt      # send a real file
    python sender_gbn.py --generate 1MB        # send random data of a size
    python sender_gbn.py --window 8 --timeout 0.3   # tune the protocol
"""

import argparse, os, socket, struct, threading, time

# Configuration constants
RECEIVER_IP = "127.0.0.1"
RECEIVER_PORT = 5001
PAYLOAD_SIZE = 1024     # file content bytes per DATA packet

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


def parse_size(text):
    """Convert a size string like '10KB', '5MB', '1024' into a byte count."""
    text = text.strip().upper()
    for suffix, mult in (("KB", 1024), ("K", 1024),
                         ("MB", 1024 * 1024), ("M", 1024 * 1024),
                         ("GB", 1024 ** 3), ("G", 1024 ** 3), ("B", 1)):
        if text.endswith(suffix):
            return int(float(text[:-len(suffix)]) * mult)
    return int(text)


def build_packets(file_bytes, file_name):
    """Build the full ordered list of GBN packets for this file."""
    packets = [packet_construction(0, 0, TYPE_START, file_name.encode("utf-8"))]
    seq = 1
    for i in range(0, len(file_bytes), PAYLOAD_SIZE):
        chunk = file_bytes[i:i + PAYLOAD_SIZE]
        packets.append(packet_construction(seq, 0, TYPE_DATA, chunk))
        seq += 1
    packets.append(packet_construction(seq, 0, TYPE_COMPLETE))
    return packets


class GBNSender:
    def __init__(self, packets, addr, window_size, timeout):
        self.packets = packets
        self.total = len(packets)
        self.addr = addr
        self.window_size = window_size
        self.timeout = timeout

        self.base = 0
        self.nextseqnum = 0
        self.timer_start = None     # monotonic time the single timer started
        
        self.acked = set()

        self.retransmissions = 0
        self.done = False

        self.lock = threading.Lock()
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.settimeout(0.2)   # lets the ACK thread poll self.done

    # -- single timer for the oldest unacknowledged packet -------------------
    def _start_timer(self):
        self.timer_start = time.perf_counter()

    def _stop_timer(self):
        self.timer_start = None

    def _timer_expired(self):
        return (self.timer_start is not None and
                time.perf_counter() - self.timer_start >= self.timeout)

    # -- background thread: process cumulative ACKs --------------------------
    def _ack_loop(self):
        while True:
            with self.lock:
                if self.done:
                    return
            try:
                raw, _ = self.sock.recvfrom(2048)
            except socket.timeout:
                continue
            except OSError:
                return
            _seq, ack_num, pkt_type, _payload = parse_packet(raw)
            if pkt_type != TYPE_ACK:
                continue
            with self.lock:
                # Mark this specific packet as acknowledged
                self.acked.add(ack_num)
                
                # If we just ACKed the base packet, slide the window forward
                # past all consecutive acknowledged packets
                if ack_num == self.base:
                    while self.base in self.acked:
                        self.base += 1
                        
                    # Manage the single timer for the new oldest unacked packet
                    if self.base == self.nextseqnum:
                        self._stop_timer()      # everything is ACKed
                    else:
                        self._start_timer()     # restart for the new base
                        
                    if self.base >= self.total:
                        self.done = True
                        
                        return

    # -- main loop: send within the window + handle timeouts -----------------
    def run(self):
        ack_thread = threading.Thread(target=self._ack_loop, daemon=True)
        ack_thread.start()

        start_time = time.perf_counter()
        while True:
            with self.lock:
                if self.done:
                    break

                # Send every packet that fits inside the window.
                while (self.nextseqnum < self.base + self.window_size
                       and self.nextseqnum < self.total):
                    self.sock.sendto(self.packets[self.nextseqnum], self.addr)
                    if self.base == self.nextseqnum:
                        self._start_timer()
                    self.nextseqnum += 1

                # Timeout -> Selective Repeat: retransmit ONLY the oldest unacked packet.
                if self._timer_expired():
                    self._start_timer()
                    
                    print(f"[SENDER] Timeout. Retransmitting oldest unacked packet: {self.base}")
                    
                    self.sock.sendto(self.packets[self.base], self.addr)
                    self.retransmissions += 1

            time.sleep(0.001)

        elapsed = time.perf_counter() - start_time
        ack_thread.join(timeout=1.0)
        self.sock.close()
        return elapsed


def main():
    ap = argparse.ArgumentParser(description="Go-Back-N UDP file sender")
    src = ap.add_mutually_exclusive_group()
    src.add_argument("--file", help="path to the file to send")
    src.add_argument("--generate",
                     help="generate random data of this size instead (e.g. 1MB)")
    ap.add_argument("--host", default=RECEIVER_IP, help="receiver host")
    ap.add_argument("--port", type=int, default=RECEIVER_PORT, help="receiver port")
    ap.add_argument("--window", type=int, default=4, help="window size N")
    ap.add_argument("--timeout", type=float, default=0.5,
                    help="retransmission timeout in seconds")
    args = ap.parse_args()

    # Obtain content + a name to advertise in the START packet.
    if args.generate:
        size = parse_size(args.generate)
        file_bytes = os.urandom(size)
        file_name = f"generated_{args.generate}.bin"
        print(f"[SENDER] Generated {size} random bytes -> {file_name}")
    elif args.file and os.path.exists(args.file):
        with open(args.file, "rb") as f:
            file_bytes = f.read()
        file_name = os.path.basename(args.file)
        print(f"[SENDER] Read {len(file_bytes)} bytes from {args.file}")
    else:
        # Built-in constant content so the program runs out of the box
        # (the spec allows program-generated content).
        file_bytes = b"CP372 Go-Back-N demo content.\n" * 2000
        file_name = "demo.txt"
        if args.file:
            print(f"[SENDER] '{args.file}' not found; using built-in demo content.")
        print(f"[SENDER] Using built-in demo content ({len(file_bytes)} bytes).")

    packets = build_packets(file_bytes, file_name)
    print(f"[SENDER] {len(packets)} packets "
          f"(1 START + {len(packets) - 2} DATA + 1 COMPLETE), "
          f"window={args.window}, timeout={args.timeout}s")

    sender = GBNSender(packets, (args.host, args.port), args.window, args.timeout)
    elapsed = sender.run()

    throughput = len(file_bytes) / elapsed if elapsed > 0 else 0.0
    print("\n[SENDER] === Transfer complete ===")
    print(f"[SENDER] File size       : {len(file_bytes)} bytes")
    print(f"[SENDER] Transfer time   : {elapsed:.4f} s")
    print(f"[SENDER] Throughput      : {throughput:.2f} bytes/s")
    print(f"[SENDER] Retransmissions : {sender.retransmissions}")


if __name__ == "__main__":
    main()