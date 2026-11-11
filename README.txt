CP372 ASSIGNMENT 2 - RELIABLE DATA TRANSFER OVER UDP
====================================================

Command-line senders and receivers that transfer files over UDP using
Stop-and-Wait and Go-Back-N (GBN). Since UDP does not guarantee delivery or
ordering, both implementations use sequence numbers, acknowledgments,
timeouts and retransmissions. The receivers can randomly drop incoming
packets to simulate packet loss, and the senders print transfer time,
throughput and retransmission counts for comparison.


REQUIREMENTS
------------
- Python 3.8 or newer
- No third-party packages. Only the standard library is used (socket,
  struct, argparse, pathlib, os, random, time).

Works on Windows, macOS and Linux. On Windows, use "python" instead of
"python3" in the commands below.


FOLDER STRUCTURE
----------------
sender_stopwait.py    Stop-and-Wait sender: send one packet and wait for its ACK
receiver_stopwait.py  Stop-and-Wait receiver: save in-order packets and simulate loss
sender_gbn.py         GBN sender: sliding window, cumulative ACKs, retransmissions
receiver_gbn.py       GBN receiver: accept expected packets and simulate loss
received/             Received files; created automatically
report.pdf            Required report with protocol design, results and analysis

Place an input file such as input.txt in the project folder, or pass its full
path to the sender. GBN can also generate constant text content with --size.


RUNNING
-------
Open two terminals in the project folder. Start the receiver first, then its
matching sender. Both receivers default to port 5001, so stop one before
starting the other, or use different ports. The protocols use different packet
formats and cannot be mixed.

1. Stop-and-Wait

   Start the receiver:

       python3 receiver_stopwait.py --port 5001 --loss 0.0

   Start the sender in the second terminal:

       python3 sender_stopwait.py input.txt --host 127.0.0.1 --port 5001 --timeout 0.001

   The received file is saved as received/input.txt. The receiver continues
   listening for new transfers until you press Ctrl+C.

2. Go-Back-N

   Start the receiver:

       python3 receiver_gbn.py --port 5001 --loss 0.0

   Start the sender in the second terminal:

       python3 sender_gbn.py --file input.txt --host 127.0.0.1 --port 5001 --window 4 --timeout 0.001

   The received file is saved as received/received_input.txt. After accepting
   END, the receiver waits briefly to acknowledge duplicate packets, then
   exits. Restart it before the next transfer.

   To generate content instead of reading a file:

       python3 sender_gbn.py --size 102400 --window 4 --timeout 0.001

   This sends 102,400 bytes and saves them as
   received/received_generated_102400.txt.


COMMAND-LINE OPTIONS
--------------------

Stop-and-Wait
  Sender    filename         Required        Path to the input file
  Sender    --host           127.0.0.1       Receiver address
  Both      --port           5001            UDP port
  Sender    --timeout        2.0             Seconds before retransmission
  Receiver  --loss           0.0             Incoming packet loss probability, 0 to 1

  The sender uses fixed 1,024-byte DATA chunks. The receiver binds to
  127.0.0.1, so this implementation is configured for testing on the same
  machine.

Go-Back-N
  Sender    --file           Required unless --size is used   Path to the input file
  Sender    --size           Required unless --file is used   Bytes of generated constant text
  Sender    --host           127.0.0.1       Receiver address
  Receiver  --host           0.0.0.0         Local address to bind
  Both      --port           5001            UDP port
  Sender    --window         4               Maximum outstanding packets
  Sender    --timeout        0.01            Seconds before retransmission
  Sender    --payload        1024            Maximum DATA payload size in bytes
  Sender    --max-timeouts   200             Consecutive timeouts allowed without ACK progress
  Receiver  --loss           0.0             Incoming packet loss probability
  Receiver  --outdir         received        Output folder
  Receiver  --linger         1.0             Idle wait after END before exiting
  Receiver  --seed           Unset           Random seed for repeatable loss simulation
  Both      -v, --verbose    Off             Print individual packet events

  Use positive window, payload and timeout values, and loss probabilities
  between 0 and 1. Keep the default payload size for the experiments. Set
  --linger comfortably above the sender timeout if changing the timeout.


PACKET LOSS TESTING
-------------------
Loss is simulated at the receiver. Each incoming transfer packet, including
START and completion packets, can be dropped before it is processed. A dropped
packet does not receive an ACK. ACK loss and packet corruption are not
simulated separately.

Run each protocol at the following loss rates:

    0%    --loss 0.0
    10%   --loss 0.1
    20%   --loss 0.2
    30%   --loss 0.3

For example, start either matching receiver with 20% loss:

    python3 receiver_stopwait.py --loss 0.2

or:

    python3 receiver_gbn.py --loss 0.2 --seed 42

Then run its sender using the commands above. Stop-and-Wait resends the
unacknowledged packet after a timeout. GBN resends every outstanding packet in
its current window.


PROTOCOL SUMMARY
----------------
All headers use network byte order (big-endian). START carries the filename,
DATA carries file bytes, and COMPLETE or END marks the end of a transfer.
Sequence numbers increase throughout a transfer rather than alternating
between 0 and 1.

Stop-and-Wait

  The header uses struct format !III and is 12 bytes long:

    Sequence number  4 bytes   START is 0, first DATA is 1, then increases by 1
    ACK number       4 bytes   Sequence number being acknowledged
    Type             4 bytes   START = 0, DATA = 1, ACK = 2, COMPLETE = 3
    Payload          Variable  Remaining bytes in the UDP datagram

  The sender transmits one packet, starts a timer and waits for the matching
  ACK from the receiver address. On timeout, it resends the same packet. The
  receiver writes only the next expected DATA packet and acknowledges
  duplicate or out-of-order DATA using its last accepted sequence number.
  Duplicate START packets during an active transfer do not reopen the file,
  and duplicate COMPLETE packets are acknowledged without writing the file
  again.

Go-Back-N

  The header uses struct format !BIIH and is 11 bytes long:

    Type             1 byte    START = 1, DATA = 2, ACK = 3, END = 4
    Sequence number  4 bytes   START is 0, first DATA is 1, then increases by 1
    ACK number       4 bytes   Last packet received in order
    Payload length   2 bytes   Number of payload bytes
    Payload          Variable  Filename or file content

  The sender tracks base, nextseqnum and window_size. It sends up to one
  window of packets without waiting, with a single timer for the oldest
  unacknowledged packet. ACK n acknowledges every packet through n. On
  timeout, all outstanding packets are retransmitted. The receiver accepts
  only its expected sequence number, discards other packets and repeats the
  last cumulative ACK when one exists.


PERFORMANCE EVALUATION
----------------------
Settings used for the results in report.pdf: Linux localhost, 1,024-byte DATA
payload, 1 ms timeout for both protocols, GBN window of 4 packets, and the
average of five trials per file size and loss rate.

Both senders print total transfer time in seconds, throughput in bytes per
second and the number of retransmitted packets. GBN also prints a RESULT line
for collecting results. Timing covers the sender's transfer sequence from
START through the completion ACK; it does not include the receiver's final
linger period.

For each protocol, test 10 KB, 50 KB, 100 KB, 500 KB, 1 MB, 5 MB, 10 MB,
50 MB and 100 MB at each required loss rate. Run each combination five times
and calculate the average time, throughput and retransmission count. This is
180 transfers per protocol, or 360 in total. State the byte convention used
for KB and MB in the report.

Use the same input files, DATA payload size and explicit timeout for both
protocols. Their default timeouts differ (2.0 s for Stop-and-Wait, 0.01 s for
GBN), so the example commands set both to 0.001 seconds (1 ms), which is the
value used for the results in report.pdf. Record the GBN window size and keep
it consistent unless testing its effect. Vary or omit the GBN seed across trials to sample different loss
patterns. These scripts print individual results; averaging and graphing are
performed separately.

To check that a received file matches the original, use this cross-platform
comparison, changing the second path to received/received_input.txt for GBN:

    python3 -c "from pathlib import Path; print('MATCH' if Path('input.txt').read_bytes() == Path('received/input.txt').read_bytes() else 'DIFFERENT')"


ASSUMPTIONS AND ERROR HANDLING
------------------------------
- Run one sender per receiver at a time. Use the matching protocol on both
  sides.
- Existing output files with the same name are overwritten. Received files are
  stored relative to the receiver's working directory.
- Transfers use raw file bytes; text input does not require a particular
  encoding. Filenames are encoded as UTF-8.
- Both receivers remove directory components from received filenames before
  saving them.
- Stop-and-Wait checks for a missing input file, a nonpositive timeout and a
  loss rate outside the allowed range. It retries indefinitely if ACKs never
  arrive; press Ctrl+C to stop it.
- GBN stops with an error when its consecutive-timeout limit is exceeded. A
  missing input file or invalid numeric settings can cause an exception
  because not all arguments are validated.
- Interrupted transfers can leave a partial output file. There is no resume
  mechanism, transfer identifier or application-level checksum.
- GBN reads the whole input file and constructs all packets in memory before
  sending. Memory usage therefore grows with file size.
- The Stop-and-Wait receiver remains available to re-ACK completion packets.
  The GBN receiver re-ACKs duplicates only during its linger period.
- Selective Repeat and checksum-based corruption simulation are not
  implemented in these four scripts.
