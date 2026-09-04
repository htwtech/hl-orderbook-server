#!/usr/bin/env python3
"""Do two nodes produce interchangeable l2Diff updates?

Subscribes to `l2Diff` on two nodes for the same coin with the same parameters,
and answers three questions in increasing order of usefulness.

1. Do the INTERVALS line up? Each update covers prevHeight -> height. The nodes
   flush on their own 50 ms phase, so one may send 100->103 where the other
   sends 102->103. Frames whose intervals differ are not comparable at all, and
   a proxy could never splice them frame for frame.

2. Where an interval IS shared, is the CONTENT identical? If not, the two nodes
   disagree about the book itself, which would be a much deeper problem than
   timing.

3. At heights both nodes reached, are the BOOKS identical? This is the one that
   decides the design. Intervals can differ freely and it still does not matter:
   if the reconstructed books agree at a shared height, a proxy can switch from
   one node to the other at that height and simply carry on applying updates --
   no snapshot, no gap, nothing for the client to notice.

Standard library only, so it runs on the node with nothing installed.

  python3 l2diff-compare.py --a ws://localhost:48001/ws --b ws://localhost:48002/ws \
      --coin BTC --levels 1000 --seconds 60
"""

import argparse
import base64
import collections
import json
import os
import socket
import ssl
import struct
import sys
import threading
import time as _time
from urllib.parse import urlparse

DEFAULT_LEVELS = 20

# Whole books kept per node so a disagreement can be measured, not just
# detected. Bounded because a 1000-level book is a few hundred KB in Python and
# a minute's run reaches several hundred heights.
SAMPLE_KEEP = 120


class Ws:
    """Just enough websocket to hold a subscription open and read text frames."""

    def __init__(self, url, token=None, timeout=30):
        u = urlparse(url)
        secure = u.scheme == "wss"
        port = u.port or (443 if secure else 80)
        self.sock = socket.create_connection((u.hostname, port), timeout=timeout)
        if secure:
            self.sock = ssl.create_default_context().wrap_socket(
                self.sock, server_hostname=u.hostname
            )
        key = base64.b64encode(os.urandom(16)).decode()
        req = [
            "GET {} HTTP/1.1".format(u.path or "/"),
            "Host: {}:{}".format(u.hostname, port),
            "Upgrade: websocket",
            "Connection: Upgrade",
            "Sec-WebSocket-Key: " + key,
            "Sec-WebSocket-Version: 13",
        ]
        if token:
            req.append("x-token: " + token)
        self.sock.sendall(("\r\n".join(req) + "\r\n\r\n").encode())

        buf = b""
        while b"\r\n\r\n" not in buf:
            chunk = self.sock.recv(4096)
            if not chunk:
                raise RuntimeError("connection closed during the handshake")
            buf += chunk
        head, _, rest = buf.partition(b"\r\n\r\n")
        status = head.decode(errors="replace").splitlines()[0]
        if "101" not in status:
            raise RuntimeError("handshake refused: " + status)
        self.buf = rest

    def _read(self, n):
        while len(self.buf) < n:
            chunk = self.sock.recv(65536)
            if not chunk:
                raise RuntimeError("connection closed by the server")
            self.buf += chunk
        out, self.buf = self.buf[:n], self.buf[n:]
        return out

    def send_text(self, text):
        payload = text.encode()
        mask = os.urandom(4)
        n = len(payload)
        if n < 126:
            hdr = struct.pack("!BB", 0x81, 0x80 | n)
        elif n < 65536:
            hdr = struct.pack("!BBH", 0x81, 0x80 | 126, n)
        else:
            hdr = struct.pack("!BBQ", 0x81, 0x80 | 127, n)
        self.sock.sendall(hdr + mask + bytes(b ^ mask[i % 4] for i, b in enumerate(payload)))

    def _frame(self):
        b0, b1 = self._read(2)
        fin, opcode, masked = bool(b0 & 0x80), b0 & 0x0F, bool(b1 & 0x80)
        n = b1 & 0x7F
        if n == 126:
            (n,) = struct.unpack("!H", self._read(2))
        elif n == 127:
            (n,) = struct.unpack("!Q", self._read(8))
        mask = self._read(4) if masked else None
        payload = self._read(n)
        if mask:
            payload = bytes(b ^ mask[i % 4] for i, b in enumerate(payload))
        return fin, opcode, payload

    def recv_text(self):
        parts = []
        while True:
            fin, opcode, payload = self._frame()
            if opcode == 0x8:
                raise RuntimeError("server closed the connection")
            if opcode == 0x9:
                self.sock.sendall(struct.pack("!BB", 0x8A, 0x80 | len(payload))
                                  + bytes(4) + payload)
                continue
            if opcode == 0xA:
                continue
            if opcode not in (0x0, 0x1, 0x2):
                continue
            parts.append(payload)
            if fin:
                return b"".join(parts).decode(errors="replace")


def book_from_levels(levels):
    return [{lv["px"]: (lv["sz"], lv["n"]) for lv in side} for side in levels]


def apply_side(side, diff):
    for px, sz, n in diff.get("upd", []):
        side[px] = (sz, n)
    for px in diff.get("del", []):
        side.pop(px, None)


def side_signature(diff):
    """Order-independent identity of one side of an update."""
    return (
        tuple(sorted(tuple(x) for x in diff.get("upd", []))),
        tuple(sorted(diff.get("del", []))),
    )


def book_signature(book):
    """Order-independent identity of a whole reconstructed book."""
    return (
        tuple(sorted(book[0].items())),
        tuple(sorted(book[1].items())),
    )


def compare_books(a, b):
    """How far apart two books are: (levels differing, their depth ranks).

    The rank matters as much as the count. Disagreement at the bottom is churn
    around the nLevels boundary and benign; disagreement near the top would mean
    the nodes see a different market.
    """
    differing, ranks = 0, []
    for side, (sa, sb) in enumerate(zip(a, b)):
        # Bids run best-first descending, asks best-first ascending.
        order = sorted(set(sa) | set(sb), key=float, reverse=(side == 0))
        for rank, px in enumerate(order):
            if sa.get(px) != sb.get(px):
                differing += 1
                ranks.append(rank)
    return differing, ranks


def quantile(values, q):
    if not values:
        return 0
    s = sorted(values)
    return s[min(len(s) - 1, int(round(q * (len(s) - 1))))]


class NodeStream(threading.Thread):
    def __init__(self, name, url, sub, token, deadline):
        super().__init__(daemon=True)
        self.name, self.url, self.sub, self.token, self.deadline = name, url, sub, token, deadline
        self.intervals = {}     # (prevHeight, height) -> content signature
        self.books = {}         # height -> book signature after applying up to it
        self.samples = collections.OrderedDict()  # height -> whole book, last SAMPLE_KEEP
        self.snapshots = self.updates = 0
        self.error = None

    def _record(self, height, book):
        self.books[height] = book_signature(book)
        self.samples[height] = [dict(book[0]), dict(book[1])]
        while len(self.samples) > SAMPLE_KEEP:
            self.samples.popitem(last=False)

    def run(self):
        try:
            ws = Ws(self.url, self.token)
            ws.send_text(json.dumps({"method": "subscribe", "subscription": self.sub}))
            book, have = [dict(), dict()], False
            while _time.time() < self.deadline:
                ws.sock.settimeout(max(1.0, self.deadline - _time.time()))
                try:
                    msg = json.loads(ws.recv_text())
                except socket.timeout:
                    return
                if msg.get("channel") == "error":
                    self.error = str(msg.get("data"))
                    return
                if msg.get("channel") != "l2Diff":
                    continue
                data = msg["data"]
                if "Snapshot" in data:
                    d = data["Snapshot"]
                    book, have = book_from_levels(d["levels"]), True
                    self.snapshots += 1
                    self._record(d["height"], book)
                else:
                    d = data["Updates"]
                    self.updates += 1
                    if not have:
                        continue
                    self.intervals[(d["prevHeight"], d["height"])] = (
                        side_signature(d["bids"]),
                        side_signature(d["asks"]),
                    )
                    apply_side(book[0], d["bids"])
                    apply_side(book[1], d["asks"])
                    self._record(d["height"], book)
        except Exception as e:  # a dead node must not take the other one with it
            self.error = "{}: {}".format(type(e).__name__, e)


def pct(n, d):
    return 0.0 if d == 0 else 100.0 * n / d


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--a", default="ws://localhost:48001/ws")
    ap.add_argument("--b", default="ws://localhost:48002/ws")
    ap.add_argument("--coin", default="BTC")
    ap.add_argument("--levels", type=int, default=1000)
    ap.add_argument("--sig-figs", type=int, default=None)
    ap.add_argument("--seconds", type=float, default=60.0)
    ap.add_argument("--token", default=None)
    args = ap.parse_args()

    sub = {"type": "l2Diff", "coin": args.coin}
    if args.levels != DEFAULT_LEVELS:
        sub["nLevels"] = args.levels
    if args.sig_figs is not None:
        sub["nSigFigs"] = args.sig_figs

    deadline = _time.time() + args.seconds
    a = NodeStream("A", args.a, sub, args.token, deadline)
    b = NodeStream("B", args.b, sub, args.token, deadline)
    print("comparing l2Diff {} for {:.0f}s".format(json.dumps(sub), args.seconds))
    print("  A: {}\n  B: {}".format(args.a, args.b))
    a.start()
    b.start()
    a.join()
    b.join()

    for s in (a, b):
        if s.error:
            print("{} failed: {}".format(s.name, s.error), file=sys.stderr)
    if a.error and b.error:
        return 1

    print("")
    print("A: {} snapshot(s), {} update(s)".format(a.snapshots, a.updates))
    print("B: {} snapshot(s), {} update(s)".format(b.snapshots, b.updates))

    # 1. intervals
    ia, ib = set(a.intervals), set(b.intervals)
    shared = ia & ib
    print("")
    print("--- 1. do the intervals line up? ---")
    print("A had {}, B had {}, shared {}  ({:.1f}% of A, {:.1f}% of B)"
          .format(len(ia), len(ib), len(shared), pct(len(shared), len(ia)), pct(len(shared), len(ib))))

    # 2. content on shared intervals
    same = sum(1 for k in shared if a.intervals[k] == b.intervals[k])
    print("")
    print("--- 2. on a shared interval, is the content identical? ---")
    if shared:
        print("identical {} of {}  ({:.1f}%)".format(same, len(shared), pct(same, len(shared))))
        for k in sorted(shared):
            if a.intervals[k] != b.intervals[k]:
                print("  first disagreement at {} -> {}".format(k[0], k[1]))
                break
    else:
        print("no shared interval to compare -- the nodes never cut the same way")

    # 3. books at common heights: the one that decides the design
    ha, hb = set(a.books), set(b.books)
    common = sorted(ha & hb)
    agree = sum(1 for h in common if a.books[h] == b.books[h])
    print("")
    print("--- 3. at a height both reached, are the BOOKS identical? ---")
    if common:
        print("identical {} of {}  ({:.1f}%)".format(agree, len(common), pct(agree, len(common))))
    else:
        print("no height reached by both -- nothing to compare")

    # How far apart, on the heights whose whole books we still hold. This is
    # what separates a timing artefact from the nodes genuinely disagreeing.
    sampled = sorted(set(a.samples) & set(b.samples))
    counts, all_ranks, depth = [], [], 0
    for h in sampled:
        n, ranks = compare_books(a.samples[h], b.samples[h])
        counts.append(n)
        all_ranks.extend(ranks)
        depth = max(depth, len(a.samples[h][0]) + len(a.samples[h][1]))
    if sampled:
        print("")
        print("--- how far apart, over {} sampled heights ---".format(len(sampled)))
        print("levels differing: median {}, p95 {}, max {}   (of {} in the book)"
              .format(quantile(counts, 0.5), quantile(counts, 0.95), max(counts), depth))
        if all_ranks:
            print("their depth rank: median {}, p95 {}, max {}   (0 = best price)"
                  .format(quantile(all_ranks, 0.5), quantile(all_ranks, 0.95), max(all_ranks)))

    print("")
    print("--- what this means ---")
    if sampled and counts:
        med, half = quantile(counts, 0.5), depth // 2
        near_top = quantile(all_ranks, 0.5) < depth // 8 if all_ranks else False
        if med == 0:
            pass
        elif med > half:
            print("The books differ across most of their depth. That is not timing:")
            print("the nodes are holding different books. Stop and find out why")
            print("before any of this is built on.")
        elif near_top:
            print("Few levels differ, but they sit near the top of the book, where")
            print("a stale price is worth the most. Worth understanding before")
            print("treating this as harmless timing.")
        else:
            print("Few levels differ and they sit deep in the book -- the signature")
            print("of snapshots taken at different moments WITHIN a block, not of")
            print("nodes disagreeing. `height` currently marks a point inside the")
            print("block's application, not its end; per-block emission is what")
            print("would make it a real checkpoint.")
    if common and agree == len(common):
        print("The books agree at every height both nodes reached. wsarb can move")
        print("from one node to the other at any such height and carry on applying")
        print("updates: no snapshot, no gap, nothing the client can see. Per-block")
        print("emission would buy nothing.")
    elif common and pct(agree, len(common)) < 100:
        print("The books DIVERGE at heights both nodes reached. That is not a")
        print("timing artefact -- the nodes disagree about the book itself, and")
        print("switching sources mid-stream would corrupt the client. Investigate")
        print("before building any failover on top of this channel.")
    if shared and same < len(shared):
        print("Shared intervals with differing content point the same way: check")
        print("whether one node is behind on ingest rather than merely out of phase.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
