#!/usr/bin/env python3
"""Do two nodes hold the same book?

Subscribes to the same coin with the same parameters on two nodes and compares
what they report, either channel:

  --channel l2Book   each frame IS a book; compared at equal `time`
  --channel l2Diff   the book is rebuilt from the updates; compared at equal `height`

Run l2Book first. It involves nothing this repository recently added, so it
separates "the nodes disagree" from "the diff channel loses something" -- and
those want completely different investigations.

For l2Diff it also reports whether the update INTERVALS (prevHeight -> height)
line up at all, since the nodes flush on their own 50 ms phase.

Standard library only, so it runs on the node with nothing installed.

  python3 l2-compare-nodes.py --channel l2Book --coin BTC --levels 1000 --seconds 60
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
# detected. Bounded: a 1000-level book is a few hundred KB in Python.
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


def compare_books(a, b):
    """How far apart two books are: (levels differing, their depth ranks).

    The rank matters as much as the count. Disagreement only at the bottom is
    churn around the nLevels boundary; disagreement spread through the depth,
    or sitting at the top, means something else.
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
        self.channel = sub["type"]
        self.intervals = {}     # l2Diff only: (prevHeight, height) -> signature
        self.samples = collections.OrderedDict()   # key -> whole book
        self.keys = set()       # every key seen, even once the book is evicted
        self.frames = 0
        self.error = None

    def _record(self, key, book):
        self.keys.add(key)
        self.samples[key] = [dict(book[0]), dict(book[1])]
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
                if msg.get("channel") != self.channel:
                    continue
                self.frames += 1
                data = msg["data"]

                if self.channel == "l2Book":
                    # Each frame is already the whole book; `time` is the only
                    # key both nodes derive from the same place.
                    self._record(data["time"], book_from_levels(data["levels"]))
                    continue

                if "Snapshot" in data:
                    d = data["Snapshot"]
                    book, have = book_from_levels(d["levels"]), True
                    self._record(d["height"], book)
                else:
                    d = data["Updates"]
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
    ap.add_argument("--channel", choices=("l2Book", "l2Diff"), default="l2Book")
    ap.add_argument("--a", default="ws://localhost:48001/ws")
    ap.add_argument("--b", default="ws://localhost:48002/ws")
    ap.add_argument("--coin", default="BTC")
    ap.add_argument("--levels", type=int, default=1000)
    ap.add_argument("--sig-figs", type=int, default=None)
    ap.add_argument("--seconds", type=float, default=60.0)
    ap.add_argument("--token", default=None)
    args = ap.parse_args()

    sub = {"type": args.channel, "coin": args.coin}
    if args.levels != DEFAULT_LEVELS:
        sub["nLevels"] = args.levels
    if args.sig_figs is not None:
        sub["nSigFigs"] = args.sig_figs
    keyed_by = "time" if args.channel == "l2Book" else "height"

    deadline = _time.time() + args.seconds
    a = NodeStream("A", args.a, sub, args.token, deadline)
    b = NodeStream("B", args.b, sub, args.token, deadline)
    print("comparing {} for {:.0f}s, keyed by {}".format(json.dumps(sub), args.seconds, keyed_by))
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
    print("A: {} frame(s), {} distinct {}".format(a.frames, len(a.keys), keyed_by))
    print("B: {} frame(s), {} distinct {}".format(b.frames, len(b.keys), keyed_by))

    if args.channel == "l2Diff":
        ia, ib = set(a.intervals), set(b.intervals)
        shared = ia & ib
        same = sum(1 for k in shared if a.intervals[k] == b.intervals[k])
        print("")
        print("--- update intervals ---")
        print("A {}, B {}, shared {}  ({:.1f}% of A)".format(
            len(ia), len(ib), len(shared), pct(len(shared), len(ia))))
        if shared:
            print("content identical on {} of {} shared  ({:.1f}%)".format(
                same, len(shared), pct(same, len(shared))))

    common_keys = sorted(a.keys & b.keys)
    print("")
    print("--- books at the same {} ---".format(keyed_by))
    print("{} in common".format(len(common_keys)))

    sampled = sorted(set(a.samples) & set(b.samples))
    if not sampled:
        print("none of them still held for comparison -- raise SAMPLE_KEEP or")
        print("shorten the run")
        return 1

    counts, all_ranks, depth, identical = [], [], 0, 0
    for k in sampled:
        n, ranks = compare_books(a.samples[k], b.samples[k])
        counts.append(n)
        all_ranks.extend(ranks)
        depth = max(depth, len(a.samples[k][0]) + len(a.samples[k][1]))
        identical += n == 0

    print("compared {}, identical {}  ({:.1f}%)".format(
        len(sampled), identical, pct(identical, len(sampled))))
    print("levels differing: median {}, p95 {}, max {}   (of {} in the book)".format(
        quantile(counts, 0.5), quantile(counts, 0.95), max(counts), depth))
    if all_ranks:
        per_side = max(1, depth // 2)
        deep = sum(1 for r in all_ranks if r > per_side * 3 // 4)
        print("their depth rank:  median {}, p95 {}, max {}   (0 = best price, {} per side)".format(
            quantile(all_ranks, 0.5), quantile(all_ranks, 0.95), max(all_ranks), per_side))
        print("in the deepest quarter of the book: {:.1f}% of them".format(pct(deep, len(all_ranks))))

    # No prose verdict here on purpose. An earlier version of this script picked
    # thresholds, called 567 differing levels out of 2000 "few", and printed two
    # contradictory conclusions in a row. The numbers above say it better.
    return 0


if __name__ == "__main__":
    sys.exit(main())
