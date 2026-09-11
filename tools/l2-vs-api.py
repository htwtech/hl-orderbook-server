#!/usr/bin/env python3
"""Our l2Book against a reference, matched frame for frame on block time.

The reference -- the public Hyperliquid API by default -- sends a frame every
few seconds; we send fifteen a second. So the slow side leads: every reference
frame with block time T is matched to our frame with the same T, and the two
books are compared right then, one line per match.

Matching waits. A reference frame at T can arrive before our frame at T does,
if our source is behind, so each one is retried for up to --wait seconds before
"no frame at T" is declared -- which is itself a finding: our source never
flushed on that block.

Books are compared at the shallower of the two depths per side, so a 1000-level
subscription on our side against a 20-level reference compares the top 20.

Beyond how far apart the books are, this reports which way: on how many levels
we show MORE than the reference, and on how many less. Phantom orders -- ones
we still hold after the chain removed them -- push that balance one way only.

Prices and sizes are compared as numbers, not strings: two servers can spell
the same price "110503" and "110503.0", and keyed on the string every level
comes out "only in ours" and "only in ref" at once -- which is exactly what the
first run against the public API produced. The raw strings are still shown,
once, so a spelling difference is seen rather than silently absorbed.

Standard library only.

  python3 l2-vs-api.py --coin BTC --seconds 120
  python3 l2-vs-api.py --ref ws://localhost:48000/ws --seconds 60   # control: must be 100% identical
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
from decimal import Decimal
from urllib.parse import urlparse

DEFAULT_LEVELS = 20
RING = 300          # block times kept on our side; ~20 s at 15/s


class Ws:
    """Just enough websocket to hold a subscription open and read text frames."""

    def __init__(self, url, timeout=30):
        u = urlparse(url)
        secure = u.scheme == "wss"
        port = u.port or (443 if secure else 80)
        self.sock = socket.create_connection((u.hostname, port), timeout=timeout)
        if secure:
            self.sock = ssl.create_default_context().wrap_socket(self.sock, server_hostname=u.hostname)
        key = base64.b64encode(os.urandom(16)).decode()
        req = [
            "GET {} HTTP/1.1".format(u.path or "/"),
            "Host: {}:{}".format(u.hostname, port),
            "Upgrade: websocket",
            "Connection: Upgrade",
            "Sec-WebSocket-Key: " + key,
            "Sec-WebSocket-Version: 13",
        ]
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
                self.sock.sendall(struct.pack("!BB", 0x8A, 0x80 | len(payload)) + bytes(4) + payload)
                continue
            if opcode == 0xA or opcode not in (0x0, 0x1, 0x2):
                continue
            parts.append(payload)
            if fin:
                return b"".join(parts).decode(errors="replace")


def book_from_levels(levels):
    # Decimal, not the wire string and not float: Decimal("110503.0") equals
    # Decimal("110503") and hashes the same, so differently spelled prices land
    # on one key, while 0.1 stays exact.
    return [{Decimal(lv["px"]): (Decimal(lv["sz"]), lv["n"]) for lv in side} for side in levels]


def wire_sample(levels):
    """The best level of each side as it came off the wire, unparsed."""
    def one(side):
        if not side:
            return "-"
        lv = side[0]
        return "{} x {} (n {})".format(lv["px"], lv["sz"], lv["n"])
    return "bid {}   ask {}".format(one(levels[0]), one(levels[1]))


def top_n(book, n):
    out = []
    for side, s in enumerate(book):
        keep = sorted(s, key=float, reverse=(side == 0))[:n]
        out.append({px: s[px] for px in keep})
    return out


def compare(ours, ref):
    """Levels differing, their depth ranks, and which way each one leans."""
    differing, ranks, ours_more, ref_more = 0, [], 0, 0
    for side, (a, b) in enumerate(zip(ours, ref)):
        order = sorted(set(a) | set(b), key=float, reverse=(side == 0))
        for rank, px in enumerate(order):
            va, vb = a.get(px), b.get(px)
            if va == vb:
                continue
            differing += 1
            ranks.append(rank)
            # A level only we have, or a bigger size, or the same size with more
            # orders on it: we show more resting interest than the reference.
            # Anything else is the reference showing more.
            if vb is None or (va is not None and va > vb):
                ours_more += 1
            else:
                ref_more += 1
    return differing, ranks, ours_more, ref_more


def kinds(ours, ref):
    k = collections.Counter()
    for a, b in zip(ours, ref):
        for px in set(a) | set(b):
            va, vb = a.get(px), b.get(px)
            if va == vb:
                continue
            if va is None:
                k["price only in ref"] += 1
            elif vb is None:
                k["price only in ours"] += 1
            elif va[0] != vb[0]:
                k["same price, different sz"] += 1
            else:
                k["same price and sz, different n"] += 1
    return k


def quantile(values, q):
    if not values:
        return 0
    s = sorted(values)
    return s[min(len(s) - 1, int(round(q * (len(s) - 1))))]


def pct(n, d):
    return 0.0 if d == 0 else 100.0 * n / d


def hhmmss(t):
    return _time.strftime("%H:%M:%S", _time.gmtime(t / 1000)) + ".{:03d}".format(int(t % 1000))


class Stream(threading.Thread):
    def __init__(self, name, url, sub, deadline, on_frame):
        super().__init__(daemon=True)
        self.name, self.url, self.sub, self.deadline, self.on_frame = name, url, sub, deadline, on_frame
        self.frames = 0
        self.error = None
        self.sample = None   # the first frame's best levels, verbatim

    def run(self):
        try:
            ws = Ws(self.url)
            ws.send_text(json.dumps({"method": "subscribe", "subscription": self.sub}))
            while _time.time() < self.deadline:
                ws.sock.settimeout(max(1.0, self.deadline - _time.time()))
                try:
                    msg = json.loads(ws.recv_text())
                except socket.timeout:
                    return
                if msg.get("channel") == "error":
                    self.error = str(msg.get("data"))
                    return
                if msg.get("channel") != "l2Book":
                    continue
                self.frames += 1
                d = msg["data"]
                if self.sample is None:
                    self.sample = wire_sample(d["levels"])
                self.on_frame(d["time"], book_from_levels(d["levels"]))
        except Exception as e:
            self.error = "{}: {}".format(type(e).__name__, e)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--ours", default="ws://localhost:48000/ws")
    ap.add_argument("--ref", default="wss://api.hyperliquid.xyz/ws")
    ap.add_argument("--coin", default="BTC")
    ap.add_argument("--levels", type=int, default=DEFAULT_LEVELS,
                    help="nLevels for both sides; 20 is sent by omission")
    ap.add_argument("--seconds", type=float, default=120.0)
    ap.add_argument("--wait", type=float, default=10.0,
                    help="how long a reference frame waits for ours at the same time")
    args = ap.parse_args()

    sub = {"type": "l2Book", "coin": args.coin}
    if args.levels != DEFAULT_LEVELS:
        sub["nLevels"] = args.levels

    lock = threading.Lock()
    # Our frames, keyed by block time -- a LIST per time, not one book. A source
    # flushes more than once inside a block, each flush a different state under
    # the same stamp, and keeping only the last one made the control run (our
    # source against itself) come out 78% identical: the reference's first
    # flush of a block was being held against our second.
    ring = collections.OrderedDict()      # time -> [book, book, ...]
    pending = collections.deque()         # reference frames awaiting a match: (time, book, arrived_at)

    def on_ours(t, book):
        with lock:
            ring.setdefault(t, []).append(book)
            while len(ring) > RING:
                ring.popitem(last=False)

    def on_ref(t, book):
        with lock:
            pending.append((t, book, _time.time()))

    deadline = _time.time() + args.seconds
    ours = Stream("ours", args.ours, sub, deadline, on_ours)
    ref = Stream("ref", args.ref, sub, deadline, on_ref)
    print("ours: {}\nref:  {}\n{} at {} levels, {:.0f}s\n".format(args.ours, args.ref, args.coin, args.levels, args.seconds))
    ours.start()
    ref.start()

    st = dict(matched=0, identical=0, unmatched=0, more_ours=0, more_ref=0, depth=0, shared=0)
    counts, all_ranks = [], []
    top_ok, top_seen = collections.Counter(), collections.Counter()   # per depth: only matches deep enough count
    all_kinds = collections.Counter()

    def settle(final):
        now = _time.time()
        with lock:
            items = list(pending)
        for item in items:
            t, rbook, arrived = item
            with lock:
                candidates = list(ring.get(t) or [])
                if not candidates and not (final or now - arrived > args.wait):
                    continue
                pending.remove(item)
            if not candidates:
                st["unmatched"] += 1
                print("{}  no frame at this time on our side".format(hhmmss(t)))
                continue

            # Within one block our source passes through several states and the
            # reference shows one of them. Compare against whichever of ours is
            # nearest: identical if any is, otherwise the least different.
            best = None
            for obook in candidates:
                n = [min(len(obook[i]), len(rbook[i])) for i in (0, 1)]
                o = [top_n(obook, n[0])[0], top_n(obook, n[1])[1]]
                r = [top_n(rbook, n[0])[0], top_n(rbook, n[1])[1]]
                differing, ranks, om, rm = compare(o, r)
                if best is None or differing < best[0]:
                    best = (differing, ranks, om, rm, o, r, n)
                if differing == 0:
                    break
            differing, ranks, om, rm, o, r, n = best
            total = n[0] + n[1]
            st["depth"] = max(st["depth"], total)
            # Shown once, before the first verdict: the spelling of price and
            # size on each side, as received. Comparison is numeric, so a
            # difference here is absorbed -- but it should be seen, not hidden.
            if st["matched"] == 0:
                print("wire sample   ours: {}\n              ref:  {}\n".format(ours.sample, ref.sample))
            st["matched"] += 1
            # Matches with at least one price on both sides. None at all across
            # a run means the books are not two views of one market.
            st["shared"] += any(set(o[i]) & set(r[i]) for i in (0, 1))
            counts.append(differing)
            all_ranks.extend(ranks)
            st["more_ours"] += om
            st["more_ref"] += rm
            all_kinds.update(kinds(o, r))

            tops = []
            for k in (5, 20):
                if k <= min(n):
                    same = top_n(o, k) == top_n(r, k)
                    top_ok[k] += same
                    top_seen[k] += 1
                    tops.append("top{} {}".format(k, "ok" if same else "DIFF"))

            if differing == 0:
                st["identical"] += 1
                print("{}  identical  0/{}".format(hhmmss(t), total))
            else:
                print("{}  differ  {:>4}/{}   {}   ours>ref {:>4}  ref>ours {:>4}".format(
                    hhmmss(t), differing, total, "  ".join(tops), om, rm))

    while _time.time() < deadline and (ours.is_alive() or ref.is_alive()):
        _time.sleep(0.5)
        settle(final=False)
    ours.join(timeout=2)
    ref.join(timeout=2)
    settle(final=True)

    for s in (ours, ref):
        if s.error:
            print("{} failed: {}".format(s.name, s.error), file=sys.stderr)

    print("\n--- summary ---")
    print("ours {} frames, ref {} frames".format(ours.frames, ref.frames))
    print("matched {} (identical {}, {:.1f}%), unmatched {}".format(
        st["matched"], st["identical"], pct(st["identical"], st["matched"]), st["unmatched"]))
    if not counts:
        print("nothing compared -- check both connections and that the coin is right")
        return 1
    print("levels differing: median {}, p95 {}, max {}   (of up to {} compared)".format(
        quantile(counts, 0.5), quantile(counts, 0.95), max(counts), st["depth"]))
    if st["shared"] == 0:
        print("no price appears on both sides in any match -- not a book divergence; "
              "check the coin and aggregation, see the wire sample")
    for k in (5, 20):
        if top_seen[k]:
            print("top {:>2} a side identical: {} of {}  ({:.1f}%)".format(
                k, top_ok[k], top_seen[k], pct(top_ok[k], top_seen[k])))
    if all_ranks:
        print("depth rank of disagreements: median {}, p95 {}   (0 = best price)".format(
            quantile(all_ranks, 0.5), quantile(all_ranks, 0.95)))

    total_dir = st["more_ours"] + st["more_ref"]
    print("")
    print("--- which way ---")
    print("ours shows more: {} levels ({:.1f}%)   ref shows more: {} levels ({:.1f}%)".format(
        st["more_ours"], pct(st["more_ours"], total_dir), st["more_ref"], pct(st["more_ref"], total_dir)))
    print("(phantom orders on our side would push this almost entirely to the left)")

    total_k = sum(all_kinds.values())
    if total_k:
        print("")
        print("--- what kind ---")
        for kind, c in all_kinds.most_common():
            print("  {:>32}: {:>6}  ({:.1f}%)".format(kind, c, pct(c, total_k)))
    return 0


if __name__ == "__main__":
    sys.exit(main())
