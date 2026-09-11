#!/usr/bin/env python3
"""Our l2Book against a reference, matched frame for frame on block time.

The reference -- the public Hyperliquid API by default -- sends a frame every
few seconds; we send fifteen a second. So the slow side leads: every reference
frame with block time T is matched to our frame with the same T, and the two
books are compared right then, one line per match.

Several of ours at once. With more than one --ours -- the two nodes behind the
proxy, say -- every source is matched to the reference on the same T, and the
sources are matched to each other on it too. One run then answers both "which
node is closer to the reference" and "how far apart are the nodes", on the
same blocks, so the numbers are comparable.

Matching waits. A reference frame at T can arrive before our frame at T does,
if our source is behind, so each one is retried for up to --wait seconds before
"no frame at T" is declared -- which is itself a finding: that source never
flushed on that block.

Books are compared at the shallower of the two depths per side, so a 1000-level
subscription on our side against a 20-level reference compares the top 20.

Beyond how far apart the books are, this reports which way: on how many levels
one side shows MORE than the other, and on how many less. Phantom orders -- ones
a node still holds after the chain removed them -- push that balance one way only.

Prices and sizes are compared as numbers, not strings: two servers can spell
the same price "110503" and "110503.0", and keyed on the string every level
comes out "only in ours" and "only in ref" at once -- which is exactly what the
first run against the public API produced. The raw strings are still shown,
once, so a spelling difference is seen rather than silently absorbed.

With -v every price the sides disagree on gets a line of its own under the
frame's verdict -- the block time, the side, the price, and each source's
sz/n there ("-" where that source has no such price) -- so a disagreement can
be looked at rather than only counted.

Standard library only.

  python3 l2-vs-api.py --coin BTC --seconds 120
  python3 l2-vs-api.py --ours ws://localhost:48001/ws ws://localhost:48002/ws --coin BTC --seconds 120
  python3 l2-vs-api.py --ours ws://localhost:48001/ws ws://localhost:48002/ws -v     # and the levels themselves
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
RING = 300          # block times kept per source; ~20 s at 15/s


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


def trimmed(a, b):
    """Both books cut to the shallower depth per side, and those depths."""
    n = [min(len(a[i]), len(b[i])) for i in (0, 1)]
    return ([top_n(a, n[0])[0], top_n(a, n[1])[1]],
            [top_n(b, n[0])[0], top_n(b, n[1])[1]], n)


def compare(a, b):
    """Levels differing, their depth ranks, and which way each one leans."""
    differing, ranks, a_more, b_more = 0, [], 0, 0
    for side, (sa, sb) in enumerate(zip(a, b)):
        order = sorted(set(sa) | set(sb), key=float, reverse=(side == 0))
        for rank, px in enumerate(order):
            va, vb = sa.get(px), sb.get(px)
            if va == vb:
                continue
            differing += 1
            ranks.append(rank)
            # A level only one side has, or a bigger size, or the same size
            # with more orders on it: that side shows more resting interest.
            if vb is None or (va is not None and va > vb):
                a_more += 1
            else:
                b_more += 1
    return differing, ranks, a_more, b_more


def kinds(a, b, la, lb):
    k = collections.Counter()
    for sa, sb in zip(a, b):
        for px in set(sa) | set(sb):
            va, vb = sa.get(px), sb.get(px)
            if va == vb:
                continue
            if va is None:
                k["price only in " + lb] += 1
            elif vb is None:
                k["price only in " + la] += 1
            elif va[0] != vb[0]:
                k["same price, different sz"] += 1
            else:
                k["same price and sz, different n"] += 1
    return k


def nearest(cands_a, cands_b):
    """The closest pair of states, one from each list.

    Within one block a source passes through several states and another
    source, or the reference, shows one or more of them. Identical if any pair
    is; otherwise the least different pair. The untrimmed state picked on the
    first side comes back last, for the level-by-level view.
    """
    best = None
    for a in cands_a:
        for b in cands_b:
            ta, tb, n = trimmed(a, b)
            differing, ranks, am, bm = compare(ta, tb)
            if best is None or differing < best[0]:
                best = (differing, ranks, am, bm, ta, tb, n, a)
            if differing == 0:
                return best
    return best


def num(d):
    """A Decimal as it reads, not as it was spelled: 78550.0 -> 78550, 0.50 -> 0.5."""
    return format(d.normalize(), "f")


def print_levels(t, books, order):
    """One line per price the present sides disagree on: each side's sz/n."""
    present = [lab for lab in order if lab in books]
    for side, name in ((0, "bid"), (1, "ask")):
        depth = min(len(books[lab][side]) for lab in present)
        cut = {lab: top_n(books[lab], depth)[side] for lab in present}
        for px in sorted(set().union(*cut.values()), key=float, reverse=(side == 0)):
            vals = [cut[lab].get(px) for lab in present]
            if all(v == vals[0] for v in vals):
                continue
            cells = []
            for lab in order:
                if lab not in books:
                    cell = "(no frame)"
                else:
                    v = cut[lab].get(px)
                    cell = "-" if v is None else "{}/{}".format(num(v[0]), v[1])
                cells.append("{} {:<14}".format(lab, cell))
            print("{}  {} {:>10}  {}".format(hhmmss(t), name, num(px), "  ".join(cells)))


def quantile(values, q):
    if not values:
        return 0
    s = sorted(values)
    return s[min(len(s) - 1, int(round(q * (len(s) - 1))))]


def pct(n, d):
    return 0.0 if d == 0 else 100.0 * n / d


def hhmmss(t):
    return _time.strftime("%H:%M:%S", _time.gmtime(t / 1000)) + ".{:03d}".format(int(t % 1000))


class Pair:
    """Everything tallied between two sides over a run."""

    def __init__(self, a, b):
        self.a, self.b = a, b
        self.matched = self.identical = self.shared = self.depth = 0
        self.more_a = self.more_b = 0
        self.counts, self.ranks = [], []
        self.top_ok, self.top_seen = collections.Counter(), collections.Counter()   # per depth: only matches deep enough count
        self.kinds = collections.Counter()

    @property
    def name(self):
        return "{}-{}".format(self.a, self.b)

    def record(self, best):
        """Tally one match; returns its cell for the per-frame line."""
        differing, ranks, am, bm, ta, tb, n, _ = best
        total = n[0] + n[1]
        self.depth = max(self.depth, total)
        self.matched += 1
        self.identical += differing == 0
        # Matches with at least one price on both sides. None at all across a
        # run means the books are not two views of one market.
        self.shared += any(set(ta[i]) & set(tb[i]) for i in (0, 1))
        self.counts.append(differing)
        self.ranks.extend(ranks)
        self.more_a += am
        self.more_b += bm
        self.kinds.update(kinds(ta, tb, self.a, self.b))
        for k in (5, 20):
            if k <= min(n):
                self.top_ok[k] += top_n(ta, k) == top_n(tb, k)
                self.top_seen[k] += 1
        return "{} {:>3}/{}  {}>{:>3} {}>{:>3}".format(self.name, differing, total, self.a, am, self.b, bm)

    def summary(self):
        print("\n--- {} ---".format(self.name))
        print("matched {} (identical {}, {:.1f}%)".format(self.matched, self.identical, pct(self.identical, self.matched)))
        if not self.counts:
            print("nothing compared")
            return
        print("levels differing: median {}, p95 {}, max {}   (of up to {} compared)".format(
            quantile(self.counts, 0.5), quantile(self.counts, 0.95), max(self.counts), self.depth))
        if self.shared == 0:
            print("no price appears on both sides in any match -- not a book divergence; "
                  "check the coin and aggregation, see the wire sample")
        for k in (5, 20):
            if self.top_seen[k]:
                print("top {:>2} a side identical: {} of {}  ({:.1f}%)".format(
                    k, self.top_ok[k], self.top_seen[k], pct(self.top_ok[k], self.top_seen[k])))
        if self.ranks:
            print("depth rank of disagreements: median {}, p95 {}   (0 = best price)".format(
                quantile(self.ranks, 0.5), quantile(self.ranks, 0.95)))
        total_dir = self.more_a + self.more_b
        print("{} shows more: {} levels ({:.1f}%)   {} shows more: {} levels ({:.1f}%)".format(
            self.a, self.more_a, pct(self.more_a, total_dir), self.b, self.more_b, pct(self.more_b, total_dir)))
        total_k = sum(self.kinds.values())
        for kind, c in self.kinds.most_common():
            print("  {:>32}: {:>6}  ({:.1f}%)".format(kind, c, pct(c, total_k)))


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
    ap.add_argument("--ours", nargs="+", default=["ws://localhost:48000/ws"], metavar="URL",
                    help="one or more of our sources; with several, they are also compared to each other")
    ap.add_argument("--ref", default="wss://api.hyperliquid.xyz/ws")
    ap.add_argument("--coin", default="BTC")
    ap.add_argument("--levels", type=int, default=DEFAULT_LEVELS,
                    help="nLevels for every side; 20 is sent by omission (the public API has no nLevels)")
    ap.add_argument("--seconds", type=float, default=120.0)
    ap.add_argument("--wait", type=float, default=10.0,
                    help="how long a reference frame waits for ours at the same time")
    ap.add_argument("-v", "--verbose", action="store_true",
                    help="under each frame, one line per price the sides disagree on: each side's sz/n")
    args = ap.parse_args()

    sub = {"type": "l2Book", "coin": args.coin}
    if args.levels != DEFAULT_LEVELS:
        sub["nLevels"] = args.levels

    # One source keeps its name; several are lettered, and the legend below
    # says which is which.
    labels = ["ours"] if len(args.ours) == 1 else [chr(ord("A") + i) for i in range(len(args.ours))]

    lock = threading.Lock()
    # Each source's frames, keyed by block time -- a LIST per time, not one
    # book. A source flushes more than once inside a block, each flush a
    # different state under the same stamp, and keeping only the last one made
    # the control run (our source against itself) come out 78% identical: the
    # reference's first flush of a block was being held against our second.
    rings = {lab: collections.OrderedDict() for lab in labels}   # time -> [book, book, ...]
    pending = collections.deque()          # reference frames awaiting a match: (time, book, arrived_at)

    def on_ours(lab):
        def on_frame(t, book):
            with lock:
                ring = rings[lab]
                ring.setdefault(t, []).append(book)
                while len(ring) > RING:
                    ring.popitem(last=False)
        return on_frame

    def on_ref(t, book):
        with lock:
            pending.append((t, book, _time.time()))

    deadline = _time.time() + args.seconds
    sources = {lab: Stream(lab, url, sub, deadline, on_ours(lab)) for lab, url in zip(labels, args.ours)}
    ref = Stream("ref", args.ref, sub, deadline, on_ref)
    width = max(len(lab) for lab in labels + ["ref"])
    for lab, url in zip(labels, args.ours):
        print("{}: {}".format(lab.ljust(width), url))
    print("{}: {}".format("ref".ljust(width), args.ref))
    print("{} at {} levels, {:.0f}s\n".format(args.coin, args.levels, args.seconds))
    for s in sources.values():
        s.start()
    ref.start()

    # Every source against the reference, then the sources against each other.
    pairs = collections.OrderedDict()
    for lab in labels:
        pairs[(lab, "ref")] = Pair(lab, "ref")
    for i, a in enumerate(labels):
        for b in labels[i + 1:]:
            pairs[(a, b)] = Pair(a, b)
    unmatched = collections.Counter()      # per source: reference times it had no frame for
    st = dict(sampled=False, ref_settled=0)

    def settle(final):
        now = _time.time()
        with lock:
            items = list(pending)
        for item in items:
            t, rbook, arrived = item
            with lock:
                cands = {lab: list(rings[lab].get(t) or []) for lab in labels}
                # Wait for every source to have T, not just one: a line that
                # compared A but gave B up early would misstate B's coverage.
                if not all(cands.values()) and not (final or now - arrived > args.wait):
                    continue
                pending.remove(item)
            st["ref_settled"] += 1

            # Shown once, before the first verdict: the spelling of price and
            # size on each side, as received. Comparison is numeric, so a
            # difference here is absorbed -- but it should be seen, not hidden.
            if not st["sampled"]:
                st["sampled"] = True
                for s in list(sources.values()) + [ref]:
                    print("wire sample   {}: {}".format(s.name.ljust(width), s.sample or "(no frame yet)"))
                print("")

            cells = []
            chosen = {"ref": rbook}    # the state of each source that stood nearest the reference
            for lab in labels:
                if cands[lab]:
                    best = nearest(cands[lab], [rbook])
                    chosen[lab] = best[-1]
                    cells.append(pairs[(lab, "ref")].record(best))
                else:
                    unmatched[lab] += 1
                    cells.append("{}-ref  no frame at this time".format(lab))
            for i, a in enumerate(labels):
                for b in labels[i + 1:]:
                    if cands[a] and cands[b]:
                        cells.append(pairs[(a, b)].record(nearest(cands[a], cands[b])))
            print("{}  {}".format(hhmmss(t), " | ".join(cells)))
            if args.verbose:
                print_levels(t, chosen, labels + ["ref"])

    while _time.time() < deadline and (any(s.is_alive() for s in sources.values()) or ref.is_alive()):
        _time.sleep(0.5)
        settle(final=False)
    for s in sources.values():
        s.join(timeout=2)
    ref.join(timeout=2)
    settle(final=True)

    for s in list(sources.values()) + [ref]:
        if s.error:
            print("{} failed: {}".format(s.name, s.error), file=sys.stderr)

    print("\n--- frames ---")
    print(", ".join("{} {}".format(s.name, s.frames) for s in list(sources.values()) + [ref]))
    for lab in labels:
        if unmatched[lab]:
            print("{} had no frame at {} of {} reference times".format(lab, unmatched[lab], st["ref_settled"]))
    if not any(p.counts for p in pairs.values()):
        print("nothing compared -- check the connections and that the coin is right")
        return 1
    for p in pairs.values():
        p.summary()
    print("\n(phantom orders on one side push \"shows more\" toward that side)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
