#!/usr/bin/env python3
"""Acceptance test for the l2Diff channel.

Subscribes to `l2Diff` and `l2Book` for the same coin and the same parameters
on one connection, rebuilds the book from the diff stream, and checks it against
what `l2Book` says at the same `time`. That is the check that matters: a diff
channel which loses a level is worse than no diff channel at all, because the
client cannot tell.

Also checks that `prevHeight` chains -- a coin is not dirty at every block, so
heights skip legitimately and each update must name the one it follows -- and
reports the byte sizes both channels actually put on the wire.

Standard library only, so it runs on the node with nothing installed.

  python3 l2diff-verify.py --coin BTC --levels 1000 --seconds 60
"""

import argparse
import base64
import json
import os
import socket
import ssl
import struct
import sys
import time as _time
from urllib.parse import urlparse

DEFAULT_LEVELS = 20


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
    """[bids, asks] of {px,sz,n} objects -> two price-keyed dicts."""
    return [{lv["px"]: (lv["sz"], lv["n"]) for lv in side} for side in levels]


def apply_side(side, diff):
    """One side of an Updates frame, applied in place."""
    for px, sz, n in diff.get("upd", []):
        side[px] = (sz, n)
    for px in diff.get("del", []):
        side.pop(px, None)


def describe(side_name, mine, theirs, limit=4):
    """The first few concrete disagreements, so a failure is actionable."""
    out = []
    for px in sorted(set(mine) | set(theirs))[:]:
        a, b = mine.get(px), theirs.get(px)
        if a != b:
            out.append("    {} {}: diff has {}, l2Book has {}".format(side_name, px, a, b))
            if len(out) >= limit:
                break
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", default="ws://localhost:48001/ws")
    ap.add_argument("--coin", default="BTC")
    ap.add_argument("--levels", type=int, default=1000)
    ap.add_argument("--sig-figs", type=int, default=None)
    ap.add_argument("--seconds", type=float, default=60.0)
    ap.add_argument("--token", default=None)
    args = ap.parse_args()

    params = {"coin": args.coin}
    if args.levels != DEFAULT_LEVELS:
        params["nLevels"] = args.levels
    if args.sig_figs is not None:
        params["nSigFigs"] = args.sig_figs

    ws = Ws(args.url, args.token)
    # Both channels on one connection, same parameters: they are fed by the same
    # flush, so a disagreement between them is the channel's fault and nothing
    # else's.
    for chan in ("l2Diff", "l2Book"):
        sub = dict(params, type=chan)
        ws.send_text(json.dumps({"method": "subscribe", "subscription": sub}))
    print("subscribed l2Diff + l2Book {} on {} for {:.0f}s"
          .format(json.dumps(params), args.url, args.seconds))

    book = [dict(), dict()]          # reconstructed from the diff stream
    have_snapshot = False
    at_height = None
    pending = {}                     # time -> book state, awaiting its l2Book frame

    snap_bytes, upd_bytes, l2book_bytes = [], [], []
    compared = matched = 0
    chain_breaks, mismatches, extra_snapshots = [], [], 0

    deadline = _time.time() + args.seconds
    while _time.time() < deadline:
        ws.sock.settimeout(max(1.0, deadline - _time.time()))
        try:
            text = ws.recv_text()
        except socket.timeout:
            break
        msg = json.loads(text)
        chan = msg.get("channel")
        size = len(text.encode())

        if chan == "error":
            print("upstream error: {}".format(msg.get("data")), file=sys.stderr)
            return 1

        if chan == "l2Diff":
            data = msg["data"]
            if "Snapshot" in data:
                d = data["Snapshot"]
                if have_snapshot:
                    # A second snapshot means the server gave up on chaining.
                    # Legal after a broadcast lag, but it should be rare.
                    extra_snapshots += 1
                book = book_from_levels(d["levels"])
                have_snapshot = True
                at_height = d["height"]
                snap_bytes.append(size)
                pending[d["time"]] = [dict(book[0]), dict(book[1])]
            else:
                d = data["Updates"]
                upd_bytes.append(size)
                if not have_snapshot:
                    chain_breaks.append("update before any snapshot")
                    continue
                if d["prevHeight"] != at_height:
                    chain_breaks.append(
                        "at height {}: prevHeight {} but we are at {}"
                        .format(d["height"], d["prevHeight"], at_height))
                apply_side(book[0], d["bids"])
                apply_side(book[1], d["asks"])
                at_height = d["height"]
                pending[d["time"]] = [dict(book[0]), dict(book[1])]

        elif chan == "l2Book":
            d = msg["data"]
            l2book_bytes.append(size)
            mine = pending.pop(d["time"], None)
            if mine is None:
                continue
            theirs = book_from_levels(d["levels"])
            compared += 1
            if mine == theirs:
                matched += 1
            elif len(mismatches) < 3:
                lines = ["  at time {}:".format(d["time"])]
                lines += describe("bid", mine[0], theirs[0])
                lines += describe("ask", mine[1], theirs[1])
                mismatches.append("\n".join(lines))
            # Anything older than the newest compared point will never be
            # matched; drop it so a long run does not grow without bound.
            for t in [t for t in pending if t < d["time"]]:
                del pending[t]

    print("")
    print("--- wire ---")
    if snap_bytes:
        print("l2Diff snapshot: {} sent, mean {:.0f} B".format(len(snap_bytes), sum(snap_bytes) / len(snap_bytes)))
    if upd_bytes:
        mean_upd = sum(upd_bytes) / len(upd_bytes)
        print("l2Diff updates:  {} sent, mean {:.0f} B".format(len(upd_bytes), mean_upd))
        if l2book_bytes:
            mean_book = sum(l2book_bytes) / len(l2book_bytes)
            print("l2Book frames:   {} sent, mean {:.0f} B".format(len(l2book_bytes), mean_book))
            print("saving:          {:.1f}x smaller per update".format(mean_book / mean_upd))
            total_diff = sum(snap_bytes) + sum(upd_bytes)
            total_book = sum(l2book_bytes)
            print("over the run:    {:.2f} MB vs {:.2f} MB  ({:.1f}x)"
                  .format(total_diff / 1e6, total_book / 1e6,
                          total_book / total_diff if total_diff else 0))

    print("")
    print("--- correctness ---")
    print("books compared against l2Book: {}, identical: {}".format(compared, matched))
    if extra_snapshots:
        print("!! {} extra snapshot(s) -- the server restarted the chain".format(extra_snapshots))
    if chain_breaks:
        print("!! {} prevHeight break(s):".format(len(chain_breaks)))
        for b in chain_breaks[:5]:
            print("   " + b)
    if mismatches:
        print("!! {} book mismatch(es), first few:".format(len(mismatches)))
        for m in mismatches:
            print(m)

    ok = compared > 0 and matched == compared and not chain_breaks
    if compared == 0:
        print("")
        print("Nothing was compared. Either the market was silent, or l2Diff and")
        print("l2Book disagree on which flushes carry news -- both would show up")
        print("as zero here, so check the frame counts above before reading on.")
    print("")
    print("RESULT: {}".format("PASSED" if ok else "FAILED"))
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
