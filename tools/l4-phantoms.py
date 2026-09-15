#!/usr/bin/env python3
"""Two servers' L4 books, order by order: which oids one has and the other has not.

l2-vs-api.py says THAT one server shows extra size at a price; this says WHICH
orders. GET /l4Book is polled on both servers once a second with the same price
band around the mid, and the two snapshots are compared by oid. An oid on one
side only, or on both with a different side, price or size, is a candidate. It
is reported once it has held for --hold polls running -- one poll is nothing:
the two snapshots are taken milliseconds apart and orders come and go in
between -- with the order's side, price, size, owner and the block height it
was first seen at, and again when it goes away. The oids are appended to --out
in the form diff-orphans.py --oids reads, so the node's own files can then tell
what really happened to each order.

The band is re-centred on A's mid every poll; the first poll is unbanded to
find it, and is slow on a deep book. An order that leaves the band because the
band moved is not "gone", and one that comes back is not news: each oid is
announced once.

  python3 l4-phantoms.py --a http://localhost:48001 --b http://localhost:48002 --coin BTC --seconds 600
  python3 diff-orphans.py --data-dir /home/hyperliquid/hl-data/data --oids phantoms.txt --hours 1
"""

import argparse
import gzip
import json
import sys
import time
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from decimal import Decimal

SIDES = ("bid", "ask")


def fetch(base, coin, lo, hi, timeout):
    """One banded snapshot: (block time, height, {oid: (side, px, sz, user)})."""
    q = {"coin": coin}
    if lo is not None:
        q["minPx"] = lo
    if hi is not None:
        q["maxPx"] = hi
    url = base.rstrip("/") + "/l4Book?" + urllib.parse.urlencode(q)
    req = urllib.request.Request(url, headers={"Accept-Encoding": "gzip"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        raw = r.read()
        if r.headers.get("Content-Encoding") == "gzip":
            raw = gzip.decompress(raw)
    snap = json.loads(raw)
    snap = snap.get("Snapshot", snap)
    orders = {}
    for side, side_orders in enumerate(snap["levels"]):
        for o in side_orders:
            user = None
            if isinstance(o, list):  # [owner, order] tuple shape
                user, o = o
            user = o.get("user", user)
            orders[int(o["oid"])] = (side, Decimal(str(o["limitPx"])), Decimal(str(o["sz"])), user)
    return int(snap["time"]), int(snap["height"]), orders


def band_around(orders, frac):
    """(lo, hi) as Decimals, or (None, None) with an empty side."""
    bids = [px for side, px, _, _ in orders.values() if side == 0]
    asks = [px for side, px, _, _ in orders.values() if side == 1]
    if not bids or not asks:
        return None, None
    mid = (max(bids) + min(asks)) / 2
    q = Decimal("1e-8")
    return (mid * (1 - frac)).quantize(q), (mid * (1 + frac)).quantize(q)


def fmt(px):
    return None if px is None else "{:f}".format(px)


def short(user):
    if not user:
        return "-"
    return user if len(user) <= 12 else user[:6] + "…" + user[-4:]


def describe(o):
    side, px, sz, user = o
    return "{} {} sz {} user {}".format(SIDES[side], px, sz, short(user))


def clock():
    return time.strftime("%H:%M:%S", time.gmtime())


class Candidate:
    def __init__(self, kind, oid, height, detail, px):
        self.kind = kind
        self.oid = oid
        self.px = px
        self.first_wall = time.time()
        self.first_height = height
        self.detail = detail
        self.polls = 0
        self.reported = False


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--a", default="http://localhost:48001", help="first server, HTTP base (its mid centres the band)")
    ap.add_argument("--b", default="http://localhost:48002", help="second server, HTTP base")
    ap.add_argument("--coin", default="BTC")
    ap.add_argument("--seconds", type=float, default=600.0)
    ap.add_argument("--every", type=float, default=1.0, help="seconds between polls")
    ap.add_argument("--hold", type=int, default=3, help="polls running before a difference is reported")
    ap.add_argument("--band", type=float, default=0.003, help="half-width of the price band as a fraction of the mid")
    ap.add_argument("--out", default="phantoms.txt", help="oids of reported orders, one per line, for diff-orphans.py --oids")
    ap.add_argument("--progress", type=float, default=30.0, help="seconds between heartbeat lines")
    args = ap.parse_args()

    labels = {"A": args.a, "B": args.b}
    print("A: {}\nB: {}\n{}: band ±{:.2%} of A's mid, poll every {}s, report after {} polls running\n".format(
        args.a, args.b, args.coin, args.band, args.every, args.hold))

    cands = {}  # (kind, oid) -> Candidate
    reported = []  # Candidates in the order reported
    announced = set()  # (kind, oid) already printed: back in the band is not news
    out = open(args.out, "a", encoding="utf-8")
    lo = hi = None
    polls = failed = 0
    last_progress = 0.0
    max_height_gap = 0
    deadline = time.time() + args.seconds
    pool = ThreadPoolExecutor(max_workers=2)
    try:
        while time.time() < deadline:
            started = time.time()
            timeout = 60.0 if lo is None else 10.0
            fa = pool.submit(fetch, args.a, args.coin, fmt(lo), fmt(hi), timeout)
            fb = pool.submit(fetch, args.b, args.coin, fmt(lo), fmt(hi), timeout)
            try:
                ta, ha, A = fa.result()
                tb, hb, B = fb.result()
            except Exception as e:  # noqa: BLE001 - one bad poll must not end the watch
                failed += 1
                print("{}  poll failed: {}".format(clock(), e))
                time.sleep(args.every)
                continue
            polls += 1
            asked_lo, asked_hi = lo, hi  # the band these snapshots were taken with
            lo, hi = band_around(A, Decimal(str(args.band)))
            max_height_gap = max(max_height_gap, abs(ha - hb))

            seen = set()
            for oid in A.keys() - B.keys():
                seen.add(("A only", oid))
                cands.setdefault(("A only", oid), Candidate("A only", oid, ha, describe(A[oid]), A[oid][1]))
            for oid in B.keys() - A.keys():
                seen.add(("B only", oid))
                cands.setdefault(("B only", oid), Candidate("B only", oid, hb, describe(B[oid]), B[oid][1]))
            for oid in A.keys() & B.keys():
                if A[oid][:3] != B[oid][:3]:
                    seen.add(("differs", oid))
                    cands.setdefault(("differs", oid), Candidate(
                        "differs", oid, ha, "A: {} | B: {}".format(describe(A[oid]), describe(B[oid])), A[oid][1]))

            for key in list(cands):
                c = cands[key]
                if key in seen:
                    c.polls += 1
                    if c.polls == args.hold:
                        c.reported = True
                        if key not in announced:
                            announced.add(key)
                            reported.append(c)
                            heights = "" if abs(ha - hb) <= 2 else "  (heights A {} B {})".format(ha, hb)
                            print("{}  {:7}  oid={} {}  since h {}{}".format(
                                clock(), c.kind, c.oid, c.detail, c.first_height, heights))
                            out.write("{}\n".format(c.oid))
                            out.flush()
                    continue
                # Not in this poll's difference set. Gone from the book -- or
                # only from the band, which moved with the price.
                in_band = asked_lo is None or asked_lo <= c.px <= asked_hi
                if c.reported and in_band:
                    print("{}  {:7}  oid={} gone after {:.0f} s  (h {} .. {})".format(
                        clock(), c.kind, c.oid, time.time() - c.first_wall, c.first_height, max(ha, hb)))
                del cands[key]

            if time.time() - last_progress >= args.progress:
                last_progress = time.time()
                held = sum(1 for c in cands.values() if c.reported)
                print("{}  A h={} {} orders | B h={} {} orders | in band: A only {}, B only {}, differ {} | held {}".format(
                    clock(), ha, len(A), hb, len(B),
                    len(A.keys() - B.keys()), len(B.keys() - A.keys()),
                    sum(1 for k in seen if k[0] == "differs"), held))

            time.sleep(max(0.0, args.every - (time.time() - started)))
    except KeyboardInterrupt:
        pass
    finally:
        pool.shutdown(wait=False)
        out.close()

    print("\n--- {} polls ({} failed), largest A/B height gap {} blocks ---".format(polls, failed, max_height_gap))
    by_kind = {}
    for c in reported:
        by_kind.setdefault(c.kind, []).append(c)
    for kind in ("A only", "B only", "differs"):
        cs = by_kind.get(kind, [])
        if not cs:
            continue
        owners = {}
        for c in cs:
            owner = c.detail.split("user ")[-1].split(" ")[0]
            owners[owner] = owners.get(owner, 0) + 1
        top = ", ".join("{} x{}".format(o, n) for o, n in sorted(owners.items(), key=lambda kv: -kv[1])[:5])
        print("{:7}: {} orders held >= {} polls; owners: {}".format(kind, len(cs), args.hold, top))
    if reported:
        print("oids appended to {} -- trace them with diff-orphans.py --oids {} --hours 1".format(args.out, args.out))
    else:
        print("nothing held for {} polls: the two books agree order for order in the band".format(args.hold))
    return 0


if __name__ == "__main__":
    sys.exit(main())
