#!/usr/bin/env python3
"""Two nodes' streaming files, block for block.

order_book_server builds its book from the line files its Hyperliquid node
writes: one line per batch, `{"local_time", "block_time", "block_number",
"events": [...]}`, under <data-dir>/<source>/hourly/YYYYMMDD/HH. Two nodes on
the same chain must write the same events for the same block; the only thing
legitimately different is local_time, the node's own clock. So this takes the
same hour file from two data directories, walks both by block number, and
reports where they part -- which is the one question that decides whether a
divergence between two servers was born in the nodes or in the servers.

Nothing is loaded whole. Both files are streamed, consecutive lines of one
block are folded together, and two cursors advance in block order: a block is
in A only, in B only, or in both and compared. Where one file ends before the
other, the rest of the longer one is "past the other's end", not missing -- the
current hour is being written right now and one node is always a little
ahead. An unfinished last line is counted apart, not as a difference.

Comparing a block: first the cheap way -- local_time cut out, the rest hashed;
equal means identical without parsing any JSON. Otherwise the exact way: both
sides parsed, events put in canonical form and compared as multisets. Equal
multisets are "same events, other order", counted apart. Anything else is a
differing block, and the first few of those are laid out: how many events on
each side, how many only on one, and the events themselves, so it can be seen
what one node wrote that the other did not.

Standard library only. A multi-GB hour takes minutes in Python; --max-blocks
and --hour bound it.

  python3 node-files-compare.py --a /nodeA/hl/data --b /nodeB/hl/data
  python3 node-files-compare.py --a … --b … --hour 20260912/14 --coin BTC
"""

import argparse
import collections
import hashlib
import json
import os
import re
import sys

SOURCES = collections.OrderedDict([
    ("statuses", "node_order_statuses_streaming"),
    ("diffs", "node_raw_book_diffs_streaming"),
    ("fills", "node_fills_streaming"),
])
LOCAL_TIME = re.compile(rb'"local_time":\s*"[^"]*"\s*,?')
BLOCK_NUMBER = re.compile(rb'"block_number":\s*(\d+)')
SHOW_EVENTS = 3      # per side, per differing block
TRUNCATE = 220       # characters of an event shown


def group(n):
    return "{:,}".format(n).replace(",", " ")


def human(n):
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if n < 1024 or unit == "TB":
            return "{:.1f} {}".format(n, unit) if unit != "B" else "{} B".format(n)
        n /= 1024.0


def hour_files(data_dir, source):
    """Every hour file of this source, keyed 'YYYYMMDD/HH'."""
    base = os.path.join(data_dir, SOURCES[source], "hourly")
    out = {}
    if not os.path.isdir(base):
        return out
    for day in os.listdir(base):
        d = os.path.join(base, day)
        if not os.path.isdir(d):
            continue
        for h in os.listdir(d):
            p = os.path.join(d, h)
            if os.path.isfile(p):
                out[day + "/" + h] = p
    return out


def hour_key(k):
    day, _, h = k.partition("/")
    return (day, int(h) if h.isdigit() else -1, h)


class Blocks:
    """A file streamed as (block_number, [raw lines]) -- consecutive lines of
    one block folded together, the file never held whole."""

    def __init__(self, path):
        self.path = path
        self.size = os.path.getsize(path)
        self.lines = self.unparsable = self.out_of_order = 0
        self.partial = False      # last line has no newline yet: still being written
        self.first = self.last = None

    def __iter__(self):
        cur, buf = None, []
        with open(self.path, "rb") as f:
            for raw in f:
                if not raw.endswith(b"\n"):
                    self.partial = True
                    break
                line = raw.rstrip(b"\r\n")
                if not line:
                    continue
                m = BLOCK_NUMBER.search(line)
                if not m:
                    self.unparsable += 1
                    continue
                n = int(m.group(1))
                if cur is not None and n != cur:
                    if n < cur:
                        self.out_of_order += 1
                    yield cur, buf
                    buf = []
                if cur is None:
                    self.first = n
                cur = n
                self.last = n
                buf.append(line)
                self.lines += 1
        if buf:
            yield cur, buf


def signature(lines):
    """The block with local_time cut out, hashed -- equal means identical
    without parsing a byte of JSON."""
    h = hashlib.blake2b(digest_size=16)
    for line in lines:
        h.update(LOCAL_TIME.sub(b"", line))
        h.update(b"\n")
    return h.digest()


def event_coin(ev):
    # statuses carry it inside `order`, diffs at the top, fills as [user, {coin, ...}]
    if isinstance(ev, dict):
        if "coin" in ev:
            return ev["coin"]
        order = ev.get("order")
        if isinstance(order, dict):
            return order.get("coin")
    elif isinstance(ev, list):
        for x in ev:
            if isinstance(x, dict) and "coin" in x:
                return x["coin"]
    return None


def parse_events(lines, coin):
    """The block's events as a multiset of canonical strings, plus its
    block_time; None if a line does not parse."""
    events = collections.Counter()
    block_time = None
    for line in lines:
        try:
            batch = json.loads(line)
        except ValueError:
            return None, None
        if block_time is None:
            block_time = batch.get("block_time")
        for ev in batch.get("events") or []:
            if coin and event_coin(ev) != coin:
                continue
            events[json.dumps(ev, sort_keys=True, separators=(",", ":"))] += 1
    return events, block_time


def show_block(n, block_time, ea, eb):
    only_a, only_b = ea - eb, eb - ea
    print("  block {} ({}): A {} events, B {}   only in A {}, only in B {}".format(
        group(n), block_time or "?", group(sum(ea.values())), group(sum(eb.values())),
        group(sum(only_a.values())), group(sum(only_b.values()))))
    for side, only in (("A", only_a), ("B", only_b)):
        for ev, c in list(only.items())[:SHOW_EVENTS]:
            shown = ev if len(ev) <= TRUNCATE else ev[:TRUNCATE] + "…"
            print("    {}{}: {}".format(side, " x{}".format(c) if c > 1 else "", shown))


def compare(source, path_a, path_b, args):
    A, B = Blocks(path_a), Blocks(path_b)
    ia, ib = iter(A), iter(B)
    a, b = next(ia, None), next(ib, None)
    st = collections.Counter()
    shown = 0
    stopped = False
    print("\n{}".format(source))

    while a is not None and b is not None:
        if args.max_blocks and st["compared"] + st["only_a"] + st["only_b"] >= args.max_blocks:
            stopped = True
            break
        if a[0] < b[0]:
            st["only_a"] += 1
            a = next(ia, None)
            continue
        if b[0] < a[0]:
            st["only_b"] += 1
            b = next(ib, None)
            continue
        st["compared"] += 1
        # With a coin filter every block has to be parsed anyway; without one
        # the hash settles almost every block without touching JSON.
        if not args.coin and signature(a[1]) == signature(b[1]):
            st["identical"] += 1
        else:
            (ea, ta), (eb, tb) = parse_events(a[1], args.coin), parse_events(b[1], args.coin)
            if ea is None or eb is None:
                st["unparsable"] += 1
            else:
                if args.coin and (ea or eb):
                    st["with_coin"] += 1
                if ea == eb:
                    st["identical" if args.coin else "reordered"] += 1
                else:
                    st["differing"] += 1
                    if shown < args.show:
                        show_block(a[0], ta or tb, ea, eb)
                        shown += 1
        a, b = next(ia, None), next(ib, None)

    # Whatever is left on one side lies past the other's end: the file of the
    # current hour is still being written and one node is simply ahead.
    past_a = past_b = 0
    if not stopped:
        if a is not None:
            past_a = 1 + sum(1 for _ in ia)
        if b is not None:
            past_b = 1 + sum(1 for _ in ib)

    blocks = {"A": st["compared"] + st["only_a"] + past_a, "B": st["compared"] + st["only_b"] + past_b}
    for label, blk in (("A", A), ("B", B)):
        span = "blocks {}..{}".format(group(blk.first), group(blk.last)) if blk.first is not None else "empty"
        print("  {}: {}   {}, {} lines, {} blocks, {}".format(
            label, blk.path, human(blk.size), group(blk.lines), group(blocks[label]), span))
        notes = []
        if blk.partial:
            notes.append("last line unfinished (still being written)")
        if blk.unparsable:
            notes.append("{} lines without a block number".format(blk.unparsable))
        if blk.out_of_order:
            notes.append("{} blocks out of order -- the merge relies on order, treat the counts with care".format(blk.out_of_order))
        for n in notes:
            print("     ! {}".format(n))

    tail = ""
    if past_a:
        tail = " (A runs {} past B's end)".format(group(past_a))
    elif past_b:
        tail = " (B runs {} past A's end)".format(group(past_b))
    if stopped:
        tail = " (stopped at --max-blocks)"
    print("  compared {} blocks{}".format(group(st["compared"]), tail))
    if st["compared"]:
        line = "  identical {} ({:.2f}%)".format(group(st["identical"]), 100.0 * st["identical"] / st["compared"])
        if args.coin:
            line += "   with {} events on either side: {}".format(args.coin, group(st["with_coin"]))
        else:
            line += "   same events, other order: {}".format(group(st["reordered"]))
        print(line)
    print("  only in A: {} blocks   only in B: {}   differing: {}{}".format(
        group(st["only_a"]), group(st["only_b"]), group(st["differing"]),
        "   unparsable: {}".format(st["unparsable"]) if st["unparsable"] else ""))
    return st["only_a"] + st["only_b"] + st["differing"]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--a", required=True, metavar="DIR", help="data directory of the first node")
    ap.add_argument("--b", required=True, metavar="DIR", help="data directory of the second node")
    ap.add_argument("--source", nargs="+", default=["statuses", "diffs"], choices=list(SOURCES),
                    help="which streams to compare (default: the two the book is built from)")
    ap.add_argument("--hour", metavar="YYYYMMDD/HH", help="hour file to compare (default: the newest both sides have)")
    ap.add_argument("--coin", help="compare only this coin's events")
    ap.add_argument("--max-blocks", type=int, default=0, help="stop after this many blocks (0 = all)")
    ap.add_argument("--show", type=int, default=5, help="differing blocks to lay out in detail")
    args = ap.parse_args()

    differences = 0
    missing = 0
    for source in args.source:
        fa, fb = hour_files(args.a, source), hour_files(args.b, source)
        if not fa or not fb:
            for label, files, d in (("A", fa, args.a), ("B", fb, args.b)):
                if not files:
                    print("{}: no hour files under {}".format(
                        source, os.path.join(d, SOURCES[source], "hourly")), file=sys.stderr)
            missing += 1
            continue
        if args.hour:
            hour = args.hour
            if hour not in fa or hour not in fb:
                print("{}: hour {} is not on both sides (A has {}, B has {})".format(
                    source, hour, "it" if hour in fa else "not", "it" if hour in fb else "not"), file=sys.stderr)
                missing += 1
                continue
        else:
            common = sorted(set(fa) & set(fb), key=hour_key)
            if not common:
                print("{}: no hour file present on both sides".format(source), file=sys.stderr)
                missing += 1
                continue
            hour = common[-1]
        differences += compare(source, fa[hour], fb[hour], args)

    if missing:
        return 2
    return 1 if differences else 0


if __name__ == "__main__":
    sys.exit(main())
