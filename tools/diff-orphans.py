#!/usr/bin/env python3
"""What are the Remove/Update diffs that name an order the book never held?

order_book_server counts them as orderbook_diff_without_order_total{outcome=
"unknown"}: a Remove or Update for an oid that is neither resting nor waiting
for its status. The counter says how many; it cannot say what they are. The
node's own files can. This walks one hour of the book-diff file, and for
every Remove/Update whose oid is not resting at that point by the diffs
alone ("orphan"), looks the oid up in the status file of that hour and classifies it:

  trigger, never triggered   stop / TP-SL: "open" with isTrigger, no "triggered"
                             -- such an order never rests, so its Remove finds
                             nothing; benign
  already gone by X, then Y  the diffs themselves had removed it (a size
                             update to zero, or an earlier Remove); benign
  ioc                        tif=Ioc: never rests
  terminal status only       filled/canceled/... this hour, no "open": the
                             order was a taker or was placed before this hour
  no status this hour        placed before this hour -- the book has it from
                             the snapshot; a live server should not find it
                             unknown
  opened this hour, no New   "open" this hour but no New diff before the
                             Remove: this is the suspicious one

Only a sample of orphans (--sample, default 300) is looked up in the status
file: it is ~10 GB an hour, and one grep for a few hundred oids is minutes
where a full parse would be far longer. The categories are what matter, not
the exact count.

Standard library only (plus grep on the status file).

  python3 diff-orphans.py --data-dir /home/hyperliquid/hl-data/data
  python3 diff-orphans.py --data-dir … --hour 20260915/10 --coin BTC
"""

import argparse
import collections
import json
import os
import random
import subprocess
import sys
import tempfile

DIFFS = "node_raw_book_diffs_streaming"
STATUSES = "node_order_statuses_streaming"


def hour_files(data_dir, source):
    base = os.path.join(data_dir, source, "hourly")
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


def diff_kind(ev):
    d = ev.get("raw_book_diff")
    if d == "remove":
        return "remove", None
    if isinstance(d, dict):
        if "new" in d:
            return "new", None
        if "update" in d:
            return "update", d["update"].get("newSz")
    return "other", None


def scan_diffs(path, coin, max_lines):
    """One pass over the hour's diffs, replaying them as a set of resting oids.
    Returns (counts, orphans) where orphans maps oid -> (kind, block_number,
    how_it_left): a Remove/Update for an oid not resting at that point, and
    if the diffs themselves had already taken it out, by what."""
    live = set()            # resting by the diff stream alone
    gone_by = {}            # oid -> "update to zero" | "remove", once it left
    counts = collections.Counter()
    orphans = collections.OrderedDict()
    with open(path, "rb") as f:
        for n, raw in enumerate(f):
            if max_lines and n >= max_lines:
                break
            if not raw.endswith(b"\n"):
                break
            try:
                batch = json.loads(raw)
            except ValueError:
                counts["unparsable lines"] += 1
                continue
            block = batch.get("block_number")
            for ev in batch.get("events") or []:
                if coin and ev.get("coin") != coin:
                    continue
                kind, new_sz = diff_kind(ev)
                oid = ev.get("oid")
                counts[kind] += 1
                if kind == "new":
                    live.add(oid)
                    gone_by.pop(oid, None)
                    continue
                if kind not in ("remove", "update"):
                    continue
                if oid in live:
                    zero = False
                    if kind == "update" and new_sz is not None:
                        try:
                            zero = float(new_sz) == 0.0
                        except ValueError:
                            pass
                    if kind == "remove" or zero:
                        live.discard(oid)
                        gone_by[oid] = "update to zero" if zero else "remove"
                    continue
                counts["orphan " + kind] += 1
                if oid not in orphans:
                    orphans[oid] = (kind, block, gone_by.get(oid))
            if n % 100000 == 0 and n:
                print("  diffs: {} lines".format(n), file=sys.stderr)
    return counts, orphans


def statuses_for(path, oids):
    """Status events for these oids, from one grep over the hour's status file."""
    # The node's spacing and field order are its own: allow a space after the
    # colon and either a comma or a closing brace after the number.
    with tempfile.NamedTemporaryFile("w", suffix=".oids", delete=False) as tmp:
        for oid in oids:
            tmp.write('"oid": ?{}[,}}]\n'.format(oid))
        patterns = tmp.name
    try:
        p = subprocess.run(["grep", "-E", "-f", patterns, path], capture_output=True)
    finally:
        os.unlink(patterns)
    wanted = set(oids)
    found = collections.defaultdict(list)     # oid -> [(block, status, order)]
    for raw in p.stdout.splitlines():
        try:
            batch = json.loads(raw)
        except ValueError:
            continue
        block = batch.get("block_number")
        for ev in batch.get("events") or []:
            order = ev.get("order") or {}
            oid = order.get("oid")
            if oid in wanted:
                found[oid].append((block, ev.get("status"), order))
    return found


def classify(kind, gone_by, statuses):
    if gone_by:
        return "already gone by " + gone_by + ", then " + kind
    if not statuses:
        return "no status this hour"
    names = [s for _, s, _ in statuses]
    order = statuses[0][2]
    if order.get("tif") == "Ioc":
        return "ioc"
    if order.get("isTrigger") and "open" in names and "triggered" not in names:
        return "trigger, never triggered"
    if "open" in names:
        return "opened this hour, no New before the " + kind
    return "terminal status only (" + ", ".join(sorted(set(names))) + ")"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--data-dir", required=True)
    ap.add_argument("--hour", metavar="YYYYMMDD/HH", help="hour to scan (default: newest present in both files)")
    ap.add_argument("--coin", help="only this coin's diffs")
    ap.add_argument("--sample", type=int, default=300, help="orphans to look up in the status file")
    ap.add_argument("--max-lines", type=int, default=0, help="stop the diff scan after this many lines (0 = all)")
    ap.add_argument("--show", type=int, default=8, help="examples to print per category")
    args = ap.parse_args()

    diffs, statuses = hour_files(args.data_dir, DIFFS), hour_files(args.data_dir, STATUSES)
    common = sorted(set(diffs) & set(statuses), key=hour_key)
    if not common:
        print("no hour present in both {} and {}".format(DIFFS, STATUSES), file=sys.stderr)
        return 2
    hour = args.hour or common[-1]
    if hour not in diffs or hour not in statuses:
        print("hour {} is not present in both files".format(hour), file=sys.stderr)
        return 2
    print("hour {}\n  diffs:    {}\n  statuses: {}".format(hour, diffs[hour], statuses[hour]))

    counts, orphans = scan_diffs(diffs[hour], args.coin, args.max_lines)
    print("\n--- diffs ---")
    for k in ("new", "update", "remove", "other", "unparsable lines"):
        if counts[k]:
            print("  {:>8}  {}".format(counts[k], k))
    print("  orphan remove: {}   orphan update: {}   (oid not resting by the diffs at that point)".format(
        counts["orphan remove"], counts["orphan update"]))
    if not orphans:
        print("nothing to classify")
        return 0

    oids = list(orphans)
    random.seed(1)
    sample = oids if len(oids) <= args.sample else random.sample(oids, args.sample)
    print("\nlooking up {} of {} orphans in the status file...".format(len(sample), len(oids)), file=sys.stderr)
    found = statuses_for(statuses[hour], sample)

    by_class = collections.defaultdict(list)
    for oid in sample:
        kind, block, gone_by = orphans[oid]
        by_class[classify(kind, gone_by, found.get(oid, []))].append(oid)

    print("\n--- what the orphans are (sample of {}) ---".format(len(sample)))
    for cls, members in sorted(by_class.items(), key=lambda kv: -len(kv[1])):
        print("  {:>5}  {:5.1f}%  {}".format(len(members), 100.0 * len(members) / len(sample), cls))
    for cls, members in sorted(by_class.items(), key=lambda kv: -len(kv[1])):
        print("\n  {}:".format(cls))
        for oid in members[:args.show]:
            kind, block, _ = orphans[oid]
            sts = found.get(oid, [])
            order = sts[0][2] if sts else {}
            trail = " -> ".join("{}@{}".format(s, b) for b, s, _ in sts) or "no status"
            print("    oid {}  {} {} @{}  coin {} {} px {} sz {} tif {} trigger {}   statuses: {}".format(
                oid, kind, "diff", block, order.get("coin", "?"), order.get("side", "?"), order.get("limitPx", "?"),
                order.get("sz", "?"), order.get("tif", "?"), order.get("isTrigger", "?"), trail))
    return 0


if __name__ == "__main__":
    sys.exit(main())
