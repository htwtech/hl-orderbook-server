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
  placed before this hour    only a terminal status this hour (canceled /
                             filled / ...), or no status at all: the New was
                             in an earlier hour, outside this replay. The
                             server holds such orders from its snapshot and
                             finds them -- an artifact of the one-hour window,
                             not something the server counts as unknown
  opened this hour, no New   "open" this hour but no New diff before the
                             Remove: this is the suspicious one

Only a sample of orphans (--sample, default 300) is looked up in the status
file: it is ~10 GB an hour, and one grep for a few hundred oids is minutes
where a full parse would be far longer. The categories are what matter, not
the exact count.

With --oids FILE the question is turned around: given oids -- the server
logs one in a thousand of its unknowns as `diff without order (sample
1/1000): remove oid=… coin=… height=…`, and the file may hold those log lines
verbatim -- every diff and status for them over the last --hours hours is
pulled from the node's files and laid out as a timeline, with a verdict per
oid. An oid whose New and open both appear in the window, yet whose Remove
the server found unknown, was lost inside the server; that is the one worth
chasing.

Standard library only (plus grep on the node's files).

  python3 diff-orphans.py --data-dir /home/hyperliquid/hl-data/data
  python3 diff-orphans.py --data-dir … --hour 20260915/10 --coin BTC
  journalctl -u <server> --since "1 hour ago" | grep "diff without order" > unknown.log
  python3 diff-orphans.py --data-dir … --oids unknown.log --hours 3
"""

import argparse
import collections
import json
import os
import random
import re
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
    gone_by = {}            # oid -> ("update to zero" | "remove", block), once it left
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
                        gone_by[oid] = ("update to zero" if zero else "remove", block)
                    continue
                counts["orphan " + kind] += 1
                if oid not in orphans:
                    orphans[oid] = (kind, block, gone_by.get(oid))
            if n % 100000 == 0 and n:
                print("  diffs: {} lines".format(n), file=sys.stderr)
    return counts, orphans


def grep_batches(path, oids):
    """Every batch line of `path` naming one of these oids, parsed. One grep:
    the node's spacing and field order are its own, so a space after the colon
    and either a comma or a closing brace after the number are allowed."""
    with tempfile.NamedTemporaryFile("w", suffix=".oids", delete=False) as tmp:
        for oid in oids:
            tmp.write('"oid": ?{}[,}}]\n'.format(oid))
        patterns = tmp.name
    try:
        p = subprocess.run(["grep", "-E", "-f", patterns, path], capture_output=True)
    finally:
        os.unlink(patterns)
    for raw in p.stdout.splitlines():
        try:
            yield json.loads(raw)
        except ValueError:
            continue


def statuses_for(path, oids):
    """Status events for these oids, from one grep over the hour's status file."""
    wanted = set(oids)
    found = collections.defaultdict(list)     # oid -> [(block, status, order)]
    for batch in grep_batches(path, oids):
        block = batch.get("block_number")
        for ev in batch.get("events") or []:
            order = ev.get("order") or {}
            oid = order.get("oid")
            if oid in wanted:
                found[oid].append((block, ev.get("status"), order))
    return found


def diffs_for(path, oids):
    """Diff events for these oids: oid -> [(block, kind, newSz)]."""
    wanted = set(oids)
    found = collections.defaultdict(list)
    for batch in grep_batches(path, oids):
        block = batch.get("block_number")
        for ev in batch.get("events") or []:
            oid = ev.get("oid")
            if oid in wanted:
                kind, new_sz = diff_kind(ev)
                found[oid].append((block, kind, new_sz))
    return found


def read_oids(path):
    """Bare numbers, or the server's own log lines (`oid=N`)."""
    out = []
    with open(path, encoding="utf-8", errors="replace") as f:
        for line in f:
            m = re.search(r"oid=(\d+)", line)
            if m:
                out.append(int(m.group(1)))
                continue
            line = line.strip()
            if line.isdigit():
                out.append(int(line))
    return list(collections.OrderedDict.fromkeys(out))


def verdict(diffs, statuses):
    kinds = [k for _, k, _ in diffs]
    names = [s for _, s, _ in statuses]
    if not diffs and not statuses:
        return "nothing in the window: older than that"
    # A New after a Remove for the same oid: the node re-placed the order under
    # the oid it already had. A server that dropped the first New on the Remove
    # and then paired the status with nothing has lost a live order.
    if "remove" in kinds and "new" in kinds[kinds.index("remove"):]:
        return "re-placed under the same oid after a Remove ({} News, {} Removes): a pending New dropped on that Remove would be a live order lost".format(
            kinds.count("new"), kinds.count("remove"))
    if "new" in kinds:
        if "open" in names or "triggered" in names:
            return "New and open both in the window: the server should have held this order -- lost inside the server"
        if names:
            return "New in the window, status " + "/".join(sorted(set(names))) + ": never rested (taker or trigger), the Remove is expected"
        return "New in the window, no status in the window"
    zero = [b for b, k, sz in diffs if k == "update" and sz is not None and float(sz) == 0.0]
    removes = [b for b, k, _ in diffs if k == "remove"]
    if zero and removes:
        return "zeroed @{} then removed @{} ({} blocks apart): the follow-up Remove, expected".format(
            zero[0], removes[0], removes[0] - zero[0])
    if "open" in names:
        return "open in the window but no New: odd"
    return "no New in the window: placed before it (statuses: {})".format("/".join(sorted(set(names))) or "none")


def trace_oids(args, diffs_files, statuses_files):
    oids = read_oids(args.oids)
    if not oids:
        print("no oids found in {}".format(args.oids), file=sys.stderr)
        return 2
    hours = sorted(set(diffs_files) & set(statuses_files), key=hour_key)[-args.hours:]
    print("tracing {} oids over {} hour(s): {}".format(len(oids), len(hours), ", ".join(hours)))
    all_diffs, all_statuses = collections.defaultdict(list), collections.defaultdict(list)
    for hour in hours:
        print("  grep {} ...".format(hour), file=sys.stderr)
        for oid, evs in diffs_for(diffs_files[hour], oids).items():
            all_diffs[oid].extend(evs)
        for oid, evs in statuses_for(statuses_files[hour], oids).items():
            all_statuses[oid].extend(evs)

    verdicts = collections.Counter()
    for oid in oids:
        d = sorted(all_diffs.get(oid, []))
        st = sorted(all_statuses.get(oid, []), key=lambda x: x[0])
        v = verdict(d, st)
        key = re.sub(r" ?@\d+| ?\(\d+ blocks apart\)| ?\(statuses: [^)]*\)| ?\(\d+ News, \d+ Removes\)", "", v)
        verdicts[re.sub(r"\s+", " ", key).replace(" :", ":").strip()] += 1
        order = st[0][2] if st else {}
        print("\noid {}  coin {} {} px {} tif {} trigger {}".format(
            oid, order.get("coin", "?"), order.get("side", "?"), order.get("limitPx", "?"),
            order.get("tif", "?"), order.get("isTrigger", "?")))
        print("  diffs:    " + (" -> ".join("{}@{}{}".format(k, b, "(sz " + sz + ")" if sz is not None else "") for b, k, sz in d) or "none"))
        print("  statuses: " + (" -> ".join("{}@{}".format(s, b) for b, s, _ in st) or "none"))
        print("  " + v)

    print("\n--- verdicts ---")
    for v, c in verdicts.most_common():
        print("  {:>5}  {}".format(c, v))
    return 0


def classify(kind, gone_by, statuses):
    if gone_by:
        return "already gone by " + gone_by[0] + ", then " + kind
    if not statuses:
        return "placed before this hour (no status this hour)"
    names = [s for _, s, _ in statuses]
    order = statuses[0][2]
    if order.get("tif") == "Ioc":
        return "ioc"
    if order.get("isTrigger") and "open" in names and "triggered" not in names:
        return "trigger, never triggered"
    if "open" in names:
        return "opened this hour, no New before the " + kind
    return "placed before this hour (only " + ", ".join(sorted(set(names))) + " this hour)"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--data-dir", required=True)
    ap.add_argument("--hour", metavar="YYYYMMDD/HH", help="hour to scan (default: newest present in both files)")
    ap.add_argument("--coin", help="only this coin's diffs")
    ap.add_argument("--sample", type=int, default=300, help="orphans to look up in the status file")
    ap.add_argument("--max-lines", type=int, default=0, help="stop the diff scan after this many lines (0 = all)")
    ap.add_argument("--show", type=int, default=8, help="examples to print per category")
    ap.add_argument("--oids", metavar="FILE", help="trace these oids instead (bare numbers, or the server's log lines)")
    ap.add_argument("--hours", type=int, default=2, help="with --oids: how many recent hours to search")
    args = ap.parse_args()

    diffs, statuses = hour_files(args.data_dir, DIFFS), hour_files(args.data_dir, STATUSES)
    common = sorted(set(diffs) & set(statuses), key=hour_key)
    if not common:
        print("no hour present in both {} and {}".format(DIFFS, STATUSES), file=sys.stderr)
        return 2
    if args.oids:
        return trace_oids(args, diffs, statuses)
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
        if cls.startswith("already gone"):
            # How far the follow-up trails what emptied the order, in blocks.
            gaps = [orphans[o][1] - orphans[o][2][1] for o in members]
            buckets = collections.Counter("same block" if g == 0 else "1-10" if g <= 10 else "11-64" if g <= 64 else ">64" for g in gaps)
            print("         blocks between: " + ", ".join("{} {}".format(buckets[k], k) for k in ("same block", "1-10", "11-64", ">64") if buckets[k]))
    for cls, members in sorted(by_class.items(), key=lambda kv: -len(kv[1])):
        print("\n  {}:".format(cls))
        for oid in members[:args.show]:
            kind, block, gone = orphans[oid]
            sts = found.get(oid, [])
            order = sts[0][2] if sts else {}
            trail = " -> ".join("{}@{}".format(s, b) for b, s, _ in sts) or "no status"
            left = "  ({} @{})".format(gone[0], gone[1]) if gone else ""
            print("    oid {}  {} {} @{}{}  coin {} {} px {} sz {} tif {} trigger {}   statuses: {}".format(
                oid, kind, "diff", block, left, order.get("coin", "?"), order.get("side", "?"), order.get("limitPx", "?"),
                order.get("sz", "?"), order.get("tif", "?"), order.get("isTrigger", "?"), trail))
    return 0


if __name__ == "__main__":
    sys.exit(main())
