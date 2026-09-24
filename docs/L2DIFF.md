# The `l2Diff` channel

`l2Diff` carries the same order book as `l2Book`, with the same parameters,
built by the same flush — but it sends one snapshot when you subscribe and
after that only the levels that changed. Subscribe to it instead of `l2Book`
whenever you want depth: at 1000 levels it is roughly a hundred times less
traffic, and the book you rebuild from it is identical, level for level.

## Why it exists

A 1000-level BTC book is about 72 KB of JSON, and it is rebuilt ~14 times a
second — one flush per block. Resending it whole is ~1 MB/s (~8 Mbit/s) for a
single coin on a single subscriber. Measured over 120 s on one connection
carrying both channels, BTC at `nLevels: 1000`:

| | frames | mean frame | total |
|---|---|---|---|
| `l2Book` | 1674 | 71 711 B | 120.04 MB |
| `l2Diff` | 1 snapshot + 1674 updates | 592 B per update | 1.06 MB |

**113× less over the run, 121× less per update.** A second server on a second
node, same run: 104× and 111×. The book rebuilt from those 1674 updates was
identical to all 1674 `l2Book` frames — see [Verify it yourself](#verify-it-yourself).

## Subscribe

```json
{ "method": "subscribe", "subscription": { "type": "l2Diff", "coin": "BTC", "nLevels": 1000 } }
```

The parameter tuple is exactly `l2Book`'s:

| parameter | values | meaning |
|---|---|---|
| `coin` | e.g. `"BTC"`, `"@107"` | required |
| `nLevels` | 1–1000, omit for 20 | levels per side |
| `nSigFigs` | 2–5 | price bucketing, as on HL's public API |
| `mantissa` | 2 or 5, requires `nSigFigs: 5` | finer bucketing |

Two things the validator is strict about: passing `nLevels: 20` explicitly is
**rejected** — omit the field to get the default — and `mantissa` without
`nSigFigs: 5` is rejected too.

Unsubscribe with the same subscription object and `"method": "unsubscribe"`.

## The two frames

**Snapshot** — the whole book at your depth. Sent when you subscribe, and
again whenever the server cannot produce a correct diff for you (see
[Continuity](#continuity)):

```json
{"channel":"l2Diff","data":{"Snapshot":{
  "coin":"BTC","time":1789558634545,"height":1149553971,"nLevels":1000,
  "levels":[
    [{"px":"75899.0","sz":"1.85926","n":11}, {"px":"75898.0","sz":"0.4","n":2}],
    [{"px":"75900.0","sz":"5.68367","n":38}, {"px":"75901.0","sz":"0.9","n":3}]
  ]}}}
```

`levels` is `[bids, asks]`, bids descending and asks ascending, exactly as in
`l2Book`. `px` and `sz` are decimal strings; `n` is the number of resting
orders at that level.

**Updates** — only what moved since the previous frame on this subscription:

```json
{"channel":"l2Diff","data":{"Updates":{
  "coin":"BTC","time":1789558634744,"height":1149553974,"prevHeight":1149553971,
  "nLevels":1000,
  "bids":{"upd":[["75899.0","2.10012",13]],"del":["75898.0"]},
  "asks":{"upd":[["75900.0","5.1",35],["75902.0","0.25",1]],"del":[]}
}}}
```

Each side carries:

- `upd` — levels to insert or overwrite, addressed by price. A positional
  triple `[px, sz, n]`, not an object: at 1000 levels the field names would be
  ~60% of the frame. `px` and `sz` are strings, `n` is a number.
- `del` — prices that are no longer in the book **at your depth**.

`nSigFigs` and `mantissa` are echoed back on both frames when you set them;
`nLevels` is always echoed.

## How to apply it

Keep two price-keyed maps and the last `height` you applied:

```python
for px, sz, n in upd:   # both sides, independently
    book[side][px] = (sz, n)
for px in del_:
    book[side].pop(px, None)
```

That is the whole algorithm — there is no sequence-number-per-level, no
positional indexing, nothing to reorder. Sort by price when you need the
ladder. A reference implementation in ~10 lines is `apply_side` in
[`tools/l2diff-verify.py`](../tools/l2diff-verify.py).

## Continuity

A coin is not dirty at every block, so `height` skips legitimately —
`height - 1` proves nothing. **`prevHeight` is the check:**

> If `prevHeight` does not equal the last `height` you applied, you missed a
> frame. Your book is stale: resubscribe (unsubscribe, subscribe) to get a
> fresh `Snapshot`.

The server also sends a `Snapshot` unasked in three cases: your first frame on
the subscription, a slow consumer that fell behind the broadcast (the frames it
missed are gone, so a diff would be wrong), and a coin the server has no prior
book for. Treat any `Snapshot` as "replace everything you have for this
subscription".

## Depth is part of the diff

Levels are truncated to **your** `nLevels` before the comparison. Two
consequences worth knowing:

- a price in `del` may simply have been pushed past your depth by a better
  level, not cancelled or filled;
- two subscribers at different depths genuinely receive different diffs for
  the same book. That is correct, not a bug.

## Silence is normal

A coin with nothing to report sends nothing at all — there are no heartbeat or
keep-alive frames on this channel. On a quiet market you can go seconds without
a frame; that means the book has not changed, not that the connection is dead.
Use `{"method":"ping"}` (answered with `{"channel":"pong"}`) if you want
liveness.

## Known caveat: use `height`, not `time`

`time` is the **block time**, and a block is applied over several flushes — so
two consecutive frames can carry the same `time` with different books. Use
`height` / `prevHeight` as the frame identity and for ordering. (`l2Book` has
no `height` field, which is one reason to prefer this channel; flushing once
per block is on the roadmap.)

## Verify it yourself

[`tools/l2diff-verify.py`](../tools/l2diff-verify.py) subscribes to `l2Diff`
and `l2Book` for the same coin and parameters on one connection, rebuilds the
book from the diff stream, and compares it against every `l2Book` frame. Python
standard library only — it runs on the node with nothing installed:

```sh
python3 tools/l2diff-verify.py --url ws://localhost:48001/ws --coin BTC --levels 1000 --seconds 120
```

```
--- correctness ---
books compared against l2Book: 1674, identical: 1674

RESULT: PASSED
```

It also checks that `prevHeight` chains without a break, reports any
unsolicited snapshot, and prints the byte sizes both channels actually put on
the wire.

## Related

- [README](../README.md) — the other channels (`bbo`, `l2Book`, `l4Book`,
  `trades`, `bookDiffs`, `orderUpdates`, `GET /l4Book`).
- [docs/CONSISTENCY.md](CONSISTENCY.md) — how the book underneath this channel
  is kept in step with the chain, and how that is verified.
