<!--
SEO
title tag:        How to Stream the Hyperliquid Order Book (L2, L4, and Diffs)
meta description: Three ways to stream the Hyperliquid order book — public API, your own node, or a hosted feed — and the two silent bugs that make a self-built book drift from the chain.
slug:             stream-hyperliquid-order-book
primary query:    stream hyperliquid order book
-->

# How to stream the Hyperliquid order book

There are three ways to stream the Hyperliquid order book: subscribe to the
public API, run your own node and build the book from its data files, or take a
feed from someone who already does. This post is about what each one actually
costs you — and about the two failure modes that make a self-built Hyperliquid
order book drift away from the chain without a single error in the log.

## Option 1: the public Hyperliquid API

`wss://api.hyperliquid.xyz/ws` gives you `l2Book` for free, in minutes. For a
dashboard or a slow strategy, stop reading — it is the right answer.

Two things push people off it. It is shared infrastructure, so you get its rate
limits and its queueing, not yours. And it is 20 aggregated levels: no order
IDs, no owners, no queue position. If your edge depends on where you sit in the
queue at a price, an L2 snapshot cannot tell you.

It is also not a reference implementation. Running two independent nodes
alongside it, we have measured minutes where the public feed sat behind both of
them on the same block — levels it had not caught up to yet. Most of the time it
agrees exactly; sometimes it does not.

## Option 2: run a node and build the book yourself

A Hyperliquid node in streaming mode writes three files an hour:
`node_order_statuses_streaming`, `node_raw_book_diffs_streaming`, and
`node_fills_streaming`. This is the real firehose — everything the exchange
did, before anyone aggregated it:

| stream | volume, per hour | what it carries |
|---|---|---|
| order statuses | ~19.7M lines, 10.8 GB | `open`, `filled`, `canceled`, with side, price, size, owner |
| raw book diffs | ~11.6M lines, 2.9 GB | `New`, `Update`, `Remove` per order ID |
| fills | — | both legs of every match |

Blocks land ~14 times a second, ~677 status lines and ~400 diff lines each.

Now build a Level 4 book out of it. An order rests only when two events meet:
its `open` status, which carries the side and price, and its `New` diff, which
carries the size and the position in the queue at that price. Two files, two
readers, one order. Do that correctly and you have something the public API
cannot give you: every resting order, by ID and owner, in queue order.

That is the appeal. Here is the part nobody warns you about.

## The two bugs that make a Hyperliquid order book drift

Both of these are real, both were found in production, and neither one prints
an error.

**The two streams race.** The status file is several times larger than the diff
file, so the diff reader routinely runs ahead. An order placed and cancelled
inside the same block arrives as `New` then `Remove` on the diff stream — while
its `open` status is still on disk. The `Remove` finds no order in the book and
does nothing. Then the `open` arrives, and the order rests. Forever. It is a
phantom: size that exists in your book and nowhere on the chain. Nothing counts
it, and it only disappears when the price finally crosses it and your own
matching logic eats it.

**The snapshot is older than you think.** You bootstrap from
`hl-node compute-l4-snapshots`, which dumps the node's *persisted* state,
`abci_state.rmp` — rewritten every 10 000 blocks, about twelve minutes. The
obvious place to read the height is `visor_abci_state.json`. That file tracks
the **head**, not the persisted state. Cut your replay at the head and you
discard every event in between: orders placed in that window are missing from
your book, orders cancelled in it stay in it. Up to twelve minutes of the
market, silently dropped, every time you start or resync.

We ran two servers on two nodes, identical code, identical input files, and they
disagreed on **511 and 833 orders** — each one carrying its own startup hole.
The full write-up, with the measurements and the fix, is in
[docs/CONSISTENCY.md](../CONSISTENCY.md).

The pattern is what matters: a book that drifts does not crash. It just quietly
stops matching the exchange, and you find out from your P&L.

## How to know your book is actually right

Ask any Hyperliquid data provider — or your own implementation — for these two
numbers.

**Order-for-order agreement between two independent builds.** Take two nodes,
two servers, and compare the full L4 book at the same block height, by order ID.
Not the top 20 levels; every order. Our current answer: **60 998 orders on one,
60 998 on the other, zero differences** — no order in one and missing from the
other, no order with a different size, over a 300-poll run with the height gap
never exceeding 2 blocks.

**Zero persistent disagreements with the public API.** Frame-by-frame diffs
against `api.hyperliquid.xyz`, matched on block time, separating real
disagreements from timing artifacts: a level that differs in one frame is the
two feeds being sampled a few milliseconds apart; a level that differs by the
same amount for several frames running is a real, wrong order. Our runs show
**zero** of the second kind, and on a quiet minute all frames match exactly,
down to the order count at the top of book.

If a provider cannot show you that second number, they have not looked.

## Streaming it without melting your bandwidth

Depth is expensive. A 1000-level BTC book is ~72 KB of JSON, rebuilt ~14 times a
second — resending it whole is about 1 MB/s per coin, per subscriber.

So there is a diff channel. One snapshot when you subscribe, then only the
levels that changed: `upd` entries as `[price, size, order_count]` triples,
`del` as bare prices. Measured over 120 seconds on BTC at 1000 levels:
**1.06 MB instead of 120.04 MB — 113× less**, with the rebuilt book identical
to all 1674 full snapshots it replaced.

```json
{ "method": "subscribe",
  "subscription": { "type": "l2Diff", "coin": "BTC", "nLevels": 1000 } }
```

Details and the continuity rules are in [docs/L2DIFF.md](../L2DIFF.md).

## What a full Hyperliquid market data feed looks like

| channel | what you get |
|---|---|
| `bbo` | top of book, per coin, on every change |
| `l2Book` | aggregated ladder, up to 1000 levels, HL's own bucketing |
| `l2Diff` | the same ladder as snapshot + diffs, ~100× less traffic |
| `l4Book` | every resting order: ID, owner, price, size, queue order |
| `bookDiffs` | the node's raw `New`/`Update`/`Remove` stream |
| `trades` | both legs, with taker side |
| `orderUpdates` | one address's order lifecycle |
| `GET /l4Book` | one-shot, price-banded L4 slice over HTTP |
| `GET /untriggeredOrders` | resting stop and TP/SL orders, which no book shows |

## Get an endpoint

We run this on our own nodes, verified continuously by the checks above: two
independent builds compared order for order, and both compared against the
public API.

Point a websocket client at it and subscribe — the message format is the one
you already know from Hyperliquid's API.

```
wss://<your-host>/ws
```

**Request access: <contact>**

Prefer to run it yourself? The consistency work described here is documented in
full in [docs/CONSISTENCY.md](../CONSISTENCY.md) — including the two bugs above,
so you can check your own implementation for them tonight.
