use alloy::primitives::Address;
use serde::{Deserialize, Serialize};

use crate::{
    order_book::types::Side,
    types::node_data::{NodeDataFill, NodeDataOrderDiff, NodeDataOrderStatus},
};

pub(crate) mod inner;
pub(crate) mod node_data;
pub(crate) mod subscription;

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct Trade {
    pub coin: String,
    side: Side,
    px: String,
    sz: String,
    hash: String,
    time: u64,
    tid: u64,
    users: [Address; 2],
}

#[derive(Debug, Clone, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub(crate) struct Level {
    px: String,
    sz: String,
    n: usize,
}

impl Level {
    pub(crate) const fn new(px: String, sz: String, n: usize) -> Self {
        Self { px, sz, n }
    }

    // Only exercised by tests since the BBO snapshot path moved to numeric dedup.
    #[cfg(test)]
    pub(crate) fn px(&self) -> &str {
        &self.px
    }

    #[cfg(test)]
    pub(crate) fn sz(&self) -> &str {
        &self.sz
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct L2Book {
    coin: String,
    time: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    n_sig_figs: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    mantissa: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    n_levels: Option<usize>,
    levels: [Vec<Level>; 2],
}

/// One level of an `l2Diff` update, as a bare `[px, sz, n]` array.
///
/// Deliberately not a `Level` object. Measured on BTC at 1000 levels an update
/// carries about 45 levels, and spelling the three keys out on each of them
/// costs some 16 bytes per level -- around 60% of the whole frame. On the one
/// channel whose entire purpose is fitting inside a client's link, that is not
/// a rounding error.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct L2DiffLevel(String, String, usize);

/// What changed on one side of the book since the previous update.
///
/// Keyed by price, not by position: at 1000 levels a new level at the top
/// shifts every level below it, so a positional diff would degenerate into a
/// full snapshot on the most ordinary event there is. By price that same event
/// is one entry in `upd` and one in `del`.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub(crate) struct L2SideDiff {
    /// Levels to insert or overwrite, addressed by price.
    upd: Vec<L2DiffLevel>,
    /// Prices no longer in the book at this depth -- cancelled, filled, or
    /// pushed past `nLevels` by something better.
    del: Vec<String>,
}

impl L2SideDiff {
    /// Diff one side of the book, `old` to `new`, both already truncated to the
    /// subscriber's depth. Truncation has to come first: which level falls off
    /// the bottom is a property of the depth that was asked for.
    pub(crate) fn between(old: &[Level], new: &[Level]) -> Self {
        let mut prev: std::collections::HashMap<&str, (&str, usize)> =
            std::collections::HashMap::with_capacity(old.len());
        for l in old {
            prev.insert(l.px.as_str(), (l.sz.as_str(), l.n));
        }
        let mut upd = Vec::new();
        for l in new {
            if prev.remove(l.px.as_str()) != Some((l.sz.as_str(), l.n)) {
                upd.push(L2DiffLevel(l.px.clone(), l.sz.clone(), l.n));
            }
        }
        // Whatever `new` did not claim is gone from the book at this depth.
        let del = prev.into_keys().map(str::to_owned).collect();
        Self { upd, del }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.upd.is_empty() && self.del.is_empty()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct L2DiffUpdates {
    coin: String,
    time: u64,
    height: u64,
    /// Height of the previous update sent on this same subscription. A coin is
    /// not dirty at every block, so heights legitimately skip and `height - 1`
    /// is no continuity check -- this is. A client whose last applied height
    /// does not match this has missed an update and needs a fresh snapshot.
    prev_height: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    n_sig_figs: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    mantissa: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    n_levels: Option<usize>,
    bids: L2SideDiff,
    asks: L2SideDiff,
}

/// The `l2Diff` channel: one snapshot when the subscription opens, then only
/// what changed. Externally tagged like [`L4Book`], so the wire shape is
/// `{"Snapshot": {..}}` / `{"Updates": {..}}`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum L2Diff {
    #[serde(rename_all = "camelCase")]
    Snapshot {
        coin: String,
        time: u64,
        height: u64,
        #[serde(skip_serializing_if = "Option::is_none")]
        n_sig_figs: Option<u32>,
        #[serde(skip_serializing_if = "Option::is_none")]
        mantissa: Option<u64>,
        #[serde(skip_serializing_if = "Option::is_none")]
        n_levels: Option<usize>,
        levels: [Vec<Level>; 2],
    },
    Updates(L2DiffUpdates),
}

impl L2Diff {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn updates(
        coin: String,
        time: u64,
        height: u64,
        prev_height: u64,
        n_sig_figs: Option<u32>,
        mantissa: Option<u64>,
        n_levels: Option<usize>,
        bids: L2SideDiff,
        asks: L2SideDiff,
    ) -> Self {
        Self::Updates(L2DiffUpdates {
            coin,
            time,
            height,
            prev_height,
            n_sig_figs,
            mantissa,
            n_levels,
            bids,
            asks,
        })
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) enum L4Book {
    Snapshot { coin: String, time: u64, height: u64, levels: [Vec<L4Order>; 2] },
    Updates(L4BookUpdates),
}

/// Best Bid/Offer - top of book only
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct Bbo {
    pub coin: String,
    pub time: u64,
    pub bid: Option<Level>,
    pub ask: Option<Level>,
}

impl L2Book {
    pub(crate) const fn from_l2_snapshot(
        coin: String,
        snapshot: [Vec<Level>; 2],
        time: u64,
        n_sig_figs: Option<u32>,
        mantissa: Option<u64>,
        n_levels: Option<usize>,
    ) -> Self {
        Self { coin, time, n_sig_figs, mantissa, n_levels, levels: snapshot }
    }

    pub(crate) const fn set_time(&mut self, time: u64) {
        self.time = time;
    }
}

impl Trade {
    /// Build one trade print from the two fill legs of a match, following the
    /// public websocket schema: `side` is the aggressing (taker) side — the
    /// leg whose `crossed` flag is set — and `users` is `[buyer, seller]`.
    ///
    /// Returns `None` if the legs do not form a valid match (coin or trade id
    /// mismatch, or not one buy leg and one sell leg); callers should skip
    /// such legs rather than emit schema-breaking output.
    pub(crate) fn from_fills(bid: NodeDataFill, ask: NodeDataFill) -> Option<Self> {
        let NodeDataFill(buyer, bid_fill) = bid;
        let NodeDataFill(seller, ask_fill) = ask;
        if bid_fill.coin != ask_fill.coin
            || bid_fill.tid != ask_fill.tid
            || bid_fill.side != Side::Bid
            || ask_fill.side != Side::Ask
        {
            return None;
        }
        // "Side is aggressing side for trades" (public API notation): the
        // taker is the leg that crossed the spread.
        let side = if ask_fill.crossed { Side::Ask } else { Side::Bid };
        Some(Self {
            coin: ask_fill.coin,
            side,
            px: ask_fill.px,
            sz: ask_fill.sz,
            hash: ask_fill.hash,
            time: ask_fill.time,
            tid: ask_fill.tid,
            users: [buyer, seller],
        })
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct L4BookUpdates {
    pub time: u64,
    pub height: u64,
    // Arc'd so the per-coin groupings built once in the listener are shared
    // across every subscribed connection instead of deep-cloned per send.
    // serde's "rc" feature serializes Arc<Vec<T>> exactly like Vec<T>.
    pub order_statuses: std::sync::Arc<Vec<NodeDataOrderStatus>>,
    pub book_diffs: std::sync::Arc<Vec<NodeDataOrderDiff>>,
}

// RawL4Order is the version of a L4Order we want to serialize and deserialize directly
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct L4Order {
    // when serializing, this field is found outside of this struct
    // when deserializing, we move it into this struct
    pub user: Option<Address>,
    pub coin: String,
    pub side: Side,
    pub limit_px: String,
    pub sz: String,
    pub oid: u64,
    pub timestamp: u64,
    pub trigger_condition: String,
    pub is_trigger: bool,
    pub trigger_px: String,
    #[serde(default)]
    pub children: Vec<serde_json::Value>,
    pub is_position_tpsl: bool,
    pub reduce_only: bool,
    pub order_type: String,
    #[serde(default)]
    pub orig_sz: String,
    pub tif: Option<String>,
    pub cloid: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub(crate) enum OrderDiff {
    #[serde(rename_all = "camelCase")]
    New {
        sz: String,
        // Oid of the resting order at the same price level that this order is placed directly in
        // front of (set for priority ALO orders). Absent means the back of the level.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        insert_before: Option<u64>,
    },
    #[serde(rename_all = "camelCase")]
    Update {
        orig_sz: String,
        new_sz: String,
    },
    Remove,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct Fill {
    pub coin: String,
    pub px: String,
    pub sz: String,
    pub side: Side,
    pub time: u64,
    pub start_position: String,
    pub dir: String,
    pub closed_pnl: String,
    pub hash: String,
    pub oid: u64,
    pub crossed: bool,
    pub fee: String,
    pub tid: u64,
    #[serde(default)]
    pub cloid: Option<String>,
    pub fee_token: String,
    #[serde(default)]
    pub twap_id: Option<u64>,
    pub liquidation: Option<Liquidation>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct Liquidation {
    pub liquidated_user: String,
    pub mark_px: String,
    pub method: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn order_diff_insert_before_serde_test() {
        // Legacy shape without the field
        let legacy = r#"{"new":{"sz":"1.5"}}"#;
        let diff: OrderDiff = serde_json::from_str(legacy).unwrap();
        let OrderDiff::New { sz, insert_before } = &diff else {
            panic!("expected New, got {diff:?}");
        };
        assert_eq!(sz, "1.5");
        assert_eq!(*insert_before, None);
        // None round-trips back to the legacy shape
        assert_eq!(serde_json::to_string(&diff).unwrap(), legacy);

        // New shape with insertBefore
        let with_anchor = r#"{"new":{"sz":"1.5","insertBefore":105338503859}}"#;
        let diff: OrderDiff = serde_json::from_str(with_anchor).unwrap();
        let OrderDiff::New { insert_before, .. } = &diff else {
            panic!("expected New, got {diff:?}");
        };
        assert_eq!(*insert_before, Some(105_338_503_859));
        assert_eq!(serde_json::to_string(&diff).unwrap(), with_anchor);
    }
}
