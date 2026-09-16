use crate::{
    listeners::order_book::{L2SnapshotParams, L2Snapshots},
    metrics::RESYNC_PHASE_DURATION,
    order_book::{Coin, Snapshot, multi_book::OrderBooks, types::InnerOrder},
    prelude::*,
    types::{
        inner::InnerLevel,
        node_data::{Batch, NodeDataFill, NodeDataOrderDiff, NodeDataOrderStatus},
    },
};
use log::{error, info};
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use std::{
    collections::{HashMap, HashSet},
    path::PathBuf,
    sync::Arc,
    time::Instant,
};
use tokio::process::Command;

use crate::SnapshotMode;

/// Configuration for snapshot fetching
#[derive(Debug, Clone)]
pub(super) struct SnapshotConfig {
    pub mode: SnapshotMode,
    pub docker_container: String,
    pub hlnode_binary: String,
    pub abci_state_path: Option<PathBuf>,
    pub snapshot_output_path: Option<PathBuf>,
    pub visor_state_path: Option<PathBuf>,
    pub data_dir: PathBuf,
}

pub(super) async fn process_rmp_file(config: &SnapshotConfig) -> Result<PathBuf> {
    info!("Triggering L4 snapshot via hl-node CLI (mode: {:?})...", config.mode);

    // The dump runs on the same host that produces and parses the stream, so
    // its wall-clock duration is the first thing to check when a re-sync
    // correlates with a latency incident.
    let dump_start = Instant::now();
    let output_path = match config.mode {
        SnapshotMode::Docker => {
            // Docker mode: run command inside container
            // data_dir should be the path containing node_*_by_block directories
            // Snapshot goes to parent of data_dir (sibling to "data" folder)
            let parent_dir = config.data_dir.parent().unwrap_or(&config.data_dir);
            let output_path = config.snapshot_output_path.clone().unwrap_or_else(|| parent_dir.join("snapshot.json"));

            let output = Command::new("docker")
                .args(&[
                    "exec",
                    &config.docker_container,
                    "./hl-node",
                    "--chain",
                    "Mainnet",
                    "compute-l4-snapshots",
                    "--include-users",
                    "--include-trigger-orders",
                    "hl/hyperliquid_data/abci_state.rmp",
                    "hl/snapshot.json",
                ])
                .output()
                .await;

            match output {
                Ok(out) => {
                    if !out.status.success() {
                        error!("hl-node compute-l4-snapshots failed: {}", String::from_utf8_lossy(&out.stderr));
                        return Err("hl-node compute-l4-snapshots failed".into());
                    }
                    info!("L4 snapshot computed successfully (docker mode)");
                }
                Err(e) => {
                    error!("Failed to execute docker command: {}", e);
                    return Err(e.into());
                }
            }

            output_path
        }
        SnapshotMode::Direct => {
            // Direct mode: run hl-node directly on host
            let abci_path = config.abci_state_path.clone().unwrap_or_else(|| {
                let parent_dir = config.data_dir.parent().unwrap_or(&config.data_dir);
                parent_dir.join("hyperliquid_data/abci_state.rmp")
            });
            let output_path =
                config.snapshot_output_path.clone().unwrap_or_else(|| PathBuf::from("/tmp/hl_snapshot.json"));

            info!(
                "Running: {} --chain Mainnet compute-l4-snapshots --include-users --include-trigger-orders {} {}",
                &config.hlnode_binary,
                abci_path.display(),
                output_path.display()
            );

            let output = Command::new(&config.hlnode_binary)
                .args(&[
                    "--chain",
                    "Mainnet",
                    "compute-l4-snapshots",
                    "--include-users",
                    "--include-trigger-orders",
                    abci_path.to_str().unwrap_or(""),
                    output_path.to_str().unwrap_or(""),
                ])
                .output()
                .await;

            match output {
                Ok(out) => {
                    if !out.status.success() {
                        error!("hl-node compute-l4-snapshots failed: {}", String::from_utf8_lossy(&out.stderr));
                        error!("stdout: {}", String::from_utf8_lossy(&out.stdout));
                        return Err("hl-node compute-l4-snapshots failed".into());
                    }
                    info!("L4 snapshot computed successfully (direct mode)");
                }
                Err(e) => {
                    error!("Failed to execute hl-node command: {}", e);
                    return Err(e.into());
                }
            }

            output_path
        }
    };

    let dump_elapsed = dump_start.elapsed();
    RESYNC_PHASE_DURATION.with_label_values(&["fetch_dump"]).observe(dump_elapsed.as_secs_f64());
    info!("hl-node compute-l4-snapshots completed in {}ms (mode: {:?})", dump_elapsed.as_millis(), config.mode);

    // Verify file exists
    if output_path.exists() {
        info!("Snapshot file found at: {:?}", output_path);
        // Return tuple (output_path, visor_path) - but for now just output_path
        // The caller needs visor_path too, so we'll store it
        return Ok(output_path);
    }

    // Debug: List directory contents if file not found
    if let Some(parent) = output_path.parent() {
        error!("File not found. Listing directory {:?}:", parent);
        if let Ok(entries) = fs::read_dir(parent) {
            for entry in entries.flatten() {
                error!(" - {:?}", entry.path());
            }
        } else {
            error!("Failed to read directory {:?}", parent);
        }
    }

    Err("Snapshot file not created".into())
}

/// The node's head height from `visor_abci_state.json`, or None if unreadable.
/// This is where the node IS, not what its persisted state is at: the file
/// tracks the head block by block, while `abci_state.rmp` -- what the L4 dump
/// is computed from -- is rewritten every 10 000 blocks. See
/// [`read_abci_state_height`] for the snapshot's own height.
pub(super) fn read_visor_height(visor_path: &std::path::Path) -> Option<u64> {
    let contents = fs::read_to_string(visor_path).ok()?;
    let visor: serde_json::Value = serde_json::from_str(&contents).ok()?;
    visor["height"].as_u64()
}

/// Where the node keeps its persisted state, `abci_state.rmp`: the file the
/// L4 dump is computed from. The same default as the direct snapshot mode;
/// in docker mode it is the host side of the container's `hl/` mount.
pub(super) fn get_abci_state_path(config: &SnapshotConfig) -> PathBuf {
    config.abci_state_path.clone().unwrap_or_else(|| {
        let parent_dir = config.data_dir.parent().unwrap_or(&config.data_dir);
        parent_dir.join("hyperliquid_data/abci_state.rmp")
    })
}

/// The height (and block time) of the node's persisted state, read from the
/// first bytes of `abci_state.rmp` itself: MessagePack, a map whose
/// `exchange.locus.ctx` carries `height` and `time`.
///
/// This is the height the L4 dump is at -- not the one in
/// `visor_abci_state.json`. The node persists its state every 10 000 blocks
/// (some twelve minutes) and the visor file tracks the head, so the two are
/// up to a persistence period apart. Cutting the replay at the visor height
/// threw away every event between the persisted state and the head: orders
/// placed in that window were missing from the book and orders cancelled in
/// it stayed, hundreds of each on every start, and nothing warned.
pub(super) fn read_abci_state_height(path: &std::path::Path) -> Option<(u64, Option<String>)> {
    use std::io::Read;
    let mut head = Vec::new();
    fs::File::open(path).ok()?.take(64 * 1024).read_to_end(&mut head).ok()?;
    let mut mp = MsgPack { buf: &head, pos: 0 };
    mp.enter("exchange")?;
    mp.enter("locus")?;
    mp.enter("ctx")?;
    let entries = mp.map_len()?;
    let (mut height, mut time) = (None, None);
    for _ in 0..entries {
        let Some(key) = mp.str() else { break };
        let read = match key {
            "height" => mp.uint().map(|h| height = Some(h)),
            "time" => mp.str().map(|t| time = Some(t.to_string())),
            _ => mp.skip(),
        };
        if read.is_none() || (height.is_some() && time.is_some()) {
            break;
        }
    }
    height.map(|h| (h, time))
}

/// The height the node's files are read from and replayed above: the
/// persisted state's, from the rmp itself. The visor's head height only when
/// the rmp cannot be read -- with the hole that leaves named in the log.
pub(super) fn persisted_height(config: &SnapshotConfig) -> Option<u64> {
    let rmp = get_abci_state_path(config);
    let head = read_visor_height(&get_visor_path(config));
    match read_abci_state_height(&rmp) {
        Some((height, time)) => {
            info!(
                "Persisted state {} is at height {height} ({}), {} blocks behind the head",
                rmp.display(),
                time.as_deref().unwrap_or("time unknown"),
                head.map_or_else(|| "?".to_string(), |h| h.saturating_sub(height).to_string())
            );
            Some(height)
        }
        None => {
            log::warn!(
                "Cannot read the persisted state height from {}; using the visor head height {head:?} instead: \
                 up to one persistence period of events below it will be missing from the book",
                rmp.display()
            );
            head
        }
    }
}

/// A cursor over a MessagePack prefix: enough of the format to walk map keys
/// and step over the values that are not wanted. Running out of bytes is
/// `None`, like any malformed input.
struct MsgPack<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> MsgPack<'a> {
    fn byte(&mut self) -> Option<u8> {
        let b = *self.buf.get(self.pos)?;
        self.pos += 1;
        Some(b)
    }

    fn bytes(&mut self, n: usize) -> Option<&'a [u8]> {
        let s = self.buf.get(self.pos..self.pos.checked_add(n)?)?;
        self.pos += n;
        Some(s)
    }

    /// A big-endian unsigned integer of `n` bytes.
    fn be(&mut self, n: usize) -> Option<u64> {
        Some(self.bytes(n)?.iter().fold(0u64, |acc, &b| (acc << 8) | u64::from(b)))
    }

    /// A map header: its number of entries.
    fn map_len(&mut self) -> Option<usize> {
        match self.byte()? {
            m @ 0x80..=0x8f => Some(usize::from(m & 0x0f)),
            0xde => self.be(2).map(|n| n as usize),
            0xdf => self.be(4).map(|n| n as usize),
            _ => None,
        }
    }

    fn str(&mut self) -> Option<&'a str> {
        let len = match self.byte()? {
            m @ 0xa0..=0xbf => usize::from(m & 0x1f),
            0xd9 => self.be(1)? as usize,
            0xda => self.be(2)? as usize,
            0xdb => self.be(4)? as usize,
            _ => return None,
        };
        std::str::from_utf8(self.bytes(len)?).ok()
    }

    fn uint(&mut self) -> Option<u64> {
        match self.byte()? {
            m @ 0x00..=0x7f => Some(u64::from(m)),
            0xcc => self.be(1),
            0xcd => self.be(2),
            0xce => self.be(4),
            0xcf => self.be(8),
            _ => None,
        }
    }

    /// Step over one value of any type.
    fn skip(&mut self) -> Option<()> {
        let m = self.byte()?;
        let (payload, items) = match m {
            0x00..=0x7f | 0xe0..=0xff | 0xc0 | 0xc2 | 0xc3 => (0, 0),
            0x80..=0x8f => (0, 2 * usize::from(m & 0x0f)),
            0x90..=0x9f => (0, usize::from(m & 0x0f)),
            0xa0..=0xbf => (usize::from(m & 0x1f), 0),
            0xc4 | 0xd9 => (self.be(1)? as usize, 0),
            0xc5 | 0xda => (self.be(2)? as usize, 0),
            0xc6 | 0xdb => (self.be(4)? as usize, 0),
            0xc7 => (self.be(1)? as usize + 1, 0),
            0xc8 => (self.be(2)? as usize + 1, 0),
            0xc9 => (self.be(4)? as usize + 1, 0),
            0xcc | 0xd0 => (1, 0),
            0xcd | 0xd1 => (2, 0),
            0xca | 0xce | 0xd2 => (4, 0),
            0xcb | 0xcf | 0xd3 => (8, 0),
            0xd4 => (2, 0),
            0xd5 => (3, 0),
            0xd6 => (5, 0),
            0xd7 => (9, 0),
            0xd8 => (17, 0),
            0xdc => (0, self.be(2)? as usize),
            0xdd => (0, self.be(4)? as usize),
            0xde => (0, 2 * self.be(2)? as usize),
            0xdf => (0, 2 * self.be(4)? as usize),
            0xc1 => return None,
        };
        self.bytes(payload)?;
        for _ in 0..items {
            self.skip()?;
        }
        Some(())
    }

    /// Into the value of `key` in the map that starts here, stepping over the
    /// other entries. None when the key is not in it.
    fn enter(&mut self, key: &str) -> Option<()> {
        for _ in 0..self.map_len()? {
            if self.str()? == key {
                return Some(());
            }
            self.skip()?;
        }
        None
    }
}

/// Get the visor state path based on config
/// Get the visor state path based on config
/// data_dir should be the path containing node_*_by_block directories
/// visor_abci_state.json is in parent/hyperliquid_data/
pub(super) fn get_visor_path(config: &SnapshotConfig) -> PathBuf {
    config.visor_state_path.clone().unwrap_or_else(|| {
        let parent_dir = config.data_dir.parent().unwrap_or(&config.data_dir);
        parent_dir.join("hyperliquid_data/visor_abci_state.json")
    })
}

impl L2SnapshotParams {
    pub(crate) const fn new(n_sig_figs: Option<u32>, mantissa: Option<u64>) -> Self {
        Self { n_sig_figs, mantissa }
    }
}

/// Build the requested L2 aggregation variants for a single coin's order book.
/// Only the shapes in `active` are produced (instead of all seven), so a server
/// whose clients use few variants does proportionally less work.
///
/// Every variant is capped at `MAX_LEVELS` per side. Subscription validation
/// rejects `n_levels > MAX_LEVELS`, so deeper levels are pure waste in CPU,
/// memory, and broadcast Arc size (BTC: ~500 -> 100 levels/side).
///
/// Each variant MUST aggregate the full book and only then truncate to the cap
/// (the cap counts aggregated *buckets*, not raw levels). Deriving aggregated
/// variants from the truncated raw base is NOT equivalent: the top-`MAX_LEVELS`
/// raw levels cluster within a few dollars of the mid, so at coarse groupings
/// (e.g. `nSigFigs=2`, $1000-wide buckets on BTC) they all collapse into ~1
/// bucket — while HL's public API serves 20 buckets spanning tens of thousands
/// of dollars for the same params. `OrderBook::to_l2_snapshot` walks the whole
/// side, bucketing as it goes and stopping once the cap in buckets is reached,
/// which matches the public API's aggregate-then-truncate semantics.
fn compute_l2_variants_for_coin<O: InnerOrder>(
    order_book: &crate::order_book::OrderBook<O>,
    active: &HashSet<L2SnapshotParams>,
) -> HashMap<L2SnapshotParams, Snapshot<InnerLevel>> {
    use crate::types::subscription::MAX_LEVELS;
    let mut out = HashMap::new();
    if active.is_empty() {
        return out;
    }
    let cap = Some(MAX_LEVELS);

    let base_params = L2SnapshotParams { n_sig_figs: None, mantissa: None };
    for params in active {
        if *params == base_params {
            continue; // inserted unconditionally below
        }
        let snapshot = order_book.to_l2_snapshot(cap, params.n_sig_figs, params.mantissa);
        out.insert(*params, snapshot);
    }
    // Always expose the raw base so raw (None, None) consumers never miss it.
    out.insert(base_params, order_book.to_l2_snapshot(cap, None, None));
    out
}

/// Incremental rebuild: recomputes variants only for `changed_coins`, reuses
/// the cached `Arc<HashMap>` for every other coin. Returns a fresh `L2Snapshots`
/// holding `Arc::clone`d entries — the outgoing broadcast message and the
/// listener-side cache share the underlying inner maps, so unchanged coins
/// cost a single Arc bump per broadcast instead of a full level-vector clone.
/// Also returned: the set of coins actually recomputed (connections use it to
/// skip subscriptions whose cached payload is still current) and whether the
/// cached coin set changed (a coin appeared or was evicted), which tells the
/// caller to rebuild the shared universe.
///
/// Also evicts cache entries for coins no longer present in `order_books`
/// (e.g. when a coin is delisted and the multi-book removes it). Without
/// this the cache would grow monotonically with the universe size.
/// Cap on present-but-uncached coins backfilled per flush. After a snapshot
/// install (or an active-shape change) clears the cache, the full universe
/// would otherwise be rebuilt in one rayon burst while the listener lock is
/// held; the cap spreads that backfill across a few throttle windows.
/// Uncapped coins are re-detected as uncached and picked up by subsequent
/// flushes, so convergence is automatic. Dirty coins are never capped: a
/// coin that actually changed must not be served stale.
const L2_BACKFILL_COINS_PER_FLUSH: usize = 32;

pub(super) fn compute_l2_snapshots_incremental<O: InnerOrder + Send + Sync>(
    order_books: &OrderBooks<O>,
    changed_coins: &HashSet<Coin>,
    active: &HashSet<L2SnapshotParams>,
    cache: &mut HashMap<Coin, Arc<HashMap<L2SnapshotParams, Snapshot<InnerLevel>>>>,
) -> (L2Snapshots, HashSet<Coin>, bool) {
    /// Below this many dirty coins (the common case is 1-3 per 50ms flush),
    /// rayon's task-dispatch overhead exceeds the rebuild work itself.
    const PAR_COMPUTE_THRESHOLD: usize = 8;

    // Evict stale entries.
    let len_before_evict = cache.len();
    cache.retain(|coin, _| order_books.as_ref().contains_key(coin));
    let mut coin_set_changed = cache.len() != len_before_evict;

    // Determine which coins we actually need to (re)compute: anything in
    // `changed_coins` that the book still contains, plus any present-but-uncached
    // coins (first-time broadcast after a snapshot reset).
    let mut to_compute: Vec<Coin> =
        changed_coins.iter().filter(|c| order_books.as_ref().contains_key(*c)).cloned().collect();
    let mut backfilled = 0usize;
    for coin in order_books.as_ref().keys() {
        if !cache.contains_key(coin) && !changed_coins.contains(coin) {
            if backfilled >= L2_BACKFILL_COINS_PER_FLUSH {
                break;
            }
            backfilled += 1;
            to_compute.push(coin.clone());
        }
    }
    coin_set_changed |= to_compute.iter().any(|coin| !cache.contains_key(coin));

    // Recompute the coins we need, building only the subscribed shapes; fan
    // out to rayon only for genuinely large rebuilds (post-snapshot recompute).
    let build = |coin: Coin| {
        order_books.as_ref().get(&coin).map(|book| (coin, Arc::new(compute_l2_variants_for_coin(book, active))))
    };
    let updates: Vec<(Coin, Arc<HashMap<L2SnapshotParams, Snapshot<InnerLevel>>>)> =
        if to_compute.len() < PAR_COMPUTE_THRESHOLD {
            to_compute.into_iter().filter_map(build).collect()
        } else {
            to_compute.into_par_iter().filter_map(build).collect()
        };
    let mut recomputed = HashSet::with_capacity(updates.len());
    for (coin, arc) in updates {
        recomputed.insert(coin.clone());
        cache.insert(coin, arc);
    }
    // A dirty coin whose book is GONE (last order cancelled -> multi-book
    // evicted it) still counts as recomputed: connections must be told the
    // book is now empty, or they keep the last snapshot forever.
    for coin in changed_coins {
        if !order_books.as_ref().contains_key(coin) {
            recomputed.insert(coin.clone());
        }
    }

    // Build the outgoing L2Snapshots from the cache. Each entry is an Arc::clone -
    // O(coins) cheap atomic bumps, no level data is copied.
    let snapshot: HashMap<Coin, Arc<HashMap<L2SnapshotParams, Snapshot<InnerLevel>>>> =
        cache.iter().map(|(c, arc)| (c.clone(), Arc::clone(arc))).collect();
    (L2Snapshots(snapshot), recomputed, coin_set_changed)
}

#[derive(Clone)]
pub(super) enum EventBatch {
    Orders(Batch<NodeDataOrderStatus>),
    BookDiffs(Batch<NodeDataOrderDiff>),
    Fills(Batch<NodeDataFill>),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        order_book::{Px, Side, Sz, multi_book::Snapshots, types::InnerOrder},
        types::inner::InnerL4Order,
    };
    use alloy::primitives::Address;
    use std::{collections::HashSet, path::PathBuf};

    fn order(oid: u64, coin: &str, side: Side, sz: &str, px: &str) -> InnerL4Order {
        InnerL4Order {
            user: Address::new([0; 20]),
            coin: Coin::new(coin),
            side,
            limit_px: Px::parse_from_str(px).unwrap(),
            sz: Sz::parse_from_str(sz).unwrap(),
            oid,
            timestamp: 0,
            trigger_condition: String::new(),
            is_trigger: false,
            trigger_px: String::new(),
            is_position_tpsl: false,
            reduce_only: false,
            order_type: String::new(),
            tif: None,
            cloid: None,
        }
    }

    /// The full set of supported L2 variant shapes (what the listener built before
    /// subscription-aware computation). Used by tests to exercise all variants.
    fn all_params() -> HashSet<L2SnapshotParams> {
        [
            L2SnapshotParams::new(None, None),
            L2SnapshotParams::new(Some(5), None),
            L2SnapshotParams::new(Some(5), Some(2)),
            L2SnapshotParams::new(Some(5), Some(5)),
            L2SnapshotParams::new(Some(4), None),
            L2SnapshotParams::new(Some(3), None),
            L2SnapshotParams::new(Some(2), None),
        ]
        .into_iter()
        .collect()
    }

    #[test]
    fn test_read_visor_height() {
        let dir = std::env::temp_dir().join(format!("obs_visor_test_{}", std::process::id()));
        fs::create_dir_all(&dir).unwrap();
        let path = dir.join("visor_abci_state.json");

        fs::write(&path, r#"{"height": 12345, "other": "x"}"#).unwrap();
        assert_eq!(read_visor_height(&path), Some(12345));

        fs::write(&path, "not json").unwrap();
        assert_eq!(read_visor_height(&path), None);

        assert_eq!(read_visor_height(&dir.join("missing.json")), None);
        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn test_l2_variants_are_capped_to_max_levels() {
        use crate::types::subscription::MAX_LEVELS;
        let mut books: OrderBooks<InnerL4Order> = OrderBooks::from_snapshots(Snapshots::new(HashMap::new()), true);
        // Add more than MAX_LEVELS distinct price levels on each side.
        for i in 0..(MAX_LEVELS + 50) {
            let bid_px = format!("{}", 1000 + i);
            let ask_px = format!("{}", 100_000 + i);
            books.add_order(order(i as u64, "BTC", Side::Bid, "1", &bid_px));
            books.add_order(order((1_000_000 + i) as u64, "BTC", Side::Ask, "1", &ask_px));
        }

        let book = books.as_ref().get(&Coin::new("BTC")).unwrap();
        let variants = compute_l2_variants_for_coin(book, &all_params());
        let base = variants.get(&L2SnapshotParams::new(None, None)).unwrap();
        let [bids, asks] = base.as_ref();
        assert!(bids.len() <= MAX_LEVELS, "base bids capped: {} <= {}", bids.len(), MAX_LEVELS);
        assert!(asks.len() <= MAX_LEVELS, "base asks capped: {} <= {}", asks.len(), MAX_LEVELS);
        // Every aggregated variant is also bounded by the cap.
        for snap in variants.values() {
            let [b, a] = snap.as_ref();
            assert!(b.len() <= MAX_LEVELS && a.len() <= MAX_LEVELS, "an aggregated variant exceeds the cap");
        }
    }

    #[test]
    fn test_incremental_reuses_arc_for_unchanged_coins() {
        let mut books: OrderBooks<InnerL4Order> = OrderBooks::from_snapshots(Snapshots::new(HashMap::new()), true);
        books.add_order(order(1, "BTC", Side::Bid, "1", "50000"));
        books.add_order(order(2, "ETH", Side::Bid, "1", "3000"));

        let mut cache = HashMap::new();
        // First call seeds the cache for both coins.
        let _ = compute_l2_snapshots_incremental(&books, &HashSet::new(), &all_params(), &mut cache);
        assert_eq!(cache.len(), 2);
        let btc_first = Arc::clone(cache.get(&Coin::new("BTC")).unwrap());
        let eth_first = Arc::clone(cache.get(&Coin::new("ETH")).unwrap());

        // Mark BTC changed; ETH unchanged. ETH's Arc must be the same object.
        let changed: HashSet<Coin> = std::iter::once(Coin::new("BTC")).collect();
        books.add_order(order(3, "BTC", Side::Bid, "2", "50001"));
        let _ = compute_l2_snapshots_incremental(&books, &changed, &all_params(), &mut cache);

        let btc_after = cache.get(&Coin::new("BTC")).unwrap();
        let eth_after = cache.get(&Coin::new("ETH")).unwrap();
        assert!(!Arc::ptr_eq(&btc_first, btc_after), "BTC should have been recomputed");
        assert!(Arc::ptr_eq(&eth_first, eth_after), "ETH must be Arc-shared (not recomputed)");
    }

    #[test]
    fn test_incremental_rebuilds_full_accumulated_set_not_just_triggering_coin() {
        // Regression for the L2 conflation bug: when a coin changes during a
        // throttle-suppressed window, the broadcast must rebuild the FULL accumulated
        // set of dirty coins, not just the coin in the triggering event. Passing the
        // accumulated set {A, B} must recompute both - A must NOT be served stale.
        let mut books: OrderBooks<InnerL4Order> = OrderBooks::from_snapshots(Snapshots::new(HashMap::new()), true);
        books.add_order(order(1, "A", Side::Bid, "1", "100"));
        books.add_order(order(2, "B", Side::Bid, "1", "200"));

        let mut cache = HashMap::new();
        // Seed both coins.
        let _ = compute_l2_snapshots_incremental(&books, &HashSet::new(), &all_params(), &mut cache);
        let a_seed = Arc::clone(cache.get(&Coin::new("A")).unwrap());
        let b_seed = Arc::clone(cache.get(&Coin::new("B")).unwrap());

        // A changes during a suppressed window; B changes in the triggering event.
        // The conflation buffer accumulates both.
        books.add_order(order(3, "A", Side::Bid, "5", "101"));
        books.add_order(order(4, "B", Side::Bid, "5", "201"));

        let dirty: HashSet<Coin> = ["A", "B"].iter().map(|c| Coin::new(c)).collect();
        let _ = compute_l2_snapshots_incremental(&books, &dirty, &all_params(), &mut cache);

        assert!(
            !Arc::ptr_eq(&a_seed, cache.get(&Coin::new("A")).unwrap()),
            "A changed during the suppressed window and must be rebuilt, not served stale"
        );
        assert!(
            !Arc::ptr_eq(&b_seed, cache.get(&Coin::new("B")).unwrap()),
            "B changed in the triggering event and must be rebuilt"
        );
    }

    #[test]
    fn test_incremental_serves_stale_when_changed_coin_omitted() {
        // Documents the pre-fix behavior the conflation buffer eliminates: if a
        // changed coin (A) is omitted from the passed set (as happened when A's change
        // landed in a throttle-suppressed event and was discarded), A is served from
        // its stale cached Arc even though the book changed.
        let mut books: OrderBooks<InnerL4Order> = OrderBooks::from_snapshots(Snapshots::new(HashMap::new()), true);
        books.add_order(order(1, "A", Side::Bid, "1", "100"));
        books.add_order(order(2, "B", Side::Bid, "1", "200"));

        let mut cache = HashMap::new();
        let _ = compute_l2_snapshots_incremental(&books, &HashSet::new(), &all_params(), &mut cache);
        let a_seed = Arc::clone(cache.get(&Coin::new("A")).unwrap());

        // A's book changes, but only B is passed as changed (A's change was dropped).
        books.add_order(order(3, "A", Side::Bid, "5", "101"));
        let only_b: HashSet<Coin> = std::iter::once(Coin::new("B")).collect();
        let _ = compute_l2_snapshots_incremental(&books, &only_b, &all_params(), &mut cache);

        assert!(
            Arc::ptr_eq(&a_seed, cache.get(&Coin::new("A")).unwrap()),
            "demonstrates the stale-serve bug: A's change is invisible when omitted from the changed set"
        );
    }

    #[test]
    fn test_incremental_reports_recomputed_and_coin_set_changes() {
        let mut books: OrderBooks<InnerL4Order> = OrderBooks::from_snapshots(Snapshots::new(HashMap::new()), true);
        books.add_order(order(1, "BTC", Side::Bid, "1", "50000"));
        books.add_order(order(2, "ETH", Side::Bid, "1", "3000"));

        let mut cache = HashMap::new();
        let (_, recomputed, changed) = compute_l2_snapshots_incremental(&books, &HashSet::new(), &all_params(), &mut cache);
        assert!(changed, "first build introduces coins to the cache");
        assert!(recomputed.contains("BTC") && recomputed.contains("ETH"));

        // Only BTC dirty: recomputed is exactly {BTC}, coin set unchanged.
        let dirty: HashSet<Coin> = std::iter::once(Coin::new("BTC")).collect();
        let (_, recomputed, changed) = compute_l2_snapshots_incremental(&books, &dirty, &all_params(), &mut cache);
        assert!(!changed, "no coin appeared or disappeared");
        assert_eq!(recomputed.len(), 1);
        assert!(recomputed.contains("BTC"));

        // Evicting a coin flags a coin-set change (universe must be rebuilt).
        books.cancel_order(crate::order_book::Oid::new(1), Coin::new("BTC"));
        let (_, recomputed, changed) = compute_l2_snapshots_incremental(&books, &HashSet::new(), &all_params(), &mut cache);
        assert!(changed, "eviction must flag a universe change");
        assert!(recomputed.is_empty());
    }

    #[test]
    fn test_backfill_is_capped_per_flush_and_converges() {
        // Post-install: empty cache, no dirty coins. Each flush must backfill
        // at most L2_BACKFILL_COINS_PER_FLUSH coins (bounding the under-lock
        // rayon burst) and repeated flushes must converge to the full universe.
        let n_coins = 3 * L2_BACKFILL_COINS_PER_FLUSH;
        let mut books: OrderBooks<InnerL4Order> = OrderBooks::from_snapshots(Snapshots::new(HashMap::new()), true);
        for i in 0..n_coins {
            books.add_order(order(i as u64, &format!("C{i}"), Side::Bid, "1", "100"));
        }

        let mut cache = HashMap::new();
        let (_, recomputed, changed) = compute_l2_snapshots_incremental(&books, &HashSet::new(), &all_params(), &mut cache);
        assert!(changed, "backfill introduces coins to the cache");
        assert_eq!(recomputed.len(), L2_BACKFILL_COINS_PER_FLUSH, "backfill must be capped per flush");
        assert_eq!(cache.len(), L2_BACKFILL_COINS_PER_FLUSH);

        let mut flushes = 1;
        while cache.len() < n_coins {
            let (_, recomputed, _) = compute_l2_snapshots_incremental(&books, &HashSet::new(), &all_params(), &mut cache);
            assert!(recomputed.len() <= L2_BACKFILL_COINS_PER_FLUSH);
            assert!(!recomputed.is_empty(), "the ramp must make progress every flush");
            flushes += 1;
        }
        assert_eq!(flushes, 3, "the ramp must converge in universe/cap flushes");
        // Converged: nothing left to backfill.
        let (_, recomputed, _) = compute_l2_snapshots_incremental(&books, &HashSet::new(), &all_params(), &mut cache);
        assert!(recomputed.is_empty());
    }

    #[test]
    fn test_dirty_coins_are_never_capped() {
        // Every dirty coin must be rebuilt in the flush that drains it, even if
        // there are more dirty coins than the backfill cap - the cap only
        // applies to present-but-uncached (backfill) coins.
        let n_coins = 2 * L2_BACKFILL_COINS_PER_FLUSH;
        let mut books: OrderBooks<InnerL4Order> = OrderBooks::from_snapshots(Snapshots::new(HashMap::new()), true);
        let mut dirty = HashSet::new();
        for i in 0..n_coins {
            books.add_order(order(i as u64, &format!("C{i}"), Side::Bid, "1", "100"));
            dirty.insert(Coin::new(&format!("C{i}")));
        }

        let mut cache = HashMap::new();
        let (_, recomputed, _) = compute_l2_snapshots_incremental(&books, &dirty, &all_params(), &mut cache);
        assert_eq!(recomputed.len(), n_coins, "dirty coins must all be rebuilt in one flush");
    }

    #[test]
    fn test_dirty_evicted_coin_is_reported_recomputed() {
        // A coin whose last order was cancelled is dirty AND gone from the
        // book. It must still appear in the recomputed set so connections are
        // told the book is now empty instead of serving the stale snapshot.
        let mut books: OrderBooks<InnerL4Order> = OrderBooks::from_snapshots(Snapshots::new(HashMap::new()), true);
        books.add_order(order(1, "BTC", Side::Bid, "1", "50000"));
        let mut cache = HashMap::new();
        let _ = compute_l2_snapshots_incremental(&books, &HashSet::new(), &all_params(), &mut cache);

        books.cancel_order(crate::order_book::Oid::new(1), Coin::new("BTC")); // book evicted
        let dirty: HashSet<Coin> = std::iter::once(Coin::new("BTC")).collect();
        let (snapshots, recomputed, _) = compute_l2_snapshots_incremental(&books, &dirty, &all_params(), &mut cache);
        assert!(recomputed.contains("BTC"), "evicted dirty coin must be reported so subscribers get an empty book");
        assert!(!snapshots.as_ref().contains_key(&Coin::new("BTC")), "the snapshot map no longer carries the coin");
    }

    #[test]
    fn test_incremental_evicts_removed_coins() {
        let mut books: OrderBooks<InnerL4Order> = OrderBooks::from_snapshots(Snapshots::new(HashMap::new()), true);
        books.add_order(order(1, "BTC", Side::Bid, "1", "50000"));
        books.add_order(order(2, "ETH", Side::Bid, "1", "3000"));

        let mut cache = HashMap::new();
        let _ = compute_l2_snapshots_incremental(&books, &HashSet::new(), &all_params(), &mut cache);
        assert!(cache.contains_key(&Coin::new("BTC")));

        // Cancel BTC's only order — the multi-book evicts the empty book, which
        // means our cache must also drop the entry on the next incremental call.
        books.cancel_order(crate::order_book::Oid::new(1), Coin::new("BTC"));
        let _ = compute_l2_snapshots_incremental(&books, &HashSet::new(), &all_params(), &mut cache);
        assert!(!cache.contains_key(&Coin::new("BTC")), "BTC entry should have been evicted from the cache");
        assert!(cache.contains_key(&Coin::new("ETH")));
    }

    #[test]
    fn test_compute_only_builds_requested_variants() {
        let mut books: OrderBooks<InnerL4Order> = OrderBooks::from_snapshots(Snapshots::new(HashMap::new()), true);
        books.add_order(order(1, "BTC", Side::Bid, "1", "50000"));
        let book = books.as_ref().get(&Coin::new("BTC")).unwrap();

        let mut active = HashSet::new();
        active.insert(L2SnapshotParams::new(Some(5), None));
        let variants = compute_l2_variants_for_coin(book, &active);

        // The requested shape plus the always-present raw base; nothing else.
        assert!(variants.contains_key(&L2SnapshotParams::new(Some(5), None)), "requested variant built");
        assert!(variants.contains_key(&L2SnapshotParams::new(None, None)), "raw base always present");
        assert!(!variants.contains_key(&L2SnapshotParams::new(Some(2), None)), "unrequested variant not built");
        assert!(!variants.contains_key(&L2SnapshotParams::new(Some(5), Some(5))), "unrequested variant not built");
        assert_eq!(variants.len(), 2);
    }

    #[test]
    fn test_compute_empty_active_builds_nothing() {
        let mut books: OrderBooks<InnerL4Order> = OrderBooks::from_snapshots(Snapshots::new(HashMap::new()), true);
        books.add_order(order(1, "BTC", Side::Bid, "1", "50000"));
        let book = books.as_ref().get(&Coin::new("BTC")).unwrap();

        let variants = compute_l2_variants_for_coin(book, &HashSet::new());
        assert!(variants.is_empty(), "empty active set computes no variants");
    }

    #[test]
    fn test_coarse_variant_aggregates_full_depth_not_truncated_base() {
        // Regression: aggregated variants used to be derived from the raw base
        // AFTER it was truncated to MAX_LEVELS raw levels. The top raw levels
        // cluster near the mid, so coarse groupings (nSigFigs=2 -> $1000-wide
        // buckets here) collapsed into 1-2 buckets and all deep far-from-mid
        // liquidity vanished. Aggregation must run over the FULL book and
        // truncate by aggregated buckets, like HL's public API.
        use crate::types::subscription::MAX_LEVELS;
        let mut books: OrderBooks<InnerL4Order> = OrderBooks::from_snapshots(Snapshots::new(HashMap::new()), true);
        let mut oid = 0u64;
        // More than MAX_LEVELS raw bid levels packed one tick apart below the mid...
        let top_bid = 64_931 + MAX_LEVELS + 20 - 1;
        for i in 0..(MAX_LEVELS + 20) {
            books.add_order(order(oid, "BTC", Side::Bid, "1", &format!("{}", 64_931 + i)));
            oid += 1;
        }
        // ...plus deep liquidity far below the mid that the truncated base never saw.
        for deep_px in [50_000, 40_000, 30_000, 20_000] {
            books.add_order(order(oid, "BTC", Side::Bid, "1", &format!("{deep_px}")));
            oid += 1;
        }
        // The ask must not cross the top bid: add_order matches, and a fill
        // would take a unit of bid liquidity out before aggregation runs.
        books.add_order(order(oid, "BTC", Side::Ask, "1", &format!("{}", top_bid + 50)));

        let mut active = HashSet::new();
        active.insert(L2SnapshotParams::new(Some(2), None));
        let variants = compute_l2_variants_for_coin(books.as_ref().get(&Coin::new("BTC")).unwrap(), &active);
        let [bids, _] = variants.get(&L2SnapshotParams::new(Some(2), None)).unwrap().as_ref();

        // 64931..=top_bid buckets to {65000, 64000} (the packed range stays
        // below 66000 for any MAX_LEVELS up to 1049); the deep levels add 4 more.
        assert_eq!(bids.len(), 6, "coarse buckets must cover the full book depth, got {bids:?}");
        let total_sz: u64 = bids.iter().map(|l| l.sz.value()).sum();
        let expected_sz = Sz::parse_from_str(&format!("{}", MAX_LEVELS + 24)).unwrap().value();
        assert_eq!(total_sz, expected_sz, "no liquidity may be dropped by aggregation");
    }

    #[test]
    fn test_requested_variant_matches_full_compute() {
        // A single-shape build must equal what the all-variants build produces
        // for the same shape (subscription-aware computation is value-correct).
        let mut books: OrderBooks<InnerL4Order> = OrderBooks::from_snapshots(Snapshots::new(HashMap::new()), true);
        for i in 0..20 {
            books.add_order(order(i, "BTC", Side::Bid, "1", &format!("{}", 50000 - i)));
            books.add_order(order(1000 + i, "BTC", Side::Ask, "1", &format!("{}", 50100 + i)));
        }
        let book = books.as_ref().get(&Coin::new("BTC")).unwrap();

        let full = compute_l2_variants_for_coin(book, &all_params());
        for shape in all_params() {
            let mut one = HashSet::new();
            one.insert(shape);
            let single = compute_l2_variants_for_coin(book, &one);
            // InnerLevel has no PartialEq; compare via Debug rendering of the levels.
            assert_eq!(
                format!("{:?}", single.get(&shape).map(Snapshot::as_ref)),
                format!("{:?}", full.get(&shape).map(Snapshot::as_ref)),
                "variant must match the all-variants build"
            );
        }
    }

    // ==================== Persisted state height ====================

    /// The first 146 bytes of a mainnet abci_state.rmp, as found on disk:
    /// {"exchange": {"locus": {"ctx": {"hardfork_height", "hardfork": {..},
    /// "height": 1149510000, "tx_index", "round", "time": "2026-09-16T...", ...
    fn abci_state_head() -> Vec<u8> {
        let hex = "81a86578636861 6e6765de003ba56c6f637573de0011a3637478 8daf68617264666f726b5f686569676874ce 444cf858\
                   a8686172 64666f726b82a776657273696f6e68a5726f756e64ce563d9c40 a6686569676874ce44842170 a874785f696e646578 0a\
                   a5726f756e64ce5678470e a474696d65bd323032362d30392d31365430383a33363a34322e313030333338303634";
        let hex: String = hex.chars().filter(|c| !c.is_whitespace()).collect();
        (0..hex.len()).step_by(2).map(|i| u8::from_str_radix(&hex[i..i + 2], 16).unwrap()).collect()
    }

    fn write_temp(name: &str, bytes: &[u8]) -> PathBuf {
        let path = std::env::temp_dir().join(format!("obs_{}_{}", std::process::id(), name));
        std::fs::write(&path, bytes).unwrap();
        path
    }

    #[test]
    fn persisted_height_is_read_from_the_rmp_header() {
        let path = write_temp("abci_state.rmp", &abci_state_head());
        let (height, time) = read_abci_state_height(&path).expect("header parses");
        assert_eq!(height, 1_149_510_000);
        assert_eq!(time.as_deref(), Some("2026-09-16T08:36:42.100338064"));
    }

    #[test]
    fn persisted_height_survives_a_header_cut_after_the_height() {
        // The height comes before the time; a prefix that ends between them
        // still yields the height, without the time.
        let head = abci_state_head();
        let cut = head.len() - 40;
        let path = write_temp("abci_state_cut.rmp", &head[..cut]);
        let (height, time) = read_abci_state_height(&path).expect("height is before the cut");
        assert_eq!(height, 1_149_510_000);
        assert_eq!(time, None);
    }

    #[test]
    fn persisted_height_is_none_without_the_expected_keys() {
        // A map with other keys, and a non-map: neither has exchange.locus.ctx.
        let path = write_temp("abci_state_other.rmp", &[0x81, 0xa3, b'f', b'o', b'o', 0x01]);
        assert_eq!(read_abci_state_height(&path), None);
        let path = write_temp("abci_state_num.rmp", &[0xce, 0x44, 0x84, 0x21, 0x70]);
        assert_eq!(read_abci_state_height(&path), None);
        assert_eq!(read_abci_state_height(std::path::Path::new("/nonexistent/abci_state.rmp")), None);
    }

    #[test]
    fn msgpack_skip_steps_over_every_shape() {
        // A map of one wanted key after values of every kind: nil, bool,
        // fixint, u16, i32, f64, fixstr, str8, bin8, fixext1, fixarray of two,
        // fixmap of one. Skipping each lands exactly on the next key.
        let mut buf = vec![0x8d];
        let mut kv = |key: &str, value: &[u8]| {
            buf.push(0xa0 | key.len() as u8);
            buf.extend_from_slice(key.as_bytes());
            buf.extend_from_slice(value);
        };
        kv("a", &[0xc0]);
        kv("b", &[0xc3]);
        kv("c", &[0x2a]);
        kv("d", &[0xcd, 0x12, 0x34]);
        kv("e", &[0xd2, 0xff, 0xff, 0xff, 0xfe]);
        kv("f", &[0xcb, 0, 0, 0, 0, 0, 0, 0, 0]);
        kv("g", &[0xa2, b'h', b'i']);
        kv("h", &[0xd9, 0x03, b'x', b'y', b'z']);
        kv("i", &[0xc4, 0x02, 0xde, 0xad]);
        kv("j", &[0xd4, 0x01, 0x07]);
        kv("k", &[0x92, 0x01, 0xa1, b'q']);
        kv("l", &[0x81, 0xa1, b'z', 0x05]);
        kv("height", &[0xce, 0x44, 0x84, 0x21, 0x70]);
        let mut mp = MsgPack { buf: &buf, pos: 0 };
        mp.enter("height").expect("every value before it is skipped cleanly");
        assert_eq!(mp.uint(), Some(1_149_510_000));
    }
}
