//! Restart safety for the intent executor (bot-strategy#1099): a durable
//! journal of the orders [`super::maker_first::MakerFirstExecutor`] sends,
//! and a startup reconcile that clears only *its own* leftovers.
//!
//! Protocol:
//! 1. **Before** a send, a `Pending` entry (symbol, side, size, price, kind)
//!    is persisted. If the process dies during the send, the entry remains
//!    even though the venue may have accepted an order whose id we never
//!    learned.
//! 2. After the venue returns an id, the entry is upgraded to `Sent { id }`.
//! 3. When the order is over on positive evidence, the entry is removed.
//!    The whole journal is cleared when a run ends with nothing unresolved.
//!
//! At startup, [`reconcile_leftovers`] walks what is left:
//! - A `Sent { id }` still open is cancelled. It is cleared on positive
//!   evidence (absent from the open orders AND listed cancelled, or no
//!   longer open and filled by its own fills).
//! - A `Pending` entry (id unknown) means any open order on that symbol
//!   could be it. Under the executor's single-writer assumption (it is the
//!   only thing trading this account/symbol), every open order on the
//!   symbol is cancelled and confirmed.
//!   Such a send may also have filled with nothing left on the book, so the
//!   symbol is reported for a position re-read.
//! - Anything that cannot be confirmed stays in the journal and is
//!   reported unresolved; the caller must not trade that symbol yet.
//!
//! Writes are atomic: write a temp file, fsync it, rename it over the
//! journal, then fsync the directory.

use std::collections::{HashMap, HashSet};
use std::io::Write;
use std::path::PathBuf;
use std::sync::Mutex;
use std::time::Duration;

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

use super::maker_first::OrderVenue;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SendKind {
    PostOnly,
    Ioc,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct JournalEntry {
    /// Monotonic per-journal key.
    pub key: u64,
    pub symbol: String,
    /// "buy" | "sell"
    pub side: String,
    pub qty: f64,
    pub price: f64,
    pub kind: SendKind,
    /// `None` until the venue returned an id (a crash or an ambiguous send
    /// leaves it unknown).
    pub order_id: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct JournalState {
    pub next_key: u64,
    pub entries: Vec<JournalEntry>,
}

/// Durable storage for [`JournalState`].
pub trait JournalStore: Send + Sync {
    fn load(&self) -> Result<JournalState>;
    fn save(&self, state: &JournalState) -> Result<()>;
}

/// Atomic JSON file store.
pub struct FileJournal {
    pub path: PathBuf,
}

impl JournalStore for FileJournal {
    fn load(&self) -> Result<JournalState> {
        match std::fs::read(&self.path) {
            Ok(b) => serde_json::from_slice(&b)
                .with_context(|| format!("corrupt journal {}", self.path.display())),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(JournalState::default()),
            Err(e) => Err(e).with_context(|| format!("read {}", self.path.display())),
        }
    }

    fn save(&self, state: &JournalState) -> Result<()> {
        let tmp = self.path.with_extension("tmp");
        {
            let mut f = std::fs::File::create(&tmp)?;
            f.write_all(&serde_json::to_vec_pretty(state)?)?;
            f.sync_all()?;
        }
        std::fs::rename(&tmp, &self.path)?;
        if let Some(dir) = self.path.parent() {
            if let Ok(d) = std::fs::File::open(dir) {
                let _ = d.sync_all();
            }
        }
        Ok(())
    }
}

/// In-memory store (tests, paper).
#[derive(Default)]
pub struct MemJournal(pub Mutex<JournalState>);

impl JournalStore for MemJournal {
    fn load(&self) -> Result<JournalState> {
        Ok(self.0.lock().unwrap_or_else(|p| p.into_inner()).clone())
    }
    fn save(&self, state: &JournalState) -> Result<()> {
        *self.0.lock().unwrap_or_else(|p| p.into_inner()) = state.clone();
        Ok(())
    }
}

/// The journal the executor writes through. Every mutation is persisted
/// before it returns; a failed persist is an error, so the caller never
/// sends an order the journal does not know about.
pub struct ExecJournal<'a> {
    pub store: &'a dyn JournalStore,
}

impl ExecJournal<'_> {
    /// Persist a pending send; returns its key.
    pub fn pending(
        &self,
        symbol: &str,
        side: &str,
        qty: f64,
        price: f64,
        kind: SendKind,
    ) -> Result<u64> {
        let mut st = self.store.load()?;
        let key = st.next_key;
        st.next_key += 1;
        st.entries.push(JournalEntry {
            key,
            symbol: symbol.to_string(),
            side: side.to_string(),
            qty,
            price,
            kind,
            order_id: None,
        });
        self.store.save(&st)?;
        Ok(key)
    }

    /// The venue returned `id` for the pending send `key`.
    pub fn sent(&self, key: u64, id: &str) -> Result<()> {
        let mut st = self.store.load()?;
        if let Some(e) = st.entries.iter_mut().find(|e| e.key == key) {
            e.order_id = Some(id.to_string());
        }
        self.store.save(&st)
    }

    /// The send `key` provably never reached the venue, or its order is
    /// over on positive evidence.
    pub fn done(&self, key: u64) -> Result<()> {
        let mut st = self.store.load()?;
        st.entries.retain(|e| e.key != key);
        self.store.save(&st)
    }

    /// Remove the entry for order `id` (over on positive evidence).
    pub fn done_id(&self, id: &str) -> Result<()> {
        let mut st = self.store.load()?;
        st.entries.retain(|e| e.order_id.as_deref() != Some(id));
        self.store.save(&st)
    }

    /// A run ended with nothing unresolved: nothing it sent can still rest.
    pub fn clear_symbol(&self, symbol: &str) -> Result<()> {
        let mut st = self.store.load()?;
        st.entries.retain(|e| e.symbol != symbol);
        self.store.save(&st)
    }

    pub fn entries(&self) -> Result<Vec<JournalEntry>> {
        Ok(self.store.load()?.entries)
    }
}

/// Outcome of the startup reconcile.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct LeftoverReport {
    /// Orders found open and cancelled with positive evidence.
    pub cancelled: Vec<String>,
    /// Journal entries cleared (their order was already over).
    pub cleared: usize,
    /// Symbols (or ids) that could not be confirmed. The caller must not
    /// trade these symbols until resolved.
    pub unresolved: Vec<String>,
    /// Symbols with a send whose id was never learned. Its order may have
    /// filled (an IOC leaves nothing on the book), so the caller re-reads
    /// the position before trusting its own idea of it.
    pub position_recheck: Vec<String>,
}

/// Cancel `id` and wait for positive evidence that it is over: absent from
/// the open orders AND listed cancelled. The fill check is the caller's.
async fn cancel_confirm(
    venue: &dyn OrderVenue,
    symbol: &str,
    id: &str,
    polls: u32,
    poll: Duration,
) -> bool {
    let _ = venue.cancel(symbol, id).await;
    for _ in 0..=polls {
        let open = venue.open_order_ids(symbol).await;
        let canceled = venue.canceled_order_ids(symbol).await;
        if let (Ok(open), Ok(canceled)) = (open, canceled) {
            if !open.contains(id) && canceled.contains(id) {
                return true;
            }
        }
        tokio::time::sleep(poll).await;
    }
    false
}

/// Whether `id` is no longer open and its own fills cover `qty` (it filled
/// out, so there is no cancel record to expect).
async fn filled_out(venue: &dyn OrderVenue, symbol: &str, id: &str, qty: f64) -> bool {
    let (Ok(open), Ok(fills)) = (
        venue.open_order_ids(symbol).await,
        venue.fills(symbol).await,
    ) else {
        return false;
    };
    let filled: f64 = fills
        .iter()
        .filter(|f| f.order_id == id)
        .map(|f| f.qty)
        .sum();
    !open.contains(id) && filled >= qty - (qty * 1e-9 + 1e-12)
}

/// Clear this executor's leftovers at startup (see the module doc).
pub async fn reconcile_leftovers(
    venue: &dyn OrderVenue,
    journal: &ExecJournal<'_>,
    polls: u32,
    poll: Duration,
) -> Result<LeftoverReport> {
    let mut report = LeftoverReport::default();
    let entries = journal.entries()?;
    let mut by_symbol: HashMap<String, Vec<JournalEntry>> = HashMap::new();
    for e in entries {
        by_symbol.entry(e.symbol.clone()).or_default().push(e);
    }
    for (symbol, entries) in by_symbol {
        let mut symbol_ok = true;
        let has_unknown = entries.iter().any(|e| e.order_id.is_none());
        if has_unknown {
            report.position_recheck.push(symbol.clone());
        }
        // Ids to settle: the known ones, plus (single-writer) every open
        // order on the symbol when some send's id is unknown.
        let mut ids: Vec<(String, f64)> = entries
            .iter()
            .filter_map(|e| e.order_id.clone().map(|id| (id, e.qty)))
            .collect();
        if has_unknown {
            match venue.open_order_ids(&symbol).await {
                Ok(open) => {
                    let known: HashSet<String> = ids.iter().map(|(i, _)| i.clone()).collect();
                    ids.extend(
                        open.into_iter()
                            .filter(|i| !known.contains(i))
                            .map(|i| (i, f64::INFINITY)),
                    );
                }
                Err(_) => symbol_ok = false,
            }
        }
        for (id, qty) in ids {
            let open = match venue.open_order_ids(&symbol).await {
                Ok(o) => o,
                Err(_) => {
                    symbol_ok = false;
                    continue;
                }
            };
            if open.contains(&id) {
                if cancel_confirm(venue, &symbol, &id, polls, poll).await {
                    report.cancelled.push(id);
                } else {
                    report.unresolved.push(id);
                    symbol_ok = false;
                }
                continue;
            }
            // Not open: over only on evidence (cancel record or filled out).
            let canceled = venue.canceled_order_ids(&symbol).await.unwrap_or_default();
            if canceled.contains(&id) || filled_out(venue, &symbol, &id, qty).await {
                report.cleared += 1;
            } else {
                report.unresolved.push(id);
                symbol_ok = false;
            }
        }
        if symbol_ok {
            journal.clear_symbol(&symbol)?;
        } else if !report.unresolved.iter().any(|u| u == &symbol) {
            report.unresolved.push(symbol);
        }
    }
    Ok(report)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::trade::execution::intent::{
        ExecIntent, ExecStyle, MakerFirstParams, PriceSource, Side,
    };
    use crate::trade::execution::maker_first::{MakerFirstExecutor, MakerFirstTiming, VenueFill};
    use async_trait::async_trait;
    use dex_connector::DexError;
    use std::sync::Arc;

    /// A scripted venue. `send_err` makes sends fail with that error;
    /// `cancel_works = false` leaves cancelled orders open.
    #[derive(Default)]
    struct Mock {
        open: Mutex<HashSet<String>>,
        canceled: Mutex<HashSet<String>>,
        fills: Mutex<Vec<VenueFill>>,
        cancel_works: bool,
        send_err: Option<fn() -> DexError>,
        sends: Mutex<u32>,
        /// Journal entries for the symbol seen at each send.
        seen_at_send: Mutex<Vec<usize>>,
        journal: Option<Arc<MemJournal>>,
    }

    impl Mock {
        fn with_open(ids: &[&str]) -> Self {
            Self {
                open: Mutex::new(ids.iter().map(|s| s.to_string()).collect()),
                cancel_works: true,
                ..Default::default()
            }
        }
        fn send(&self) -> Result<String, DexError> {
            *self.sends.lock().unwrap() += 1;
            if let Some(j) = &self.journal {
                self.seen_at_send
                    .lock()
                    .unwrap()
                    .push(j.load().unwrap().entries.len());
            }
            match self.send_err {
                Some(e) => Err(e()),
                None => {
                    let id = format!("o{}", self.sends.lock().unwrap());
                    self.open.lock().unwrap().insert(id.clone());
                    Ok(id)
                }
            }
        }
    }

    #[async_trait]
    impl OrderVenue for Mock {
        async fn touch(&self, _: &str) -> Result<(f64, f64), DexError> {
            Ok((99.9, 100.1))
        }
        async fn position(&self, _: &str) -> Result<f64, DexError> {
            Ok(0.0)
        }
        async fn place_post_only(
            &self,
            _: &str,
            _: Side,
            _: f64,
            _: f64,
            _: bool,
        ) -> Result<String, DexError> {
            self.send()
        }
        async fn cancel(&self, _: &str, id: &str) -> Result<(), DexError> {
            if self.cancel_works && self.open.lock().unwrap().remove(id) {
                self.canceled.lock().unwrap().insert(id.to_string());
            }
            Ok(())
        }
        async fn open_order_ids(&self, _: &str) -> Result<HashSet<String>, DexError> {
            Ok(self.open.lock().unwrap().clone())
        }
        async fn canceled_order_ids(&self, _: &str) -> Result<HashSet<String>, DexError> {
            Ok(self.canceled.lock().unwrap().clone())
        }
        async fn fills(&self, _: &str) -> Result<Vec<VenueFill>, DexError> {
            Ok(self.fills.lock().unwrap().clone())
        }
        async fn place_ioc(
            &self,
            _: &str,
            _: Side,
            _: f64,
            _: f64,
            _: bool,
        ) -> Result<String, DexError> {
            self.send()
        }
    }

    fn entry(key: u64, symbol: &str, id: Option<&str>, qty: f64) -> JournalEntry {
        JournalEntry {
            key,
            symbol: symbol.into(),
            side: "buy".into(),
            qty,
            price: 99.9,
            kind: SendKind::PostOnly,
            order_id: id.map(str::to_string),
        }
    }

    fn store(entries: Vec<JournalEntry>) -> MemJournal {
        MemJournal(Mutex::new(JournalState {
            next_key: 100,
            entries,
        }))
    }

    const POLL: Duration = Duration::from_millis(10);

    #[tokio::test(start_paused = true)]
    async fn a_known_open_leftover_is_cancelled_and_cleared() {
        let v = Mock::with_open(&["a"]);
        let st = store(vec![entry(1, "BTC", Some("a"), 1.0)]);
        let j = ExecJournal { store: &st };
        let r = reconcile_leftovers(&v, &j, 5, POLL).await.unwrap();
        assert_eq!(r.cancelled, vec!["a".to_string()]);
        assert!(r.unresolved.is_empty() && r.position_recheck.is_empty());
        assert!(j.entries().unwrap().is_empty());
        assert!(v.open.lock().unwrap().is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn an_unknown_send_cancels_every_open_order_on_its_symbol_only() {
        // Single-writer: "x" may be the order whose id was never learned.
        let v = Mock::with_open(&["x"]);
        let st = store(vec![entry(1, "BTC", None, 1.0)]);
        let j = ExecJournal { store: &st };
        let r = reconcile_leftovers(&v, &j, 5, POLL).await.unwrap();
        assert_eq!(r.cancelled, vec!["x".to_string()]);
        assert_eq!(r.position_recheck, vec!["BTC".to_string()]);
        assert!(r.unresolved.is_empty());
        assert!(j.entries().unwrap().is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn a_known_order_gone_without_evidence_stays_unresolved() {
        // Not open, no cancel record, no fills: a cache gap is not proof.
        let v = Mock::with_open(&[]);
        let st = store(vec![entry(1, "BTC", Some("a"), 1.0)]);
        let j = ExecJournal { store: &st };
        let r = reconcile_leftovers(&v, &j, 5, POLL).await.unwrap();
        assert!(r.unresolved.contains(&"a".to_string()));
        assert!(r.unresolved.contains(&"BTC".to_string()));
        assert_eq!(j.entries().unwrap().len(), 1, "the entry is kept");
    }

    #[tokio::test(start_paused = true)]
    async fn a_known_order_that_filled_out_is_cleared() {
        let v = Mock::with_open(&[]);
        *v.fills.lock().unwrap() = vec![VenueFill {
            order_id: "a".into(),
            trade_id: "t1".into(),
            qty: 1.0,
            price: 99.9,
            fee_usd: Some(0.0),
            price_source: PriceSource::Venue,
        }];
        let st = store(vec![entry(1, "BTC", Some("a"), 1.0)]);
        let j = ExecJournal { store: &st };
        let r = reconcile_leftovers(&v, &j, 5, POLL).await.unwrap();
        assert_eq!(r.cleared, 1);
        assert!(r.unresolved.is_empty());
        assert!(j.entries().unwrap().is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn an_unconfirmed_cancel_keeps_its_symbol_but_clears_the_others() {
        let mut v = Mock::with_open(&["a", "b"]);
        v.cancel_works = false;
        *v.canceled.lock().unwrap() = ["b".to_string()].into();
        v.open.lock().unwrap().remove("b");
        let st = store(vec![
            entry(1, "BTC", Some("a"), 1.0),
            entry(2, "ETH", Some("b"), 1.0),
        ]);
        let j = ExecJournal { store: &st };
        let r = reconcile_leftovers(&v, &j, 5, POLL).await.unwrap();
        assert_eq!(r.unresolved, vec!["a".to_string(), "BTC".to_string()]);
        let left = j.entries().unwrap();
        assert_eq!(left.len(), 1);
        assert_eq!(left[0].symbol, "BTC");
    }

    fn intent() -> ExecIntent {
        ExecIntent {
            symbol: "BTC".into(),
            side: Side::Buy,
            qty: 1.0,
            reference_price: 100.0,
            reduce_only: false,
            deadline_ms: None,
            max_slip_bps: Some(50.0),
            style: ExecStyle::MakerFirst(MakerFirstParams {
                maker_window_ms: 1_000,
                requote_bps: 5.0,
            }),
        }
    }

    fn timing() -> MakerFirstTiming {
        MakerFirstTiming {
            poll: POLL,
            cancel_confirm_polls: 5,
            settle_polls: 5,
            ioc_fill_polls: 5,
        }
    }

    #[tokio::test(start_paused = true)]
    async fn an_ambiguous_send_is_journaled_before_it_goes_out_and_kept() {
        let st = Arc::new(MemJournal::default());
        let v = Mock {
            send_err: Some(|| DexError::Transient("timeout".into())),
            journal: Some(st.clone()),
            cancel_works: true,
            ..Default::default()
        };
        let j = ExecJournal { store: &*st };
        let ex = MakerFirstExecutor {
            venue: &v,
            timing: timing(),
        };
        let o = ex.execute_journaled(&intent(), &j).await.unwrap();
        assert!(!o.unresolved.is_empty());
        // The entry existed when the send went out, and survives the run
        // with no id (the order may be live).
        assert_eq!(*v.seen_at_send.lock().unwrap(), vec![1]);
        let left = j.entries().unwrap();
        assert_eq!(left.len(), 1);
        assert_eq!(left[0].order_id, None);
        // ...and the next run refuses until it is reconciled.
        assert!(ex.execute_journaled(&intent(), &j).await.is_err());
    }

    #[tokio::test(start_paused = true)]
    async fn a_refused_send_leaves_no_entry() {
        let st = Arc::new(MemJournal::default());
        let v = Mock {
            send_err: Some(|| DexError::ServerResponse("post-only would cross".into())),
            journal: Some(st.clone()),
            cancel_works: true,
            ..Default::default()
        };
        let j = ExecJournal { store: &*st };
        let ex = MakerFirstExecutor {
            venue: &v,
            timing: timing(),
        };
        let o = ex.execute_journaled(&intent(), &j).await.unwrap();
        assert!(o.unresolved.is_empty(), "{:?}", o.unresolved);
        assert!(*v.sends.lock().unwrap() > 0);
        assert!(j.entries().unwrap().is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn a_clean_run_clears_its_entries() {
        // Rests, is cancelled at the window end, then the taker IOC's id is
        // left open by the mock: the run ends unresolved on the IOC, so use
        // a run that never reaches the taker: max_slip None.
        let st = Arc::new(MemJournal::default());
        let v = Mock {
            journal: Some(st.clone()),
            cancel_works: true,
            ..Default::default()
        };
        let j = ExecJournal { store: &*st };
        let ex = MakerFirstExecutor {
            venue: &v,
            timing: timing(),
        };
        let mut i = intent();
        i.max_slip_bps = None;
        let o = ex.execute_journaled(&i, &j).await.unwrap();
        assert!(o.unresolved.is_empty(), "{:?}", o.unresolved);
        assert_eq!(*v.sends.lock().unwrap(), 1);
        assert!(j.entries().unwrap().is_empty());
    }

    struct Broken;
    impl JournalStore for Broken {
        fn load(&self) -> Result<JournalState> {
            Ok(JournalState::default())
        }
        fn save(&self, _: &JournalState) -> Result<()> {
            anyhow::bail!("disk full")
        }
    }

    #[tokio::test(start_paused = true)]
    async fn a_failed_journal_write_sends_nothing() {
        let v = Mock::with_open(&[]);
        let j = ExecJournal { store: &Broken };
        let ex = MakerFirstExecutor {
            venue: &v,
            timing: timing(),
        };
        let o = ex.execute_journaled(&intent(), &j).await.unwrap();
        assert_eq!(*v.sends.lock().unwrap(), 0);
        assert!(o
            .unresolved
            .iter()
            .any(|u| u.starts_with("journal_write_failed")));
    }

    #[test]
    fn the_file_journal_round_trips_atomically() {
        let dir = tempfile::tempdir().unwrap();
        let f = FileJournal {
            path: dir.path().join("exec-journal.json"),
        };
        assert_eq!(f.load().unwrap(), JournalState::default());
        let j = ExecJournal { store: &f };
        let k = j.pending("BTC", "buy", 1.0, 99.9, SendKind::Ioc).unwrap();
        j.sent(k, "o1").unwrap();
        let st = f.load().unwrap();
        assert_eq!(st.entries[0].order_id.as_deref(), Some("o1"));
        assert_eq!(st.next_key, 1);
        assert!(!dir.path().join("exec-journal.tmp").exists());
        std::fs::write(&f.path, b"{trunc").unwrap();
        assert!(
            f.load().is_err(),
            "a corrupt journal is an error, not empty"
        );
        // An unreadable journal (here: a directory) is an error, not empty:
        // only a missing file means "no leftovers".
        let unreadable = FileJournal {
            path: dir.path().to_path_buf(),
        };
        assert!(unreadable.load().is_err());
    }
}
