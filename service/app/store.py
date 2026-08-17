"""DuckDB + parquet. Writes are synchronous (volumes are small) and called from asyncio
via to_thread; two locks separate DB access from runtime dicts."""
import os
import time
import threading
import datetime as dt
import duckdb

DATA_DIR = os.environ.get("VM_DATA", os.path.join(os.path.dirname(__file__), "..", "data"))
SNAP_DIR = os.path.join(DATA_DIR, "snapshots")

SCHEMA = """
CREATE TABLE IF NOT EXISTS venues (
  venue_id TEXT PRIMARY KEY, name TEXT, category TEXT, chain TEXT,
  status TEXT DEFAULT 'live', first_seen TIMESTAMP, notes TEXT
);
CREATE TABLE IF NOT EXISTS instruments (
  instrument_id TEXT PRIMARY KEY, venue_id TEXT, symbol TEXT,
  underlying TEXT, asset_type TEXT, listing_seen_at TIMESTAMP, delisted_at TIMESTAMP
);
CREATE TABLE IF NOT EXISTS ticker_snap (
  ts TIMESTAMP, instrument_id TEXT, mid DOUBLE, vol24h_usd DOUBLE,
  oi_usd DOUBLE, spread_bps DOUBLE, funding_rate DOUBLE
);
CREATE TABLE IF NOT EXISTS events (
  ts TIMESTAMP, venue_id TEXT, kind TEXT, detail TEXT
);
CREATE TABLE IF NOT EXISTS depth_snap (
  ts TIMESTAMP, instrument_id TEXT, spread_bps DOUBLE,
  d10_bid DOUBLE, d10_ask DOUBLE, d25_bid DOUBLE, d25_ask DOUBLE, d50_bid DOUBLE, d50_ask DOUBLE
);
CREATE TABLE IF NOT EXISTS daily_metrics (
  date DATE, instrument_id TEXT, adv_usd DOUBLE, oi_avg DOUBLE,
  spread_avg_bps DOUBLE, depth25_avg_usd DOUBLE, funding_avg DOUBLE, n_snaps INTEGER
);
"""


class Store:
    def __init__(self):
        os.makedirs(DATA_DIR, exist_ok=True)
        os.makedirs(SNAP_DIR, exist_ok=True)
        self.db = duckdb.connect(os.path.join(DATA_DIR, "venues.duckdb"))
        self.db.execute(SCHEMA)
        self.lock = threading.Lock()        # serializes DuckDB access
        self.rt_lock = threading.RLock()    # guards runtime dicts (latest/known/depth) across threads
        # runtime state for the API
        self.latest: dict[str, dict] = {}          # instrument_id -> latest snap
        self.venue_meta: dict[str, dict] = {}      # venue_id -> {name, category, ...}
        self.venue_health: dict[str, dict] = {}    # venue_id -> {last_ok, last_err, err}
        self.known_instruments: dict[str, dict] = {}
        self.miss_count: dict[str, int] = {}
        self.depth_latest: dict[str, dict] = {}    # instrument_id -> latest depth calc
        self.validator = None          # set from api: price check against NYSE close / ranges
        self.quarantined: set[str] = set()
        self.seeded_venues: set[str] = set()
        self._load_instruments()

    def _load_instruments(self):
        for row in self.db.execute(
            "SELECT instrument_id, venue_id, symbol, underlying, asset_type, listing_seen_at FROM instruments WHERE delisted_at IS NULL"
        ).fetchall():
            self.known_instruments[row[0]] = dict(
                venue_id=row[1], symbol=row[2], underlying=row[3], asset_type=row[4], listing_seen_at=str(row[5]))

    def upsert_venue(self, venue_id, name, category, chain="", notes=""):
        with self.rt_lock:
            self.venue_meta[venue_id] = dict(venue_id=venue_id, name=name, category=category, chain=chain, notes=notes)
        with self.lock:
            self.db.execute(
                "INSERT INTO venues (venue_id, name, category, chain, first_seen, notes) VALUES (?,?,?,?,now(),?) "
                "ON CONFLICT (venue_id) DO UPDATE SET name=excluded.name, category=excluded.category",
                [venue_id, name, category, chain, notes])

    def record_snaps(self, venue_id: str, snaps: list[dict]):
        """snaps: dicts(symbol, underlying, asset_type, mid, vol24h_usd, oi_usd, spread_bps, funding_rate)."""
        now = dt.datetime.utcnow()
        new_listed, rows = [], []
        seen_ids = set()
        # ── every runtime-dict mutation happens under rt_lock (read by API/depth threads) ──
        with self.rt_lock:
            if self.validator:
                ok_snaps = []
                for s in snaps:
                    if self.validator(s):
                        ok_snaps.append(s)
                    else:
                        iid = f"{venue_id}:{s['symbol']}"
                        if iid not in self.quarantined:
                            self.quarantined.add(iid)
                            import logging
                            logging.getLogger("vm").warning(
                                "quarantine: %s mid=%s — price does not look like %s", iid, s.get("mid"), s.get("underlying"))
                snaps = ok_snaps
            # a connector's first snap is registry bootstrap — no listing events;
            # real listings are only what appears in later cycles
            seeding = venue_id not in self.seeded_venues
            fresh = []  # newly added to the registry (persisted into instruments)
            for s in snaps:
                iid = f"{venue_id}:{s['symbol']}"
                seen_ids.add(iid)
                if iid not in self.known_instruments:
                    self.known_instruments[iid] = dict(
                        venue_id=venue_id, symbol=s["symbol"], underlying=s["underlying"],
                        asset_type=s["asset_type"], listing_seen_at=str(now))
                    fresh.append(iid)
                    if not seeding:  # listing event — only when this is not the initial bootstrap
                        new_listed.append(iid)
                self.latest[iid] = {**s, "ts": now.isoformat(), "venue_id": venue_id}
                rows.append([now, iid, s.get("mid"), s.get("vol24h_usd"), s.get("oi_usd"),
                             s.get("spread_bps"), s.get("funding_rate")])
            if seen_ids:
                self.seeded_venues.add(venue_id)
            # delisting: an instrument missing from the venue's output 5 cycles in a row.
            # FLAP GUARD: a partial response (<60% of the venue's known instruments seen)
            # is a fetch failure, not a delisting; miss_count is left untouched.
            delisted = []
            known_here = [iid for iid, m in self.known_instruments.items() if m["venue_id"] == venue_id]
            partial = len(known_here) > 5 and len(seen_ids) < 0.6 * len(known_here)
            if not partial:
                for iid in known_here:
                    if iid in seen_ids:
                        self.miss_count[iid] = 0
                    else:
                        self.miss_count[iid] = self.miss_count.get(iid, 0) + 1
                        if self.miss_count[iid] >= 5:
                            delisted.append(iid)
            for iid in delisted:
                self.known_instruments.pop(iid, None)
                self.latest.pop(iid, None)
            snap_meta = {i: dict(self.known_instruments[i]) for i in fresh if i in self.known_instruments}
        with self.lock:
            if snap_meta:
                self.db.executemany(
                    "INSERT OR IGNORE INTO instruments VALUES (?,?,?,?,?,?,NULL)",
                    [[i, venue_id, m["symbol"], m["underlying"], m["asset_type"], now]
                     for i, m in snap_meta.items()])
            if new_listed:
                self.db.executemany("INSERT INTO events VALUES (?,?,?,?)",
                                    [[now, venue_id, "listing", i] for i in new_listed])
            if delisted:
                self.db.executemany("UPDATE instruments SET delisted_at=? WHERE instrument_id=?",
                                    [[now, i] for i in delisted])
                self.db.executemany("INSERT INTO events VALUES (?,?,?,?)",
                                    [[now, venue_id, "delisted", i] for i in delisted])
            if rows:
                self.db.executemany("INSERT INTO ticker_snap VALUES (?,?,?,?,?,?,?)", rows)
        with self.rt_lock:
            self.venue_health[venue_id] = {"last_ok": time.time(), "err": None,
                                           "state": "partial" if partial else "live"}
        return new_listed, delisted

    # ── snapshots under rt_lock for safe reads from API threads ──
    def latest_list(self):
        with self.rt_lock:
            return [dict(v) for v in self.latest.values()]

    def latest_for_venue(self, venue_id):
        with self.rt_lock:
            return [dict(v) for v in self.latest.values() if v.get("venue_id") == venue_id]

    def latest_for_underlying(self, underlying):
        with self.rt_lock:
            return [dict(v) for v in self.latest.values() if v.get("underlying") == underlying]

    def depth_get(self, iid):
        with self.rt_lock:
            d = self.depth_latest.get(iid)
            return dict(d) if d else None

    def known_get(self, iid):
        with self.rt_lock:
            m = self.known_instruments.get(iid)
            return dict(m) if m else {}

    def record_error(self, venue_id: str, err: str, state: str = "api_error"):
        with self.rt_lock:
            h = self.venue_health.setdefault(venue_id, {})
            h["err"] = err[:300]
            h["last_err"] = time.time()
            h["state"] = state  # rate_limited | api_error

    def record_aux_error(self, venue_id: str, component: str, err: str, state: str = "api_error"):
        """Status of an auxiliary component (OI/funding refresh), separate from the venue itself."""
        with self.rt_lock:
            aux = self.venue_health.setdefault(venue_id, {}).setdefault("aux", {})
            aux[component] = {"state": state, "err": err[:300], "ts": time.time()}

    def record_aux_ok(self, venue_id: str, component: str):
        with self.rt_lock:
            aux = self.venue_health.setdefault(venue_id, {}).setdefault("aux", {})
            aux[component] = {"state": "live", "err": None, "ts": time.time()}

    def venues_snapshot(self):
        """Copies of venue_meta + venue_health under the lock — for _compute_venues/health."""
        with self.rt_lock:
            def _cp(h):  # deep-copy aux (the nested dict is read from API threads)
                c = dict(h)
                if "aux" in c:
                    c["aux"] = {k: dict(v) for k, v in c["aux"].items()}
                return c
            return ({v: dict(m) for v, m in self.venue_meta.items()},
                    {v: _cp(h) for v, h in self.venue_health.items()})

    def record_depth(self, iid: str, d: dict):
        now = dt.datetime.utcnow()
        with self.rt_lock:
            self.depth_latest[iid] = {**d, "ts": now.isoformat()}
        with self.lock:
            self.db.execute("INSERT INTO depth_snap VALUES (?,?,?,?,?,?,?,?,?)",
                            [now, iid, d.get("spread_bps"), d.get("d10_bid"), d.get("d10_ask"),
                             d.get("d25_bid"), d.get("d25_ask"), d.get("d50_bid"), d.get("d50_ask")])

    def retention(self, ticker_days: int = 30, depth_days: int = 90):
        """Minute snaps kept 30 days, depth 90, daily/parquet — forever.
        CHECKPOINT after deletes so the DuckDB file does not balloon."""
        with self.lock:
            tb = self.db.execute(
                "SELECT count(*) FROM ticker_snap WHERE ts < now() - INTERVAL (?) DAY", [ticker_days]).fetchone()[0]
            self.db.execute("DELETE FROM ticker_snap WHERE ts < now() - INTERVAL (?) DAY", [ticker_days])
            self.db.execute("DELETE FROM depth_snap WHERE ts < now() - INTERVAL (?) DAY", [depth_days])
            self.db.execute("CHECKPOINT")
        return tb

    def rollup_daily(self):
        """Roll up yesterday's UTC day into daily_metrics (idempotent)."""
        day = (dt.datetime.utcnow() - dt.timedelta(days=1)).date()
        with self.lock:
            self.db.execute("DELETE FROM daily_metrics WHERE date = ?", [day])
            self.db.execute("""
                INSERT INTO daily_metrics
                SELECT CAST(? AS DATE), t.instrument_id,
                       avg(t.vol24h_usd), avg(t.oi_usd), avg(t.spread_bps),
                       (SELECT avg(d.d25_bid + d.d25_ask) FROM depth_snap d
                        WHERE d.instrument_id = t.instrument_id AND CAST(d.ts AS DATE) = CAST(? AS DATE)),
                       avg(t.funding_rate), count(*)
                FROM ticker_snap t
                WHERE CAST(t.ts AS DATE) = CAST(? AS DATE)
                GROUP BY t.instrument_id""", [day, day, day])
        return str(day)

    def dump_parquet_hour(self):
        day = dt.datetime.utcnow().strftime("%Y-%m-%d")
        hour = dt.datetime.utcnow().strftime("%H")
        d = os.path.join(SNAP_DIR, day)
        os.makedirs(d, exist_ok=True)
        path = os.path.join(d, f"ticker_{hour}.parquet")
        with self.lock:
            self.db.execute(
                f"COPY (SELECT * FROM ticker_snap WHERE ts > now() - INTERVAL 70 MINUTE) TO '{path}' (FORMAT PARQUET)")
        return path
