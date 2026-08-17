"""Massive (ex-Polygon): daily NYSE closes (basis) + US ticker catalog (coverage).
Key: MASSIVE_API_KEY in the environment. Without a key the service runs fine,
basis/coverage simply stay empty."""
import asyncio
import datetime as dt
import logging
import os
import aiohttp
from .known import set_us_reference

log = logging.getLogger("vm")
KEY = os.environ.get("MASSIVE_API_KEY", "")
BASE = "https://api.massive.com"

# top US market caps for the "uncovered" list (static; a proper cap ranking needs a paid tier)
TOP_US = ("NVDA MSFT AAPL GOOGL AMZN META AVGO TSLA BRKB LLY WMT JPM V ORCL MA NFLX XOM COST PG JNJ "
          "HD ABBV BAC KO PLTR PM UNH CRM GE CSCO WFC IBM CVX ABT MCD LIN AMD NOW MS ISRG ACN AXP "
          "GS PEP T DIS UBER RTX TMO ADBE QCOM VZ AMGN SPGI CAT TXN BKNG BSX SYK PGR HON BLK ETN "
          "GILD TJX AMAT C MU LOW UNP COP SCHW PFE DE NEE LMT BA ANET FI MDT PANW KKR ADP CB MMC "
          "PLD SBUX BMY UPS SO INTC MO ICE ELV DUK WM CME CI KLAC SHW MCK ABNB").split()


class Reference:
    def __init__(self, store):
        self.store = store
        with store.lock:
            store.db.execute("CREATE TABLE IF NOT EXISTS ref_caps (ticker TEXT PRIMARY KEY, cap DOUBLE, ts TIMESTAMP)")
            self.caps = {t: c for t, c in store.db.execute("SELECT ticker, cap FROM ref_caps").fetchall()}
        self.closes: dict[str, float] = {}
        self.closes_date: str | None = None
        self.us_total: int = 0
        self.us_tickers: set[str] = set()
        self.names: dict[str, str] = {}
        self.updated: str | None = None

    async def _grouped_close(self, sess):
        d = dt.date.today()
        for back in range(1, 6):
            day = (d - dt.timedelta(days=back)).isoformat()
            async with sess.get(f"{BASE}/v2/aggs/grouped/locale/us/market/stocks/{day}",
                                params={"adjusted": "true", "apiKey": KEY}) as r:
                j = await r.json(content_type=None)
            if j.get("resultsCount"):
                self.closes = {x["T"].replace(".", ""): x["c"] for x in j["results"] if x.get("c")}
                self.closes_date = day
                log.info("reference: closes %s — %d tickers", day, len(self.closes))
                return

    async def _tickers(self, sess):
        url = f"{BASE}/v3/reference/tickers"
        params = {"market": "stocks", "active": "true", "limit": "1000", "apiKey": KEY}
        cs, etf, total = set(), set(), 0
        retries = 0
        while True:
            async with sess.get(url, params=params) as r:
                if r.status == 429:  # Massive rate limit — wait and retry the same page
                    retries += 1
                    if retries > 12:
                        log.warning("reference: repeated 429s, catalog incomplete (%d)", total)
                        break
                    await asyncio.sleep(16)
                    continue
                j = await r.json(content_type=None)
            retries = 0
            res = j.get("results", [])
            for x in res:
                t = x["ticker"].replace(".", "")
                if x.get("type") in ("CS", "ADRC"):
                    cs.add(t)
                elif x.get("type") == "ETF":
                    etf.add(t)
                if x.get("name"):
                    self.names[t] = x["name"]
            total += len(res)
            nxt = j.get("next_url")
            if not nxt or total > 15000:
                break
            url, params = nxt, {"apiKey": KEY}
            await asyncio.sleep(13)  # stay at ~5 req/min; the full catalog is ~13 pages
        if cs:
            self.us_tickers, self.us_total = cs | etf, len(cs | etf)
            set_us_reference(cs, etf)  # the classifier now knows EVERY US ticker
            log.info("reference: catalog of %d US tickers (CS %d / ETF %d)", self.us_total, len(cs), len(etf))

    async def run(self):
        if not KEY:
            log.warning("reference: MASSIVE_API_KEY not set — basis/coverage disabled")
            return
        while True:
            try:
                async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=60)) as sess:
                    await self._grouped_close(sess)
                    await self._tickers(sess)
                self.updated = dt.datetime.utcnow().isoformat()
            except Exception as e:
                log.warning("reference: %r", e)
                await asyncio.sleep(120)  # failure → quick retry, not 6 hours
                continue
            await asyncio.sleep(6 * 3600)

    async def _caps_loop(self):
        """Market caps: covered tickers + top uncovered. Persisted in ref_caps."""
        while not self.store.latest_list() or not self.us_tickers:
            await asyncio.sleep(15)
        await asyncio.sleep(30)
        while True:
            covered = {s.get("underlying") for s in self.store.latest_list()
                       if s.get("asset_type") in ("single_stock", "etf")}
            todo = [t for t in sorted(covered & self.us_tickers) + TOP_US if t not in self.caps]
            got = 0
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30)) as sess:
                for t in dict.fromkeys(todo):
                    try:
                        async with sess.get(f"{BASE}/v3/reference/tickers/{t}",
                                            params={"apiKey": KEY}) as r:
                            if r.status == 429:
                                await asyncio.sleep(20)
                                continue
                            j = await r.json(content_type=None)
                        cap = (j.get("results") or {}).get("market_cap")
                        if cap:
                            self.caps[t] = float(cap)
                            got += 1
                            with self.store.lock:
                                self.store.db.execute(
                                    "INSERT OR REPLACE INTO ref_caps VALUES (?,?,?)",
                                    [t, float(cap), dt.datetime.utcnow()])
                    except Exception:
                        pass
                    await asyncio.sleep(0.5)
            if got:
                log.info("caps: +%d, total %d", got, len(self.caps))
            await asyncio.sleep(6 * 3600)

    @staticmethod
    def cap_bucket(cap):
        if not cap:
            return None
        if cap > 200e9: return "mega"
        if cap > 10e9: return "large"
        if cap > 2e9: return "mid"
        if cap > 0.3e9: return "small"
        return "micro"

    # price ranges for tickers without NYSE closes (filters crypto collisions: SPX6900, GOLD-token etc.)
    RANGES = {"SPX": (1000, 50000), "SP500": (1000, 50000), "NDX": (3000, 100000),
              "JP225": (10000, 100000), "KR200": (100, 5000),
              "XAU": (1000, 10000), "GOLD": (1000, 10000), "XAG": (10, 500), "SILVER": (10, 500),
              "XPT": (300, 5000), "PLATINUM": (300, 5000), "PALLADIUM": (300, 5000), "XPD": (300, 5000),
              "CL": (20, 400), "BZ": (20, 400), "BRENTOIL": (20, 400), "WTICRUDE": (20, 400),
              "NATGAS": (0.5, 30), "COPPER": (1, 30), "ALUMINIUM": (0.5, 10),
              "EUR": (0.7, 1.6), "GBP": (0.9, 2.2)}  # JPY unchecked: both quote conventions exist

    def validate(self, snap: dict) -> bool:
        """False = quarantine (crypto ticker collision, or a dead/unsplit pair)."""
        mid, u, at = snap.get("mid"), snap.get("underlying"), snap.get("asset_type")
        if not mid:
            return True
        if at in ("single_stock", "etf"):
            c = self.closes.get(u)
            if c and abs(mid / c - 1) > 0.40:
                return False
            # no close AND the ticker is missing from the US catalog → crypto collision;
            # if the ticker is in the catalog (just didn't trade yesterday) — let it through
            if c is None and len(self.closes) > 1000 and u not in self.us_tickers:
                return False
        elif at in ("index", "commodity", "fx"):
            r = self.RANGES.get(u)
            if r and not (r[0] <= mid <= r[1]):
                return False
        return True

    def basis_bps(self, underlying: str, mid: float | None):
        c = self.closes.get(underlying)
        if c and mid:
            return (mid / c - 1) * 1e4
        return None

    def coverage(self):
        covered, vol = {}, {}
        vmeta, _ = self.store.venues_snapshot()
        vcat = {v: m.get("category") for v, m in vmeta.items()}
        for s in self.store.latest_list():
            u = s.get("underlying")
            if s.get("asset_type") in ("single_stock", "etf") and u in self.us_tickers:
                covered.setdefault(u, {})[s["venue_id"]] = vcat.get(s["venue_id"], "")
                vol[u] = vol.get(u, 0.0) + (s.get("vol24h_usd") or 0.0)
        buckets = {}
        for u in covered:
            b = self.cap_bucket(self.caps.get(u)) or "no cap"
            buckets[b] = buckets.get(b, 0) + 1
        top_venues = sorted({v for vs in covered.values() for v in vs},
                            key=lambda v: -sum(1 for vs in covered.values() if v in vs))[:16]
        matrix = [dict(ticker=u, name=self.names.get(u, ""), cap=self.caps.get(u),
                       vol=vol.get(u, 0.0), venues=covered[u])
                  for u in sorted(covered, key=lambda x: -vol.get(x, 0.0))]
        return dict(
            us_total=self.us_total, covered=len(covered),
            closes_date=self.closes_date, updated=self.updated,
            buckets=buckets, top_venues=top_venues, matrix=matrix[:60],
            top_uncovered=[dict(ticker=t, name=self.names.get(t, ""), cap=self.caps.get(t),
                                bucket=self.cap_bucket(self.caps.get(t)))
                           for t in TOP_US if t not in covered][:30])
