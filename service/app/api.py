"""FastAPI: /api/* + static frontend. Single process: uvicorn app.api:app."""
import asyncio
import logging
import os
import time
import datetime as dt
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse

from .store import Store
from .depth import depth_loop
from .reference import Reference
from .pollers.hyperliquid import Hip3Poller, HypercoreSpotPoller
from .pollers.lighter import LighterPoller
from .pollers.binance_fut import BinanceFutPoller
from .pollers.cex_spot import (BybitSpotPoller, BinanceBstocksPoller, GateSpotPoller,
                               MexcSpotPoller, BitgetSpotPoller, GeminiPoller, BitmartPoller,
                               LbankPoller, XtPoller, BitruePoller)
from .pollers.kraken_ws import KrakenSpotPoller
from .pollers.cex_fut import (OkxFutPoller, BitgetFutPoller, GateFutPoller, KucoinFutPoller,
                              MexcFutPoller, KrakenFutPoller, CryptocomPoller, IntxPoller,
                              BingxFutPoller, HtxFutPoller)
from .pollers.dex_more import AsterPoller, OstiumPoller, GrvtPoller, EdgexPoller
from .pollers.ondo_perps import OndoPerpsPoller
from .pollers.codex_onchain import CodexOnchainPoller
from .pollers.gecko_onchain import GeckoOnchainPoller

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("vm")

app = FastAPI(title="venues-monitor", docs_url=None, redoc_url=None)
app.add_middleware(GZipMiddleware, minimum_size=2048)
store = Store()
ref = Reference(store)
store.validator = ref.validate
POLLERS = [Hip3Poller, HypercoreSpotPoller, LighterPoller, BinanceFutPoller, BybitSpotPoller,
           BinanceBstocksPoller, GateSpotPoller, MexcSpotPoller, BitgetSpotPoller,
           KrakenSpotPoller,
           OkxFutPoller, BitgetFutPoller, GateFutPoller, KucoinFutPoller, MexcFutPoller,
           KrakenFutPoller, CryptocomPoller, IntxPoller, BingxFutPoller, HtxFutPoller,
           AsterPoller, OstiumPoller, GrvtPoller, EdgexPoller, OndoPerpsPoller,
           GeminiPoller, BitmartPoller, LbankPoller, XtPoller, BitruePoller,
           GeckoOnchainPoller]
START = time.time()


TASKS: list[asyncio.Task] = []


async def _supervise(name, factory, restart_delay=15):
    """Keeps a long-lived coroutine alive: on crash → log + restart with delay,
    so a dead background task never goes unnoticed while the service looks 'up'."""
    while True:
        try:
            await factory()
            return  # clean exit (e.g. ref.run without a key) — do not restart
        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.error("task %s crashed: %r — restarting in %ds", name, e, restart_delay)
            await asyncio.sleep(restart_delay)


def _spawn(name, factory):
    t = asyncio.create_task(_supervise(name, factory), name=name)
    TASKS.append(t)


@app.on_event("startup")
async def startup():
    from .pollers.base import DISABLED_VENUES
    active = [P for P in POLLERS if P.venue_id not in DISABLED_VENUES]
    if DISABLED_VENUES:
        log.info("venues disabled (VM_DISABLE_VENUES): %s", ", ".join(sorted(DISABLED_VENUES)))
    for i, P in enumerate(active):
        async def run_poller(P=P, delay=i * 2.0):
            await asyncio.sleep(delay)  # stagger startup so external APIs are not hit all at once
            await P(store).run()
        _spawn(f"poller:{P.__name__}", run_poller)
    _spawn("hourly", _hourly)
    _spawn("depth", lambda: depth_loop(store))
    _spawn("reference", ref.run)
    _spawn("caps", ref._caps_loop)
    _spawn("cache", _cache_loop)
    _spawn("spark", _spark_loop)
    _spawn("daily", _daily)
    log.info("started %d pollers + depth + reference (supervised)", len(active))


@app.on_event("shutdown")
async def shutdown():
    for t in TASKS:
        t.cancel()
    await asyncio.gather(*TASKS, return_exceptions=True)
    log.info("background tasks stopped")


async def _hourly():
    while True:
        await asyncio.sleep(3600 - time.time() % 3600 + 60)  # one minute past the hour
        try:
            p = store.dump_parquet_hour()
            log.info("parquet: %s", p)
        except Exception as e:
            log.warning("parquet dump: %r", e)


async def _daily():
    while True:
        now = dt.datetime.utcnow()
        target = (now + dt.timedelta(days=1)).replace(hour=0, minute=10, second=0, microsecond=0)
        await asyncio.sleep((target - now).total_seconds())
        try:
            log.info("daily rollup: %s", await asyncio.to_thread(store.rollup_daily))
        except Exception as e:
            log.warning("rollup: %r", e)
        try:  # retention: minute snaps 30 days, depth 90; daily/parquet — forever
            purged = await asyncio.to_thread(store.retention)
            log.info("retention: purged %d old ticker snaps", purged)
        except Exception as e:
            log.warning("retention: %r", e)


CACHE = {"venues": None, "assets": None}
SPARK: dict[str, list] = {}


def _compute_spark():
    """Hourly volume-weighted mid per underlying over 7 days — for sparklines."""
    with store.lock:
        rows = store.db.execute("""
            SELECT i.underlying, date_trunc('hour', t.ts) AS h, avg(t.mid) AS m
            FROM ticker_snap t JOIN instruments i ON i.instrument_id = t.instrument_id
            WHERE t.ts > now() - INTERVAL 7 DAY AND t.mid IS NOT NULL
            GROUP BY 1, 2 ORDER BY 1, 2""").fetchall()
    out: dict[str, list] = {}
    for u, h, m in rows:
        out.setdefault(u, []).append(m)
    return {u: v[-56:] for u, v in out.items()}  # ≤56 points


async def _spark_loop():
    while True:
        try:
            SPARK.clear()
            SPARK.update(await asyncio.to_thread(_compute_spark))
        except Exception as e:
            log.warning("spark: %r", e)
        await asyncio.sleep(600)


def _compute_assets():
    meta, _ = store.venues_snapshot()
    vcat = {v: m.get("category") for v, m in meta.items()}
    agg = {}
    for s in store.latest_list():
        u = s.get("underlying")
        a = agg.setdefault(u, dict(underlying=u, asset_type=s.get("asset_type"),
                                   venues=set(), cats={}, vol=0.0, oi=0.0,
                                   fund=[], basis_num=0.0, basis_den=0.0))
        a["venues"].add(s["venue_id"])
        c = vcat.get(s["venue_id"], "")
        a["cats"][c] = a["cats"].get(c, 0) + 1
        a["vol"] += s.get("vol24h_usd") or 0.0
        a["oi"] += s.get("oi_usd") or 0.0
        if s.get("funding_rate") is not None:
            a["fund"].append(s["funding_rate"])
        b = ref.basis_bps(u, s.get("mid")) if s.get("asset_type") in ("single_stock", "etf") else None
        if b is not None and s.get("vol24h_usd"):
            a["basis_num"] += b * s["vol24h_usd"]
            a["basis_den"] += s["vol24h_usd"]
    out = []
    for a in agg.values():
        out.append(dict(
            underlying=a["underlying"], asset_type=a["asset_type"],
            name=ref.names.get(a["underlying"], ""), cap=ref.caps.get(a["underlying"]),
            n_venues=len(a["venues"]), cats=a["cats"],
            vol24h_usd=a["vol"], oi_usd=a["oi"],
            funding_spread=(max(a["fund"]) - min(a["fund"])) if len(a["fund"]) > 1 else None,
            basis_bps=(a["basis_num"] / a["basis_den"]) if a["basis_den"] else None,
            spark=SPARK.get(a["underlying"], [])[-28:]))
    out.sort(key=lambda x: -x["vol24h_usd"])
    return out


async def _cache_loop():
    while True:
        try:
            CACHE["venues"] = await asyncio.to_thread(_compute_venues)
            CACHE["assets"] = await asyncio.to_thread(_compute_assets)
        except Exception as e:
            log.warning("cache: %r", e)
        await asyncio.sleep(20)


def _venue_agg():
    agg = {}
    for s in store.latest_list():
        v = s["venue_id"]
        a = agg.setdefault(v, dict(n=0, vol=0.0, oi=0.0, fresh=None))
        a["n"] += 1
        a["vol"] += s.get("vol24h_usd") or 0.0
        a["oi"] += s.get("oi_usd") or 0.0
        a["fresh"] = max(a["fresh"] or s["ts"], s["ts"])
    return agg


def _venue_status(h, now):
    """Detailed status: live | partial | rate_limited | api_error | stale.
    stale overrides everything (no fresh data for > 15 min)."""
    last_ok = h.get("last_ok")
    age = now - last_ok if last_ok else None
    if age is None or age > 900:
        return "stale", age
    return h.get("state", "live"), age


def _aux_view(h, now):
    """Aux components (OI/funding) carry their own age — a venue can be live
    while an individual field is stale or rate-limited."""
    out = {}
    for comp, a in (h.get("aux") or {}).items():
        age = now - a["ts"] if a.get("ts") else None
        st = a.get("state", "live")
        if age is not None and age > 900 and st == "live":
            st = "stale"
        out[comp] = dict(state=st, age_sec=round(age) if age is not None else None)
    return out


@app.get("/api/health")
def health():
    now = time.time()
    _, hmap = store.venues_snapshot()
    out = {}
    for vid, h in hmap.items():
        status, age = _venue_status(h, now)
        out[vid] = dict(age_sec=round(age) if age is not None else None,
                        status=status, ok=status in ("live", "partial"), err=h.get("err"),
                        aux=_aux_view(h, now))
    return dict(uptime_sec=round(now - START), venues=out,
                n_instruments=len(store.latest_list()))


def _compute_venues():
    agg = _venue_agg()
    meta_map, hmap = store.venues_snapshot()
    out = []
    now = time.time()
    for vid, meta in meta_map.items():
        a = agg.get(vid, {})
        h = hmap.get(vid, {})
        status, age = _venue_status(h, now)
        out.append(dict(**meta, n_instruments=a.get("n", 0),
                        vol24h_usd=a.get("vol", 0.0), oi_usd=a.get("oi", 0.0),
                        status=status, aux=_aux_view(h, now),
                        snap_age_sec=round(age) if age is not None else None))
    out.sort(key=lambda x: -x["vol24h_usd"])
    return out


@app.get("/api/venues")
def venues():
    return CACHE["venues"] if CACHE["venues"] is not None else _compute_venues()


def _enrich(r: dict) -> dict:
    iid = f"{r['venue_id']}:{r['symbol']}"
    d = store.depth_get(iid)
    out = dict(r)
    if d:
        out["depth25_usd"] = (d.get("d25_bid") or 0) + (d.get("d25_ask") or 0)
        if r.get("spread_bps") is None:
            out["spread_bps"] = d.get("spread_bps")
    out["basis_bps"] = ref.basis_bps(r.get("underlying"), r.get("mid")) \
        if r.get("asset_type") in ("single_stock", "etf") else None
    return out


@app.get("/api/venue/{vid}")
def venue(vid: str):
    meta_map, _ = store.venues_snapshot()
    if vid not in meta_map:
        raise HTTPException(404)
    rows = [_enrich(s) for s in store.latest_for_venue(vid)]
    for r in rows:
        r["listing_seen_at"] = store.known_get(f"{vid}:{r['symbol']}").get("listing_seen_at")
    rows.sort(key=lambda x: -(x.get("vol24h_usd") or 0))
    return dict(venue=meta_map[vid], instruments=rows)


@app.get("/api/asset/{ticker}")
def asset(ticker: str):
    t = ticker.upper()
    rows = [_enrich(s) for s in store.latest_for_underlying(t)]
    if not rows:
        raise HTTPException(404)
    rows.sort(key=lambda x: -(x.get("vol24h_usd") or 0))
    f = [r["funding_rate"] for r in rows if r.get("funding_rate") is not None]
    return dict(underlying=t, instruments=rows,
                total_vol24h=sum(s.get("vol24h_usd") or 0 for s in rows),
                total_oi=sum(s.get("oi_usd") or 0 for s in rows),
                nyse_close=ref.closes.get(t), closes_date=ref.closes_date,
                funding_spread_8h=(max(f) - min(f)) if len(f) > 1 else None)


@app.get("/api/coverage")
def coverage():
    return ref.coverage()


@app.get("/api/assets")
def assets():
    return CACHE["assets"] if CACHE["assets"] is not None else _compute_assets()


@app.get("/api/events")
def events(limit: int = Query(200, ge=1, le=2000), kind: str = ""):
    """Listings/delistings. An instrument with >3 status flips in 24h is flagged flap=true."""
    with store.lock:
        flappers = {r[0] for r in store.db.execute(
            "SELECT detail FROM events WHERE ts > now() - INTERVAL 24 HOUR "
            "GROUP BY detail HAVING count(*) > 3").fetchall()}
        q = "SELECT ts, venue_id, kind, detail FROM events"
        params = []
        if kind:
            q += " WHERE kind = ?"; params.append(kind)
        q += " ORDER BY ts DESC LIMIT ?"; params.append(limit)
        rows = store.db.execute(q, params).fetchall()
        agg = store.db.execute(
            "SELECT kind, count(*) FROM events WHERE ts > now() - INTERVAL 24 HOUR "
            "AND detail NOT IN (SELECT detail FROM events WHERE ts > now() - INTERVAL 24 HOUR "
            "GROUP BY detail HAVING count(*) > 3) GROUP BY kind").fetchall()
    return dict(
        last24h={k: n for k, n in agg},
        events=[dict(ts=str(r[0]), venue_id=r[1], kind=r[2],
                     symbol=r[3].split(":", 1)[1] if ":" in r[3] else r[3],
                     underlying=store.known_get(r[3]).get("underlying", ""),
                     flap=r[3] in flappers)
                for r in rows])


@app.get("/api/history/{venue_id}/{symbol}")
def history(venue_id: str, symbol: str, hours: int = Query(24, ge=1, le=720)):
    iid = f"{venue_id}:{symbol}"
    with store.lock:
        rows = store.db.execute(
            "SELECT ts, mid, vol24h_usd, oi_usd, funding_rate FROM ticker_snap "
            "WHERE instrument_id=? AND ts > now() - INTERVAL (?) HOUR ORDER BY ts LIMIT 50000",
            [iid, hours]).fetchall()
    return [dict(ts=str(r[0]), mid=r[1], vol=r[2], oi=r[3], funding=r[4]) for r in rows]


WEB = os.path.join(os.path.dirname(__file__), "web")


@app.middleware("http")
async def cache_headers(request, call_next):
    resp = await call_next(request)
    path = request.url.path
    if "/static/fonts/" in path or path.endswith(".woff2"):
        resp.headers["Cache-Control"] = "public, max-age=2592000, immutable"
    elif "/static/" in path:
        resp.headers["Cache-Control"] = "public, max-age=300"
    return resp

app.mount("/static", StaticFiles(directory=WEB), name="static")


@app.get("/")
def index():
    return FileResponse(os.path.join(WEB, "index.html"))


@app.get("/{page}.html")
def page(page: str):
    p = os.path.join(WEB, f"{page}.html")
    if not os.path.exists(p):
        raise HTTPException(404)
    return FileResponse(p)
