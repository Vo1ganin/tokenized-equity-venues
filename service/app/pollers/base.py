import asyncio
import logging
import os
import random
import time
from urllib.parse import urlsplit
import aiohttp

log = logging.getLogger("vm")

# ── configurable intervals (env) — 90-120s is gentler on public APIs than an aggressive 60 ──
INTERVAL_CEX = int(os.environ.get("VM_POLL_INTERVAL_CEX", "90"))
INTERVAL_DEX = int(os.environ.get("VM_POLL_INTERVAL_DEX", "60"))
DEPTH_INTERVAL = int(os.environ.get("VM_DEPTH_INTERVAL", "3600"))
DISABLED_VENUES = {v.strip() for v in os.environ.get("VM_DISABLE_VENUES", "").split(",") if v.strip()}

# ── process-wide per-domain rate limit: requests/sec (soft, to stay clear of 429s) ──
_HOST_RPS = {
    "api.binance.com": 8, "fapi.binance.com": 8,
    "api.hyperliquid.xyz": 5,
    "api.gateio.ws": 6, "www.okx.com": 8, "api.bybit.com": 8,
    "contract.mexc.com": 5, "api.mexc.com": 5,
    "api.bitget.com": 8, "api-futures.kucoin.com": 5,
    "futures.kraken.com": 5, "ws.kraken.com": 5,
    "api.international.coinbase.com": 5, "api.crypto.com": 5,
    "open-api.bingx.com": 4, "api.hbdm.com": 5,
    "fapi.asterdex.com": 5, "mainnet.zklighter.elliot.ai": 5,
    "market-data.grvt.io": 4, "pro.edgex.exchange": 4,
    "metadata-backend.ostium.io": 4,
    "api.ondoperps.xyz": 4,
    "api.geckoterminal.com": 0.5,  # 30 req/min — hard public limit
    "graph.codex.io": 2,
    "api.massive.com": 4,
    "api.gemini.com": 3, "api-cloud.bitmart.com": 4, "api.lbkex.com": 4,
    "sapi.xt.com": 4, "openapi.bitrue.com": 4,
}
_DEFAULT_RPS = 5


class _HostLimiter:
    """Minimum interval between requests to one host (process-wide throttling)."""
    def __init__(self, rps):
        self.min_int = 1.0 / rps if rps > 0 else 0.0
        self.lock = asyncio.Lock()
        self.next_at = 0.0

    async def acquire(self):
        async with self.lock:
            now = time.monotonic()
            wait = self.next_at - now
            if wait > 0:
                await asyncio.sleep(wait)
            self.next_at = max(now, self.next_at) + self.min_int


_limiters: dict[str, _HostLimiter] = {}


def _limiter(host):
    lim = _limiters.get(host)
    if lim is None:
        lim = _limiters[host] = _HostLimiter(_HOST_RPS.get(host, _DEFAULT_RPS))
    return lim


class RateLimited(Exception):
    """429 from an external API — triggers exponential backoff for the venue."""


# ── module-level HTTP helpers: limiter + _parse, usable outside Poller too ──
async def request_json(session, method, url, **kwargs):
    await _limiter(urlsplit(url).hostname or "").acquire()
    async with session.request(method, url, **kwargs) as r:
        return await _parse(r, url)


async def get_json(session, url, **kwargs):
    return await request_json(session, "GET", url, **kwargs)


async def post_json(session, url, payload=None, **kwargs):
    if payload is not None:
        kwargs["json"] = payload
    return await request_json(session, "POST", url, **kwargs)


class Poller:
    """Base loop: every interval seconds fetch() → store.record_snaps().
    Default interval comes from env per category (CEX/DEX)."""
    venue_id = "base"
    name = "base"
    category = "dex_perp"
    chain = ""
    interval = None  # None → auto per category (INTERVAL_CEX/INTERVAL_DEX)

    def __init__(self, store):
        self.store = store
        self.session: aiohttp.ClientSession | None = None

    @property
    def base_interval(self):
        if self.interval is not None:
            return self.interval
        return INTERVAL_CEX if self.category in ("cex_spot", "cex_fut") else INTERVAL_DEX

    async def fetch(self) -> list[dict]:  # overridden by subclasses
        raise NotImplementedError

    def register(self):
        self.store.upsert_venue(self.venue_id, self.name, self.category, self.chain)

    async def run(self):
        self.register()
        self.session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=25),
            headers={"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) venues-monitor/1.0"})
        rl_strikes = 0          # consecutive 429s → growing backoff
        try:
            while True:
                state, extra = "live", 1.0
                try:
                    snaps = await self.fetch()
                    if snaps:
                        new, dele = await asyncio.to_thread(self.store.record_snaps, self.venue_id, snaps)
                        if new:
                            log.info("%s: +%d new instruments %s", self.venue_id, len(new), new[:5])
                        if dele:
                            log.warning("%s: delisted %s", self.venue_id, dele)
                    rl_strikes = 0
                except asyncio.CancelledError:
                    raise
                except RateLimited as e:
                    rl_strikes += 1
                    extra = (1, 2, 5, 10)[min(rl_strikes - 1, 3)]  # 1→2→5→10× the interval
                    state = "rate_limited"
                    log.warning("%s: 429, backoff ×%g", self.venue_id, extra)
                    self.store.record_error(self.venue_id, repr(e), state="rate_limited")
                except Exception as e:  # noqa
                    state = "api_error"
                    log.warning("%s: %r", self.venue_id, e)
                    self.store.record_error(self.venue_id, repr(e), state="api_error")
                # sleep with ±20% jitter so tasks do not hit APIs in sync
                base = self.base_interval * extra
                await asyncio.sleep(base * random.uniform(0.9, 1.2))
        finally:
            await self.session.close()  # clean close on cancel/shutdown

    async def get_json(self, url, **kw):
        return await get_json(self.session, url, **kw)

    async def post_json(self, url, payload=None, **kw):
        return await post_json(self.session, url, payload, **kw)


async def _parse(r, url):
    """Explicit HTTP status/body check: 429 → RateLimited (backoff); 4xx/5xx and HTML
    (Cloudflare)/broken JSON → RuntimeError. record_snaps is NOT called on error —
    only a valid, complete response participates in delisting logic."""
    import json as _json
    host = urlsplit(url).hostname or url
    if r.status == 429:
        raise RateLimited(f"429 {host}")
    if r.status >= 400:
        raise RuntimeError(f"HTTP {r.status} {host}")
    txt = await r.text()
    ct = r.headers.get("Content-Type", "")
    if "html" in ct.lower() or txt[:1] in ("<",):
        raise RuntimeError(f"non-JSON response (Cloudflare/WAF?) from {host}")
    try:
        return _json.loads(txt)
    except ValueError as e:
        raise RuntimeError(f"broken JSON from {host}: {e}")


def fnum(x, default=None):
    try:
        v = float(x)
        return v if v == v else default  # NaN guard
    except (TypeError, ValueError):
        return default
