"""Ondo Perps (launched 2026-07-07): equity/ETF/commodity perpetuals, tokenized stocks as collateral.
Public REST, no key: /v1/perps/{mark_prices,volume,open_interest}; funding is per-market → aux loop."""
import asyncio
import time
from .base import Poller, fnum, RateLimited
from ..known import normalize, asset_type, is_tradfi

BASE = "https://api.ondoperps.xyz/v1"


class OndoPerpsPoller(Poller):
    venue_id = "ondo_perps"
    name = "Ondo Perps"
    category = "dex_perp"
    chain = "Ondo"

    def __init__(self, store):
        super().__init__(store)
        self._fund: dict[str, float] = {}
        self._fund_ts = 0.0
        self._fund_task = None

    async def _refresh_funding(self, markets):
        for m in markets:
            try:
                d = await self.get_json(f"{BASE}/perps/funding_rates", params={"market": m})
                r = d.get("result") or {}
                if r.get("rate") is not None:
                    self._fund[m] = fnum(r["rate"])
            except RateLimited:
                raise
            except Exception:
                pass  # keep the previous funding value
            await asyncio.sleep(0.2)

    async def _run_refresh_funding(self, markets):
        try:
            await self._refresh_funding(markets)
            self._fund_ts = time.time()
            self.store.record_aux_ok(self.venue_id, "funding")
        except RateLimited as e:
            self.store.record_aux_error(self.venue_id, "funding", repr(e), "rate_limited")
        except Exception as e:
            self.store.record_aux_error(self.venue_id, "funding", repr(e), "api_error")

    async def fetch(self):
        marks, vol, oi = await asyncio.gather(
            self.get_json(f"{BASE}/perps/mark_prices"),
            self.get_json(f"{BASE}/perps/volume"),
            self.get_json(f"{BASE}/perps/open_interest"))
        vmap = {x["market"]: fnum(x.get("quoteVolume"), 0.0) for x in (vol.get("result") or [])}
        oimap = {x["market"]: fnum(x.get("notionalValue")) for x in (oi.get("result") or [])}
        snaps, markets = [], []
        for m, x in (marks.get("result") or {}).items():
            base = (x.get("pair") or {}).get("base") or m.split("-")[0]
            u = normalize(base, issuer_suffixes=())
            if not is_tradfi(u):
                continue  # crypto perps (BTC/ETH/...) are out of scope
            markets.append(m)
            snaps.append(dict(
                symbol=m, underlying=u, asset_type=asset_type(u),
                mid=fnum(x.get("markPrice")) or fnum(x.get("price")),
                vol24h_usd=vmap.get(m, 0.0), oi_usd=oimap.get(m),
                spread_bps=None, funding_rate=self._fund.get(m)))
        if time.time() - self._fund_ts > 300 and markets and (not self._fund_task or self._fund_task.done()):
            self._fund_task = asyncio.create_task(self._run_refresh_funding(markets))
        return snaps
