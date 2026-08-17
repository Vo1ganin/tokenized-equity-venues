"""Binance USDT-M: TradFi perps (underlyingType EQUITY/KR_EQUITY/PREMARKET/COMMODITY/INDEX).
OI needs one call per symbol — refreshed every 5 minutes in a background task."""
import asyncio
import time
from .base import Poller, fnum, RateLimited
from ..known import normalize, asset_type

FAPI = "https://fapi.binance.com"
TYPES = {"EQUITY", "KR_EQUITY", "PREMARKET", "COMMODITY", "INDEX"}


class BinanceFutPoller(Poller):
    venue_id = "binance_fut"
    name = "Binance Futures · TradFi"
    category = "cex_fut"
    chain = ""

    def __init__(self, store):
        super().__init__(store)
        self._symbols: dict[str, str] = {}   # symbol -> underlyingType
        self._sym_ts = 0.0
        self._oi: dict[str, float] = {}
        self._oi_ts = 0.0
        self._oi_task = None

    async def _refresh_symbols(self):
        ei = await self.get_json(f"{FAPI}/fapi/v1/exchangeInfo")
        self._symbols = {s["symbol"]: s["underlyingType"] for s in ei["symbols"]
                         if s.get("underlyingType") in TYPES and s.get("status") == "TRADING"}
        self._sym_ts = time.time()

    async def _refresh_oi(self):
        for sym in list(self._symbols):
            try:
                d = await self.get_json(f"{FAPI}/fapi/v1/openInterest", params={"symbol": sym})
                self._oi[sym] = fnum(d.get("openInterest"), 0.0)
            except RateLimited:
                raise
            except Exception:
                pass  # keep the previous OI value
            await asyncio.sleep(0.12)

    async def _run_refresh_oi(self):
        try:
            await self._refresh_oi()
            self._oi_ts = time.time()  # timestamp only on clean completion
            self.store.record_aux_ok(self.venue_id, "oi")
        except RateLimited as e:
            self.store.record_aux_error(self.venue_id, "oi", repr(e), "rate_limited")
        except Exception as e:
            self.store.record_aux_error(self.venue_id, "oi", repr(e), "api_error")

    async def fetch(self):
        if time.time() - self._sym_ts > 3600 or not self._symbols:
            await self._refresh_symbols()
        t24, prem = await asyncio.gather(
            self.get_json(f"{FAPI}/fapi/v1/ticker/24hr"),
            self.get_json(f"{FAPI}/fapi/v1/premiumIndex"))
        if time.time() - self._oi_ts > 300 and (not self._oi_task or self._oi_task.done()):
            self._oi_task = asyncio.create_task(self._run_refresh_oi())
        tmap = {t["symbol"]: t for t in t24}
        pmap = {p["symbol"]: p for p in prem}
        snaps = []
        for sym, utype in self._symbols.items():
            t = tmap.get(sym)
            if not t:
                continue
            u = normalize(sym)
            at = "pre_ipo" if utype == "PREMARKET" else ("kr_stock" if utype == "KR_EQUITY" else asset_type(u))
            mark = fnum(pmap.get(sym, {}).get("markPrice")) or fnum(t.get("lastPrice"))
            oi = self._oi.get(sym)
            snaps.append(dict(
                symbol=sym, underlying=u, asset_type=at, mid=mark,
                vol24h_usd=fnum(t.get("quoteVolume"), 0.0),
                oi_usd=oi * mark if (oi and mark) else None,
                spread_bps=None,
                funding_rate=fnum(pmap.get(sym, {}).get("lastFundingRate"))))
        return snaps
