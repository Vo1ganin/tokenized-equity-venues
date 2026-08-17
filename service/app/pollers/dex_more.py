"""Second-wave DEX perps: Aster, Ostium, GRVT, edgeX.
Vest and Extended sit behind Cloudflare bot protection (datacenter IPs blocked) — watchlist.
ApeX Omni was removed 2026-08: its API endpoints stopped resolving."""
import asyncio
import time
from .base import Poller, fnum, RateLimited
from ..known import normalize, asset_type, is_tradfi


def _mk(symbol, u, mid, vol, oi_usd=None, spread=None, funding=None):
    return dict(symbol=symbol, underlying=u, asset_type=asset_type(u), mid=mid,
                vol24h_usd=vol or 0.0, oi_usd=oi_usd, spread_bps=spread, funding_rate=funding)


class AsterPoller(Poller):
    venue_id = "aster"
    name = "Aster"
    category = "dex_perp"
    chain = "BNB"

    def __init__(self, store):
        super().__init__(store)
        self._oi: dict[str, float] = {}
        self._oi_ts = 0.0
        self._oi_task = None

    async def _refresh_oi(self, syms):
        for s in syms:
            try:
                d = await self.get_json("https://fapi.asterdex.com/fapi/v1/openInterest", params={"symbol": s})
                self._oi[s] = fnum(d.get("openInterest"), 0.0)
            except RateLimited:
                raise
            except Exception:
                pass  # keep the previous OI value
            await asyncio.sleep(0.2)

    async def _run_refresh_oi(self, syms):
        try:
            await self._refresh_oi(syms)
            self._oi_ts = time.time()
            self.store.record_aux_ok(self.venue_id, "oi")
        except RateLimited as e:
            self.store.record_aux_error(self.venue_id, "oi", repr(e), "rate_limited")
        except Exception as e:
            self.store.record_aux_error(self.venue_id, "oi", repr(e), "api_error")

    async def fetch(self):
        t, prem = await asyncio.gather(
            self.get_json("https://fapi.asterdex.com/fapi/v1/ticker/24hr"),
            self.get_json("https://fapi.asterdex.com/fapi/v1/premiumIndex"))
        pmap = {p["symbol"]: p for p in prem} if isinstance(prem, list) else {}
        snaps, syms = [], []
        for x in t:
            s = x["symbol"]
            if not s.endswith("USDT"):
                continue
            u = normalize(s[:-4], issuer_suffixes=())
            if not is_tradfi(u):
                continue
            syms.append(s)
            last = fnum(x.get("lastPrice"))
            oi = self._oi.get(s)
            snaps.append(_mk(s, u, last, fnum(x.get("quoteVolume"), 0.0),
                             oi_usd=oi * last if (oi and last) else None,
                             funding=fnum(pmap.get(s, {}).get("lastFundingRate"))))
        if time.time() - self._oi_ts > 300 and syms and (not self._oi_task or self._oi_task.done()):
            self._oi_task = asyncio.create_task(self._run_refresh_oi(syms))
        return snaps


class OstiumPoller(Poller):
    venue_id = "ostium"
    name = "Ostium"
    category = "dex_perp"
    chain = "Arbitrum"
    # latest-prices returns bid/mid/ask (Nasdaq-licensed feed); volume/OI live in the subgraph

    async def fetch(self):
        t = await self.get_json("https://metadata-backend.ostium.io/PricePublish/latest-prices")
        snaps = []
        for x in t:
            frm, to = x.get("from", ""), x.get("to", "")
            if to != "USD":
                continue
            u = normalize(frm, issuer_suffixes=())
            if not is_tradfi(u):
                continue
            bid, ask, mid = fnum(x.get("bid")), fnum(x.get("ask")), fnum(x.get("mid"))
            snaps.append(_mk(f"{frm}-USD", u, mid, 0.0,
                             spread=(ask - bid) / mid * 1e4 if (bid and ask and mid) else None))
        return snaps


class GrvtPoller(Poller):
    venue_id = "grvt"
    name = "GRVT"
    category = "dex_perp"
    chain = "ZKsync"

    def __init__(self, store):
        super().__init__(store)
        self._instr: list[str] = []
        self._ts = 0.0

    async def fetch(self):
        if time.time() - self._ts > 3600 or not self._instr:
            d = await self.post_json("https://market-data.grvt.io/full/v1/instruments",
                                     {"kind": ["PERPETUAL"], "quote": ["USDT"], "is_active": True})
            self._instr = [i["instrument"] for i in d.get("result", [])
                           if is_tradfi(normalize(i.get("base", ""), issuer_suffixes=()))]
            self._ts = time.time()
        snaps = []
        for ins in self._instr:
            try:
                d = await self.post_json("https://market-data.grvt.io/full/v1/ticker", {"instrument": ins})
            except Exception:
                continue
            r = d.get("result", {})
            u = normalize(ins.split("_")[0], issuer_suffixes=())
            bid, ask = fnum(r.get("best_bid_price")), fnum(r.get("best_ask_price"))
            mid = fnum(r.get("mid_price")) or ((bid + ask) / 2 if (bid and ask) else fnum(r.get("last_price")))
            oi = fnum(r.get("open_interest"), 0.0) or 0.0
            snaps.append(_mk(ins, u, mid,
                             fnum(r.get("buy_volume_24h_q"), 0.0) + fnum(r.get("sell_volume_24h_q"), 0.0)
                             if r.get("buy_volume_24h_q") else fnum(r.get("volume_24h_q"), 0.0),
                             oi_usd=oi * mid if (oi and mid and oi < 1e10) else None,
                             spread=(ask - bid) / mid * 1e4 if (bid and ask and mid) else None,
                             funding=fnum(r.get("funding_rate_8h_curr"))))
            await asyncio.sleep(0.1)
        return snaps


class EdgexPoller(Poller):
    venue_id = "edgex"
    name = "edgeX"
    category = "dex_perp"
    chain = "own L2"
    interval = 120  # one API call per contract

    def __init__(self, store):
        super().__init__(store)
        self._contracts: list[tuple[str, str]] = []  # (contractId, name)
        self._ts = 0.0

    async def fetch(self):
        if time.time() - self._ts > 3600 or not self._contracts:
            d = await self.get_json("https://pro.edgex.exchange/api/v1/public/meta/getMetaData")
            self._contracts = []
            for c in d["data"]["contractList"]:
                nm = c.get("contractName", "")
                base = nm.replace("USD", "").rstrip("2")
                u = normalize(base, issuer_suffixes=())
                if is_tradfi(u):
                    self._contracts.append((c["contractId"], nm))
            self._ts = time.time()
        snaps = []
        for cid, nm in self._contracts:
            try:
                d = await self.get_json("https://pro.edgex.exchange/api/v1/public/quote/getTicker",
                                        params={"contractId": cid})
            except Exception:
                continue
            for x in d.get("data", []):
                u = normalize(nm.replace("USD", "").rstrip("2"), issuer_suffixes=())
                px = fnum(x.get("markPrice")) or fnum(x.get("oraclePrice")) or fnum(x.get("close"))
                oi = fnum(x.get("openInterest"), 0.0) or 0.0
                snaps.append(_mk(nm, u, px, fnum(x.get("value"), 0.0),
                                 oi_usd=oi * px if (oi and px) else None,
                                 funding=fnum(x.get("fundingRate"))))
            await asyncio.sleep(0.15)
        return snaps
