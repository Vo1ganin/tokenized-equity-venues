"""CEX equity futures: OKX, Bitget, Gate, KuCoin, MEXC, Kraken, Crypto.com, INTX, BingX, HTX.
All filters go through known.is_tradfi on the underlying (plain names, issuer suffixes where used)."""
import asyncio
import time
from .base import Poller, fnum, RateLimited
from ..known import normalize, asset_type, is_tradfi


def _mk(symbol, u, mid, vol, oi_usd=None, spread=None, funding=None):
    return dict(symbol=symbol, underlying=u, asset_type=asset_type(u), mid=mid,
                vol24h_usd=vol or 0.0, oi_usd=oi_usd, spread_bps=spread, funding_rate=funding)


class OkxFutPoller(Poller):
    venue_id = "okx_fut"
    name = "OKX Futures"
    category = "cex_fut"

    def __init__(self, store):
        super().__init__(store)
        self._fund: dict[str, float] = {}
        self._fund_ts = 0.0
        self._fund_task = None
        self._stock_ids: set = set()   # instIds of stock swaps (instCategory=3)
        self._stock_ts = 0.0

    async def _refresh_funding(self, ids):
        for i in ids:
            try:
                d = await self.get_json("https://www.okx.com/api/v5/public/funding-rate", params={"instId": i})
                if d.get("data"):
                    self._fund[i] = fnum(d["data"][0].get("fundingRate"))
            except RateLimited:
                raise
            except Exception:
                pass  # keep the previous funding value
            await asyncio.sleep(0.15)

    async def _run_refresh_funding(self, ids):
        try:
            await self._refresh_funding(ids)
            self._fund_ts = time.time()
            self.store.record_aux_ok(self.venue_id, "funding")
        except RateLimited as e:
            self.store.record_aux_error(self.venue_id, "funding", repr(e), "rate_limited")
        except Exception as e:
            self.store.record_aux_error(self.venue_id, "funding", repr(e), "api_error")

    async def fetch(self):
        if time.time() - self._stock_ts > 3600 or not self._stock_ids:
            ins = await self.get_json("https://www.okx.com/api/v5/public/instruments",
                                      params={"instType": "SWAP"})
            ids = {x["instId"] for x in ins.get("data", [])
                   if x.get("state") == "live" and x.get("instCategory") == "3"}
            if ids:
                self._stock_ids, self._stock_ts = ids, time.time()
        t, oi = await asyncio.gather(
            self.get_json("https://www.okx.com/api/v5/market/tickers", params={"instType": "SWAP"}),
            self.get_json("https://www.okx.com/api/v5/public/open-interest", params={"instType": "SWAP"}))
        oimap = {x["instId"]: fnum(x.get("oiCcy"), 0.0) for x in oi.get("data", [])}
        snaps, ids = [], []
        for x in t.get("data", []):
            iid = x["instId"]
            base = iid.split("-")[0]
            u = normalize(base, issuer_suffixes=())
            if self._stock_ids:
                if iid not in self._stock_ids:  # official OKX marker: instCategory=3
                    continue
            elif not is_tradfi(u):
                continue
            ids.append(iid)
            last = fnum(x.get("last"))
            bid, ask = fnum(x.get("bidPx")), fnum(x.get("askPx"))
            mid = (bid + ask) / 2 if (bid and ask) else last
            snaps.append(_mk(iid, u, mid,
                             (fnum(x.get("volCcy24h"), 0.0) or 0.0) * (last or 0.0),
                             oi_usd=oimap.get(iid, 0.0) * (last or 0.0) or None,
                             spread=(ask - bid) / mid * 1e4 if (bid and ask and mid) else None,
                             funding=self._fund.get(iid)))
        if time.time() - self._fund_ts > 300 and (not self._fund_task or self._fund_task.done()):
            self._fund_task = asyncio.create_task(self._run_refresh_funding(ids))
        return snaps


class BitgetFutPoller(Poller):
    venue_id = "bitget_fut"
    name = "Bitget Futures"
    category = "cex_fut"

    async def fetch(self):
        t = await self.get_json("https://api.bitget.com/api/v2/mix/market/tickers",
                                params={"productType": "usdt-futures"})
        snaps = []
        for x in t.get("data", []):
            s = x["symbol"]
            if not s.endswith("USDT"):
                continue
            u = normalize(s[:-4], issuer_suffixes=())
            if not is_tradfi(u):
                continue
            last = fnum(x.get("lastPr"))
            bid, ask = fnum(x.get("bidPr")), fnum(x.get("askPr"))
            mid = (bid + ask) / 2 if (bid and ask) else last
            hold = fnum(x.get("holdingAmount"), 0.0) or fnum(x.get("openInterest"), 0.0) or 0.0
            snaps.append(_mk(s, u, mid, fnum(x.get("usdtVolume"), 0.0),
                             oi_usd=hold * (last or 0.0) or None,
                             spread=(ask - bid) / mid * 1e4 if (bid and ask and mid) else None,
                             funding=fnum(x.get("fundingRate"))))
        return snaps


class GateFutPoller(Poller):
    venue_id = "gate_fut"
    name = "Gate Futures"
    category = "cex_fut"

    def __init__(self, store):
        super().__init__(store)
        self._mult: dict[str, float] = {}
        self._mult_ts = 0.0

    async def fetch(self):
        if time.time() - self._mult_ts > 3600 or not self._mult:
            cs = await self.get_json("https://api.gateio.ws/api/v4/futures/usdt/contracts")
            self._mult = {c["name"]: fnum(c.get("quanto_multiplier"), 1.0) for c in cs}
            self._mult_ts = time.time()
        t = await self.get_json("https://api.gateio.ws/api/v4/futures/usdt/tickers")
        snaps = []
        for x in t:
            s = x.get("contract", "")
            base = s.split("_")[0]
            u = normalize(base, issuer_suffixes=("X", "ON"))
            if not is_tradfi(u) or u == base and not is_tradfi(base):
                continue
            last = fnum(x.get("last"))
            oi_size = fnum(x.get("total_size"), 0.0) or 0.0
            snaps.append(_mk(s, u, last, fnum(x.get("volume_24h_settle"), 0.0),
                             oi_usd=oi_size * self._mult.get(s, 1.0) * (last or 0.0) or None,
                             funding=fnum(x.get("funding_rate"))))
        return snaps


class KucoinFutPoller(Poller):
    venue_id = "kucoin_fut"
    name = "KuCoin Futures"
    category = "cex_fut"

    async def fetch(self):
        d = await self.get_json("https://api-futures.kucoin.com/api/v1/contracts/active")
        snaps = []
        for x in d.get("data", []):
            s = x["symbol"]
            base = s.replace("USDTM", "")
            u = normalize(base, issuer_suffixes=())
            if not is_tradfi(u):
                continue
            last = fnum(x.get("lastTradePrice"))
            oi = (fnum(x.get("openInterest"), 0.0) or 0.0) * (fnum(x.get("multiplier"), 1.0) or 1.0)
            snaps.append(_mk(s, u, last, fnum(x.get("turnoverOf24h"), 0.0),
                             oi_usd=oi * (last or 0.0) or None,
                             funding=fnum(x.get("fundingFeeRate"))))
        return snaps


class MexcFutPoller(Poller):
    venue_id = "mexc_fut"
    name = "MEXC Futures"
    category = "cex_fut"

    def __init__(self, store):
        super().__init__(store)
        self._size: dict[str, float] = {}
        self._size_ts = 0.0

    async def fetch(self):
        if time.time() - self._size_ts > 3600 or not self._size:
            d = await self.get_json("https://contract.mexc.com/api/v1/contract/detail")
            self._size = {x["symbol"]: fnum(x.get("contractSize"), 1.0) for x in d.get("data", [])}
            self._size_ts = time.time()
        t = await self.get_json("https://contract.mexc.com/api/v1/contract/ticker")
        snaps = []
        for x in t.get("data", []):
            s = x["symbol"]
            if "STOCK_USDT" not in s:
                continue
            u = normalize(s.replace("STOCK_USDT", ""), issuer_suffixes=())
            last = fnum(x.get("lastPrice"))
            bid, ask = fnum(x.get("bid1")), fnum(x.get("ask1"))
            mid = (bid + ask) / 2 if (bid and ask) else last
            oi = (fnum(x.get("holdVol"), 0.0) or 0.0) * (self._size.get(s, 1.0) or 1.0)
            snaps.append(_mk(s, u, mid, fnum(x.get("amount24"), 0.0),
                             oi_usd=oi * (last or 0.0) or None,
                             spread=(ask - bid) / mid * 1e4 if (bid and ask and mid) else None,
                             funding=fnum(x.get("fundingRate"))))
        return snaps


class KrakenFutPoller(Poller):
    venue_id = "kraken_fut"
    name = "Kraken Derivatives"
    category = "cex_fut"

    async def fetch(self):
        d = await self.get_json("https://futures.kraken.com/derivatives/api/v3/tickers")
        snaps = []
        for x in d.get("tickers", []):
            s = x.get("symbol", "")
            if not s.lower().startswith("pf_"):
                continue
            base = s[3:].lower().replace("usd", "").upper()
            u = normalize(base, issuer_suffixes=("X",))
            if not is_tradfi(u):
                continue
            last = fnum(x.get("last")) or fnum(x.get("markPrice"))
            bid, ask = fnum(x.get("bid")), fnum(x.get("ask"))
            mid = (bid + ask) / 2 if (bid and ask) else last
            snaps.append(_mk(s, u, mid, (fnum(x.get("vol24h"), 0.0) or 0.0) * (last or 0.0),
                             oi_usd=(fnum(x.get("openInterest"), 0.0) or 0.0) * (last or 0.0) or None,
                             spread=(ask - bid) / mid * 1e4 if (bid and ask and mid) else None,
                             funding=fnum(x.get("fundingRate"))))
        return snaps


class CryptocomPoller(Poller):
    venue_id = "cryptocom"
    name = "Crypto.com Exchange"
    category = "cex_fut"

    def __init__(self, store):
        super().__init__(store)
        self._eq: set[str] = set()
        self._eq_ts = 0.0

    async def fetch(self):
        if time.time() - self._eq_ts > 3600 or not self._eq:
            d = await self.get_json("https://api.crypto.com/exchange/v1/public/get-instruments")
            self._eq = {x["symbol"] for x in d["result"]["data"]
                        if x.get("product_type") == "EQUITY" or "IPOUSD-PERP" in x.get("symbol", "")}
            self._eq_ts = time.time()
        t = await self.get_json("https://api.crypto.com/exchange/v1/public/get-tickers")
        snaps = []
        for x in t["result"]["data"]:
            s = x.get("i", "")
            if s not in self._eq:
                continue
            u = normalize(s.replace("USD-PERP", "").replace("IPO", ""), issuer_suffixes=())
            last = fnum(x.get("a"))
            bid, ask = fnum(x.get("b")), fnum(x.get("k"))
            mid = (bid + ask) / 2 if (bid and ask) else last
            at = "pre_ipo" if "IPO" in s else asset_type(u)
            snaps.append({**_mk(s, u, mid, fnum(x.get("vv"), 0.0),
                                spread=(ask - bid) / mid * 1e4 if (bid and ask and mid) else None),
                          "asset_type": at})
        return snaps


class IntxPoller(Poller):
    venue_id = "coinbase_intx"
    name = "Coinbase INTX"
    category = "cex_fut"

    def __init__(self, store):
        super().__init__(store)
        self._syms: list[str] = []
        self._ts = 0.0

    async def fetch(self):
        if time.time() - self._ts > 3600 or not self._syms:
            d = await self.get_json("https://api.international.coinbase.com/api/v1/instruments")
            self._syms, self._adq = [], {}
            for x in d:
                u = normalize(x["symbol"].replace("-PERP", ""), issuer_suffixes=())
                if x.get("type") == "PERP" and is_tradfi(u) and u != "STX":
                    self._syms.append(x["symbol"])
                    self._adq[x["symbol"]] = fnum(x.get("avg_daily_quantity"), 0.0)
            self._ts = time.time()
        snaps = []
        for s in self._syms:
            try:
                q = await self.get_json(f"https://api.international.coinbase.com/api/v1/instruments/{s}/quote")
            except Exception:
                continue
            u = normalize(s.replace("-PERP", ""), issuer_suffixes=())
            bid, ask = fnum(q.get("best_bid_price")), fnum(q.get("best_ask_price"))
            mid = (bid + ask) / 2 if (bid and ask) else fnum(q.get("trade_price"))
            snaps.append(_mk(s, u, mid,
                             (self._adq.get(s) or 0.0) * (mid or 0.0),  # ADV proxy: avg_daily_quantity
                             spread=(ask - bid) / mid * 1e4 if (bid and ask and mid) else None,
                             funding=fnum(q.get("predicted_funding"))))
            await asyncio.sleep(0.1)
        return snaps


class BingxFutPoller(Poller):
    venue_id = "bingx_fut"
    name = "BingX Futures"
    category = "cex_fut"

    async def fetch(self):
        t = await self.get_json("https://open-api.bingx.com/openApi/swap/v2/quote/ticker")
        snaps = []
        for x in t.get("data", []):
            s = x.get("symbol", "")
            base = s.split("-")[0]
            u = normalize(base, issuer_suffixes=("X",))
            if u == base and not is_tradfi(base):
                continue
            if not is_tradfi(u):
                continue
            last = fnum(x.get("lastPrice"))
            snaps.append(_mk(s, u, last, fnum(x.get("quoteVolume"), 0.0)))
        return snaps


class HtxFutPoller(Poller):
    venue_id = "htx_fut"
    name = "HTX Futures"
    category = "cex_fut"

    async def fetch(self):
        t = await self.get_json("https://api.hbdm.com/linear-swap-ex/market/detail/batch_merged")
        try:
            fr = await self.get_json("https://api.hbdm.com/linear-swap-api/v1/swap_batch_funding_rate")
            fmap = {x["contract_code"]: fnum(x.get("funding_rate")) for x in fr.get("data", [])}
        except Exception:
            fmap = {}
        snaps = []
        for x in t.get("ticks", []):
            s = x.get("contract_code", "")
            base = s.split("-")[0]
            u = normalize(base, issuer_suffixes=("X",))
            if not is_tradfi(u):
                continue
            close = fnum(x.get("close"))
            snaps.append(_mk(s, u, close, fnum(x.get("trade_turnover"), 0.0), funding=fmap.get(s)))
        return snaps
