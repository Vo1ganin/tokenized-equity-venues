"""CEX spot tokenized stocks: Bybit (symbolType=xstocks), Binance bStocks, Gate, MEXC, Bitget.
Plain public REST everywhere — no keys required."""
import asyncio
import time
from .base import Poller, fnum
from ..known import normalize, asset_type, is_tradfi


class BybitSpotPoller(Poller):
    venue_id = "bybit_spot"
    name = "Bybit"
    category = "cex_spot"

    def __init__(self, store):
        super().__init__(store)
        self._syms, self._ts = set(), 0.0

    async def fetch(self):
        if time.time() - self._ts > 3600 or not self._syms:
            d = await self.get_json("https://api.bybit.com/v5/market/instruments-info",
                                    params={"category": "spot", "limit": "1000"})
            self._syms = {x["symbol"] for x in d["result"]["list"] if x.get("symbolType") == "xstocks"}
            self._ts = time.time()
        t = await self.get_json("https://api.bybit.com/v5/market/tickers", params={"category": "spot"})
        snaps = []
        for x in t["result"]["list"]:
            if x["symbol"] not in self._syms:
                continue
            u = normalize(x["symbol"])
            bid, ask = fnum(x.get("bid1Price")), fnum(x.get("ask1Price"))
            mid = (bid + ask) / 2 if (bid and ask) else fnum(x.get("lastPrice"))
            snaps.append(dict(symbol=x["symbol"], underlying=u, asset_type=asset_type(u), mid=mid,
                              vol24h_usd=fnum(x.get("turnover24h"), 0.0), oi_usd=None,
                              spread_bps=(ask - bid) / mid * 1e4 if (bid and ask and mid) else None,
                              funding_rate=None))
        return snaps


class BinanceBstocksPoller(Poller):
    venue_id = "binance_bstocks"
    name = "Binance bStocks"
    category = "cex_spot"
    chain = "BNB Chain"

    async def fetch(self):
        t = await self.get_json("https://api.binance.com/api/v3/ticker/24hr")
        snaps = []
        for x in t:
            s = x["symbol"]
            if not s.endswith("USDT"):
                continue
            base = s[:-4]
            if not base.endswith("B"):
                continue
            u = normalize(base, issuer_suffixes=("B",))
            if u == base or not is_tradfi(u):  # B-suffix not stripped by normalization → not a bStock
                continue
            bid, ask = fnum(x.get("bidPrice")), fnum(x.get("askPrice"))
            mid = (bid + ask) / 2 if (bid and ask) else fnum(x.get("lastPrice"))
            snaps.append(dict(symbol=s, underlying=u, asset_type=asset_type(u), mid=mid,
                              vol24h_usd=fnum(x.get("quoteVolume"), 0.0), oi_usd=None,
                              spread_bps=(ask - bid) / mid * 1e4 if (bid and ask and mid) else None,
                              funding_rate=None))
        return snaps


class GateSpotPoller(Poller):
    venue_id = "gate_spot"
    name = "Gate"
    category = "cex_spot"

    async def fetch(self):
        t = await self.get_json("https://api.gateio.ws/api/v4/spot/tickers")
        snaps = []
        for x in t:
            pair = x.get("currency_pair", "")
            if not pair.endswith("_USDT"):
                continue
            base = pair[:-5]
            u = normalize(base)
            # tokenized stocks only: X/ON suffix stripped by normalization
            if u == base or not is_tradfi(u):
                continue
            bid, ask = fnum(x.get("highest_bid")), fnum(x.get("lowest_ask"))
            mid = (bid + ask) / 2 if (bid and ask) else fnum(x.get("last"))
            snaps.append(dict(symbol=pair, underlying=u, asset_type=asset_type(u), mid=mid,
                              vol24h_usd=fnum(x.get("quote_volume"), 0.0), oi_usd=None,
                              spread_bps=(ask - bid) / mid * 1e4 if (bid and ask and mid) else None,
                              funding_rate=None))
        return snaps


class MexcSpotPoller(Poller):
    venue_id = "mexc_spot"
    name = "MEXC"
    category = "cex_spot"

    def __init__(self, store):
        super().__init__(store)
        self._ondo: set = set()     # pairs whose fullName carries "(Ondo)" — official tokenized-stock marker
        self._active: set = set()   # active pairs (filters delisted zombies out of ticker/24hr)
        self._ei_ts = 0.0

    async def fetch(self):
        if time.time() - self._ei_ts > 3600 or not self._ondo:
            ei = await self.get_json("https://api.mexc.com/api/v3/exchangeInfo")
            syms = ei.get("symbols", [])
            if syms:
                self._ondo = {y["symbol"] for y in syms
                              if "(Ondo)" in (y.get("fullName") or "") and y.get("quoteAsset") == "USDT"}
                self._active = {y["symbol"] for y in syms if y.get("status") == "1"}
                self._ei_ts = time.time()
        t = await self.get_json("https://api.mexc.com/api/v3/ticker/24hr")
        snaps = []
        for x in t:
            s = x["symbol"]
            if not s.endswith("USDT"):
                continue
            if self._active and s not in self._active:
                continue
            base = s[:-4]
            u = normalize(base)
            if s in self._ondo:
                if u == base and base.endswith("ON"):
                    u = base[:-2]  # ticker unknown to the reference, but the Ondo marker is reliable — strip ON manually
            elif u == base or not is_tradfi(u):
                continue
            bid, ask = fnum(x.get("bidPrice")), fnum(x.get("askPrice"))
            mid = (bid + ask) / 2 if (bid and ask) else fnum(x.get("lastPrice"))
            snaps.append(dict(symbol=s, underlying=u, asset_type=asset_type(u), mid=mid,
                              vol24h_usd=fnum(x.get("quoteVolume"), 0.0), oi_usd=None,
                              spread_bps=(ask - bid) / mid * 1e4 if (bid and ask and mid) else None,
                              funding_rate=None))
        return snaps


class BitgetSpotPoller(Poller):
    venue_id = "bitget_spot"
    name = "Bitget"
    category = "cex_spot"

    async def fetch(self):
        t = await self.get_json("https://api.bitget.com/api/v2/spot/market/tickers")
        snaps = []
        for x in t.get("data", []):
            s = x["symbol"]
            if not s.endswith("USDT"):
                continue
            base = s[:-4]
            u = normalize(base)
            if u == base or not is_tradfi(u):
                continue
            bid, ask = fnum(x.get("bidPr")), fnum(x.get("askPr"))
            mid = (bid + ask) / 2 if (bid and ask) else fnum(x.get("lastPr"))
            snaps.append(dict(symbol=s, underlying=u, asset_type=asset_type(u), mid=mid,
                              vol24h_usd=fnum(x.get("usdtVolume"), 0.0), oi_usd=None,
                              spread_bps=(ask - bid) / mid * 1e4 if (bid and ask and mid) else None,
                              funding_rate=None))
        return snaps


def _tokenized_underlying(base_lower: str):
    """base without the quote currency, lowercase. Returns the underlying ONLY for
    tokenized stocks (issuer suffix x/on/d), else None — keeps crypto/FX/commodities
    off the long-tail CEXes."""
    for suf in ("on", "x", "d"):
        if base_lower.endswith(suf) and len(base_lower) > len(suf):
            cand = normalize(base_lower[:-len(suf)].upper(), issuer_suffixes=())
            if asset_type(cand) in ("single_stock", "etf", "pre_ipo", "kr_stock"):
                return cand
    return None


class GenericTickerPoller(Poller):
    """Long-tail CEX spot with cookie-cutter ticker endpoints: filtered via known underlyings."""
    category = "cex_spot"
    url = ""
    path = ()            # JSON path down to the ticker list
    sym_key = "symbol"
    last_key = "last"
    vol_key = "quoteVolume"
    issuer = ("X", "ON")

    def _extract(self, j):
        for k in self.path:
            j = j.get(k, {}) if isinstance(j, dict) else {}
        return j or []

    async def fetch(self):
        t = await self.get_json(self.url)
        snaps = []
        for x in self._extract(t):
            s = str(x.get(self.sym_key, ""))
            base = s.replace("_", "").replace("-", "").replace("/", "").lower()
            for q in ("usdt", "usdc", "usd"):
                if base.endswith(q):
                    base = base[:-len(q)]
                    break
            u = _tokenized_underlying(base)  # explicitly tokenized only (x/on/d suffix)
            if not u:
                continue
            last = fnum(x.get(self.last_key))
            snaps.append(dict(symbol=s, underlying=u, asset_type=asset_type(u), mid=last,
                              vol24h_usd=fnum(x.get(self.vol_key), 0.0), oi_usd=None,
                              spread_bps=None, funding_rate=None))
        return snaps


class GeminiPoller(GenericTickerPoller):
    venue_id = "gemini"; name = "Gemini"
    async def fetch(self):
        syms = await self.get_json("https://api.gemini.com/v1/symbols")
        snaps = []
        for s in syms:
            su = s.upper()
            if not (su.endswith("USD") or su.endswith("USDT")):
                continue
            base = (su[:-4] if su.endswith("USDT") else su[:-3]).lower()
            u = _tokenized_underlying(base)
            if not u:
                continue
            try:
                t = await self.get_json(f"https://api.gemini.com/v1/pubticker/{s}")
            except Exception:
                continue
            last = fnum(t.get("last"))
            vol = t.get("volume", {})
            snaps.append(dict(symbol=su, underlying=u, asset_type=asset_type(u), mid=last,
                              vol24h_usd=(fnum(vol.get("USD"), 0.0) or 0.0),
                              oi_usd=None, spread_bps=None, funding_rate=None))
            await asyncio.sleep(0.1)
        return snaps


class BitmartPoller(GenericTickerPoller):
    venue_id = "bitmart"; name = "BitMart"
    url = "https://api-cloud.bitmart.com/spot/quotation/v3/tickers"
    def _extract(self, j):
        # v3: data = [[symbol, last, v24h_base, qv24h_quote, open, high, low, change, ...], …]
        d = j.get("data")
        return [dict(symbol=r[0], last=r[1], quoteVolume=r[3]) for r in d] if isinstance(d, list) else []


class LbankPoller(GenericTickerPoller):
    venue_id = "lbank"; name = "LBank"
    url = "https://api.lbkex.com/v2/ticker/24hr.do?symbol=all"
    def _extract(self, j):
        return [dict(symbol=r.get("symbol"), last=(r.get("ticker") or {}).get("latest"),
                     quoteVolume=(r.get("ticker") or {}).get("turnover")) for r in j.get("data", [])]


class XtPoller(GenericTickerPoller):
    venue_id = "xt"; name = "XT.com"
    url = "https://sapi.xt.com/v4/public/ticker"
    def _extract(self, j):
        return [dict(symbol=r.get("s"), last=r.get("c"), quoteVolume=r.get("v")) for r in j.get("result", [])]


class BitruePoller(GenericTickerPoller):
    venue_id = "bitrue"; name = "Bitrue"
    url = "https://openapi.bitrue.com/api/v1/ticker/24hr"
    sym_key = "symbol"; last_key = "lastPrice"; vol_key = "quoteVolume"
    def _extract(self, j):
        return j if isinstance(j, list) else []
