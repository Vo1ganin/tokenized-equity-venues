"""Kraken spot xStocks: the pairs are only visible over WS v2 with include_tokenized_assets.
A background WS task keeps a ticker cache; fetch() materializes snaps every cycle."""
import asyncio
import json
import time
from .base import Poller, fnum, log
from ..known import normalize, asset_type

WS = "wss://ws.kraken.com/v2"


class KrakenSpotPoller(Poller):
    venue_id = "kraken_spot"
    name = "Kraken"
    category = "cex_spot"

    def __init__(self, store):
        super().__init__(store)
        self.cache: dict[str, dict] = {}   # 'TSLAx/USD' -> ticker
        self.tokenized: set[str] = set()
        self._ws_started = False

    async def _ws_loop(self):
        while True:
            if self.session.closed:  # session closed on shutdown — exit quietly
                return
            try:
                async with self.session.ws_connect(WS, heartbeat=25) as ws:
                    await ws.send_json({"method": "subscribe", "params": {
                        "channel": "instrument", "include_tokenized_assets": True}})
                    async for msg in ws:
                        if msg.type.name != "TEXT":
                            continue
                        d = json.loads(msg.data)
                        ch = d.get("channel")
                        if ch == "instrument":
                            pairs = d.get("data", {}).get("pairs", [])
                            toks = {p["symbol"] for p in pairs
                                    if p.get("symbol", "").split("/")[0].endswith("x")}
                            new = toks - self.tokenized
                            self.tokenized |= toks
                            if new:
                                syms = sorted(new)
                                for i in range(0, len(syms), 50):
                                    await ws.send_json({"method": "subscribe", "params": {
                                        "channel": "ticker", "symbol": syms[i:i + 50]}})
                                log.info("kraken: subscribed to %d tokenized pairs", len(new))
                        elif ch == "ticker":
                            for t in d.get("data", []):
                                self.cache[t["symbol"]] = {**t, "_ts": time.time()}
            except asyncio.CancelledError:
                raise
            except Exception as e:
                if self.session.closed:
                    return
                log.warning("kraken ws reconnect: %r", e)
                await asyncio.sleep(10)

    async def fetch(self):
        if not self._ws_started:
            self._ws_started = True
            asyncio.create_task(self._ws_loop())
            await asyncio.sleep(8)  # give the WS time to warm up before the first snap
        snaps = []
        for sym, t in self.cache.items():
            if time.time() - t.get("_ts", 0) > 600:
                continue
            base = sym.split("/")[0].replace(".", "")
            if base.endswith("x"):
                base = base[:-1]
            u = normalize(base.upper(), issuer_suffixes=())
            bid, ask, last = fnum(t.get("bid")), fnum(t.get("ask")), fnum(t.get("last"))
            mid = (bid + ask) / 2 if (bid and ask) else last
            snaps.append(dict(symbol=sym, underlying=u, asset_type=asset_type(u), mid=mid,
                              vol24h_usd=(fnum(t.get("volume"), 0.0) or 0.0) * (mid or 0.0),
                              oi_usd=None,
                              spread_bps=(ask - bid) / mid * 1e4 if (bid and ask and mid) else None,
                              funding_rate=None))
        return snaps
