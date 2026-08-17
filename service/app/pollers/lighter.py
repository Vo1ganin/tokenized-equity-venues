"""Lighter (zk-rollup): orderBookDetails carries everything (price, volume, OI); funding-rates separately."""
from .base import Poller, fnum
from ..known import normalize, asset_type, is_tradfi

BASE = "https://mainnet.zklighter.elliot.ai/api/v1"


class LighterPoller(Poller):
    venue_id = "lighter"
    name = "Lighter"
    category = "dex_perp"
    chain = "own zk-rollup"

    async def fetch(self):
        details = await self.get_json(f"{BASE}/orderBookDetails")
        try:
            fr = await self.get_json(f"{BASE}/funding-rates")
            fmap = {r["symbol"]: fnum(r.get("rate")) for r in fr.get("funding_rates", [])
                    if r.get("exchange") == "lighter"}
        except Exception:
            fmap = {}
        snaps = []
        for ob in details.get("order_book_details", []):
            if ob.get("status") != "active":
                continue
            sym = ob["symbol"]
            u = normalize(sym)
            if not is_tradfi(u):
                continue  # Lighter's crypto perps are out of scope
            last = fnum(ob.get("last_trade_price"))
            oi = fnum(ob.get("open_interest"), 0.0)
            snaps.append(dict(
                symbol=sym, underlying=u, asset_type=asset_type(u), mid=last,
                vol24h_usd=fnum(ob.get("daily_quote_token_volume"), 0.0),
                oi_usd=oi * last if (last and oi and oi < 1e12) else None,
                spread_bps=None, funding_rate=fmap.get(sym)))
        return snaps
