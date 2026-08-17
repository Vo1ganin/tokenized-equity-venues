"""Hyperliquid: every HIP-3 deployer via perpDexs (auto-discovery) + HyperCore spot token-stocks.
The core (crypto) perp dex is out of scope."""
import asyncio
from .base import Poller, fnum
from ..known import normalize, asset_type, is_tradfi

INFO = "https://api.hyperliquid.xyz/info"
# dex codename → display name (others fall back to fullName from the API)
NICE = {"xyz": "trade.xyz"}


class Hip3Poller(Poller):
    venue_id = "hip3"  # actual venue_ids: hip3:<dex>
    name = "Hyperliquid HIP-3"
    category = "dex_perp"
    chain = "HL L1"

    def register(self):
        pass  # venues register dynamically, one per discovered dex

    async def fetch(self):
        dexs = await self.post_json(INFO, {"type": "perpDexs"})
        out = []
        for dex in dexs:
            if not dex:  # null = core dex, crypto only — skip
                continue
            dname = dex.get("name")
            try:
                meta_ctx = await self.post_json(INFO, {"type": "metaAndAssetCtxs", "dex": dname})
            except Exception:
                continue
            meta, ctxs = meta_ctx[0]["universe"], meta_ctx[1]
            vid = f"hip3:{dname}"
            snaps = []
            for m, c in zip(meta, ctxs):
                if m.get("isDelisted"):
                    continue
                sym = m["name"]  # 'xyz:AAPL'
                u = normalize(sym)
                mark = fnum(c.get("markPx"))
                oi_base = fnum(c.get("openInterest"), 0.0)
                bid, ask = None, None
                if c.get("impactPxs"):
                    bid, ask = fnum(c["impactPxs"][0]), fnum(c["impactPxs"][1])
                spread = (ask - bid) / mark * 1e4 if (bid and ask and mark) else None
                snaps.append(dict(
                    symbol=sym, underlying=u, asset_type=asset_type(u), mid=mark,
                    vol24h_usd=fnum(c.get("dayNtlVlm"), 0.0),
                    oi_usd=oi_base * mark if (mark and oi_base) else None,
                    spread_bps=spread, funding_rate=fnum(c.get("funding"))))
            if snaps:  # dexes with no live markets (shut-down deployers) are not registered
                self.store.upsert_venue(vid, NICE.get(dname, dex.get("fullName") or dname), "dex_perp", "HL L1",
                                        notes=f"deployer {dex.get('deployer','')[:10]}…")
                await asyncio.to_thread(self.store.record_snaps, vid, snaps)
            await asyncio.sleep(0.3)  # be gentle with the rate limit
        return []  # snaps are recorded per venue above


def _stock_token(name: str):
    """Token-stock detector for HyperCore spot: accept only names where an issuer
    suffix (X = Backed/xStocks, D = Dinari) was actually stripped to a known
    tradfi underlying. Comparing against suffix-free normalization rejects
    plain-name synonym hits (e.g. a memecoin literally named 'SPACEX')."""
    with_suffix = normalize(name, issuer_suffixes=("X", "D"))
    plain = normalize(name, issuer_suffixes=())
    if with_suffix != plain and is_tradfi(with_suffix):
        return with_suffix
    return None


class HypercoreSpotPoller(Poller):
    venue_id = "hypercore_spot"
    name = "HyperCore Spot"
    category = "onchain_amm"
    chain = "HL L1"
    interval = 120

    async def fetch(self):
        meta, ctxs = await self.post_json(INFO, {"type": "spotMetaAndAssetCtxs"})
        tok_name = {t["index"]: t["name"] for t in meta.get("tokens", [])}
        snaps = []
        for pair, c in zip(meta.get("universe", []), ctxs):
            base = tok_name.get((pair.get("tokens") or [None])[0], "")
            u = _stock_token(base)
            if not u:
                continue
            vol = fnum(c.get("dayNtlVlm"), 0.0)
            # pre-trading pairs carry placeholder book prices — show no price until trades exist
            mid = (fnum(c.get("midPx")) or fnum(c.get("markPx"))) if vol else None
            snaps.append(dict(
                symbol=base, underlying=u, asset_type=asset_type(u), mid=mid,
                vol24h_usd=vol, oi_usd=None,
                spread_bps=None, funding_rate=None))
        return snaps
