"""On-chain spot tokenized stocks via GeckoTerminal (free, keyless, works from server IPs) —
category E. Covers Raydium/Jupiter/Pancake/Uniswap/Felix: GT aggregates pools per network.
Discovery by issuer phrases. Alternative to Codex (which needs a paid plan — 402 from datacenters)."""
import asyncio
from .base import Poller, fnum
from ..known import normalize, asset_type

SEARCH = "https://api.geckoterminal.com/api/v2/search/pools"
PHRASES = ["xStock", "Ondo", "PreStock"]
GT_NET = {
    "solana": ("sol_dex", "Solana DEX", "Solana"),
    "eth": ("eth_dex", "Ethereum DEX", "Ethereum"),
    "bsc": ("bnb_dex", "BNB DEX", "BNB"),
    "base": ("base_dex", "Base DEX", "Base"),
    "hyperevm": ("hyperevm_dex", "HyperEVM DEX", "HyperEVM"),
    "polygon_pos": ("poly_dex", "Polygon DEX", "Polygon"),
    "arbitrum": ("arbitrum_dex", "Arbitrum DEX", "Arbitrum"),
}


class GeckoOnchainPoller(Poller):
    venue_id = "gecko"  # actual venues: sol_dex/bnb_dex/… register dynamically
    name = "GeckoTerminal on-chain"
    category = "onchain_amm"
    interval = 120

    def register(self):
        pass

    async def fetch(self):
        best: dict[tuple, dict] = {}  # (net, underlying) → best pool (by liquidity)
        for ph in PHRASES:
            try:
                d = await self.get_json(SEARCH, params={"query": ph}, headers={"Accept": "application/json"})
            except Exception:
                continue
            for p in d.get("data", []):
                net = p.get("id", "").split("_")[0]
                if net not in GT_NET:
                    continue
                a = p["attributes"]
                base_sym = (a.get("name") or "").split(" / ")[0].strip()
                u = normalize(base_sym, issuer_suffixes=("X", "ON", "D"))
                at = asset_type(u)
                if at == "other":
                    continue
                liq = fnum(a.get("reserve_in_usd"), 0.0) or 0.0
                if liq > 300e6:  # >$300M in a tokenized-stock pool = bogus reserve, skip
                    continue
                key = (net, u)
                if key not in best or liq > (best[key]["oi_usd"] or 0):
                    best[key] = dict(
                        symbol=f"{base_sym}@{net}", underlying=u, asset_type=at,
                        mid=fnum(a.get("base_token_price_usd")),
                        vol24h_usd=fnum((a.get("volume_usd") or {}).get("h24"), 0.0),
                        oi_usd=liq,  # for AMMs the OI column shows pool liquidity
                        spread_bps=None, funding_rate=None, _net=net)
            await asyncio.sleep(0.5)
        by_venue: dict[str, list] = {}
        for snap in best.values():
            by_venue.setdefault(GT_NET[snap.pop("_net")][0], []).append(snap)
        for net_key, (vid, vname, chain) in GT_NET.items():
            snaps = by_venue.get(vid)
            if not snaps:
                continue
            self.store.upsert_venue(vid, vname, "onchain_amm", chain, notes="GeckoTerminal pool aggregate")
            await asyncio.to_thread(self.store.record_snaps, vid, snaps)
        return []
