"""On-chain spot tokenized stocks via Codex.io (graph.codex.io) — category E.
One connector covers Raydium/Jupiter/Pancake/Uniswap/Felix: Codex aggregates a token's
liquidity across all pools per network. Discovery by issuer phrases (xStock/Ondo/PreStock).
Key: CODEX_API_KEY. Optional — GeckoTerminal covers the same ground without a key."""
import asyncio
import os
import aiohttp
from .base import Poller, fnum, post_json, RateLimited
from ..known import normalize, asset_type

GRAPH = "https://graph.codex.io/graphql"
KEY = os.environ.get("CODEX_API_KEY", "")
PHRASES = ["xStock", "Ondo", "PreStock"]

# networkId → (venue_id, name, chain)
NET = {
    1399811149: ("sol_dex", "Solana DEX", "Solana"),
    56: ("bnb_dex", "BNB DEX", "BNB"),
    1: ("eth_dex", "Ethereum DEX", "Ethereum"),
    999: ("hyperevm_dex", "HyperEVM DEX", "HyperEVM"),
    8453: ("base_dex", "Base DEX", "Base"),
    42161: ("arb_dex", "Arbitrum DEX", "Arbitrum"),
    137: ("poly_dex", "Polygon DEX", "Polygon"),
}


class CodexOnchainPoller(Poller):
    venue_id = "codex"  # actual venues: sol_dex/bnb_dex/… register dynamically
    name = "Codex on-chain"
    category = "onchain_amm"
    interval = 120

    def register(self):
        pass

    async def _query(self, phrase):
        q = ('{filterTokens(phrase:"%s",limit:200,filters:{liquidity:{gt:15000}}){results{'
             'token{symbol name address networkId}priceUSD volume24 liquidity}}}' % phrase)
        try:
            # shared helper (limiter + _parse); 402/403 = "no paid plan" → special skip
            j = await post_json(self.session, GRAPH, {"query": q}, headers={"Authorization": KEY})
        except aiohttp.ClientResponseError:
            self._blocked = True
            return []
        except RuntimeError as e:
            if "HTTP 402" in str(e) or "HTTP 403" in str(e):
                self._blocked = True
                return []
            raise
        return (j.get("data") or {}).get("filterTokens", {}).get("results", []) or []

    async def fetch(self):
        if not KEY or getattr(self, "_blocked", False):
            return []  # 402 without a paid plan — the connector sleeps instead of spamming
        seen = set()
        by_venue: dict[str, list] = {}
        failures = 0
        for ph in PHRASES:
            try:
                results = await self._query(ph)
            except RateLimited:
                raise  # 429 → Poller.run marks rate_limited, not a "successful" cycle
            except Exception:
                failures += 1
                continue
            for x in results:
                t = x.get("token") or {}
                net = t.get("networkId")
                nm = (t.get("name") or "")
                if net not in NET or nm.lower().startswith("few wrapped"):
                    continue  # skip wrappers/derivatives
                addr = t.get("address")
                key = (net, addr)
                if key in seen:
                    continue
                seen.add(key)
                u = normalize(t.get("symbol", ""), issuer_suffixes=("X", "ON", "D"))
                at = asset_type(u)
                if at == "other":
                    continue
                vid = NET[net][0]
                by_venue.setdefault(vid, []).append(dict(
                    symbol=f"{t.get('symbol')}@{net}", underlying=u, asset_type=at,
                    mid=fnum(x.get("priceUSD")), vol24h_usd=fnum(x.get("volume24"), 0.0),
                    oi_usd=fnum(x.get("liquidity")),  # for AMMs the OI column shows pool liquidity
                    spread_bps=None, funding_rate=None))
            await asyncio.sleep(0.3)
        if failures == len(PHRASES):
            raise RuntimeError("Codex: every phrase query failed")
        for vid, snaps in by_venue.items():
            vname, chain = NET[[k for k, v in NET.items() if v[0] == vid][0]][1:3]
            self.store.upsert_venue(vid, vname, "onchain_amm", chain, notes="Codex.io pool aggregate")
            await asyncio.to_thread(self.store.record_snaps, vid, snaps)
        return []
