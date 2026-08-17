"""Depth@±10/25/50 bps: one full pass per hour, spread out over time.
Measures order-book notional within X bps of mid, bid and ask separately."""
import asyncio
import datetime as dt
import logging
from collections import Counter
import aiohttp
from .pollers.base import fnum, DEPTH_INTERVAL, get_json, post_json, RateLimited

log = logging.getLogger("vm")
BPS = (10, 25, 50)


def calc_depth(bids, asks):
    """bids/asks: [(price, size)]. Returns a dict with depth and spread."""
    if not bids or not asks:
        return None
    bb, ba = bids[0][0], asks[0][0]
    if not bb or not ba or bb <= 0:
        return None
    mid = (bb + ba) / 2
    out = {"spread_bps": (ba - bb) / mid * 1e4}
    for bps in BPS:
        lo, hi = mid * (1 - bps / 1e4), mid * (1 + bps / 1e4)
        out[f"d{bps}_bid"] = sum(p * s for p, s in bids if p >= lo)
        out[f"d{bps}_ask"] = sum(p * s for p, s in asks if p <= hi)
    return out


def _pairs(levels, pk=0, sk=1):
    out = []
    for lv in levels or []:
        if isinstance(lv, dict):
            p, s = fnum(lv.get("px") or lv.get("price")), fnum(lv.get("sz") or lv.get("size"))
        else:
            p, s = fnum(lv[pk]), fnum(lv[sk])
        if p and s:
            out.append((p, s))
    return out


async def fetch_book(sess, venue_id, symbol):
    """→ (bids, asks). Uses the shared get_json/post_json (limiter + 429→RateLimited).
    RateLimited propagates for accounting; other errors → None."""
    if venue_id.startswith("hip3:"):
        j = await post_json(sess, "https://api.hyperliquid.xyz/info", {"type": "l2Book", "coin": symbol})
        lv = j.get("levels", [[], []])
        return _pairs(lv[0]), _pairs(lv[1])
    if venue_id == "binance_fut":
        j = await get_json(sess, "https://fapi.binance.com/fapi/v1/depth", params={"symbol": symbol, "limit": 500})
        return _pairs(j.get("bids")), _pairs(j.get("asks"))
    if venue_id == "binance_bstocks":
        j = await get_json(sess, "https://api.binance.com/api/v3/depth", params={"symbol": symbol, "limit": 500})
        return _pairs(j.get("bids")), _pairs(j.get("asks"))
    if venue_id == "okx_fut":
        j = await get_json(sess, "https://www.okx.com/api/v5/market/books", params={"instId": symbol, "sz": "200"})
        d = (j.get("data") or [{}])[0]
        return _pairs(d.get("bids")), _pairs(d.get("asks"))
    if venue_id == "bybit_spot":
        j = await get_json(sess, "https://api.bybit.com/v5/market/orderbook",
                           params={"category": "spot", "symbol": symbol, "limit": 200})
        d = j.get("result", {})
        return _pairs(d.get("b")), _pairs(d.get("a"))
    if venue_id == "gate_spot":
        j = await get_json(sess, "https://api.gateio.ws/api/v4/spot/order_book",
                           params={"currency_pair": symbol, "limit": 100})
        return _pairs(j.get("bids")), _pairs(j.get("asks"))
    if venue_id == "mexc_spot":
        j = await get_json(sess, "https://api.mexc.com/api/v3/depth", params={"symbol": symbol, "limit": 100})
        return _pairs(j.get("bids")), _pairs(j.get("asks"))
    if venue_id == "bitget_spot":
        j = await get_json(sess, "https://api.bitget.com/api/v2/spot/market/orderbook",
                           params={"symbol": symbol, "limit": "150"})
        d = j.get("data", {})
        return _pairs(d.get("bids")), _pairs(d.get("asks"))
    if venue_id == "aster":
        j = await get_json(sess, "https://fapi.asterdex.com/fapi/v1/depth", params={"symbol": symbol, "limit": 500})
        return _pairs(j.get("bids")), _pairs(j.get("asks"))
    return None


async def depth_loop(store):
    await asyncio.sleep(180)  # let tickers load first
    while True:
        started = dt.datetime.utcnow()
        targets = [(f"{s['venue_id']}:{s['symbol']}", s["venue_id"], s["symbol"]) for s in store.latest_list()]
        n_ok = 0
        err_by_venue: Counter = Counter()
        rl_by_venue: Counter = Counter()
        cooldown_venues: set = set()  # venue returned 429 → leave it alone for the rest of the pass
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=20)) as sess:
            for iid, vid, sym in targets:
                if vid in cooldown_venues:
                    continue
                try:
                    book = await fetch_book(sess, vid, sym)
                except RateLimited:
                    rl_by_venue[vid] += 1
                    cooldown_venues.add(vid)
                    log.warning("depth: %s rate-limited, skipping the venue for the rest of this pass", vid)
                    continue
                except Exception:
                    err_by_venue[vid] += 1
                    continue
                if book:
                    d = calc_depth(*book)
                    if d:
                        await asyncio.to_thread(store.record_depth, iid, d)
                        n_ok += 1
                await asyncio.sleep(0.35)  # spread the hour out: ~1200 instruments ≈ 7 min — fine
        msg = "depth pass: %d/%d in %.0fs" % (n_ok, len(targets), (dt.datetime.utcnow() - started).total_seconds())
        if rl_by_venue:
            msg += " | rate-limited: " + ", ".join(f"{v}×{n}" for v, n in rl_by_venue.most_common(5))
        if err_by_venue:
            msg += " | err: " + ", ".join(f"{v}×{n}" for v, n in err_by_venue.most_common(5))
        log.info(msg)
        await asyncio.sleep(max(60, DEPTH_INTERVAL - (dt.datetime.utcnow() - started).total_seconds()))
