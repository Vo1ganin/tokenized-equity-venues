# Architecture

One Python process: FastAPI + asyncio tasks. No queues, no workers, no external services —
public REST/WS in, DuckDB + parquet out, vanilla-JS frontend on top.

```
service/app/
├── api.py            FastAPI: /api/* + static; server-side caches (venues/assets/spark)
├── store.py          DuckDB + parquet; instrument registry, listings/delistings, daily rollup
├── known.py          underlying classification (stock/etf/index/...); symbol normalization;
│                     crypto-ticker collision guards (AMBIGUOUS_CRYPTO, DENY_STRIP)
├── reference.py      Massive: NYSE closes (basis) + US ticker catalog + market caps (coverage)
├── depth.py          hourly order-book pass → depth@±10/25/50 bps
├── pollers/
│   ├── base.py           poller loop, per-domain rate limiter, 429 backoff
│   ├── hyperliquid.py    HIP-3 auto-discovery of every deployer + HyperCore spot token-stocks
│   ├── ondo_perps.py     Ondo Perps public REST
│   ├── lighter.py        Lighter (zk-rollup)
│   ├── binance_fut.py    Binance TradFi perps (underlyingType filter)
│   ├── cex_fut.py        OKX/Bitget/Gate/KuCoin/MEXC/Kraken/Crypto.com/INTX/BingX/HTX
│   ├── cex_spot.py       Bybit/Binance bStocks/Gate/MEXC/Bitget + long tail (BitMart/LBank/XT/Bitrue/Gemini)
│   ├── kraken_ws.py      Kraken spot xStocks (WS v2, include_tokenized_assets)
│   ├── gecko_onchain.py  on-chain AMM spot via GeckoTerminal (category E)
│   └── codex_onchain.py  Codex.io alternative for category E (optional, paid key)
└── web/              frontend (vanilla JS, 7 tabs), self-hosted fonts
```

## Data model

Normalization is the core idea: one underlying (TSLA) ↔ many instruments
(TSLA on trade.xyz, TSLAX on Bybit, TSLAB on Binance, TSLASTOCK_USDT on MEXC,
PF_TSLAXUSD on Kraken Futures, TSLA-USD.P on Ondo Perps…). `known.normalize()`
strips venue prefixes/suffixes and issuer suffixes (X = Backed/xStocks, ON = Ondo,
B = Binance bStocks, D = Dinari) — but only when the stripped base is a known
tradfi ticker, which keeps crypto tickers (PLB, SQD, MAX…) from turning into
false stock matches.

```
venues:         venue_id, name, category, chain, status, first_seen, notes
instruments:    instrument_id (= venue:symbol), underlying, asset_type,
                listing_seen_at, delisted_at
ticker_snap:    ts, instrument_id, mid, vol24h_usd, oi_usd, spread_bps, funding_rate
depth_snap:     ts, instrument_id, spread_bps, depth@±10/25/50bps bid/ask
events:         ts, venue_id, listing|delisted, instrument_id
daily_metrics:  date, instrument_id, adv_usd, oi_avg, spread_avg, depth25_avg, funding_avg
```

Categories: `dex_perp` (A) · `cex_spot` (B) · `cex_fut` (C) · pre-IPO instruments are
tagged by `asset_type=pre_ipo` inside A/C (D) · `onchain_amm` (E).

## Polling

| What | Interval |
|---|---|
| tickers / volume / OI | 60 s (DEX) / 90 s (CEX), jittered |
| funding (where a separate call is needed) | 5 min, background aux task |
| order-book depth, full pass | hourly, spread out (~0.35 s/instrument) |
| instrument discovery | every cycle (symbol caches refresh hourly) |
| US reference catalog + NYSE closes | daily (Massive key required) |
| daily rollup, retention | 00:10 UTC |

One process-wide rate limiter per API domain (defaults ~4–8 rps, GeckoTerminal 0.5 rps).
429 → typed `RateLimited` → exponential per-venue backoff (1→2→5→10× interval).
Every long-lived task runs under a supervisor that logs and restarts it on crash.

## Discovery filters that matter

Most venues label their tradfi sections explicitly — discovery is a filter, not a hardcoded list:

- Binance Futures: `underlyingType ∈ {EQUITY, KR_EQUITY, PREMARKET, COMMODITY, INDEX}`
- Bybit: `symbolType=xstocks` (spot), `symbolType=stock` (futures)
- OKX: `instCategory=3`
- MEXC Futures: `*STOCK_USDT` suffix; MEXC spot: `"(Ondo)"` in fullName
- Kraken spot: pairs exist only over WS v2 with `include_tokenized_assets: true`
- Kraken Futures: `PF_<ticker>X USD` mask
- Crypto.com: `product_type=EQUITY` + `*IPOUSD-PERP`
- Hyperliquid: `perpDexs` → every HIP-3 deployer, no hardcoding; HyperCore spot
  tokens accepted only when an issuer suffix (X/D) strips to a known ticker
- Everywhere else: whitelist via the known-underlying classifier

## Data quality

- **Price validator / quarantine**: with the reference loaded, a stock/ETF quote
  further than 40% from the last NYSE close is quarantined (catches crypto-ticker
  collisions and dead pairs); indices/commodities are checked against static ranges.
- **Flap guard**: a partial venue response (<60% of known instruments) does not
  advance delisting counters; an instrument is `delisted` only after missing from
  5 consecutive complete responses.
- **Bootstrap vs listing**: a connector's first snap seeds the registry silently;
  only instruments appearing in later cycles produce listing events.
- **Self-reported volumes**: CEX 24h volumes are taken as reported and wash trading
  is not filtered out; order-book depth is the primary liquidity metric.

## Storage

DuckDB (`venues.duckdb`) + hourly parquet partitions under `data/snapshots/YYYY-MM-DD/`.
Minute snaps kept 30 days, depth 90 days, daily rollups and parquet — indefinitely.
Roughly 1.5 GB/month at ~2,500 instruments.

## API

`/api/venues` · `/api/venue/{id}` · `/api/asset/{ticker}` · `/api/assets` ·
`/api/coverage` · `/api/events` · `/api/history/{venue}/{symbol}?hours=` · `/api/health`

`/api/health` reports per-connector age and state (`live | partial | rate_limited |
api_error | stale`) plus separate states for aux components (OI/funding refreshers).

## Deployment

`docker compose up --build` → port 8090. Single container, one volume for data.
Behind a reverse proxy add auth if you expose it; the service itself has none.
