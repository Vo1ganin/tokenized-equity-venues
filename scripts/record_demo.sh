#!/usr/bin/env bash
# Record a static snapshot of the live dashboard into demo/ — a frozen copy that
# works on any static host (GitHub Pages): the frontend fetches the same relative
# api/* paths, served as plain files.
#
# Usage: ./scripts/record_demo.sh [base_url]   (default http://127.0.0.1:8090)
set -euo pipefail

BASE="${1:-http://127.0.0.1:8090}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
DEMO="$ROOT/demo"
WEB="$ROOT/service/app/web"

rm -rf "$DEMO"
mkdir -p "$DEMO/api" "$DEMO/static"

# frontend: pages at the demo root, assets under static/ (same layout the service serves)
cp "$WEB"/*.html "$DEMO/"
cp -R "$WEB"/live.js "$WEB"/style.css "$WEB"/fonts.css "$WEB"/fonts "$DEMO/static/"

# top-level API responses
for ep in health venues assets events coverage; do
  curl -sf "$BASE/api/$ep" -o "$DEMO/api/$ep"
done

# per-venue responses
mkdir -p "$DEMO/api/venue"
python3 - "$BASE" "$DEMO" <<'PY'
import json, sys, urllib.request, urllib.parse
base, demo = sys.argv[1], sys.argv[2]
venues = json.load(urllib.request.urlopen(f"{base}/api/venues"))
for v in venues:
    vid = v["venue_id"]
    data = urllib.request.urlopen(f"{base}/api/venue/{urllib.parse.quote(vid)}").read()
    # ':' is not checkout-safe on Windows — store as '_' (the HTML shim rewrites fetches)
    with open(f"{demo}/api/venue/{vid.replace('/', '_').replace(':', '_')}", "wb") as f:
        f.write(data)
# per-asset responses for the top 100 underlyings by volume
import os
os.makedirs(f"{demo}/api/asset", exist_ok=True)
assets = json.load(urllib.request.urlopen(f"{base}/api/assets"))
for a in assets[:100]:
    u = a["underlying"]
    data = urllib.request.urlopen(f"{base}/api/asset/{urllib.parse.quote(u)}").read()
    with open(f"{demo}/api/asset/{u}", "wb") as f:
        f.write(data)
print(f"recorded {len(venues)} venues, {min(len(assets),100)} assets")
PY

# a banner so viewers know it is a frozen snapshot
STAMP=$(date -u +"%Y-%m-%d %H:%M UTC")
for f in "$DEMO"/*.html; do
  python3 - "$f" "$STAMP" <<'PY'
import sys
p, stamp = sys.argv[1], sys.argv[2]
s = open(p, encoding="utf-8").read()
banner = ('<div style="background:#8b6c1a22;border-bottom:1px solid #8b6c1a55;color:#c9a227;'
          'font:11px \'JetBrains Mono\',monospace;text-align:center;padding:4px 8px">'
          f'static demo snapshot · recorded {stamp} · data is frozen</div>'
          # snapshot files use '_' where venue ids contain ':' (Windows-safe filenames)
          '<script>const _f=window.fetch.bind(window);window.fetch=(u,o)=>_f('
          "typeof u==='string'?u.replace(/api\\/venue\\/([^?]*)/,(m,p)=>'api/venue/'+p.replace(/[:\\/]/g,'_')):u,o);"
          '</script>')
s = s.replace("<body>", "<body>" + banner, 1)
open(p, "w", encoding="utf-8").write(s)
PY
done

echo "demo/ ready ($(du -sh "$DEMO" | cut -f1)) — serve with: python3 -m http.server -d demo"
