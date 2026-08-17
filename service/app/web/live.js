// shared code for the live frontend
const CAT = {
  dex_perp:  { tag: 'A · DEX perps',  color: 'var(--c-dexperp)', title: 'Equity perps, on-chain' },
  cex_spot:  { tag: 'B · CEX spot',   color: 'var(--c-cexspot)', title: 'Tokenized stocks on CEX, spot' },
  cex_fut:   { tag: 'C · CEX perps',  color: 'var(--c-cexfut)',  title: 'Equity perps & futures, CEX' },
  onchain_amm: { tag: 'E · On-chain AMM', color: 'var(--c-amm)', title: 'Tokenized stocks in AMM pools' },
};
const AT = { single_stock: 'stock', kr_stock: 'KR', etf: 'ETF', index: 'index', pre_ipo: 'pre-IPO', commodity: 'commodity', other: 'other' };

if (localStorage.getItem('vm-theme') === 'light') document.documentElement.classList.add('light');
function bindTheme() {
  document.querySelectorAll('.theme-btn').forEach(b => {
    const sync = () => b.textContent = document.documentElement.classList.contains('light') ? '☾ dark' : '☀ light';
    sync();
    b.onclick = () => {
      document.documentElement.classList.toggle('light');
      localStorage.setItem('vm-theme', document.documentElement.classList.contains('light') ? 'light' : 'dark');
      sync();
    };
  });
}

const fm = (v, d = 1) => {
  if (v == null || isNaN(v)) return '—';
  const a = Math.abs(v);
  if (a >= 1e9) return '$' + (v / 1e9).toFixed(2) + 'B';
  if (a >= 1e6) return '$' + (v / 1e6).toFixed(1) + 'M';
  if (a >= 1e3) return '$' + (v / 1e3).toFixed(0) + 'k';
  return '$' + v.toFixed(d);
};
const fpx = v => v == null ? '—' : (v >= 1000 ? v.toLocaleString('en', {maximumFractionDigits: 1}) : v >= 1 ? v.toFixed(2) : v.toPrecision(4));
const ffund = r => {
  if (r == null) return '<span class="mut">—</span>';
  const y = r * 3 * 365 * 100; // 8h rate → %/yr
  const c = y >= 0 ? 'up' : 'dn';
  return `<span class="${c}">${y >= 0 ? '+' : ''}${y.toFixed(1)}%</span>`;
};
const fbasis = v => v == null ? '<span class="mut">—</span>' : `<span class="${v>=0?'up':'dn'}">${v>=0?'+':''}${v.toFixed(0)} bps</span>`;
const fbps = v => v == null ? '<span class="mut">—</span>' : v.toFixed(1) + ' bps';
const dotc = s => (s === 'live' || s === 'partial') ? 'live' : (s === 'rate_limited' ? 'degr' : 'dead');
const STLABEL = {live:'live', partial:'partial', rate_limited:'rate-limited', api_error:'api error', stale:'stale', disabled:'disabled'};

function sparkSvg(v, color) {
  if (!v || v.length < 3) return '<span class="mut">—</span>';
  const w = 70, h = 18, mx = Math.max(...v), mn = Math.min(...v);
  const pts = v.map((y, i) => `${(i/(v.length-1))*w},${h-2-((y-mn)/(mx-mn||1))*(h-4)}`).join(' ');
  const up = v[v.length-1] >= v[0];
  return `<svg class="spark" width="${w}" height="${h}"><polyline points="${pts}" stroke="${color || (up ? 'var(--green)' : 'var(--red)')}"/></svg>`;
}
function esc(s){return String(s==null?'':s).replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));}
function eurl(s){return encodeURIComponent(String(s==null?'':s));}
async function j(url) { const r = await fetch(url); if (!r.ok) throw new Error(url + ' ' + r.status); return r.json(); }

// sorting (same as in the mockups)
function vmParse(s) {
  s = s.replace(/−/g, '-').replace(/[\s  ]/g, '');
  if (!s || s === '—' || s === '±') return -Infinity;
  const m = s.match(/-?\d+(\.\d+)?/); if (!m) return -Infinity;
  let v = parseFloat(m[0]);
  if (/B/.test(s)) v *= 1e9; else if (/M/.test(s)) v *= 1e6; else if (/k/i.test(s)) v *= 1e3;
  return v;
}
function bindSort(scope = document) {
  scope.querySelectorAll('table thead th').forEach(th => th.onclick = () => {
    const tb = th.closest('table').querySelector('tbody');
    const idx = [...th.parentNode.children].indexOf(th);
    const asc = th.classList.contains('sorted') && !th.classList.contains('asc');
    th.closest('table').querySelectorAll('th').forEach(x => x.classList.remove('sorted', 'asc'));
    th.classList.add('sorted'); if (asc) th.classList.add('asc');
    [...tb.children]
      .sort((a, b) => { const A = vmParse(a.children[idx]?.textContent || ''), B = vmParse(b.children[idx]?.textContent || ''); return asc ? A - B : B - A; })
      .forEach(r => tb.appendChild(r));
  });
}

// volume filter: rows must carry data-v (in $)
let minVol = 0;
function applyVol() {
  document.querySelectorAll('tr[data-v]').forEach(tr =>
    tr.classList.toggle('hid', parseFloat(tr.dataset.v) < minVol));
  document.querySelectorAll('section.cat[data-auto]').forEach(sec =>
    sec.style.display = sec.querySelectorAll('tr[data-v]:not(.hid)').length ? '' : 'none');
}
function bindVolFilter() {
  document.querySelectorAll('.fbtn[data-min]').forEach(b => b.onclick = () => {
    document.querySelectorAll('.fbtn[data-min]').forEach(x => x.classList.remove('act'));
    b.classList.add('act'); minVol = parseFloat(b.dataset.min) * 1e6; applyVol();
  });
  const inp = document.getElementById('vol-free');
  if (inp) inp.oninput = () => {
    document.querySelectorAll('.fbtn[data-min]').forEach(x => x.classList.remove('act'));
    minVol = (parseFloat(inp.value) || 0) * 1e6; applyVol();
  };
}

async function renderHealth() {
  try {
    const h = await j('api/health');
    const ok = Object.values(h.venues).filter(v => v.ok).length, all = Object.keys(h.venues).length;
    const el = document.getElementById('hdr-health');
    if (el) el.innerHTML = `<span class="dot ${ok === all ? 'live' : 'degr'}"></span>` +
      `${new Date().toISOString().slice(0, 19).replace('T', ' ')} UTC · <b>${ok}/${all} connectors ok</b><br>` +
      `instruments: ${h.n_instruments} · uptime ${(h.uptime_sec / 3600).toFixed(1)} h`;
  } catch (e) { /* ignore */ }
}
