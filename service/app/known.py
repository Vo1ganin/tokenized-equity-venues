"""Underlying ticker classification. Seeded from a live sweep of venue APIs (June 2026),
extended by the dynamic US reference (reference.py) once loaded."""

STOCKS = set("""AAOI AAPL ADBE AMAT AMD AMZN ARM ASML ASTS AVGO AXTI BABA BBX BE BMNR BRKB BX CBRS COHR COIN
COST CRCL CRDO CRM CRWD CRWV CSCO DELL DIS DKNG EBAY FLNC GLW GME GOOG GOOGL HD HIMS HOOD HPE IBM INTC IREN
JPM LITE LLY MCD META MRVL MSFT MSTR MU NBIS NFLX NOK NOW NVDA NVO ONDS ORCL PAYP PL PLTR QCOM RIVN RKLB
SMCI SNDK TSLA TSM UBER UNH V WDC WMT ZM AMC COP OUST DRAM SE SHOP SQ PYPL NIO XOM CVX JNJ PG MA ABBV
HANMI BIRD BB SBET BA BAC C KO GE GEV LMT RTX NKE SBUX OXY PANW INTU LRCX TXN VZ WFC SNOW SPOT IONQ APLD APP
RDDT RDW LUNR FUTU JD PDD CIEN FIG VRT NOK ABT AZN ANET CEG CLSK CMCSA CORZ DHR ETN HON HUT KLAC LNG MARA MDT
MRK NET NLR OKLO OPEN PEP PFE PM PWR RBLX RCAT RIOT SMR STRC TER TMO TMUS WBD WULF BTBT BTGO DFDV ENHA FAAA
FGDL FLBL FLQM FSML GLXY JAAA JPST KRAQ MOO SATA VIDA YLDE AMBR BITX COPX ABTX ACN INTC""".split())

ETFS = set("""EWJ EWY EWZ EWT IWM QQQ SPY SOXL URNM USAR UVXY XLE GLD SLV VOO KWEB GDX SMH SOXX TQQQ
VTI VUG VXUS VGK SCHF SGOV IEMG IJR FEZ PPLT URA XOP EWG EWQ EWU""".split())
INDICES = set("""SPX NDX DJI VIX DEFI TRADFI SP500 JP225 KR200 XYZ100 NIKKEI DAX FTSE
MAG7 BIOTECH DEFENSE ENERGY INFOTECH NUCLEAR ROBOT SEMIS USTECH USBOND""".split())
PRE_IPO = set("SPCX OPENAI ANTHROPIC ANTH PERPLEXITY XAI CEREBRAS CURSOR STRIPE ANDURIL REVOLUT MONZO QNTX".split())
COMMODITIES = set("XAU XAG XPT XPD CL BZ NATGAS COPPER GOLD SILVER BRENTOIL WTICRUDE OIL ALUMINIUM URANIUM COCOA COFFEE PLATINUM PALLADIUM WHEAT SOY USOIL WTI".split())
FX = set("EUR GBP JPY AUD CHF CAD".split())
KR_EQUITY = set("SAMSUNG SKHYNIX HYUNDAI SMSN SKHX SKHY".split())

_SUFFIXES = ("USDTM", "USDT", "USDC", "USD", "PERP", "STOCK")
# symbol synonyms → canonical underlying (for cross-venue joins)
SYNONYMS = {"SMSN": "SAMSUNG", "SKHX": "SKHYNIX", "SKHY": "SKHYNIX", "NOKIA": "NOK", "SPACEX": "SPCX",
            "ANTH": "ANTHROPIC", "GOLD": "XAU", "SILVER": "XAG", "PLATINUM": "XPT",
            "PALLADIUM": "XPD", "BRENTOIL": "BZ", "BRENT": "BZ", "WTICRUDE": "CL", "USOIL": "CL", "WTI": "CL",
            "SP500": "SPX", "US500": "SPX", "US100": "NDX", "BRK.B": "BRKB"}
# dynamic US ticker reference (populated by reference.py after the Massive catalog loads)
DYN_CS: set = set()
DYN_ETF: set = set()

def set_us_reference(cs: set, etf: set):
    DYN_CS.clear(); DYN_CS.update(cs)
    DYN_ETF.clear(); DYN_ETF.update(etf)
# crypto tickers that would falsely turn into stocks when a suffix is stripped (MAX→MA, VON→V…)
_DENY_STRIP = {"MAX", "VON", "BXX", "WMTX", "SQD", "PLB", "SEON", "COPX",
               "BX", "QNTX", "STXX", "SNXX"}  # BX≠B, QNTX≠QNT, STXX≠STX, SNXX≠SNX


def normalize(symbol: str, issuer_suffixes: tuple = ("X", "ON")) -> str:
    """TSLAXUSDT→TSLA, TSLASTOCK_USDT→TSLA, PF_TSLAXUSD→TSLA, xyz:AAPL→AAPL.

    issuer_suffixes — which issuer suffixes to strip ON THIS venue:
    X (Backed/xStocks), ON (Ondo), B (Binance bStocks), D (Dinari).
    Stripping B/D globally would be unsafe: crypto tickers PLB/SQD would
    produce false PL/SQ matches."""
    s = symbol.upper()
    if ":" in s:
        s = s.split(":", 1)[1]
    for pre in ("PF_", "FF_"):
        if s.startswith(pre):
            s = s[len(pre):]
    s = s.split("-")[0].split("_")[0].split("/")[0]
    changed = True
    while changed:
        changed = False
        for suf in _SUFFIXES:
            if s.endswith(suf) and len(s) > len(suf):
                s = s[: -len(suf)]
                changed = True
                break
    if s in _DENY_STRIP:
        return SYNONYMS.get(s, s)
    for suf in issuer_suffixes:
        base = s[: -len(suf)]
        if s.endswith(suf) and base and (base in (STOCKS | ETFS | INDICES | PRE_IPO | KR_EQUITY)
                                         or base in DYN_CS or base in DYN_ETF):
            s = base
            break
    return SYNONYMS.get(s, s)


def asset_type(underlying: str) -> str:
    u = underlying.upper()
    if u in STOCKS:
        return "single_stock"
    if u in KR_EQUITY:
        return "kr_stock"
    if u in ETFS:
        return "etf"
    if u in INDICES:
        return "index"
    if u in PRE_IPO:
        return "pre_ipo"
    if u in COMMODITIES:
        return "commodity"
    if u in FX:
        return "fx"
    if u in DYN_CS:
        return "single_stock"
    if u in DYN_ETF:
        return "etf"
    return "other"


# tickers where a crypto asset and a stock collide too closely for the price validator
# (Quant vs Quantinuum): on unmarked exchanges a plain ticker is treated as crypto;
# marked listings (MEXC *STOCK etc.) do not go through is_tradfi
AMBIGUOUS_CRYPTO = set("""QNT LTC BCH ADA SOL XRP ETH BNB LINK SUI DOGE AVAX DOT TRX ATOM UNI
APE APT NEAR FIL ICP HBAR ALGO VET EOS AAVE MKR COMP SNX CRV SUSHI GRT ENA ENS IMX SAND MANA AXS
GALA CHZ FLOW EGLD XTZ THETA KAVA ZEC DASH XMR NEO IOTA QTUM ONT ZIL BAT ZRX KNC REN STORJ OCEAN
BAL LRC YFI UMA BAND NMR PERP DYDX GMX OP ARB STRK JTO PYTH TIA SEI WLD JUP W ONDO ENA PENGU TON
TAO BONK WIF PEPE FLOKI SHIB FART HYPE PUMP""".split())


def is_tradfi(underlying: str) -> bool:
    if underlying in AMBIGUOUS_CRYPTO:
        return False
    return asset_type(underlying) != "other"
