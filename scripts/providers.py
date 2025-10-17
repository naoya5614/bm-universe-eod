import os, time, json, requests, hashlib, math, datetime as dt
import yfinance as yf

TIINGO_KEY = os.getenv("TIINGO_API_KEY")
ALPHA_KEY  = os.getenv("ALPHAVANTAGE_API_KEY")

CACHE_DIR = os.path.join(os.path.dirname(__file__), "..", "cache", "alpha")
os.makedirs(CACHE_DIR, exist_ok=True)

class RateLimit(Exception): pass

# ---------- Price / FX ----------
def price_tiingo(ticker):
    if not TIINGO_KEY: raise RateLimit("no-tiingo-key")
    url = f"https://api.tiingo.com/tiingo/daily/{ticker}/prices"
    params = {"token": TIINGO_KEY, "resampleFreq": "daily", "format":"json"}
    r = requests.get(url, params=params, timeout=20)
    if r.status_code == 429: raise RateLimit("tiingo-429")
    r.raise_for_status()
    data = r.json()
    return float(data[-1]["close"]) if data else None

def price_yfinance(ticker):
    t = yf.Ticker(ticker)
    hist = t.history(period="5d", auto_adjust=False)
    if hist is None or hist.empty: return None
    return float(hist["Close"].dropna().iloc[-1])

def usd_jpy_yfinance():
    j = yf.Ticker("JPY=X").history(period="5d")
    return float(j["Close"].dropna().iloc[-1])

def next_event_yfinance(ticker):
    t = yf.Ticker(ticker)
    try:
        cal = t.calendar
        # pandas DF (index) 想定: "Earnings Date"
        if cal is not None and not cal.empty:
            if "Earnings Date" in cal.index:
                v = cal.loc["Earnings Date"].values[0]
                s = str(v)
                return s[:10]
    except Exception:
        pass
    return None

# ---------- Alpha Vantage: OVERVIEW / statements (with cache) ----------
def _alpha_get(function, symbol):
    if not ALPHA_KEY:
        raise RateLimit("no-alpha-key")
    url = "https://www.alphavantage.co/query"
    params = {"function": function, "symbol": symbol, "apikey": ALPHA_KEY}
    r = requests.get(url, params=params, timeout=30)
    # Alpha Vantage: 5 req/min, 25 req/day（無料）
    if r.status_code == 429 or (r.text.strip().startswith("{"Note":")):
        raise RateLimit("alpha-rl")
    r.raise_for_status()
    return r.json()

def alpha_overview(symbol):
    return _alpha_get("OVERVIEW", symbol)

def _cache_path(symbol, kind):
    fn = f"{symbol.replace('/', '_')}_{kind}.json"
    return os.path.join(CACHE_DIR, fn)

def _load_cache(symbol, kind):
    p = _cache_path(symbol, kind)
    if os.path.exists(p):
        with open(p, "r") as f:
            js = json.load(f)
        return js
    return None

def _save_cache(symbol, kind, js):
    p = _cache_path(symbol, kind)
    with open(p, "w") as f:
        json.dump({"_ts": dt.datetime.utcnow().isoformat(), "payload": js}, f)

def alpha_statements(symbol, refresh=False, refresh_days=30, budget=None):
    """Return dict with 'income','balance','cashflow' JSON payloads.
       Uses disk cache. If refresh=True or cache is older than refresh_days, re-fetch.
    """
    out = {}
    now = dt.datetime.utcnow()
    for kind, func in (("income","INCOME_STATEMENT"), ("balance","BALANCE_SHEET"), ("cashflow","CASH_FLOW")):
        cached = _load_cache(symbol, kind)
        need = True
        if cached and not refresh:
            try:
                ts = dt.datetime.fromisoformat(cached.get("_ts","").replace("Z",""))
                if (now - ts).days < refresh_days:
                    need = False
            except Exception:
                pass
        if need:
            if budget is not None and budget.remaining() <= 0:
                # budget exhausted -> return cached or empty
                if cached:
                    out[kind] = cached["payload"]
                    continue
                else:
                    out[kind] = None
                    continue
            js = _alpha_get(func, symbol)
            _save_cache(symbol, kind, js)
            if budget is not None:
                budget.consume(1)
            out[kind] = js
            # be nice to rate limits
            time.sleep(13)  # ~5/min
        else:
            out[kind] = cached["payload"]
    return out

class AlphaBudget:
    def __init__(self, daily_limit=24):
        self._remain = int(daily_limit)
    def remaining(self): return self._remain
    def consume(self, n): self._remain = max(0, self._remain - int(n))

# ---------- Helpers to extract series ----------
def latest_annual(js, path):
    try:
        arr = js.get(path, [])
        return arr[0] if arr else None
    except Exception:
        return None

def annual_series(js, path, n=6):
    try:
        arr = js.get(path, [])
        return arr[:n] if arr else []
    except Exception:
        return []

def to_float(x):
    try:
        return None if x in (None,"", "None") else float(x)
    except Exception:
        return None
