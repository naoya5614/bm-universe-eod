import os
import time
import json
import random
import requests
import datetime as dt
from typing import Optional

import yfinance as yf
from pandas_datareader import data as pdr

# ---- API Keys ----
TIINGO_KEY = os.getenv("TIINGO_API_KEY")
ALPHA_KEY = os.getenv("ALPHAVANTAGE_API_KEY")

# ---- Cache ----
CACHE_DIR = os.path.join(os.path.dirname(__file__), "..", "cache", "alpha")
os.makedirs(CACHE_DIR, exist_ok=True)


class RateLimit(Exception):
    """Raised when a provider signals rate limiting."""
    pass


# ---------- Utilities ----------
def yf_symbol(ticker: str) -> str:
    """
    Normalize symbol for Yahoo Finance.
    Examples:
      BRK.B -> BRK-B
      BF.B  -> BF-B
    """
    if "." in ticker:
        return ticker.replace(".", "-")
    return ticker


def _backoff_sleep(attempt: int, base: float = 1.0, cap: float = 16.0):
    """Exponential backoff with jitter."""
    sleep = min(cap, base * (2 ** attempt)) + random.uniform(0, 0.5)
    time.sleep(sleep)


def _to_float(x):
    try:
        if x in (None, "", "None"):
            return None
        return float(x)
    except Exception:
        return None


# ---------- Price / FX ----------
def price_tiingo(ticker: str) -> Optional[float]:
    """Return latest daily close via Tiingo. Raises RateLimit on 429 or missing key."""
    if not TIINGO_KEY:
        raise RateLimit("no-tiingo-key")
    url = f"https://api.tiingo.com/tiingo/daily/{ticker}/prices"
    params = {"token": TIINGO_KEY, "resampleFreq": "daily", "format": "json"}
    r = requests.get(url, params=params, timeout=20)
    if r.status_code == 429:
        raise RateLimit("tiingo-429")
    r.raise_for_status()
    data = r.json()
    return float(data[-1]["close"]) if data else None


def price_stooq(ticker: str) -> Optional[float]:
    """
    Fallback daily close via Stooq (free). Stooq uses Yahoo-like tickers for US stocks.
    """
    try:
        df = pdr.DataReader(yf_symbol(ticker), "stooq")
        if df is None or df.empty:
            return None
        return float(df["Close"].dropna().iloc[-1])
    except Exception:
        return None


_YF_SESSION = requests.Session()
# Give Yahoo a sane UA; helps with some environments
_YF_SESSION.headers.update({"User-Agent": "Mozilla/5.0 (compatible; BloomoEOD/1.0)"})


def price_yfinance(ticker: str, retries: int = 3) -> Optional[float]:
    sym = yf_symbol(ticker)
    for attempt in range(retries):
        try:
            t = yf.Ticker(sym, session=_YF_SESSION)
            hist = t.history(period="5d", auto_adjust=False)
            if hist is None or hist.empty:
                # Try stooq fallback immediately if "possibly delisted" or empty
                return price_stooq(ticker)
            return float(hist["Close"].dropna().iloc[-1])
        except Exception as e:
            msg = str(e)
            # 429 / rate-limiting detection
            if "Too Many Requests" in msg or "429" in msg:
                _backoff_sleep(attempt)
                continue
            # Other transient issues -> brief backoff then retry
            _backoff_sleep(attempt, base=0.5, cap=4.0)
    # Final fallback
    return price_stooq(ticker)


def usd_jpy_yfinance(retries: int = 3) -> Optional[float]:
    sym = "JPY=X"
    for attempt in range(retries):
        try:
            j = yf.Ticker(sym, session=_YF_SESSION).history(period="5d")
            if j is None or j.empty:
                # Stooq FX pair: not universally available; return None to let callers handle.
                return None
            return float(j["Close"].dropna().iloc[-1])
        except Exception as e:
            if "Too Many Requests" in str(e) or "429" in str(e):
                _backoff_sleep(attempt)
                continue
            _backoff_sleep(attempt, base=0.5, cap=4.0)
    return None


def next_event_yfinance(ticker: str, retries: int = 1) -> Optional[str]:
    """
    Conservative: try once. If 429 or any error -> return N/A (None).
    We don’t want to burn retries on a non-core field.
    """
    sym = yf_symbol(ticker)
    for attempt in range(retries):
        try:
            t = yf.Ticker(sym, session=_YF_SESSION)
            cal = t.calendar
            if cal is not None and not cal.empty and "Earnings Date" in cal.index:
                v = cal.loc["Earnings Date"].values[0]
                s = str(v)
                return s[:10]
            return None
        except Exception as e:
            # If rate-limited, no retry beyond minimal
            if "Too Many Requests" in str(e) or "429" in str(e):
                return None
            return None
    return None


# ---------- Alpha Vantage: OVERVIEW / statements (with cache) ----------
def _alpha_get(function: str, symbol: str):
    """Low-level Alpha Vantage GET with basic rate-limit detection."""
    if not ALPHA_KEY:
        raise RateLimit("no-alpha-key")
    url = "https://www.alphavantage.co/query"
    params = {"function": function, "symbol": symbol, "apikey": ALPHA_KEY}
    r = requests.get(url, params=params, timeout=30)

    # Alpha Vantage free tier: ~5 req/min, ~25 req/day
    text = r.text.strip() if isinstance(r.text, str) else ""
    if r.status_code == 429 or text.startswith('{"Note":'):
        # Daily/minute limit note
        raise RateLimit("alpha-rl")
    r.raise_for_status()
    return r.json()


def alpha_overview(symbol: str):
    return _alpha_get("OVERVIEW", symbol)


def _cache_path(symbol: str, kind: str):
    fn = f"{symbol.replace('/', '_')}_{kind}.json"
    return os.path.join(CACHE_DIR, fn)


def _load_cache(symbol: str, kind: str):
    p = _cache_path(symbol, kind)
    if os.path.exists(p):
        with open(p, "r") as f:
            js = json.load(f)
        return js
    return None


def _save_cache(symbol: str, kind: str, js: dict):
    p = _cache_path(symbol, kind)
    with open(p, "w") as f:
        json.dump({"_ts": dt.datetime.utcnow().isoformat(), "payload": js}, f)


class AlphaBudget:
    """Simple daily budget tracker for Alpha Vantage calls."""

    def __init__(self, daily_limit: int = 24):
        self._remain = int(daily_limit)

    def remaining(self) -> int:
        return self._remain

    def consume(self, n: int):
        self._remain = max(0, self._remain - int(n))


def alpha_statements(
    symbol: str,
    refresh: bool = False,
    refresh_days: int = 30,
    budget: Optional[AlphaBudget] = None
):
    """
    Return dict with 'income','balance','cashflow' JSON payloads.
    Uses disk cache. If refresh=True or cache is older than refresh_days, re-fetch.
    Respects optional AlphaBudget (consumes 1 per call).
    """
    out = {}
    now = dt.datetime.utcnow()
    for kind, func in (
        ("income", "INCOME_STATEMENT"),
        ("balance", "BALANCE_SHEET"),
        ("cashflow", "CASH_FLOW"),
    ):
        cached = _load_cache(symbol, kind)
        need = True
        if cached and not refresh:
            try:
                ts = dt.datetime.fromisoformat(cached.get("_ts", "").replace("Z", ""))
                if (now - ts).days < refresh_days:
                    need = False
            except Exception:
                pass

        if need:
            if budget is not None and budget.remaining() <= 0:
                # Budget exhausted -> return cached (if exists) or None
                out[kind] = cached["payload"] if cached else None
                continue
            js = _alpha_get(func, symbol)
            _save_cache(symbol, kind, js)
            if budget is not None:
                budget.consume(1)
            out[kind] = js
            # be nice to minute rate-limit
            time.sleep(13)  # ~5/min
        else:
            out[kind] = cached["payload"]
    return out


# ---------- Helpers to extract/convert ----------
def latest_annual(js: dict, path: str):
    try:
        arr = js.get(path, [])
        return arr[0] if arr else None
    except Exception:
        return None


def annual_series(js: dict, path: str, n: int = 6):
    try:
        arr = js.get(path, [])
        return arr[:n] if arr else []
    except Exception:
        return []


def to_float(x):
    try:
        if x in (None, "", "None"):
            return None
        return float(x)
    except Exception:
        return None
