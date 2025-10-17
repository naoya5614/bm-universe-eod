import os
import time
import json
import random
import requests
import datetime as dt
from typing import Optional, Dict, List, Tuple

import pandas as pd
import yfinance as yf
from pandas_datareader import data as pdr  # Stooq fallback

# ---- API Keys ----
TIINGO_KEY = os.getenv("TIINGO_API_KEY")
ALPHA_KEY = os.getenv("ALPHAVANTAGE_API_KEY")

# ---- Tunables (env) ----
YF_BATCH_SIZE = int(os.getenv("YF_BATCH_SIZE", "80"))
YF_BATCH_SLEEP = float(os.getenv("YF_BATCH_SLEEP", "8"))   # sec between batches
YF_INFO_BUDGET = int(os.getenv("YF_INFO_BUDGET", "120"))   # per run
YF_EVENT_BUDGET = int(os.getenv("YF_EVENT_BUDGET", "50"))  # per run

# ---- Cache ----
BASE_DIR = os.path.dirname(__file__)
CACHE_DIR = os.path.join(BASE_DIR, "..", "cache")
os.makedirs(CACHE_DIR, exist_ok=True)
ALPHA_STMT_DIR = os.path.join(CACHE_DIR, "alpha")
os.makedirs(ALPHA_STMT_DIR, exist_ok=True)

# ---- Errors ----
class RateLimit(Exception):
    """Raised when a provider signals rate limiting."""
    pass


# ---------- Utilities ----------
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


def yf_symbol(ticker: str) -> str:
    """Normalize for Yahoo (BRK.B -> BRK-B etc.)."""
    return ticker.replace(".", "-")


# ---------- Yahoo batched prices ----------
def yf_download_prices(tickers: List[str]) -> Dict[str, Optional[float]]:
    """
    Use yf.download to fetch last 5d EOD prices in batches.
    This endpoint is CSV-like and less prone to 429 than quoteSummary.
    """
    out: Dict[str, Optional[float]] = {t: None for t in tickers}
    if not tickers:
        return out

    # Normalize symbols for Yahoo
    y_syms = [yf_symbol(t) for t in tickers]

    try:
        df = yf.download(
            tickers=" ".join(y_syms),
            period="5d",
            interval="1d",
            group_by="ticker",
            auto_adjust=False,
            threads=False,
            progress=False,
        )
        # If single ticker, df is a normal frame
        if isinstance(df.columns, pd.MultiIndex):
            # Multi-ticker
            for t, y in zip(tickers, y_syms):
                try:
                    sub = df[y]["Close"].dropna()
                    if not sub.empty:
                        out[t] = float(sub.iloc[-1])
                except Exception:
                    pass
        else:
            # Single ticker
            try:
                sub = df["Close"].dropna()
                if not sub.empty:
                    out[tickers[0]] = float(sub.iloc[-1])
            except Exception:
                pass
    except Exception:
        # batch failed -> return all None; caller will fallback
        pass

    return out


def yf_download_prices_batched(all_tickers: List[str]) -> Dict[str, Optional[float]]:
    prices: Dict[str, Optional[float]] = {}
    n = len(all_tickers)
    if n == 0:
        return prices

    for i in range(0, n, YF_BATCH_SIZE):
        batch = all_tickers[i:i + YF_BATCH_SIZE]
        res = yf_download_prices(batch)
        prices.update(res)
        # polite pause
        if i + YF_BATCH_SIZE < n:
            time.sleep(YF_BATCH_SLEEP)
    return prices


# ---------- Other price providers ----------
def price_tiingo(ticker: str) -> Optional[float]:
    """Tiingo official EOD close."""
    if not TIINGO_KEY:
        raise RateLimit("no-tiingo-key")
    url = f"https://api.tiingo.com/tiingo/daily/{ticker}/prices"
    params = {"token": TIINGO_KEY, "resampleFreq": "daily", "format": "json"}
    r = requests.get(url, params=params, timeout=20)
    if r.status_code == 429:
        raise RateLimit("tiingo-429")
    r.raise_for_status()
    js = r.json()
    return float(js[-1]["close"]) if js else None


def price_alpha_global_quote(ticker: str, retries: int = 3) -> Optional[float]:
    """Alpha Vantage GLOBAL_QUOTE; 1 call / ticker."""
    if not ALPHA_KEY:
        raise RateLimit("no-alpha-key")
    for attempt in range(retries):
        url = "https://www.alphavantage.co/query"
        params = {"function": "GLOBAL_QUOTE", "symbol": ticker, "apikey": ALPHA_KEY}
        r = requests.get(url, params=params, timeout=20)
        text = r.text.strip()
        if r.status_code == 429 or text.startswith('{"Note":'):
            raise RateLimit("alpha-rl")
        try:
            r.raise_for_status()
            js = r.json()
            q = js.get("Global Quote", {})
            p = q.get("05. price")
            if p:
                return float(p)
        except Exception:
            pass
        _backoff_sleep(attempt, base=2.0, cap=8.0)
    return None


def price_stooq(ticker: str) -> Optional[float]:
    """Stooq fallback (free)."""
    try:
        df = pdr.DataReader(yf_symbol(ticker), "stooq")
        if df is None or df.empty:
            return None
        return float(df["Close"].dropna().iloc[-1])
    except Exception:
        return None


def fill_missing_prices(prices: Dict[str, Optional[float]]) -> Dict[str, Optional[float]]:
    """
    For tickers with None price, try Tiingo -> Alpha -> Stooq.
    """
    out = dict(prices)
    for t, v in prices.items():
        if v is not None:
            continue
        # Tiingo
        try:
            out[t] = price_tiingo(t)
            if out[t] is not None:
                continue
        except RateLimit:
            pass
        except Exception:
            pass
        # Alpha
        try:
            out[t] = price_alpha_global_quote(t)
            if out[t] is not None:
                continue
        except RateLimit:
            pass
        except Exception:
            pass
        # Stooq
        out[t] = price_stooq(t)
    return out


# ---------- FX USD/JPY ----------
def fx_usdjpy_alpha(retries: int = 3) -> Optional[float]:
    """Alpha CURRENCY_EXCHANGE_RATE (USDJPY)."""
    if not ALPHA_KEY:
        raise RateLimit("no-alpha-key")
    for attempt in range(retries):
        url = "https://www.alphavantage.co/query"
        params = {
            "function": "CURRENCY_EXCHANGE_RATE",
            "from_currency": "USD",
            "to_currency": "JPY",
            "apikey": ALPHA_KEY,
        }
        r = requests.get(url, params=params, timeout=20)
        text = r.text.strip()
        if r.status_code == 429 or text.startswith('{"Note":'):
            raise RateLimit("alpha-rl")
        try:
            r.raise_for_status()
            js = r.json()
            val = js.get("Realtime Currency Exchange Rate", {}).get("5. Exchange Rate")
            if val:
                return float(val)
        except Exception:
            pass
        _backoff_sleep(attempt, base=2.0, cap=8.0)
    return None


def fx_usdjpy_exrate_host() -> Optional[float]:
    """exchangerate.host (free, no key)."""
    try:
        r = requests.get("https://api.exchangerate.host/latest?base=USD&symbols=JPY", timeout=15)
        r.raise_for_status()
        js = r.json()
        return float(js["rates"]["JPY"])
    except Exception:
        return None


def usd_jpy_rate() -> Optional[float]:
    """FX provider priority: Alpha -> exchangerate.host"""
    try:
        val = fx_usdjpy_alpha()
        if val is not None:
            return val
    except RateLimit:
        pass
    except Exception:
        pass
    return fx_usdjpy_exrate_host()


# ---------- Alpha Vantage: OVERVIEW / statements (with cache) ----------
def _alpha_get(function: str, symbol: str) -> dict:
    if not ALPHA_KEY:
        raise RateLimit("no-alpha-key")
    url = "https://www.alphavantage.co/query"
    params = {"function": function, "symbol": symbol, "apikey": ALPHA_KEY}
    r = requests.get(url, params=params, timeout=30)
    txt = r.text.strip()
    if r.status_code == 429 or txt.startswith('{"Note":'):
        raise RateLimit("alpha-rl")
    r.raise_for_status()
    return r.json()


def alpha_overview(symbol: str) -> dict:
    return _alpha_get("OVERVIEW", symbol)


def _stmt_cache_path(symbol: str, kind: str) -> str:
    fn = f"{symbol.replace('/', '_')}_{kind}.json"
    return os.path.join(ALPHA_STMT_DIR, fn)


def _load_stmt_cache(symbol: str, kind: str) -> Optional[dict]:
    p = _stmt_cache_path(symbol, kind)
    if os.path.exists(p):
        with open(p, "r") as f:
            return json.load(f)
    return None


def _save_stmt_cache(symbol: str, kind: str, payload: dict):
    p = _stmt_cache_path(symbol, kind)
    with open(p, "w") as f:
        json.dump({"_ts": dt.datetime.utcnow().isoformat(), "payload": payload}, f)


class AlphaBudget:
    """Simple daily budget tracker for Alpha Vantage calls."""
    def __init__(self, daily_limit: int = 24):
        self._remain = int(daily_limit)

    def remaining(self) -> int:
        return self._remain

    def consume(self, n: int):
        self._remain = max(0, self._remain - int(n))


def alpha_statements(symbol: str, refresh: bool = False, refresh_days: int = 30, budget: Optional[AlphaBudget] = None):
    """
    Return dict with 'income','balance','cashflow' JSON payloads.
    Uses disk cache. If refresh=True or cache is older than refresh_days, re-fetch.
    """
    out = {}
    now = dt.datetime.utcnow()
    for kind, func in (("income", "INCOME_STATEMENT"), ("balance", "BALANCE_SHEET"), ("cashflow", "CASH_FLOW")):
        cached = _load_stmt_cache(symbol, kind)
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
                out[kind] = cached["payload"] if cached else None
                continue
            js = _alpha_get(func, symbol)
            _save_stmt_cache(symbol, kind, js)
            if budget is not None:
                budget.consume(1)
            out[kind] = js
            time.sleep(13)  # ~5/min
        else:
            out[kind] = cached["payload"]
    return out


# ---------- Yahoo “light” info / events with budgets ----------
def yf_info_pe_ps_div(ticker: str) -> Tuple[Optional[float], Optional[float], Optional[float]]:
    """
    Minimal use of quoteSummary through yfinance.info for fallbacks only.
    Returns (pe, ps, div_yield_pct) with aggressive error handling.
    """
    try:
        info = yf.Ticker(yf_symbol(ticker)).info
        pe = info.get("trailingPE")
        ps = info.get("priceToSalesTrailing12Months")
        yld = info.get("dividendYield")
        return _to_float(pe), _to_float(ps), (_to_float(yld) * 100 if yld is not None else None)
    except Exception:
        return None, None, None


def yf_next_event_date(ticker: str) -> Optional[str]:
    """Try earnings date once; failure -> None."""
    try:
        cal = yf.Ticker(yf_symbol(ticker)).calendar
        if cal is not None and not cal.empty and "Earnings Date" in cal.index:
            v = cal.loc["Earnings Date"].values[0]
            return str(v)[:10]
    except Exception:
        return None
    return None
