from datetime import datetime
import pytz, math

def today_jst():
    return datetime.now(pytz.timezone("Asia/Tokyo")).date()

def is_weekend(d):
    return d.weekday() >= 5

def pct(a, b):
    try:
        if a is None or b in (None, 0): return None
        return (a/b - 1.0) * 100.0
    except Exception:
        return None

def cagr(latest, base, years):
    try:
        if latest is None or base in (None, 0) or years <= 0: return None
        return ((latest / base) ** (1.0/years) - 1.0) * 100.0
    except Exception:
        return None

def safe_div(a, b):
    try:
        if a is None or b in (None, 0): return None
        return a / b
    except Exception:
        return None

def fmt(x, kind=None):
    if x is None:
        return "N/A"
    try:
        if kind == "pct":
            return f"{x:.2f}"
        if kind == "int":
            return f"{int(round(x))}"
        if isinstance(x, float):
            return f"{x:.2f}" if abs(x) < 1000 else f"{x:.0f}"
        return str(x)
    except Exception:
        return str(x)
