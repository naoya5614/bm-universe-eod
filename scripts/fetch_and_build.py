import argparse
import os
from typing import List, Dict, Any

import pandas as pd

from scripts.providers import (
    yf_download_prices_batched,
    fill_missing_prices,
    usd_jpy_rate,
    alpha_overview,
    alpha_statements,
    AlphaBudget,
    yf_info_pe_ps_div,
    yf_next_event_date,
)

CORE_COLS = [
    "Ticker", "Name", "Sector", "Industry",
    "Price", "Change%", "MarketCap(USD)", "ADV20(USD)",
    "P/E", "P/S", "DividendYield%", "NextEvent",
    "USD/JPY",
]

EXT_COLS = [
    "Revenue_TTM", "OperatingCF_TTM", "FreeCF_TTM",
    "GrossMargin%", "OperatingMargin%", "NetMargin%",
    "DebtToEquity", "CurrentRatio",
]

def fmt(x):
    if x is None:
        return None
    try:
        if isinstance(x, (int, float)):
            return x
        return x
    except Exception:
        return x

def _fmt_num(x):
    return fmt(x)

def _save_df_outputs(df: pd.DataFrame, outdir: str, d: str, base_name: str = "bloomo_eod_full") -> None:
    """
    Save the consolidated DataFrame as **CSV only** under outdir/d/.
    - CSV: UTF-8, header, no index
    """
    import os
    os.makedirs(f"{outdir}/{d}", exist_ok=True)
    df.to_csv(f"{outdir}/{d}/{base_name}.csv", index=False)
    return None

def _zip_output_dir(outdir: str, d: str, zip_name: str = "bloomo_eod_full_csv.zip") -> str:
    """Zip the entire outdir/d directory for distribution and return the zip path."""
    import os, shutil
    day_dir = f"{outdir}/{d}"
    os.makedirs(day_dir, exist_ok=True)
    zip_path = f"{day_dir}/{zip_name}"
    # create zip archive (overwrite if exists)
    if os.path.exists(zip_path):
        os.remove(zip_path)
    shutil.make_archive(base_name=zip_path[:-4], format="zip", root_dir=day_dir)
    return zip_path

def _collect_rows(universe: List[Dict[str, Any]], budget: AlphaBudget) -> List[List[Any]]:
    rows: List[List[Any]] = []
    miss: List[List[str]] = []

    # === Yahoo batched prices (Close & ADV) ===
    tickers = [u["ticker"] for u in universe]
    prices = yf_download_prices_batched(tickers)  # dict: ticker -> {price, change_pct, adv20_usd}
    prices = fill_missing_prices(tickers, prices)

    # === USD/JPY ===
    fx = usd_jpy_rate()

    for u in universe:
        t = u["ticker"]
        name = u.get("name")
        sector = u.get("sector")
        industry = u.get("industry")

        p = prices.get(t, {})
        price = p.get("price")
        chg = p.get("change_pct")
        adv20 = p.get("adv20_usd")

        # === Fundamentals: Alpha first, then Yahoo info fallback ===
        ov = alpha_overview(t, budget)
        st = alpha_statements(t, budget)

        pe = None
        ps = None
        div_y = None
        if ov is not None:
            pe = ov.get("pe")
            ps = ov.get("ps")
            div_y = ov.get("dividend_yield")

        # Yahoo info fallback (P/E, P/S, Dividend)
        if pe is None or ps is None or div_y is None:
            yinfo = yf_info_pe_ps_div(t)
            if pe is None:
                pe = yinfo.get("pe")
            if ps is None:
                ps = yinfo.get("ps")
            if div_y is None:
                div_y = yinfo.get("dividend_yield")

        # Next event
        nextev = yf_next_event_date(t)

        # statements (TTM, margins, leverage…)
        rev_ttm = None
        ocf_ttm = None
        fcf_ttm = None
        gm = None
        om = None
        nm = None
        dte = None
        cr = None
        if st is not None:
            rev_ttm = st.get("revenue_ttm")
            ocf_ttm = st.get("operating_cf_ttm")
            fcf_ttm = st.get("free_cf_ttm")
            gm = st.get("gross_margin")
            om = st.get("operating_margin")
            nm = st.get("net_margin")
            dte = st.get("debt_to_equity")
            cr = st.get("current_ratio")

        row = [
            t, name, sector, industry,
            _fmt_num(price), _fmt_num(chg), _fmt_num(u.get("marketcap_usd")), _fmt_num(adv20),
            _fmt_num(pe), _fmt_num(ps), _fmt_num(div_y), nextev,
            _fmt_num(fx),
            _fmt_num(rev_ttm), _fmt_num(ocf_ttm), _fmt_num(fcf_ttm),
            _fmt_num(gm), _fmt_num(om), _fmt_num(nm),
            _fmt_num(dte), _fmt_num(cr),
        ]
        rows.append(row)

        # missing summary (example)
        if price is None:
            miss.append([t, "Price", "Missing", "Yahoo batch not available"])

    return rows

def main(universe_path: str, outdir: str):
    # Load universe JSON (list of dicts)
    uni = pd.read_json(universe_path, orient="records")
    universe = uni.to_dict(orient="records")

    # Alpha free-budget manager
    budget = AlphaBudget(daily_limit=250)  # free tier example

    # Collect
    rows = _collect_rows(universe, budget)

    # Build DataFrame
    cols = CORE_COLS + EXT_COLS
    df = pd.DataFrame(rows, columns=cols)

    # CSV only output
    d = pd.Timestamp.utcnow().tz_localize("UTC").tz_convert("Asia/Tokyo").strftime("%Y-%m-%d")
    _save_df_outputs(df, outdir=outdir, d=d, base_name="bloomo_eod_full")
    # Zip the day's outputs for convenience (zip名は既存ルールを維持)
    _zip_output_dir(outdir=outdir, d=d, zip_name="bloomo_eod_full_csv.zip")

    # Missing/Stale summary
    # （※この md は運用メモ用。必要に応じて CSV 化可能）
    missing = []  # ここに必要なら _collect_rows から返す
    ms = pd.DataFrame(missing, columns=["銘柄", "項目", "状態", "理由"])
    os.makedirs(f"{outdir}/{d}", exist_ok=True)
    with open(f"{outdir}/{d}/missing_stale.md", "w", encoding="utf-8") as f:
        f.write("## Missing/Stale サマリー（無料ソース制約・回転未到達・レート制御など）\n\n")
        if len(ms):
            # ここも CSV にしたい場合は df.to_csv を推奨（現状はmdメモ）
            f.write(ms.to_markdown(index=False))
        else:
            f.write("なし")

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--universe", required=True)
    ap.add_argument("--outdir", required=True)
    args = ap.parse_args()
    main(args.universe, args.outdir)
