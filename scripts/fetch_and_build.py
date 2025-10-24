import argparse
import os
from typing import List, Dict, Any

import pandas as pd
import pathlib
import json

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

# ===== 追加：ユニバースの堅牢ローダー =====
def _first_existing_file_in_dir(dir_path: pathlib.Path, patterns) -> pathlib.Path | None:
    for pat in patterns:
        for p in sorted(dir_path.glob(pat)):
            if p.is_file() and p.stat().st_size > 0:
                return p
    return None

def _sniff_text_is_json_array_or_object(text: str) -> str | None:
    # 先頭の空白除去後の1文字で簡易判定
    s = text.lstrip()
    if not s:
        return None
    c = s[0]
    if c == "{" or c == "[":
        return "json"
    return None

def _load_universe(universe_path: str) -> pd.DataFrame:
    """
    入力の自動判別ローダー：
      - file: *.json（通常のJSON配列 or NDJSON） / *.ndjson / *.jsonl / *.csv / *.parquet
      - dir : 上記の優先順で最初に見つかったファイルを読む
    レコードは list[dict] 前提（CSV/Parquet の場合は列名に合わせて解釈）。
    """
    p = pathlib.Path(universe_path)
    if not p.exists():
        raise FileNotFoundError(f"universe path not found: {universe_path}")

    # ディレクトリなら候補を探索
    if p.is_dir():
        candidate = _first_existing_file_in_dir(
            p,
            patterns=["*.json", "*.ndjson", "*.jsonl", "*.csv", "*.parquet"]
        )
        if candidate is None:
            raise ValueError(f"no usable universe file in dir: {universe_path}")
        p = candidate

    # ファイル種類で分岐
    lower = p.name.lower()
    if lower.endswith(".csv"):
        df = pd.read_csv(p)
        return df

    if lower.endswith(".parquet"):
        df = pd.read_parquet(p)
        return df

    # JSON/NDJSON 系
    if lower.endswith(".json") or lower.endswith(".ndjson") or lower.endswith(".jsonl"):
        # まず中身を軽く検査
        with open(p, "r", encoding="utf-8") as f:
            head = f.read(4096)

        if not head.strip():
            raise ValueError(f"universe file is empty: {p}")

        # 拡張子に関わらず、JSON配列/オブジェクトか、NDJSONか判別して読み分け
        kind = _sniff_text_is_json_array_or_object(head)
        if kind == "json":
            # 通常の JSON（配列 or オブジェクト）
            try:
                # JSON配列（list[dict]）想定
                return pd.read_json(p, orient="records")
            except ValueError:
                # 一部のケースで単一オブジェクトやキー違いもあるので素読み→正規化
                with open(p, "r", encoding="utf-8") as f:
                    obj = json.load(f)
                if isinstance(obj, list):
                    return pd.json_normalize(obj)
                elif isinstance(obj, dict):
                    # 1レコード扱い
                    return pd.json_normalize([obj])
                else:
                    raise ValueError(f"unsupported JSON structure in {p}")
        else:
            # NDJSON（行ごとに JSON）
            try:
                return pd.read_json(p, lines=True)
            except ValueError as e:
                raise ValueError(f"failed to read NDJSON: {p} ({e})")

    # 拡張子で判別できない場合、テキストを嗅ぎ分け
    with open(p, "r", encoding="utf-8", errors="ignore") as f:
        sniff = f.read(1024)
    if _sniff_text_is_json_array_or_object(sniff) == "json":
        try:
            return pd.read_json(p, orient="records")
        except Exception:
            return pd.read_json(p, lines=True)
    else:
        # 最後のフォールバック：CSVとして読む（失敗時は例外）
        return pd.read_csv(p)

# ====== 出力：CSVのみ＋Zip ======
def _save_df_outputs(df: pd.DataFrame, outdir: str, d: str, base_name: str = "bloomo_eod_full") -> None:
    """
    Save the consolidated DataFrame as **CSV only** under outdir/d/.
    - CSV: UTF-8, header, no index
    """
    os.makedirs(f"{outdir}/{d}", exist_ok=True)
    df.to_csv(f"{outdir}/{d}/{base_name}.csv", index=False)
    return None

def _zip_output_dir(outdir: str, d: str, zip_name: str = "bloomo_eod_full_csv.zip") -> str:
    """Zip the entire outdir/d directory for distribution and return the zip path."""
    import shutil
    day_dir = f"{outdir}/{d}"
    os.makedirs(day_dir, exist_ok=True)
    zip_path = f"{day_dir}/{zip_name}"
    # overwrite if exists
    if os.path.exists(zip_path):
        os.remove(zip_path)
    shutil.make_archive(base_name=zip_path[:-4], format="zip", root_dir=day_dir)
    return zip_path

# ===== データ収集 =====
def _collect_rows(universe: List[Dict[str, Any]], budget: AlphaBudget) -> List[List[Any]]:
    rows: List[List[Any]] = []
    miss: List[List[str]] = []

    # Yahoo batched prices (Close & ADV)
    tickers = [u["ticker"] for u in universe]
    prices = yf_download_prices_batched(tickers)  # dict: ticker -> {price, change_pct, adv20_usd}
    prices = fill_missing_prices(tickers, prices)

    # USD/JPY
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

        # Fundamentals: Alpha first, then Yahoo info fallback
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

        # missing summary（例示・必要なら活用）
        if price is None:
            miss.append([t, "Price", "Missing", "Yahoo batch not available"])

    return rows

# ===== メイン =====
def main(universe_path: str, outdir: str):
    # ユニバース読込（自動判別）
    uni = _load_universe(universe_path)
    if uni.empty:
        raise ValueError(f"universe is empty: {universe_path}")

    # 入力は list[dict] 相当を想定（CSV/Parquetでも列があればOK）
    # ticker 列の存在チェック
    if "ticker" not in uni.columns:
        # よくある別名に対応
        for alt in ["Ticker", "symbol", "Symbol"]:
            if alt in uni.columns:
                uni = uni.rename(columns={alt: "ticker"})
                break
    if "ticker" not in uni.columns:
        raise ValueError("universe must contain 'ticker' column")

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
    # Zip（Zip名は既存ルールを維持）
    _zip_output_dir(outdir=outdir, d=d, zip_name="bloomo_eod_full_csv.zip")

    # Missing/Stale summary（運用メモ；必要ならCSV化も可）
    missing = []  # 必要に応じ _collect_rows から返す
    ms = pd.DataFrame(missing, columns=["銘柄", "項目", "状態", "理由"])
    os.makedirs(f"{outdir}/{d}", exist_ok=True)
    with open(f"{outdir}/{d}/missing_stale.md", "w", encoding="utf-8") as f:
        f.write("## Missing/Stale サマリー（無料ソース制約・回転未到達・レート制御など）\n\n")
        if len(ms):
            f.write(ms.to_markdown(index=False))
        else:
            f.write("なし")

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--universe", required=True)
    ap.add_argument("--outdir", required=True)
    args = ap.parse_args()
    main(args.universe, args.outdir)
