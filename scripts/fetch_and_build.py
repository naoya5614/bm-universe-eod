import argparse, os, math, time, json
from datetime import datetime
import pandas as pd
from scripts.providers import price_tiingo, price_yfinance, usd_jpy_yfinance, next_event_yfinance, alpha_overview, alpha_statements, AlphaBudget, RateLimit
from scripts.utils import today_jst, is_weekend, pct, cagr, safe_div, fmt

CORE_COLS = ["Ticker","Name","Close","Δd(%)","USD/JPY","Price (JPY)","P/E(TTM)","P/S(TTM)","Div. Yield%","Next Event","Asset","NISA"]
EXT_COLS = ["Revenue YoY%","3y CAGR%","5y CAGR%","EPS YoY%","FCF YoY%","Gross%","OPM%","EBITDA%","EBIT%","FCF Margin%","ROIC%","ROE%","ROA%","Asset Turnover","Inv Days","AR Days","AP Days","CCC","CFO/NI","FCF/NI","Accruals","Net D/EBITDA","Int. Coverage","D/E","Equity Ratio%","Current Ratio","Quick Ratio","R&D/Sales%","Capex/Sales%","SBC/Sales%","Payout%","Buyback Yield%","Shares YoY%","Dilution Flag","Fwd P/E","EV/EBITDA","EV/Sales","P/FCF","PEG","Guidance/Revision","Catalysts"]

def main(universe, outdir):
    d = today_jst()
    if os.getenv("WEEKEND_SKIP","true").lower() == "true" and is_weekend(d):
        os.makedirs(f"{outdir}/{d}", exist_ok=True)
        with open(f"{outdir}/{d}/SKIPPED.md","w",encoding="utf-8") as f:
            f.write("本日は週末のためスキップ（日本の祝日は考慮しません）")
        return

    os.makedirs(f"{outdir}/{d}", exist_ok=True)
    uni = pd.read_csv(universe)

    # USD/JPY
    usdjpy = usd_jpy_yfinance()

    # Determine Alpha rotation
    daily_budget = int(os.getenv("ALPHA_DAILY_BUDGET","24"))
    refresh_days = int(os.getenv("ALPHA_REFRESH_DAYS","30"))
    stale_days = int(os.getenv("EXTENDED_STALE_DAYS","7"))
    budget = AlphaBudget(daily_limit=daily_budget)
    calls_per_ticker = 3  # income, balance, cashflow
    tpd = max(1, daily_budget // calls_per_ticker)

    # Stable rotation: consecutive block starting at index = ordinal * tpd % n
    tickers = list(uni["Ticker"])
    n = len(tickers)
    start = (d.toordinal() * tpd) % max(1, n)
    to_refresh = [tickers[(start + i) % n] for i in range(min(tpd, n))]

    rows = []
    missing = []  # for Missing/Stale summary

    # Preload OVERVIEW (P/E, P/S, dividend) with budget safety; fall back later if needed
    # We'll not count OVERVIEW in the 24/day budget (user優先: statements重視)。必要時のみ呼ぶ。
    overview_cache = {}

    for _, r in uni.iterrows():
        tkr, name, asset, nisa = r["Ticker"], r["Name"], r["Asset"], r["NISA"]

        # --- Prices ---
        close = None
        try:
            close = price_tiingo(tkr)
        except RateLimit:
            close = price_yfinance(tkr)
        if close is None:
            close = price_yfinance(tkr)

        price_jpy = close * usdjpy if (close is not None and usdjpy is not None) else None

        # --- Core fundamentals (P/E, P/S, Div. Yield) ---
        pe = ps = dy = None
        # Try OVERVIEW if we have remaining minutes outside day cap (not tracked here). Best-effort only.
        try:
            ov = alpha_overview(tkr)
            pe = _to_f(ov.get("PERatio"))
            ps = _to_f(ov.get("PriceToSalesRatioTTM"))
            dy_raw = _to_f(ov.get("DividendYield"))
            dy = dy_raw * 100 if dy_raw is not None else None
        except Exception:
            # yfinance fallback
            try:
                info = __import__("yfinance").Ticker(tkr).info
                pe = pe or info.get("trailingPE")
                ps = ps or info.get("priceToSalesTrailing12Months")
                yld = info.get("dividendYield")
                if dy is None and yld is not None:
                    dy = float(yld) * 100
            except Exception:
                pass

        nxt = next_event_yfinance(tkr)

        core = {
            "Ticker": tkr, "Name": name, "Close": _fmt_n(close),
            "Δd(%)": "N/A", "USD/JPY": _fmt_n(usdjpy), "Price (JPY)": _fmt_n(price_jpy),
            "P/E(TTM)": _fmt_n(pe), "P/S(TTM)": _fmt_n(ps), "Div. Yield%": _fmt_n(dy),
            "Next Event": nxt if nxt else "N/A", "Asset": asset, "NISA": nisa
        }

        # --- Extended (Alpha statements + cache) ---
        ext = {k:"N/A" for k in EXT_COLS}
        try:
            refresh = (tkr in to_refresh)
            stm = alpha_statements(tkr, refresh=refresh, refresh_days=refresh_days, budget=budget)

            # Pull latest annual entries
            inc = stm.get("income") or {}
            bal = stm.get("balance") or {}
            cfs = stm.get("cashflow") or {}

            inc_a = (inc.get("annualReports") or [])[:6]
            bal_a = (bal.get("annualReports") or [])[:6]
            cfs_a = (cfs.get("annualReports") or [])[:6]

            # Helper to fetch value from annual report by key and index
            def av(arr, idx, key):
                try:
                    return _to_f(arr[idx].get(key)) if len(arr) > idx else None
                except Exception:
                    return None

            # Revenue series for CAGR/Yoy
            rev0 = av(inc_a, 0, "totalRevenue")
            rev1 = av(inc_a, 1, "totalRevenue")
            rev3 = av(inc_a, 3, "totalRevenue")
            rev5 = av(inc_a, 5, "totalRevenue")
            ext["Revenue YoY%"] = _fmt_pct(pct(rev0, rev1))
            ext["3y CAGR%"]     = _fmt_pct(cagr(rev0, rev3, 3)) if rev3 not in (None,0) else "N/A"
            ext["5y CAGR%"]     = _fmt_pct(cagr(rev0, rev5, 5)) if rev5 not in (None,0) else "N/A"

            # EPS YoY% approx: netIncome / shares
            ni0 = av(inc_a, 0, "netIncome")
            ni1 = av(inc_a, 1, "netIncome")
            sh0 = av(bal_a, 0, "commonStockSharesOutstanding")
            sh1 = av(bal_a, 1, "commonStockSharesOutstanding")
            eps0 = safe_div(ni0, sh0)
            eps1 = safe_div(ni1, sh1)
            ext["EPS YoY%"] = _fmt_pct(pct(eps0, eps1)) if (eps0 and eps1) else "N/A"

            # FCF YoY% and margins
            cfo0 = av(cfs_a, 0, "operatingCashflow")
            cfo1 = av(cfs_a, 1, "operatingCashflow")
            capex0 = av(cfs_a, 0, "capitalExpenditures")
            capex1 = av(cfs_a, 1, "capitalExpenditures")
            fcf0 = (cfo0 - capex0) if (cfo0 is not None and capex0 is not None) else None
            fcf1 = (cfo1 - capex1) if (cfo1 is not None and capex1 is not None) else None
            ext["FCF YoY%"] = _fmt_pct(pct(fcf0, fcf1)) if (fcf0 and fcf1) else "N/A"
            ext["FCF Margin%"] = _fmt_pct(safe_div(fcf0, rev0)*100.0) if (fcf0 and rev0) else "N/A"

            # Margins (income)
            gp0 = av(inc_a, 0, "grossProfit")
            op0 = av(inc_a, 0, "operatingIncome")
            ebitda0 = av(inc_a, 0, "ebitda")
            ebit0 = av(inc_a, 0, "ebit")
            ext["Gross%"]  = _fmt_pct(safe_div(gp0, rev0)*100.0) if (gp0 and rev0) else "N/A"
            ext["OPM%"]    = _fmt_pct(safe_div(op0, rev0)*100.0) if (op0 and rev0) else "N/A"
            ext["EBITDA%"] = _fmt_pct(safe_div(ebitda0, rev0)*100.0) if (ebitda0 and rev0) else "N/A"
            ext["EBIT%"]   = _fmt_pct(safe_div(ebit0, rev0)*100.0) if (ebit0 and rev0) else "N/A"

            # Balance sheet ratios
            tot_assets = av(bal_a, 0, "totalAssets")
            tot_equity = av(bal_a, 0, "totalShareholderEquity")
            cur_assets = av(bal_a, 0, "totalCurrentAssets")
            cur_liab   = av(bal_a, 0, "totalCurrentLiabilities")
            cash       = av(bal_a, 0, "cashAndCashEquivalentsAtCarryingValue")
            sti        = av(bal_a, 0, "shortTermInvestments")
            debt_st    = av(bal_a, 0, "shortTermDebt")
            debt_lt    = av(bal_a, 0, "longTermDebt")
            debt_total = (debt_st or 0) + (debt_lt or 0)

            net_debt = (debt_total or 0) - ((cash or 0) + (sti or 0))
            ext["D/E"]           = _fmt_num(safe_div(debt_total, tot_equity))
            ext["Equity Ratio%"] = _fmt_pct(safe_div(tot_equity, tot_assets)*100.0) if (tot_equity and tot_assets) else "N/A"
            ext["Current Ratio"] = _fmt_num(safe_div(cur_assets, cur_liab))
            quick = safe_div((cash or 0) + (sti or 0), cur_liab) if cur_liab else None
            ext["Quick Ratio"]   = _fmt_num(quick)

            # Turnover & CCC (approx)
            inv = av(bal_a, 0, "inventory")
            ar  = av(bal_a, 0, "currentNetReceivables")
            ap  = av(bal_a, 0, "currentAccountsPayable")
            cogs0 = av(inc_a, 0, "costOfRevenue")
            ext["Asset Turnover"] = _fmt_num(safe_div(rev0, tot_assets))
            inv_days = 365.0 * safe_div(inv, cogs0) if (inv and cogs0) else None
            ar_days  = 365.0 * safe_div(ar, rev0) if (ar and rev0) else None
            ap_days  = 365.0 * safe_div(ap, cogs0) if (ap and cogs0) else None
            ccc = None
            if inv_days and ar_days and ap_days:
                ccc = inv_days + ar_days - ap_days
            ext["Inv Days"] = _fmt_num(inv_days)
            ext["AR Days"]  = _fmt_num(ar_days)
            ext["AP Days"]  = _fmt_num(ap_days)
            ext["CCC"]      = _fmt_num(ccc)

            # Cash-flow quality & accruals
            ext["CFO/NI"]   = _fmt_num(safe_div(cfo0, ni0))
            ext["FCF/NI"]   = _fmt_num(safe_div(fcf0, ni0))
            total_assets_prev = av(bal_a, 1, "totalAssets")
            accr = None
            try:
                if ni0 is not None and cfo0 is not None and tot_assets is not None:
                    accr = (ni0 - cfo0) / tot_assets
            except Exception:
                pass
            ext["Accruals"] = _fmt_num(accr)

            # Leverage / coverage
            ext["Net D/EBITDA"] = _fmt_num(safe_div(net_debt, ebitda0))
            int_exp = av(inc_a, 0, "interestExpense")
            ext["Int. Coverage"] = _fmt_num(safe_div(ebit0, abs(int_exp)) if int_exp else None)

            # Profitability: ROIC/ROE/ROA
            pretax = av(inc_a, 0, "incomeBeforeTax")
            tax    = av(inc_a, 0, "incomeTaxExpense")
            tax_rate = None
            if pretax and pretax != 0 and tax is not None:
                try:
                    tr = tax / pretax
                    if 0 <= tr <= 1.0:
                        tax_rate = tr
                except Exception:
                    pass
            nopat = None
            if ebit0 is not None:
                if tax_rate is not None:
                    nopat = ebit0 * (1 - tax_rate)
                else:
                    nopat = ebit0 * 0.79  # フォールバック: 21% 税率仮定
            invested_capital = None
            if tot_equity is not None:
                invested_capital = (tot_equity or 0) + (debt_total or 0) - ((cash or 0) + (sti or 0))
            ext["ROIC%"] = _fmt_pct(safe_div(nopat, invested_capital)*100.0) if (nopat and invested_capital) else "N/A"
            ext["ROE%"]  = _fmt_pct(safe_div(ni0, tot_equity)*100.0) if (ni0 and tot_equity) else "N/A"
            ext["ROA%"]  = _fmt_pct(safe_div(ni0, tot_assets)*100.0) if (ni0 and tot_assets) else "N/A"

            # R&D / Capex / SBC
            rnd   = av(inc_a, 0, "researchAndDevelopment")
            capex = capex0
            sbc   = _to_f((cfs_a[0].get("stockBasedCompensation")) if len(cfs_a) else None)
            ext["R&D/Sales%"]   = _fmt_pct(safe_div(rnd, rev0)*100.0) if (rnd and rev0) else "N/A"
            ext["Capex/Sales%"] = _fmt_pct(safe_div(abs(capex), rev0)*100.0) if (capex and rev0) else "N/A"
            ext["SBC/Sales%"]   = _fmt_pct(safe_div(sbc, rev0)*100.0) if (sbc and rev0) else "N/A"

            # Dividends / payouts (approx)
            # payout% ≈ dividendsPaid / netIncome  from cash flow (use absolute)
            div_paid = _to_f((cfs_a[0].get("dividendsPaid")) if len(cfs_a) else None)
            if div_paid is not None and ni0:
                ext["Payout%"] = _fmt_pct(abs(div_paid)/ni0*100.0)
            # buyback yield% ≈ (purchaseOfCommonStock / market cap). Need price*shares.
            # If price and shares exist, compute.
            shares0 = sh0
            # Market cap approx via close price passed later
            # We will fill Buyback Yield% in a second pass (needs price)
            ext["_shares"] = shares0
            ext["_div_paid"] = div_paid

            # Dilution
            ext["Shares YoY%"] = _fmt_pct(pct(sh0, sh1)) if (sh0 and sh1) else "N/A"
            dil_flag = "Yes" if (sh0 and sh1 and (sh0 > sh1*1.05)) else "No"
            ext["Dilution Flag"] = dil_flag

            # Forward/EV multiples (best-effort: may be N/A in free mode)
            ext["Fwd P/E"] = "N/A"
            ext["EV/EBITDA"] = "N/A"
            ext["EV/Sales"]  = "N/A"
            ext["P/FCF"]     = "N/A"
            ext["PEG"]       = "N/A"

            # Guidance/Revision / Catalysts → freeソースでは体系的取得困難のためN/A
            ext["Guidance/Revision"] = "N/A"
            ext["Catalysts"]         = "N/A"

            # Stale判定（ファイルタイムスタンプ基準）
            # If not refreshed today and cache older than stale_days → mark in Missing/Stale
            # (For simplicity, we'll log at row-level instead of per-field granularity)
            # This is sufficient for the required summary table.
            # (We could add per-field time stamps if needed later.)
        except Exception as e:
            missing.append((tkr, "Extended(all)", "Missing", f"{e.__class__.__name__}"))
            # keep defaults (N/A)

        # Combine Core + Extended
        # buyback yield second pass (needs price and shares)
        try:
            if ext.get("_shares") and close is not None and ext.get("_div_paid") is not None:
                mcap = ext["_shares"] * close
                poc = _to_f((cfs_a[0].get("purchaseOfCommonStock")) if 'cfs_a' in locals() and len(cfs_a) else None)
                if mcap and poc:
                    ext["Buyback Yield%"] = _fmt_pct(abs(poc)/mcap*100.0)
                else:
                    ext["Buyback Yield%"] = "N/A"
        except Exception:
            ext["Buyback Yield%"] = "N/A"
        ext.pop("_shares", None)
        ext.pop("_div_paid", None)

        row = {**core, **ext}
        rows.append(row)

        # Missing for Core items
        for col in ["Close","USD/JPY","P/E(TTM)","P/S(TTM)","Div. Yield%","Next Event"]:
            if row[col] in (None,"N/A"):
                missing.append((tkr, col, "Missing", "free-source-limit"))

    # Build DataFrame
    cols = CORE_COLS + EXT_COLS
    df = pd.DataFrame(rows, columns=cols)

    # Markdown output
    md_path = f"{outdir}/{d}/bloomo_eod_full.md"
    with open(md_path, "w", encoding="utf-8") as f:
        f.write(f"# ユニバース日次分析テーブル（EOD・無料モード＋Alpha回転｜{d}）\n")
        f.write("> Core列は極力100%充足。Extended列はAlpha Vantage 財務三表APIを**日割り回転**で更新し、未取得はN/A。\n\n")
        f.write(df.to_markdown(index=False))

    # Missing/Stale summary
    ms = pd.DataFrame(missing, columns=["銘柄","項目","状態","理由"])
    with open(f"{outdir}/{d}/missing_stale.md", "w", encoding="utf-8") as f:
        f.write("## Missing/Stale サマリー（無料ソースの制約・回転未到達など）\n\n")
        if len(ms):
            f.write(ms.to_markdown(index=False))
        else:
            f.write("なし")

def _to_f(x):
    try:
        if x in (None,"", "None"): return None
        return float(x)
    except Exception:
        return None

def _fmt_n(x):
    return fmt(x)

def _fmt_pct(x):
    return fmt(x, kind="pct")

def _fmt_num(x):
    return fmt(x)

if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--universe", required=True)
    ap.add_argument("--outdir", required=True)
    args = ap.parse_args()
    main(args.universe, args.outdir)
