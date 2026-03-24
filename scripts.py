import yfinance as yf
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import http.server
import socketserver
import webbrowser
import time
import threading

# --- CONFIGURATION ---
all_tickers = [
    'SPY','QQQ','TLT', 'IEF' 'GLD', 'XLK','XLV','XLF','XLY','XLP','XLE','XLI','XLB','XLU','XLRE','XLC'
    
]
UPDATE_INTERVAL = 60

print("🚀 Launching v8.6: Table 0 + After-Hours Performance...")

db = {"daily": pd.DataFrame(), "intra": pd.DataFrame(), "after_hours": pd.DataFrame(), "ohlc_raw": {}}

def safe_download(tickers, period, interval):
    """Regular market hours data"""
    try:
        data = yf.download(
            tickers,
            period=period,
            interval=interval,
            progress=False,
            auto_adjust=True,
            ignore_tz=True
        )
        return data if not data.empty else pd.DataFrame()
    except:
        return pd.DataFrame()

def safe_download_prepost(tickers, period, interval):
    """Extended hours (pre-market + after-hours) data"""
    try:
        data = yf.download(
            tickers,
            period=period,
            interval=interval,
            prepost=True,        # ← Enable extended hours
            progress=False,
            auto_adjust=True,
            ignore_tz=True
        )
        return data if not data.empty else pd.DataFrame()
    except:
        return pd.DataFrame()

def initial_sync():
    print("🔄 Full Syncing Database...")
    d_raw = safe_download(all_tickers, "max", "1d")
    i_raw = safe_download(all_tickers, "730d", "1h")
    
    # After-hours: last 5 days, 1-hour intervals (max available with prepost)
    ah_raw = safe_download_prepost(all_tickers, "5d", "1h")

    if not d_raw.empty:
        if hasattr(d_raw.index, "tz") and d_raw.index.tz is not None:
            d_raw.index = d_raw.index.tz_localize(None)
        db["daily"] = d_raw['Close'].ffill()
        db["ohlc_raw"]['daily'] = d_raw

    if not i_raw.empty:
        if hasattr(i_raw.index, "tz") and i_raw.index.tz is not None:
            i_raw.index = i_raw.index.tz_localize(None)
        db["intra"] = i_raw['Close'].ffill()
        db["ohlc_raw"]['intra'] = i_raw
    
    if not ah_raw.empty:
        if hasattr(ah_raw.index, "tz") and ah_raw.index.tz is not None:
            ah_raw.index = ah_raw.index.tz_localize(None)
        db["after_hours"] = ah_raw['Close'].ffill()
        db["ohlc_raw"]['after_hours'] = ah_raw
        print("✅ After-hours data loaded")
    else:
        print("⚠️  No after-hours data available")

def normalize_index(series):
    s = series.dropna().copy()
    if hasattr(s.index, "tz") and s.index.tz is not None:
        s.index = s.index.tz_localize(None)
    if s.index.dtype == 'object' or not isinstance(s.index, pd.DatetimeIndex):
        s.index = pd.to_datetime(s.index)
    s.index = s.index.normalize()
    return s

def build_perf_ohlc_from_series(ticker, period, timeframe, now):
    """
    Returns OHLC for the CURRENT candle in each period.
    """
    df_raw = db["ohlc_raw"].get(timeframe)
    if df_raw is None or df_raw.empty:
        return None

    # Handle MultiIndex columns
    if isinstance(df_raw.columns, pd.MultiIndex):
        if ticker not in df_raw.columns.get_level_values(1):
            return None
        df = df_raw.xs(ticker, level=1, axis=1)
    else:
        return None

    df = df.dropna()
    if len(df) < 2:
        return None

    # 1D: TODAY's candle (last row)
    if period == '1D':
        today = df.iloc[-1]
        return today['Open'], today['High'], today['Low'], today['Close']

    # 1H: CURRENT HOUR candle (resample to 1H, take last)
    if period == '1H':
        hourly = df.resample('1H').agg({
            'Open': 'first',
            'High': 'max',
            'Low': 'min',
            'Close': 'last'
        }).dropna()
        if len(hourly) < 1:
            return None
        current = hourly.iloc[-1]
        return current['Open'], current['High'], current['Low'], current['Close']

    # 1W, 1M, 1Y: Current period candle
    if period == '1W':
        start_dt = now - timedelta(days=7)
    elif period == '1M':
        start_dt = now - timedelta(days=30)
    elif period == '1Y':
        start_dt = now - timedelta(days=365)
    else:
        return None

    df = df[df.index <= now]
    idx = df.index[df.index <= start_dt]
    if len(idx) == 0:
        subset = df
    else:
        start_i = idx[-1]
        subset = df.loc[start_i:now]
    
    if len(subset) < 1:
        return None

    o = subset['Open'].iloc[0]
    h = subset['High'].max()
    l = subset['Low'].min()
    c = subset['Close'].iloc[-1]
    return o, h, l, c

def build_anchor_cell(ticker, period, timeframe, now):
    try:
        df_raw = db["ohlc_raw"].get(timeframe)
        if df_raw is None or ticker not in df_raw.columns.get_level_values(1):
            return "<span style='color:#444'>-</span>"

        df = df_raw.xs(ticker, level=1, axis=1).dropna()
        if len(df) < 2:
            return "<span style='color:#444'>-</span>"

        # --- Get CURRENT candle's OHLC (for display) ---
        if period == '1D':
            current = df.iloc[-1]
            prev = df.iloc[-2]
        elif period == '1H':
            hourly = df.resample('1H').agg({'Open':'first','High':'max','Low':'min','Close':'last'}).dropna()
            if len(hourly) < 2:
                return "<span style='color:#444'>-</span>"
            current = hourly.iloc[-1]
            prev = hourly.iloc[-2]
        elif period == '1W':
            start_dt = now - timedelta(days=7)
            subset = df[df.index <= now]
            idx = subset.index[subset.index <= start_dt]
            if len(idx) == 0:
                return "<span style='color:#444'>-</span>"
            subset = subset.loc[idx[-1]:now]
            if len(subset) < 2:
                return "<span style='color:#444'>-</span>"
            current = pd.Series({
                'Open': subset['Open'].iloc[0],
                'High': subset['High'].max(),
                'Low': subset['Low'].min(),
                'Close': subset['Close'].iloc[-1]
            })
            prev = pd.Series({'Close': subset['Close'].iloc[0]})
        elif period == '1M':
            start_dt = now - timedelta(days=30)
            subset = df[df.index <= now]
            idx = subset.index[subset.index <= start_dt]
            if len(idx) == 0:
                return "<span style='color:#444'>-</span>"
            subset = subset.loc[idx[-1]:now]
            if len(subset) < 2:
                return "<span style='color:#444'>-</span>"
            current = pd.Series({
                'Open': subset['Open'].iloc[0],
                'High': subset['High'].max(),
                'Low': subset['Low'].min(),
                'Close': subset['Close'].iloc[-1]
            })
            prev = pd.Series({'Close': subset['Close'].iloc[0]})
        elif period == '1Y':
            start_dt = now - timedelta(days=365)
            subset = df[df.index <= now]
            idx = subset.index[subset.index <= start_dt]
            if len(idx) == 0:
                return "<span style='color:#444'>-</span>"
            subset = subset.loc[idx[-1]:now]
            if len(subset) < 2:
                return "<span style='color:#444'>-</span>"
            current = pd.Series({
                'Open': subset['Open'].iloc[0],
                'High': subset['High'].max(),
                'Low': subset['Low'].min(),
                'Close': subset['Close'].iloc[-1]
            })
            prev = pd.Series({'Close': subset['Close'].iloc[0]})
        else:
            return "<span style='color:#444'>-</span>"

        o, h, l, c = current['Open'], current['High'], current['Low'], current['Close']
        prev_close = prev['Close']

        if pd.isna(o) or pd.isna(c) or pd.isna(prev_close):
            return "<span style='color:#444'>-</span>"

        # ✅ Return = (current_close - prev_close) / prev_close
        ret = ((c / prev_close) - 1) * 100
        color = "#00ff88" if ret >= 0 else "#ff3333"

        rng = (h - l) if (h - l) != 0 else 1
        p_open = np.clip(((o - l) / rng) * 100, 0, 100)
        p_close = np.clip(((c - l) / rng) * 100, 0, 100)

        return f'''
        <div style="min-width:90px; padding: 2px;">
            <div style="display:grid;grid-template-columns:1fr 1fr;font-size:9px;gap:2px; opacity:0.8;">
                <div>O: {o:.2f}</div><div>H: {h:.2f}</div>
                <div>L: {l:.2f}</div><div style="color:{color}">C: {c:.2f}</div>
            </div>
            <div style="color:{color};font-size:11px;font-weight:bold;margin:2px 0;">{ret:+.2f}%</div>
            <svg width="100%" height="8" style="display:block;">
                <rect x="0" y="3" width="100%" height="2" fill="#333" rx="1"/>
                <line x1="{p_open}%" y1="4" x2="{p_close}%" y2="4" stroke="{color}" stroke-width="3" stroke-linecap="round"/>
                <circle cx="{p_close}%" cy="4" r="2.5" fill="white" stroke="{color}" stroke-width="1"/>
            </svg>
        </div>
        '''
    except:
        return "<span style='color:#444'>-</span>"

def get_after_hours_perf(ticker, now):
    """
    Calculate after-hours return: from yesterday's regular close to latest after-hours close.
    Returns HTML-formatted string for anchor_df.
    """
    ah_df = db.get("after_hours")
    d_df = db.get("daily")
    
    if ah_df is None or ah_df.empty or ticker not in ah_df.columns:
        return "<span style='color:#444'>-</span>"
    if d_df is None or ticker not in d_df.columns:
        return "<span style='color:#444'>-</span>"
    
    # Get yesterday's close (regular session)
    if len(d_df[ticker]) < 2:
        return "<span style='color:#444'>-</span>"
    prev_close = d_df[ticker].iloc[-2]
    
    # Get latest after-hours close
    if len(ah_df[ticker]) < 1:
        return "<span style='color:#444'>-</span>"
    ah_close = ah_df[ticker].iloc[-1]
    
    if pd.isna(prev_close) or pd.isna(ah_close) or prev_close == 0:
        return "<span style='color:#444'>-</span>"
    
    ret = ((ah_close / prev_close) - 1) * 100
    color = "#00ff88" if ret >= 0 else "#ff3333"
    
    return f"<span style='color:{color}; font-size:11px; font-weight:bold;'>{ret:+.2f}%"

def get_color(val, min_v, max_v):
    if pd.isna(val) or max_v == min_v:
        return "rgb(0,0,0)", 0
    abs_max = max(abs(min_v), abs(max_v))
    norm = np.clip(0.5 + (val / (2 * abs_max)) if abs_max != 0 else 0.5, 0, 1)
    if norm < 0.5:
        r, g, b = int(64 + (norm * 2 * 191)), 0, int(128 * (1 - norm * 2))
    else:
        r, g, b = 255, int((norm - 0.5) * 2 * 165), 0
    return f'rgb({r},{g},{b})', norm

def get_perf_from_series(series, period, now=None):
    s = normalize_index(series)
    if len(s) < 2:
        return np.nan
    if now is None:
        now = datetime.now().replace(microsecond=0)

    if period in ('1H', '1D'):
        return round((s.iloc[-1] / s.iloc[-2] - 1) * 100, 2)

    if period == '1W':
        start_dt = now - timedelta(days=7)
    elif period == '1M':
        start_dt = now - timedelta(days=30)
    elif period == '1Y':
        start_dt = now - timedelta(days=365)
    else:
        return np.nan

    s = s[s.index <= now]
    idx = s.index[s.index <= start_dt]
    if len(idx) == 0:
        return np.nan

    p0 = s.loc[idx[-1]]
    if pd.isna(p0) or p0 == 0:
        return np.nan
    return round((s.iloc[-1] / p0 - 1) * 100, 2)

def calculate_drawdown_high_based(series, window=252):
    s = normalize_index(series)
    if len(s) < 2:
        return np.nan, np.nan
    rolling_max = s.rolling(window=window, min_periods=1).max()
    dd = (s - rolling_max) / rolling_max * 100
    dd_now = dd.iloc[-1]
    max_dd = dd.min()
    return round(dd_now, 2), round(max_dd, 2)

def calculate_metrics():
    df_d, df_i = db["daily"], db["intra"]
    if df_d.empty or 'SPY' not in df_d.columns:
        return None

    valid = [t for t in all_tickers if t in df_d.columns]
    now = datetime.now().replace(microsecond=0)

    anchor_periods = ['1H', '1D', '1W', '1M', '1Y', 'D+AH']
    anchor_sources = {'1H': 'intra', '1D': 'daily', '1W': 'daily', '1M': 'daily', '1Y': 'daily', 'D+AH': 'after_hours'}

    anchor_df = pd.DataFrame(index=anchor_periods, columns=valid)
    
    # Fill regular periods (1H, 1D, 1W, 1M, 1Y)
    for period in ['1H', '1D', '1W', '1M', '1Y']:
        src = anchor_sources[period]
        for tk in valid:
            anchor_df.at[period, tk] = build_anchor_cell(tk, period, src, now)
    
    # Fill After-Hours row
    for tk in valid:
        anchor_df.at['D+AH', tk] = get_after_hours_perf(tk, now)

    t_names = ['perf', 'avg_ret', 'vol', 'zsc_ret', 'beta', 'alpha', 'zsc_act', 'act_ret', 'act_risk', 'info_r']
    tables = {name: pd.DataFrame(index=valid) for name in t_names}

    for tk in valid:
        try:
            s, si = df_d[tk], df_i[tk]
            tables['perf'].at[tk, 'PRICE'] = round(s.iloc[-1], 2)
            tables['perf'].at[tk, '1H%'] = get_perf_from_series(si, '1H', now)
            tables['perf'].at[tk, '1D%'] = get_perf_from_series(s, '1D', now)
            tables['perf'].at[tk, '1W%'] = get_perf_from_series(s, '1W', now)
            tables['perf'].at[tk, '1M%'] = get_perf_from_series(s, '1M', now)
            tables['perf'].at[tk, '1Y%'] = get_perf_from_series(s, '1Y', now)
            tables['perf'].at[tk, '5Y%'] = round(((s.iloc[-1] / s.iloc[-min(len(s), 252*5)]) - 1) * 100, 2) if len(s) > 1 else np.nan
            tables['perf'].at[tk, '10Y%'] = round(((s.iloc[-1] / s.iloc[-min(len(s), 252*10)]) - 1) * 100, 2) if len(s) > 1 else np.nan
            dd_now, max_dd = calculate_drawdown_high_based(s, window=252)
            tables['perf'].at[tk, 'DD_NOW'] = dd_now
            tables['perf'].at[tk, 'MAX_DD'] = max_dd
        except:
            pass

    freq_rets = {
        '1H': df_i.pct_change(fill_method=None),
        '1D': df_d.pct_change(fill_method=None),
        '1W': df_d.pct_change(7, fill_method=None),
        '1M': df_d.pct_change(30, fill_method=None),
        '1Y': df_d.pct_change(365, fill_method=None)
    }

    for label, rets_df in freq_rets.items():
        if 'SPY' not in rets_df.columns:
            continue
        m_rets = rets_df['SPY'].dropna()
        for tk in valid:
            s_rets = rets_df[tk].dropna()
            if len(s_rets) < 5:
                continue
            mu, sigma = s_rets.mean(), s_rets.std()
            tables['avg_ret'].at[tk, label] = round(mu * 100, 3)
            tables['vol'].at[tk, label] = round(sigma * 100, 2)
            tables['zsc_ret'].at[tk, label] = round((s_rets.iloc[-1] - mu) / sigma, 2) if sigma > 0 else 0
            common = s_rets.index.intersection(m_rets.index)
            if len(common) > 5:
                sc, mc = s_rets.loc[common], m_rets.loc[common]
                beta = np.cov(sc, mc)[0, 1] / np.var(mc) if np.var(mc) > 0 else 1.0
                tables['beta'].at[tk, label] = round(beta, 2)
                tables['act_ret'].at[tk, label] = round((sc.iloc[-1] - mc.iloc[-1]) * 100, 2)
                tables['alpha'].at[tk, label] = round((sc.iloc[-1] - (beta * mc.iloc[-1])) * 100, 2)
                diff = sc - mc
                tables['zsc_act'].at[tk, label] = round((diff.iloc[-1] - diff.mean()) / diff.std(), 2) if diff.std() > 0 else 0
                te = diff.std() * 100
                tables['act_risk'].at[tk, label] = round(te, 2)
                tables['info_r'].at[tk, label] = round((sc.iloc[-1] - mc.iloc[-1]) * 100 / te, 2) if te > 0 else 0

    # Enforce exact ticker order
    for name in tables:
        tables[name] = tables[name].reindex(valid)

    return anchor_df, tables

def build_html_table(df, title, tid, is_anchor=False):
    style = "white-space: nowrap;" if is_anchor else ""
    html = f"<h3>{title}</h3><table id='{tid}' class='display' style='width:100%; {style}'><thead><tr><th>TK</th>"
    for c in df.columns:
        html += f"<th>{c}</th>"
    html += "</tr></thead><tbody>"
    for tk, row in df.iterrows():
        html += f"<tr><td class='ticker-col'>{tk}</td>"
        for c in df.columns:
            val = row[c]
            if is_anchor:
                html += f"<td>{val}</td>"
            else:
                bg, norm = ("#000", 0) if c == 'PRICE' else get_color(val, df[c].min(), df[c].max())
                tc = "black" if norm > 0.6 else "white"   #### COR DO TEXTO DE ACORDO COM A COR DE FUNDO <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
                html += f"<td style='background-color:{bg}; color:{tc}'>{val if not pd.isna(val) else '-'}</td>"
        html += "</tr>"
    return html + "</tbody></table>"

def generate_report():
    res = calculate_metrics()
    if not res:
        return
    anchor_df, tables = res

    full_html = f"""
    <html><head>
    <meta http-equiv="refresh" content="15">
    <link rel="stylesheet" type="text/css" href="https://cdn.datatables.net/1.13.6/css/jquery.dataTables.min.css">
    <script src="https://code.jquery.com/jquery-3.7.0.js"></script>
    <script src="https://cdn.datatables.net/1.13.6/js/jquery.dataTables.min.js"></script>
    <style>
    body {{ background:#000; color:#fff; font-family:'Consolas',monospace; padding:10px; }}
    .anchor-wrap {{ overflow-x: auto; width: 100%; border: 1px solid #333; margin-bottom: 20px; }}
    .anchor-wrap table {{ width: max-content !important; border-collapse: collapse; }}
    .triple-container {{ display: flex; gap: 8px; margin-top: 10px; flex-wrap: wrap; }}
    .triple-item {{ flex: 1; min-width: 30%; }}
    table.dataTable {{ background:#000!important; border:1px solid #444!important; }}
    td {{ border:1px solid #222!important; text-align:center; font-weight:bold; padding:4px!important; font-size:11px; vertical-align: middle; }}
    th {{ background:#0c0c0c!important; color:#00d4ff!important; border-bottom:2px solid #00d4ff!important; font-size:10px; cursor: pointer; }}
    .ticker-col {{ background:#000!important; color:#fff!important; border-right:2px solid #00d4ff!important; min-width:45px; }}
    h3 {{ color:#00d4ff; text-transform:uppercase; margin:8px 0; border-left:4px solid #fff; padding-left:8px; font-size:13px; }}
    </style>
    </head><body>
    <h2 style='text-align:center; color:#555;'>Sector Metrics | SYNC: {datetime.now().strftime('%H:%M:%S')}</h2>
    <div class="anchor-wrap">{build_html_table(anchor_df, "0. Anchor Calendar Map + After-Hours", "t0", True)}</div>
    <div style="width:100%;">{build_html_table(tables['perf'], "1. Performance & Drawdown", "t1")}</div>
    <div class="triple-container">
    <div class="triple-item">{build_html_table(tables['avg_ret'], "2. Mean Return (%)", "t2")}</div>
    <div class="triple-item">{build_html_table(tables['vol'], "3. Volatility (%)", "t3")}</div>
    <div class="triple-item">{build_html_table(tables['zsc_ret'], "4. Z-Score (Return)", "t4")}</div>
    </div>
    <div class="triple-container">
    <div class="triple-item">{build_html_table(tables['beta'], "5. Beta (Systemic)", "t5")}</div>
    <div class="triple-item">{build_html_table(tables['alpha'], "6. Alpha (%)", "t6")}</div>
    <div class="triple-item">{build_html_table(tables['zsc_act'], "7. Z-Score (Active Ret)", "t7")}</div>
    </div>
    <div class="triple-container">
    <div class="triple-item">{build_html_table(tables['act_ret'], "8. Active Return (%)", "t8")}</div>
    <div class="triple-item">{build_html_table(tables['act_risk'], "9. Active Risk (TE %)", "t9")}</div>
    <div class="triple-item">{build_html_table(tables['info_r'], "10. Information Ratio", "t10")}</div>
    </div>
    <script>
    $(document).ready(function() {{
        // Table 0: no sorting, fixed order
        $('#t0').DataTable({{
            "paging": false,
            "info": false,
            "searching": false,
            "ordering": false
        }});

        // All other tables: sortable, but start in current row order
        $('table.display').not('#t0').each(function() {{
            $(this).DataTable({{
                "paging": false,
                "info": false,
                "searching": false,
                "ordering": true,
                "order": []  // no initial sort → keeps DataFrame (all_tickers) order
            }});
        }});
    }});
    </script>
    </body></html>
    """
    with open("terminal_live.html", "w", encoding="utf-8") as f:
        f.write(full_html)

def update_loop():
    while True:
        try:
            initial_sync()
            generate_report()
        except Exception as e:
            print(f"Update error: {e}")
        time.sleep(UPDATE_INTERVAL)

initial_sync()
generate_report()
threading.Thread(target=update_loop, daemon=True).start()

PORT = 8021
with socketserver.TCPServer(("", PORT), http.server.SimpleHTTPRequestHandler) as httpd:
    webbrowser.open(f"http://localhost:{PORT}/terminal_live.html")
    httpd.serve_forever()
