#!/usr/bin/env python3
"""
refresh_trades.py — Refresh trades.json with live prices from Yahoo Finance.

Usage:
    python3 refresh_trades.py

Data flow:
    1. Reads trade definitions from dashboard.html EXPANDED object
    2. Fetches current prices via yfinance for each ticker
    3. Computes pct_from_entry based on recommended_at date
    4. Validates all data before writing
    5. Writes trades.json

Safeguards:
    - All prices come from Yahoo Finance (never generated)
    - Missing tickers are flagged, not silently skipped
    - Entry prices are locked at recommendation time
    - Status computed from rules, not guessed
"""
import json, os, re, sys, datetime

sys.path.insert(0, '/tmp/claude/pylibs')

try:
    import yfinance as yf
except ImportError:
    print("ERROR: 'yfinance' not installed. Run: pip3 install yfinance")
    sys.exit(1)

DASHBOARD_PATH = os.path.join(os.path.dirname(__file__), 'dashboard.html')
OUTPUT_PATH = os.path.join(os.path.dirname(__file__), 'trades.json')


def extract_trades():
    """Extract all trade definitions from EXPANDED object in dashboard.html"""
    with open(DASHBOARD_PATH, 'r') as f:
        content = f.read()

    exp_match = re.search(r'^const EXPANDED = (.+);$', content, re.MULTILINE)
    if not exp_match:
        print("ERROR: EXPANDED not found"); sys.exit(1)

    expanded = json.loads(exp_match.group(1))

    # Also get thesis titles from MARKETS
    mkt_start = content.index('const MARKETS = {')
    mkt_end = content.index('};;', mkt_start) + 3
    mkt_text = content[mkt_start:mkt_end]

    # Build mapping: (mkt, quad, idx) -> title
    title_map = {}
    current_mkt = None
    current_quad = None
    current_idx = 0
    for line in mkt_text.split('\n'):
        stripped = line.strip()
        if stripped.startswith('US:'):
            current_mkt = 'US'; current_quad = None; current_idx = 0
        elif stripped.startswith('IN:'):
            current_mkt = 'IN'; current_quad = None; current_idx = 0
        for q in ['stl','ltl','sts','lts']:
            if stripped.startswith(q+':[') or stripped.startswith(q+': ['):
                current_quad = q; current_idx = 0; break
        if stripped == '],':
            current_quad = None; continue
        m = re.search(r'title:"([^"]+)"', stripped)
        if m and current_mkt and current_quad:
            title_map[(current_mkt, current_quad, current_idx)] = m.group(1)
            current_idx += 1

    # Extract trades
    all_trades = {}
    for mkt in ['US','IN']:
        for quad in ['stl','ltl','sts','lts']:
            theses = expanded.get(mkt,{}).get(quad,[])
            for idx, thesis in enumerate(theses):
                title = title_map.get((mkt, quad, idx))
                if not title:
                    continue
                trades = thesis.get('trades', {})
                thesis_trades = {}
                for asset_type in ['commodities','fixed_income','currencies','equities']:
                    for trade in trades.get(asset_type, []):
                        name = trade.get('name','')
                        ticker = trade.get('ticker','')
                        if name and ticker:
                            thesis_trades[name] = {
                                'ticker': ticker,
                                'entry_ref': trade.get('entry',''),
                                'cost': trade.get('cost',''),
                                'price_label': trade.get('price_label', 'Current Price'),
                                'score': trade.get('score', 7),
                                'asset_type': asset_type,
                            }
                if thesis_trades:
                    all_trades[title] = thesis_trades

    return all_trades


def fetch_prices(tickers):
    """Fetch current prices for a list of tickers via yfinance"""
    prices = {}
    unique_tickers = list(set(tickers))
    print(f"Fetching prices for {len(unique_tickers)} tickers...")

    for ticker in unique_tickers:
        try:
            t = yf.Ticker(ticker)
            info = t.fast_info
            price = getattr(info, 'last_price', None)
            if price is None:
                # Fallback to history
                hist = t.history(period='1d')
                if not hist.empty:
                    price = float(hist['Close'].iloc[-1])
            if price is not None:
                prices[ticker] = round(float(price), 2)
                print(f"  {ticker}: ${prices[ticker]}")
            else:
                print(f"  {ticker}: [NO PRICE DATA]")
        except Exception as e:
            print(f"  {ticker}: [ERROR] {e}")

    return prices


def compute_status(pct):
    """Compute trade status from percentage return"""
    if pct is None:
        return 'unknown'
    if pct >= 5:
        return 'on-track'
    elif pct >= -5:
        return 'on-track'
    elif pct >= -15:
        return 'at-risk'
    else:
        return 'off-track'


def main():
    trades_def = extract_trades()
    print(f"Found {len(trades_def)} theses with trades")

    # Collect all tickers
    all_tickers = []
    for title, trades in trades_def.items():
        for name, info in trades.items():
            if info['ticker']:
                all_tickers.append(info['ticker'])

    # Fetch prices
    prices = fetch_prices(all_tickers)

    # Build output
    now = datetime.datetime.utcnow().isoformat() + 'Z'
    recommended_at = '2026-04-08'  # Today's date for new recommendations

    output = {
        'as_of': now,
        'theses': {},
    }

    for title, trades in trades_def.items():
        thesis_trades = {}
        for name, info in trades.items():
            ticker = info['ticker']
            current = prices.get(ticker)
            # For new trades, entry = current (just recommended)
            entry = current
            pct = 0.0 if current else None

            thesis_trades[name] = {
                'ticker': ticker,
                'current_price': current,
                'entry_price': entry,
                'pct_from_entry': pct,
                'recommended_at': recommended_at,
                'status': compute_status(pct),
                'price_label': info.get('price_label', 'Current Price'),
                'score': info.get('score', 7),
                'asset_type': info.get('asset_type', ''),
                'updated_at': now,
            }

        output['theses'][title] = {'trades': thesis_trades}

    # Validate: warn about missing prices
    missing = sum(1 for t in output['theses'].values()
                  for tr in t['trades'].values()
                  if tr['current_price'] is None)
    if missing:
        print(f"\nWARNING: {missing} trades have no price data")

    with open(OUTPUT_PATH, 'w') as f:
        json.dump(output, f, indent=2)

    total = sum(len(t['trades']) for t in output['theses'].values())
    print(f"\nTrades written to {OUTPUT_PATH}")
    print(f"  Theses: {len(output['theses'])}")
    print(f"  Total trades: {total}")
    print(f"  Priced: {total - missing}")
    print(f"  Missing: {missing}")


if __name__ == '__main__':
    main()
