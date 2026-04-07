"""
refresh_trades.py — Daily trade price updater for the Financial Intelligence Dashboard.

Fetches current underlying prices from yfinance for every trade in every thesis,
calculates movement vs entry, writes trades.json, snapshots to research_archive.json,
and git-pushes.

Run:  python3 ~/opsmatters-dashboard/refresh_trades.py
LaunchAgent: org.opsmatters.trades-refresh.plist (every 86400 s / daily)
"""

import subprocess, sys, os, json, re, logging, time
from pathlib import Path
from datetime import datetime, timezone

subprocess.run(
    [sys.executable, '-m', 'pip', 'install', '--target=/tmp/claude/pylibs', 'yfinance'],
    capture_output=True,
)
sys.path.insert(0, '/tmp/claude/pylibs')
import yfinance as yf

DASHBOARD_DIR = Path('~/opsmatters-dashboard').expanduser()
TRADES_JSON   = DASHBOARD_DIR / 'trades.json'
ARCHIVE_JSON  = DASHBOARD_DIR / 'research_archive.json'
# Look in opsmatters-dashboard first (permanent); fall back to /tmp/claude (session copy)
CARDS_JSON    = DASHBOARD_DIR / 'cards_data.json'
if not CARDS_JSON.exists():
    CARDS_JSON = Path('/tmp/claude/cards_data.json')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s  %(levelname)-8s  %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger(__name__)

# Normalize ticker for yfinance (strip slash-pairs like CAD/USD → CAD=X)
def normalize_ticker(raw):
    t = raw.strip()
    # Currency pair "CAD/USD" → "CADUSD=X", "EUR/NOK" → "EURNOK=X"
    if re.match(r'^[A-Z]{3}/[A-Z]{3}$', t):
        return t.replace('/', '') + '=X'
    # Already valid or close enough
    return t

def fetch_price(ticker_raw):
    ticker = normalize_ticker(ticker_raw)
    try:
        t = yf.Ticker(ticker)
        info = t.fast_info
        price = getattr(info, 'last_price', None) or getattr(info, 'regular_market_price', None)
        if price is None:
            hist = t.history(period='1d', interval='1m')
            price = float(hist['Close'].iloc[-1]) if not hist.empty else None
        return round(float(price), 4) if price else None
    except Exception as e:
        log.warning('    Price fetch failed for %s (%s): %s', ticker_raw, ticker, e)
        return None

def parse_entry_price(entry_str):
    """Extract a numeric price from entry strings like 'Buy $115 strike call'."""
    nums = re.findall(r'\$?([\d,]+(?:\.\d+)?)', entry_str or '')
    for n in nums:
        s = n.replace(',', '')
        if s:
            try:
                return float(s)
            except ValueError:
                continue
    return None

def trade_status(current, entry_ref, is_long):
    """Determine if a trade is tracking as expected."""
    if current is None or entry_ref is None:
        return 'unknown'
    pct = (current - entry_ref) / entry_ref * 100
    if is_long:
        return 'on-track' if pct >= -5 else 'at-risk' if pct >= -15 else 'off-track'
    else:
        return 'on-track' if pct <= 5 else 'at-risk' if pct <= 15 else 'off-track'

def main():
    start = datetime.now(timezone.utc)
    log.info('=== refresh_trades.py start ===')

    if not CARDS_JSON.exists():
        log.error('cards_data.json not found at %s', CARDS_JSON)
        sys.exit(1)

    cards = json.loads(CARDS_JSON.read_text())
    archive = json.loads(ARCHIVE_JSON.read_text()) if ARCHIVE_JSON.exists() else {'theses': {}}

    trades_output = {
        'as_of': start.strftime('%Y-%m-%dT%H:%M:%SZ'),
        'theses': {}
    }

    TRADE_CATS = ['commodities', 'fixed_income', 'currencies', 'equities']

    for card in cards:
        title = card['title']
        mkt   = card['mkt']
        quad  = card['quad']
        idx   = card['idx']
        is_bull = quad in ('stl', 'ltl')

        log.info('  [%s %s[%d]] %s', mkt, quad, idx, title)
        thesis_trades = {}

        for cat in TRADE_CATS:
            for trade in card.get('trades', {}).get(cat, []):
                name   = trade.get('name', '')
                ticker = trade.get('ticker', '')
                entry  = trade.get('entry', '')
                if not ticker:
                    continue

                current_price = fetch_price(ticker)
                entry_ref     = parse_entry_price(entry)
                pct_change    = None
                if current_price and entry_ref:
                    pct_change = round((current_price - entry_ref) / entry_ref * 100, 2)

                status = trade_status(current_price, entry_ref, is_bull)

                trade_data = {
                    'ticker':          ticker,
                    'cat':             cat,
                    'current_price':   current_price,
                    'entry_ref':       entry_ref,
                    'pct_from_entry':  pct_change,
                    'status':          status,
                    'updated_at':      start.strftime('%Y-%m-%dT%H:%M:%SZ'),
                }
                thesis_trades[name] = trade_data
                log.info('    %s (%s): %s → %s (%s%%)',
                         name[:30], ticker, entry_ref, current_price,
                         f'{pct_change:+.1f}' if pct_change is not None else '?')
                time.sleep(0.2)

        trades_output['theses'][title] = {
            'mkt': mkt, 'quad': quad, 'idx': idx,
            'trades': thesis_trades,
        }

        # Append trade snapshot to archive
        if title in archive.get('theses', {}):
            th = archive['theses'][title]
            th.setdefault('trades_history', []).append({
                'as_of':  start.strftime('%Y-%m-%dT%H:%M:%SZ'),
                'trades': thesis_trades,
            })
            # Keep last 30 daily snapshots
            th['trades_history'] = th['trades_history'][-30:]
            th['last_active'] = start.strftime('%Y-%m-%dT%H:%M:%SZ')

    TRADES_JSON.write_text(json.dumps(trades_output, indent=2, ensure_ascii=False))
    ARCHIVE_JSON.write_text(json.dumps(archive, indent=2, ensure_ascii=False))
    log.info('Wrote trades.json (%d theses) and updated archive', len(trades_output['theses']))

    # Git push
    try:
        import subprocess as sp
        ts = start.strftime('%Y-%m-%d %H:%M UTC')
        sp.run(['git', 'add', 'trades.json', 'research_archive.json'],
               cwd=DASHBOARD_DIR, check=True, capture_output=True)
        result = sp.run(['git', 'diff', '--cached', '--quiet'],
                        cwd=DASHBOARD_DIR, capture_output=True)
        if result.returncode != 0:
            sp.run(['git', 'commit', '-m', f'trades: daily price update {ts}'],
                   cwd=DASHBOARD_DIR, check=True, capture_output=True)
            sp.run(['git', 'push'], cwd=DASHBOARD_DIR, check=True, capture_output=True)
            log.info('Pushed trades.json to GitHub Pages')
        else:
            log.info('No trade changes — skipped git push')
    except Exception as e:
        log.warning('Git push failed (non-fatal): %s', e)

    log.info('=== refresh_trades.py done in %.1fs ===',
             (datetime.now(timezone.utc) - start).total_seconds())

if __name__ == '__main__':
    main()
