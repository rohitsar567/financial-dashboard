"""
refresh_trades.py — Real-time trade price updater for the Financial Intelligence Dashboard.

Runs every 15 minutes via LaunchAgent. For every trade in every thesis:
  - Fetches current underlying price via yfinance
  - On FIRST EVER run for a trade: records baseline_price + recommended_at
  - Calculates two returns:
      pct_from_entry   = (current - entry_ref) / entry_ref × 100   [entry_ref from card text]
      return_since_rec = (current - baseline_price) / baseline_price × 100 [actual rec date price]
  - Writes trades.json (loaded by dashboard every 60s)
  - Archives to research_archive.json (trades_history, capped at 30 snapshots)
  - Git-pushes only when prices have changed materially (>0.1%)

Run:  python3 ~/opsmatters-dashboard/refresh_trades.py
LaunchAgent: org.opsmatters.trades-refresh.plist (every 900 s / 15 min)
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
CARDS_JSON    = DASHBOARD_DIR / 'cards_data.json'
if not CARDS_JSON.exists():
    CARDS_JSON = Path('/tmp/claude/cards_data.json')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s  %(levelname)-8s  %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Price helpers
# ---------------------------------------------------------------------------

def normalize_ticker(raw):
    """Normalize a ticker string: 'CAD/USD' → 'CADUSD=X', 'TIP vs TLT' → 'TLT'."""
    t = raw.strip()
    if re.match(r'^[A-Z]{3}/[A-Z]{3}$', t):
        return t.replace('/', '') + '=X'
    # Multi-ticker strings like 'TIP vs TLT' → use the last clean ticker
    parts = re.split(r'\s+(?:vs?|VS?)\s+', t)
    if len(parts) > 1:
        return parts[-1].strip()
    return t


def fetch_price(ticker_raw):
    ticker = normalize_ticker(ticker_raw)
    try:
        t = yf.Ticker(ticker)
        info = t.fast_info
        price = getattr(info, 'last_price', None) or getattr(info, 'regular_market_price', None)
        if price is None:
            hist = t.history(period='5d', interval='1d')
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
                v = float(s)
                if v > 0:
                    return v
            except ValueError:
                continue
    return None


def trade_status(current, baseline, is_long):
    """
    Status is relative to the baseline (actual recommendation-date price).
    on-track  : long +0%+, short flat/negative
    at-risk   : moderate adverse move
    off-track : significant adverse move
    """
    if current is None or baseline is None:
        return 'unknown'
    pct = (current - baseline) / baseline * 100
    if is_long:
        return 'on-track' if pct >= -5 else 'at-risk' if pct >= -15 else 'off-track'
    else:
        return 'on-track' if pct <= 5 else 'at-risk' if pct <= 15 else 'off-track'


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    start = datetime.now(timezone.utc)
    today = start.strftime('%Y-%m-%d')
    log.info('=== refresh_trades.py start ===')

    if not CARDS_JSON.exists():
        log.error('cards_data.json not found at %s', CARDS_JSON)
        sys.exit(1)

    cards   = json.loads(CARDS_JSON.read_text())
    archive = json.loads(ARCHIVE_JSON.read_text()) if ARCHIVE_JSON.exists() else {'theses': {}}

    # Load previous trades.json for change detection (skip git push if prices barely moved)
    prev_trades = {}
    if TRADES_JSON.exists():
        try:
            prev_trades = json.loads(TRADES_JSON.read_text()).get('theses', {})
        except Exception:
            pass

    trades_output = {
        'as_of': start.strftime('%Y-%m-%dT%H:%M:%SZ'),
        'theses': {}
    }

    TRADE_CATS = ['commodities', 'fixed_income', 'currencies', 'equities']

    for card in cards:
        title   = card['title']
        mkt     = card['mkt']
        quad    = card['quad']
        idx     = card['idx']
        is_bull = quad in ('stl', 'ltl')

        # Thesis recommendation date from archive (first_seen)
        thesis_arch   = archive.get('theses', {}).get(title, {})
        thesis_rec_dt = thesis_arch.get('first_seen', today)[:10]

        # Per-thesis trade baselines: {trade_name: {baseline_price, recommended_at, ticker}}
        baselines = thesis_arch.setdefault('trade_baselines', {})

        log.info('  [%s %s[%d]] %s', mkt, quad, idx, title)
        thesis_trades = {}

        for cat in TRADE_CATS:
            for trade in card.get('trades', {}).get(cat, []):
                name   = trade.get('name', '')
                ticker = trade.get('ticker', '')
                entry  = trade.get('entry', '')
                if not ticker or not name:
                    continue

                current_price = fetch_price(ticker)

                # Establish baseline on first encounter
                if name not in baselines:
                    baselines[name] = {
                        'ticker':         ticker,
                        'baseline_price': current_price,
                        'recommended_at': today,
                    }
                    log.info('    NEW baseline for %s: %.4f @ %s',
                             name[:35], current_price or 0, today)

                baseline_price   = baselines[name].get('baseline_price')
                recommended_at   = baselines[name].get('recommended_at', today)
                entry_ref        = parse_entry_price(entry)

                # pct_from_entry: vs the text entry price in the card
                pct_from_entry = None
                if current_price and entry_ref:
                    pct_from_entry = round((current_price - entry_ref) / entry_ref * 100, 2)

                # return_since_rec: vs actual price at recommendation date
                return_since_rec = None
                if current_price and baseline_price:
                    return_since_rec = round((current_price - baseline_price) / baseline_price * 100, 2)

                status = trade_status(current_price, baseline_price, is_bull)

                trade_data = {
                    'ticker':           ticker,
                    'cat':              cat,
                    'current_price':    current_price,
                    'entry_ref':        entry_ref,
                    'baseline_price':   baseline_price,
                    'recommended_at':   recommended_at,
                    'pct_from_entry':   pct_from_entry,
                    'return_since_rec': return_since_rec,
                    'status':           status,
                    'updated_at':       start.strftime('%Y-%m-%dT%H:%M:%SZ'),
                }
                thesis_trades[name] = trade_data

                rec_pct = f'{return_since_rec:+.1f}%' if return_since_rec is not None else '?%'
                log.info('    %-32s (%s) rec=%s  curr=%-10s  ret=%s  [%s]',
                         name[:32], ticker, recommended_at, current_price or '—', rec_pct, status)
                time.sleep(0.15)

        trades_output['theses'][title] = {
            'mkt': mkt, 'quad': quad, 'idx': idx,
            'thesis_rec_date': thesis_rec_dt,
            'trades': thesis_trades,
        }

        # Save baselines back to archive
        if title in archive.get('theses', {}):
            archive['theses'][title]['trade_baselines'] = baselines

        # Snapshot: only if any price changed materially (>0.1%) vs last snapshot
        prev_thesis = prev_trades.get(title, {})
        changed = False
        for tname, td in thesis_trades.items():
            prev_td = prev_thesis.get('trades', {}).get(tname, {})
            prev_p  = prev_td.get('current_price')
            curr_p  = td.get('current_price')
            if curr_p and prev_p and abs(curr_p - prev_p) / prev_p > 0.001:
                changed = True
                break
            if curr_p and not prev_p:
                changed = True
                break

        if changed and title in archive.get('theses', {}):
            th = archive['theses'][title]
            th.setdefault('trades_history', []).append({
                'as_of':  start.strftime('%Y-%m-%dT%H:%M:%SZ'),
                'trades': thesis_trades,
            })
            # Keep last 96 snapshots (15-min × 96 = 24 hours of intraday history)
            th['trades_history'] = th['trades_history'][-96:]
            th['last_active']    = start.strftime('%Y-%m-%dT%H:%M:%SZ')

    TRADES_JSON.write_text(json.dumps(trades_output, indent=2, ensure_ascii=False))
    ARCHIVE_JSON.write_text(json.dumps(archive, indent=2, ensure_ascii=False))
    log.info('Wrote trades.json (%d theses)', len(trades_output['theses']))

    # Git push
    try:
        ts = start.strftime('%Y-%m-%d %H:%M UTC')
        subprocess.run(['git', 'add', 'trades.json', 'research_archive.json'],
                       cwd=DASHBOARD_DIR, check=True, capture_output=True)
        result = subprocess.run(['git', 'diff', '--cached', '--quiet'],
                                cwd=DASHBOARD_DIR, capture_output=True)
        if result.returncode != 0:
            subprocess.run(['git', 'commit', '-m', f'trades: price update {ts}'],
                           cwd=DASHBOARD_DIR, check=True, capture_output=True)
            subprocess.run(['git', 'push'],
                           cwd=DASHBOARD_DIR, check=True, capture_output=True)
            log.info('Pushed to GitHub Pages')
        else:
            log.info('No material price changes — skipped push')
    except Exception as e:
        log.warning('Git push failed (non-fatal): %s', e)

    log.info('=== refresh_trades.py done in %.1fs ===',
             (datetime.now(timezone.utc) - start).total_seconds())


if __name__ == '__main__':
    main()
