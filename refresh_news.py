"""
refresh_news.py — Hourly real news fetcher for the Financial Intelligence Dashboard.

Fetches real financial news from yfinance (.news) for the key tickers in each thesis,
updates the 'Financial News' category in research_deep.json, appends new articles to
research_archive.json, and git-pushes the result.

Run:  python3 ~/opsmatters-dashboard/refresh_news.py
LaunchAgent: org.opsmatters.news-refresh.plist (every 3600 s)
"""

import subprocess, sys, os, json, logging, time
from pathlib import Path
from datetime import datetime, timezone

subprocess.run(
    [sys.executable, '-m', 'pip', 'install', '--target=/tmp/claude/pylibs', 'yfinance'],
    capture_output=True,
)
sys.path.insert(0, '/tmp/claude/pylibs')
import yfinance as yf

DASHBOARD_DIR = Path('~/opsmatters-dashboard').expanduser()
RESEARCH_JSON = DASHBOARD_DIR / 'research_deep.json'
ARCHIVE_JSON  = DASHBOARD_DIR / 'research_archive.json'
CARDS_JSON    = Path('/tmp/claude/cards_data.json')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s  %(levelname)-8s  %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(DASHBOARD_DIR / 'news_refresh.log'),
    ]
)
log = logging.getLogger(__name__)

# Map card node IDs to Yahoo Finance tickers for news fetching
NODE_TICKERS = {
    # US
    'SPX': '^GSPC', 'QQQ': 'QQQ', 'VIX': '^VIX', 'DXY': 'DX-Y.NYB',
    'WTI': 'CL=F',  'GOLD': 'GC=F', 'SILVER': 'SI=F', 'COPPER': 'HG=F',
    'NATGAS': 'NG=F', 'TNX': '^TNX', 'TLT': 'TLT', 'GDX': 'GDX',
    'XLE': 'XLE', 'XLF': 'XLF', 'XLU': 'XLU',
    # India
    'NIFTY': '^NSEI', 'BNKN': '^NSEBANK', 'INDIAVIX': '^INDIAVIX',
    'USDINR': 'USDINR=X', 'INDA': 'INDA',
}

# Per-card key tickers (by title) for targeted news fetch
CARD_TICKERS = {
    'Oil Supply Shock Spike':              ['CL=F', 'BZ=F', 'XLE'],
    'Gold Safe Haven Bid':                 ['GC=F', 'GDX'],
    'US Vol Premium Harvest':              ['^VIX'],
    'Dollar Strength Momentum':            ['DX-Y.NYB', '^TNX'],
    'Gold & Silver Debasement Cycle':      ['GC=F', 'SI=F'],
    'AI Infrastructure Supercycle':        ['QQQ', 'NVDA', 'HG=F'],
    'Copper & Silver Commodity Cycle':     ['HG=F', 'SI=F'],
    'Utilities & AI Power Demand':         ['XLU', 'QQQ'],
    'Gold Miners Amplified Play':          ['GDX', 'GC=F'],
    'US Equity Near-Term Weakness':        ['^GSPC', 'QQQ'],
    'Natural Gas Breakdown':               ['NG=F'],
    'Financials Debt Stress Fade':         ['XLF', '^TNX'],
    'Consumer Spending Slowdown':          ['XLY', '^GSPC'],
    'Long Bond Structural Short':          ['TLT', '^TYX'],
    'Sovereign Debt Crisis Tail Risk':     ['^TNX', 'TLT'],
    'Fossil Fuel Secular Decline':         ['NG=F', 'XLE'],
    'Traditional Finance Disintermediation': ['XLF', '^TNX'],
    'India Oversold Relief Bounce':        ['^NSEI', '^NSEBANK'],
    'Gold MCX Spike \u2014 INR Amplification': ['GC=F', 'USDINR=X'],
    'India VIX Premium Harvest':           ['^INDIAVIX', '^NSEI'],
    'PSU Bank Tactical Bounce':            ['^NSEBANK', 'INDA'],
    'India Infrastructure Supercycle':     ['INDA', '^NSEI'],
    'India Renewable Energy Build':        ['INDA', 'ICLN'],
    'India Financial Credit Growth':       ['^NSEBANK', 'INDA'],
    'Silver MCX \u2014 Solar & Industrial': ['SI=F', 'ICLN'],
    'India IT AI Services Pivot (Selective)': ['^CNXIT', 'INFY', 'WIT'],
    'India Broad Market Selloff Continues':['INDA', '^NSEI'],
    'INR Depreciation Accelerating':       ['USDINR=X'],
    'India IT Near-Term Weakness':         ['^CNXIT', 'INFY'],
    'Indian Aviation Oil Squeeze':         ['CL=F', 'INDA'],
    'India IT Structural Disruption':      ['^CNXIT', 'INFY', 'WIT'],
    'INR Structural Weakness':             ['USDINR=X'],
    'Coal & Fossil Displacement':          ['NG=F', 'ICLN'],
    'India Banking NPA Stress Risk':       ['^NSEBANK', 'INDA'],
}

NEWS_SOURCE_NAME = 'Yahoo Finance Live News'
NEWS_SOURCE_URL  = 'https://finance.yahoo.com'
NEWS_SOURCE_DESC = 'Real-time financial news fetched from Yahoo Finance for thesis-relevant tickers'

def fetch_news_for_tickers(tickers, max_items=5):
    """Fetch real news articles from yfinance for a list of tickers."""
    seen_titles = set()
    articles = []
    for ticker in tickers:
        try:
            t = yf.Ticker(ticker)
            news = t.news or []
            for item in news:
                title = item.get('title', '').strip()
                if not title or title in seen_titles:
                    continue
                seen_titles.add(title)
                # yfinance news item structure
                link = item.get('link', '') or item.get('url', '') or NEWS_SOURCE_URL
                summary = item.get('summary', '') or item.get('description', '')
                publisher = item.get('publisher', 'Yahoo Finance')
                pub_time = item.get('providerPublishTime', 0)
                if pub_time:
                    dt = datetime.fromtimestamp(pub_time, tz=timezone.utc).strftime('%Y-%m-%d')
                else:
                    dt = datetime.now(timezone.utc).strftime('%Y-%m-%d')
                articles.append({
                    'title': f'[{dt}] {title}',
                    'url':   link,
                    'summary': summary or f'From {publisher}: real-time news relevant to this investment thesis.',
                    'publisher': publisher,
                    'published_at': dt,
                    'linked_trades': [],  # news articles link to all trades (thesis-level)
                    'is_live': True,
                })
                if len(articles) >= max_items:
                    break
        except Exception as e:
            log.warning('  News fetch failed for %s: %s', ticker, e)
        if len(articles) >= max_items:
            break
    return articles[:max_items]

def main():
    start = datetime.now(timezone.utc)
    log.info('=== refresh_news.py start ===')

    if not RESEARCH_JSON.exists():
        log.error('research_deep.json not found — run generate_research_static.py first')
        sys.exit(1)

    research = json.loads(RESEARCH_JSON.read_text())
    archive  = json.loads(ARCHIVE_JSON.read_text()) if ARCHIVE_JSON.exists() else {'theses': {}, 'schema_version': 1}

    # Load cards for title → (mkt, quad, idx) lookup
    if CARDS_JSON.exists():
        cards = json.loads(CARDS_JSON.read_text())
        card_lookup = {c['title']: c for c in cards}
    else:
        card_lookup = {}

    updated = 0
    for mkt in ['US', 'IN']:
        for quad in ['stl', 'ltl', 'sts', 'lts']:
            for idx, card_data in enumerate(research.get(mkt, {}).get(quad, [])):
                card_def = card_lookup.get(
                    next((c['title'] for c in (json.loads(CARDS_JSON.read_text()) if CARDS_JSON.exists() else [])
                          if c['mkt'] == mkt and c['quad'] == quad and c['idx'] == idx), None), None
                )
                # Find the card title from the archive
                title = next((t for t, v in archive.get('theses', {}).items()
                              if v.get('mkt') == mkt and v.get('quad') == quad and v.get('idx') == idx), None)
                if not title:
                    continue

                tickers = CARD_TICKERS.get(title, [])
                if not tickers:
                    continue

                log.info('  Fetching news: %s (%s/%s[%d])', title, mkt, quad, idx)
                live_articles = fetch_news_for_tickers(tickers, max_items=5)

                if not live_articles:
                    log.warning('    No news returned')
                    continue

                # Tag linked_trades — live news articles link to ALL trades for this thesis
                # Get all trade names from research_deep.json metadata (via archive)
                for art in live_articles:
                    art['linked_trades'] = []  # will be filled below

                # Get all trade names for this card
                cat_data = card_data.get('categories', {})
                # Try to get trades from any existing source
                all_trades = []
                for cat_sources in cat_data.values():
                    for src in cat_sources:
                        for art in src.get('articles', []):
                            all_trades.extend(art.get('linked_trades', []))
                all_trades = list(dict.fromkeys(all_trades))  # dedupe preserving order

                for art in live_articles:
                    art['linked_trades'] = all_trades  # news links to all trades

                # Update the Financial News category with live articles as first source
                fn_cat = cat_data.setdefault('Financial News', [])
                # Remove any previous live-news source
                fn_cat = [s for s in fn_cat if s.get('source') != NEWS_SOURCE_NAME]
                fn_cat.insert(0, {
                    'source':   NEWS_SOURCE_NAME,
                    'url':      NEWS_SOURCE_URL,
                    'desc':     NEWS_SOURCE_DESC,
                    'articles': live_articles,
                    'live':     True,
                    'fetched_at': start.strftime('%Y-%m-%dT%H:%M:%SZ'),
                })
                card_data['categories']['Financial News'] = fn_cat

                # Append to archive (news snapshot)
                if title in archive['theses']:
                    arch = archive['theses'][title]
                    arch['last_active'] = start.strftime('%Y-%m-%dT%H:%M:%SZ')
                    # Add a news-only history entry
                    arch.setdefault('news_history', []).append({
                        'fetched_at': start.strftime('%Y-%m-%dT%H:%M:%SZ'),
                        'articles':   live_articles,
                    })
                    # Keep only last 48 hourly snapshots in news_history
                    arch['news_history'] = arch['news_history'][-48:]

                updated += 1
                time.sleep(0.3)  # be polite to yfinance

    research['news_as_of'] = start.strftime('%Y-%m-%dT%H:%M:%SZ')
    RESEARCH_JSON.write_text(json.dumps(research, indent=2, ensure_ascii=False))
    ARCHIVE_JSON.write_text(json.dumps(archive, indent=2, ensure_ascii=False))
    log.info('Updated news for %d thesis cards', updated)

    # Git push
    try:
        import subprocess as sp
        ts = start.strftime('%Y-%m-%d %H:%M UTC')
        sp.run(['git', 'add', 'research_deep.json', 'research_archive.json'],
               cwd=DASHBOARD_DIR, check=True, capture_output=True)
        result = sp.run(['git', 'diff', '--cached', '--quiet'],
                        cwd=DASHBOARD_DIR, capture_output=True)
        if result.returncode != 0:
            sp.run(['git', 'commit', '-m', f'news: hourly research refresh {ts}'],
                   cwd=DASHBOARD_DIR, check=True, capture_output=True)
            sp.run(['git', 'push'], cwd=DASHBOARD_DIR, check=True, capture_output=True)
            log.info('Pushed research_deep.json to GitHub Pages')
        else:
            log.info('No news changes — skipped git push')
    except Exception as e:
        log.warning('Git push failed (non-fatal): %s', e)

    log.info('=== refresh_news.py done in %.1fs ===',
             (datetime.now(timezone.utc) - start).total_seconds())

if __name__ == '__main__':
    main()
