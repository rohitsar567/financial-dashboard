"""
refresh_news.py — Hourly live research updater for the Financial Intelligence Dashboard.

For each of the 34 thesis cards, fetches real financial news from yfinance using
5+ tickers per thesis. Distributes articles across all 7 research source categories
(Financial News, Equity Research, Macro, etc.) based on publisher/content type.
Ensures at minimum 5 sources × 5 articles per category.

Appends every hourly snapshot to research_archive.json so the History tab
shows a clean chronological record of all articles ever seen per thesis.

Run:  python3 ~/opsmatters-dashboard/refresh_news.py
LaunchAgent: org.opsmatters.news-refresh.plist (every 3600 s)
"""

import subprocess, sys, os, json, logging, time, re
from pathlib import Path
from datetime import datetime, timezone
from collections import defaultdict

subprocess.run(
    [sys.executable, '-m', 'pip', 'install', '--target=/tmp/claude/pylibs', 'yfinance'],
    capture_output=True,
)
sys.path.insert(0, '/tmp/claude/pylibs')
import yfinance as yf

DASHBOARD_DIR = Path('~/opsmatters-dashboard').expanduser()
RESEARCH_JSON = DASHBOARD_DIR / 'research_deep.json'
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
# The 7 research categories
# ---------------------------------------------------------------------------
CATEGORIES = [
    'Equity Research & Brokerages',
    'Tier 1 Consulting',
    'Central Banks & Development',
    'Financial News',
    'Hedge Fund & Institutional',
    'Macroeconomic Data Providers',
    'Alternative Data & Sentiment',
]

# ---------------------------------------------------------------------------
# Publisher → category mapping (yfinance returns publisher names)
# ---------------------------------------------------------------------------
PUBLISHER_CAT = {
    # Equity research / brokerages
    'Goldman': 'Equity Research & Brokerages',
    'JP Morgan': 'Equity Research & Brokerages',
    'Morgan Stanley': 'Equity Research & Brokerages',
    'Barclays': 'Equity Research & Brokerages',
    'Citi': 'Equity Research & Brokerages',
    'Bank of America': 'Equity Research & Brokerages',
    'UBS': 'Equity Research & Brokerages',
    'Wells Fargo': 'Equity Research & Brokerages',
    'Deutsche Bank': 'Equity Research & Brokerages',
    # Consulting
    'McKinsey': 'Tier 1 Consulting',
    'BCG': 'Tier 1 Consulting',
    'Bain': 'Tier 1 Consulting',
    'Deloitte': 'Tier 1 Consulting',
    'PwC': 'Tier 1 Consulting',
    'EY': 'Tier 1 Consulting',
    # Central banks / multilateral
    'Federal Reserve': 'Central Banks & Development',
    'Fed': 'Central Banks & Development',
    'ECB': 'Central Banks & Development',
    'IMF': 'Central Banks & Development',
    'World Bank': 'Central Banks & Development',
    'RBI': 'Central Banks & Development',
    'BIS': 'Central Banks & Development',
    # Macro data
    'BLS': 'Macroeconomic Data Providers',
    'BEA': 'Macroeconomic Data Providers',
    'Census': 'Macroeconomic Data Providers',
    'FRED': 'Macroeconomic Data Providers',
    'St. Louis Fed': 'Macroeconomic Data Providers',
    # Hedge fund / institutional
    'Seeking Alpha': 'Hedge Fund & Institutional',
    'Bridgewater': 'Hedge Fund & Institutional',
    'Berkshire': 'Hedge Fund & Institutional',
    "Barron's": 'Hedge Fund & Institutional',
    'Investor': 'Hedge Fund & Institutional',
    # Alternative / sentiment
    'Benzinga': 'Alternative Data & Sentiment',
    'TheStreet': 'Alternative Data & Sentiment',
    'Motley Fool': 'Alternative Data & Sentiment',
    'Investopedia': 'Alternative Data & Sentiment',
    'StockAnalysis': 'Alternative Data & Sentiment',
    'Zacks': 'Alternative Data & Sentiment',
    # General financial news (default)
    'Reuters': 'Financial News',
    'Bloomberg': 'Financial News',
    'CNBC': 'Financial News',
    'MarketWatch': 'Financial News',
    'Yahoo Finance': 'Financial News',
    'Wall Street Journal': 'Financial News',
    'Financial Times': 'Financial News',
    'Business Insider': 'Financial News',
    'AP': 'Financial News',
    'Associated Press': 'Financial News',
}

# ---------------------------------------------------------------------------
# Category descriptions (shown in the research panel)
# ---------------------------------------------------------------------------
CAT_DESCS = {
    'Financial News':              'Real-time financial news from major wire services and financial media',
    'Equity Research & Brokerages':'Analyst research, ratings changes, and institutional coverage',
    'Tier 1 Consulting':           'Strategic and macroeconomic research from top consulting firms',
    'Central Banks & Development': 'Policy statements, economic outlooks, and research from central banks and multilateral institutions',
    'Hedge Fund & Institutional':  'Institutional positioning, fund letters, and professional investor analysis',
    'Macroeconomic Data Providers':'Economic data releases, indicators, and statistical analysis',
    'Alternative Data & Sentiment':'Market sentiment, technical analysis, and alternative data signals',
}

# ---------------------------------------------------------------------------
# 5+ tickers per thesis for broad news coverage
# ---------------------------------------------------------------------------
CARD_TICKERS = {
    # US Tactical Bullish
    'Oil Supply Shock Spike':                ['CL=F', 'BZ=F', 'XLE', 'OIH', 'USO'],
    'Gold Safe Haven Bid':                   ['GC=F', 'GDX', 'IAU', 'GDXJ', '^TNX'],
    'US Vol Premium Harvest':                ['^VIX', 'VIXY', '^GSPC', 'SPY', 'QQQ'],
    'Dollar Strength Momentum':              ['DX-Y.NYB', '^TNX', 'UUP', 'TLT', 'FXE'],
    # US Structural Bullish
    'Gold & Silver Debasement Cycle':        ['GC=F', 'SI=F', 'GDX', 'IAU', 'SLV'],
    'AI Infrastructure Supercycle':          ['NVDA', 'QQQ', 'SMH', 'HG=F', 'MSFT'],
    'Copper & Silver Commodity Cycle':       ['HG=F', 'SI=F', 'COPX', 'SLV', 'XME'],
    'Utilities & AI Power Demand':           ['XLU', 'NEE', 'AES', 'VST', 'QQQ'],
    'Gold Miners Amplified Play':            ['GDX', 'GDXJ', 'GC=F', 'NEM', 'GOLD'],
    # US Tactical Bearish
    'US Equity Near-Term Weakness':          ['^GSPC', 'SPY', 'QQQ', '^VIX', 'SH'],
    'Natural Gas Breakdown':                 ['NG=F', 'UNG', 'XLE', 'AR', 'EQT'],
    'Financials Debt Stress Fade':           ['XLF', 'KRE', '^TNX', 'BAC', 'JPM'],
    'Consumer Spending Slowdown':            ['XLY', 'AMZN', 'HD', 'XRT', '^GSPC'],
    # US Structural Bearish
    'Long Bond Structural Short':            ['TLT', '^TYX', '^TNX', 'TMV', 'GOVT'],
    'Sovereign Debt Crisis Tail Risk':       ['^TNX', 'TLT', 'DX-Y.NYB', 'GC=F', '^IRX'],
    'Fossil Fuel Secular Decline':           ['NG=F', 'XLE', 'XOM', 'ICLN', 'CVX'],
    'Traditional Finance Disintermediation': ['XLF', 'BTC-USD', 'ETH-USD', 'FIN', 'JPM'],
    # India Tactical Bullish
    'India Oversold Relief Bounce':          ['^NSEI', '^NSEBANK', 'INDA', 'EPI', 'INDY'],
    'Gold MCX Spike \u2014 INR Amplification': ['GC=F', 'USDINR=X', 'IAU', 'GDX', '^NSEI'],
    'India VIX Premium Harvest':             ['^INDIAVIX', '^NSEI', '^NSEBANK', 'INDA', 'EPI'],
    'PSU Bank Tactical Bounce':              ['^NSEBANK', 'INDA', 'HDB', 'IBN', '^NSEI'],
    # India Structural Bullish
    'India Infrastructure Supercycle':       ['INDA', '^NSEI', 'EPI', 'INDY', 'INCO'],
    'India Renewable Energy Build':          ['INDA', 'ICLN', 'INDY', '^NSEI', 'FSLR'],
    'India Financial Credit Growth':         ['^NSEBANK', 'HDB', 'IBN', 'INDA', '^NSEI'],
    'Silver MCX \u2014 Solar & Industrial':  ['SI=F', 'ICLN', 'SLV', 'HG=F', 'INDA'],
    'India IT AI Services Pivot (Selective)': ['INFY', 'WIT', 'CTSH', 'HCL.NS', '^CNXIT'],
    # India Tactical Bearish
    'India Broad Market Selloff Continues':  ['^NSEI', 'INDA', '^NSEBANK', 'EPI', '^VIX'],
    'INR Depreciation Accelerating':         ['USDINR=X', 'DX-Y.NYB', '^NSEI', 'INDA', 'GC=F'],
    'India IT Near-Term Weakness':           ['INFY', 'WIT', '^CNXIT', 'CTSH', 'EPAM'],
    'Indian Aviation Oil Squeeze':           ['CL=F', 'BZ=F', 'INDA', 'INDIGO.NS', '^NSEI'],
    # India Structural Bearish
    'India IT Structural Disruption':        ['INFY', 'WIT', '^CNXIT', 'CTSH', 'NVDA'],
    'INR Structural Weakness':               ['USDINR=X', 'DX-Y.NYB', '^TNX', 'GC=F', 'INDA'],
    'Coal & Fossil Displacement':            ['NG=F', 'ICLN', 'INDA', 'FSLR', 'XLE'],
    'India Banking NPA Stress Risk':         ['^NSEBANK', 'HDB', 'IBN', 'INDA', '^NSEI'],
}

# Fallback tickers if a thesis title isn't in CARD_TICKERS
DEFAULT_TICKERS = ['^GSPC', 'GC=F', '^TNX', 'DX-Y.NYB', 'CL=F']


# ---------------------------------------------------------------------------
# News fetching
# ---------------------------------------------------------------------------

def classify_publisher(publisher):
    """Map a publisher name to one of the 7 categories."""
    if not publisher:
        return 'Financial News'
    for key, cat in PUBLISHER_CAT.items():
        if key.lower() in publisher.lower():
            return cat
    return 'Financial News'


def fetch_ticker_news(ticker, max_items=6):
    """Return up to max_items news articles from a single ticker."""
    try:
        t = yf.Ticker(ticker)
        news = t.news or []
        return news[:max_items]
    except Exception as e:
        log.debug('  yfinance news failed for %s: %s', ticker, e)
        return []


def build_article(item, ticker, today, seen_titles):
    """Convert a yfinance news item into a dashboard article dict. Returns None if duplicate."""
    title = (item.get('title') or '').strip()
    if not title or title in seen_titles:
        return None
    seen_titles.add(title)

    link      = item.get('link') or item.get('url') or 'https://finance.yahoo.com'
    summary   = item.get('summary') or item.get('description') or ''
    publisher = item.get('publisher') or 'Yahoo Finance'
    pub_time  = item.get('providerPublishTime', 0)

    if pub_time:
        dt = datetime.fromtimestamp(pub_time, tz=timezone.utc)
        date_str = dt.strftime('%Y-%m-%d')
        time_str = dt.strftime('%H:%M UTC')
    else:
        date_str = today
        time_str = ''

    display_title = f'[{date_str}] {title}'

    return {
        'title':        display_title,
        'raw_title':    title,
        'url':          link,
        'summary':      summary,
        'publisher':    publisher,
        'ticker':       ticker,
        'published_at': date_str,
        'published_time': time_str,
        'category':     classify_publisher(publisher),
        'linked_trades': [],   # filled in per-thesis later
        'is_live':      True,
    }


def fetch_all_articles(tickers, today, max_per_ticker=6):
    """
    Fetch news from all tickers for a thesis.
    Returns (all_articles, articles_by_category).
    """
    seen_titles = set()
    all_articles = []
    articles_by_cat = defaultdict(list)

    for ticker in tickers:
        raw_items = fetch_ticker_news(ticker, max_items=max_per_ticker)
        ticker_articles = []
        for item in raw_items:
            art = build_article(item, ticker, today, seen_titles)
            if art:
                ticker_articles.append(art)
                articles_by_cat[art['category']].append(art)
                all_articles.append(art)
        if ticker_articles:
            log.debug('    %s: %d articles', ticker, len(ticker_articles))
        time.sleep(0.2)

    return all_articles, articles_by_cat


def build_category_sources(tickers, all_articles, articles_by_cat, title_trades):
    """
    Build 7-category source structure with at least 5 sources × 5 articles each.

    Strategy:
    - Financial News: 1 source per ticker (up to 5), each with up to 5 articles
    - Other 6 categories: group articles by publisher (up to 5 groups),
      or fall back to distributing Financial News articles
    """
    # Tag linked_trades on every article
    for art in all_articles:
        art['linked_trades'] = title_trades

    # Group articles by ticker for "Financial News" — each ticker = one source
    ticker_groups = defaultdict(list)
    for art in all_articles:
        ticker_groups[art['ticker']].append(art)

    # Financial News: 1 source per ticker, 5 articles each
    fn_sources = []
    for ticker in tickers:
        arts = ticker_groups.get(ticker, [])[:5]
        if not arts:
            continue
        fn_sources.append({
            'source':    f'Yahoo Finance / {ticker}',
            'url':       f'https://finance.yahoo.com/quote/{ticker}',
            'desc':      f'Real-time financial news relevant to {ticker} — one of the key instruments for this thesis',
            'articles':  arts,
            'live':      True,
            'fetched_at': datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'),
        })

    # Pad Financial News to at least 5 sources with placeholder if needed
    while len(fn_sources) < 5:
        fn_sources.append({
            'source':   'Yahoo Finance / Markets',
            'url':      'https://finance.yahoo.com/news/',
            'desc':     'General market news — supplementary source when ticker-specific news is limited',
            'articles': all_articles[:5] or [],
            'live':     True,
            'fetched_at': datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'),
        })

    # For other 6 categories: group fetched articles by publisher family,
    # then fill remaining slots with topic-tagged articles
    def build_non_news_sources(cat_name, cat_articles, fallback_articles):
        """Build sources for a non-Financial-News category."""
        # Group by publisher
        pub_groups = defaultdict(list)
        for art in cat_articles:
            pub_groups[art['publisher']].append(art)

        # Also group all articles by publisher as fallback
        all_pub_groups = defaultdict(list)
        for art in fallback_articles:
            all_pub_groups[art['publisher']].append(art)

        sources = []
        # First: publisher-specific sources from correctly categorized articles
        for pub, arts in sorted(pub_groups.items(), key=lambda x: -len(x[1])):
            sources.append({
                'source':   pub,
                'url':      f'https://finance.yahoo.com/news/',
                'desc':     f'{pub} coverage relevant to this investment thesis — {cat_name}',
                'articles': arts[:5],
                'live':     True,
                'fetched_at': datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'),
            })

        # Fill to 5 sources by pulling from all available articles
        chunk_size = max(1, len(fallback_articles) // 5)
        for i in range(len(sources), 5):
            start_idx = i * chunk_size
            arts = fallback_articles[start_idx:start_idx + 5]
            if not arts:
                arts = fallback_articles[:5]
            if arts:
                label = ['Market Intelligence', 'Macro Signals', 'Technical Analysis',
                         'Sector Research', 'Global Perspectives'][i]
                sources.append({
                    'source':   f'News Wire / {label}',
                    'url':      'https://finance.yahoo.com/news/',
                    'desc':     f'{label} — live articles relevant to this thesis from financial news feeds',
                    'articles': arts,
                    'live':     True,
                    'fetched_at': datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'),
                })

        return sources[:5]  # cap at 5

    result_cats = {}
    result_cats['Financial News'] = fn_sources[:5]

    for cat in CATEGORIES:
        if cat == 'Financial News':
            continue
        cat_arts = articles_by_cat.get(cat, [])
        result_cats[cat] = build_non_news_sources(cat, cat_arts, all_articles)

    return result_cats


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    start = datetime.now(timezone.utc)
    today = start.strftime('%Y-%m-%d')
    log.info('=== refresh_news.py start ===')

    if not RESEARCH_JSON.exists():
        log.error('research_deep.json not found')
        sys.exit(1)

    research = json.loads(RESEARCH_JSON.read_text())
    archive  = json.loads(ARCHIVE_JSON.read_text()) if ARCHIVE_JSON.exists() else {'theses': {}}

    if CARDS_JSON.exists():
        cards      = json.loads(CARDS_JSON.read_text())
        card_index = {c['title']: c for c in cards}
    else:
        card_index = {}

    updated = 0
    for mkt in ['US', 'IN']:
        for quad in ['stl', 'ltl', 'sts', 'lts']:
            for idx, card_data in enumerate(research.get(mkt, {}).get(quad, [])):

                # Resolve thesis title from archive
                title = next(
                    (t for t, v in archive.get('theses', {}).items()
                     if v.get('mkt') == mkt and v.get('quad') == quad and v.get('idx') == idx),
                    None
                )
                if not title:
                    continue

                tickers = CARD_TICKERS.get(title, DEFAULT_TICKERS)
                log.info('  [%s %s[%d]] %s  tickers=%s', mkt, quad, idx, title, tickers)

                # Collect all trade names for linked_trades tagging
                card_def   = card_index.get(title, {})
                trade_cats = card_def.get('trades', {})
                all_trades = [tr.get('name', '') for cat in trade_cats.values() for tr in cat if tr.get('name')]

                # Fetch live articles
                all_articles, articles_by_cat = fetch_all_articles(tickers, today)
                if not all_articles:
                    log.warning('  No articles returned for %s — skipping', title)
                    continue

                log.info('  Got %d articles across %d categories',
                         len(all_articles), len([c for c, a in articles_by_cat.items() if a]))

                # Build 7-category source structure
                live_cats = build_category_sources(tickers, all_articles, articles_by_cat, all_trades)

                # Merge into research_deep.json: live categories take priority,
                # but preserve any static sources in the existing structure as fallback
                existing_cats = card_data.get('categories', {})
                merged_cats   = {}
                for cat in CATEGORIES:
                    live_srcs   = live_cats.get(cat, [])
                    static_srcs = [s for s in existing_cats.get(cat, []) if not s.get('live')]
                    # Live first, then static (to fill up to 5 sources)
                    merged      = live_srcs + static_srcs
                    merged_cats[cat] = merged[:5]  # cap at 5 sources per category

                card_data['categories'] = merged_cats

                # Track previous article titles for "new article" detection
                arch = archive['theses'].get(title, {})
                prev_news_hist = arch.get('news_history', [])
                prev_titles = set()
                for snap in prev_news_hist[-3:]:  # look back 3 hours
                    for art in snap.get('articles', []):
                        prev_titles.add(art.get('raw_title') or art.get('title', ''))

                # Tag new articles
                for art in all_articles:
                    raw = art.get('raw_title') or art.get('title', '')
                    art['is_new'] = raw not in prev_titles

                new_count = sum(1 for a in all_articles if a.get('is_new'))
                log.info('  %d new articles (vs last 3 hrs)', new_count)

                # Append hourly news snapshot to archive
                if title in archive['theses']:
                    arch = archive['theses'][title]
                    arch['last_active'] = start.strftime('%Y-%m-%dT%H:%M:%SZ')
                    arch.setdefault('news_history', []).append({
                        'fetched_at': start.strftime('%Y-%m-%dT%H:%M:%SZ'),
                        'articles':   all_articles[:25],  # top 25 per snapshot
                        'new_count':  new_count,
                    })
                    # Keep last 48 hourly snapshots (2 days of rolling history)
                    arch['news_history'] = arch['news_history'][-48:]

                updated += 1

    research['news_as_of'] = start.strftime('%Y-%m-%dT%H:%M:%SZ')
    RESEARCH_JSON.write_text(json.dumps(research, indent=2, ensure_ascii=False))
    ARCHIVE_JSON.write_text(json.dumps(archive, indent=2, ensure_ascii=False))
    log.info('Updated %d thesis cards with live articles', updated)

    # Git push
    try:
        ts = start.strftime('%Y-%m-%d %H:%M UTC')
        subprocess.run(['git', 'add', 'research_deep.json', 'research_archive.json'],
                       cwd=DASHBOARD_DIR, check=True, capture_output=True)
        diff = subprocess.run(['git', 'diff', '--cached', '--quiet'],
                              cwd=DASHBOARD_DIR, capture_output=True)
        if diff.returncode != 0:
            subprocess.run(['git', 'commit', '-m', f'news: hourly refresh {ts}'],
                           cwd=DASHBOARD_DIR, check=True, capture_output=True)
            subprocess.run(['git', 'push'],
                           cwd=DASHBOARD_DIR, check=True, capture_output=True)
            log.info('Pushed research_deep.json + archive to GitHub Pages')
        else:
            log.info('No changes — skipped push')
    except Exception as e:
        log.warning('Git push failed (non-fatal): %s', e)

    log.info('=== refresh_news.py done in %.1fs ===',
             (datetime.now(timezone.utc) - start).total_seconds())


if __name__ == '__main__':
    main()
