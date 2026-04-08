#!/usr/bin/env python3
"""
refresh_news.py — Refresh research_deep.json with real article URLs via Brave Search API.

Usage:
    BRAVE_API_KEY=xxx python3 refresh_news.py

Data flow:
    1. Reads thesis titles from dashboard.html MARKETS object
    2. For each thesis × category × provider, searches Brave for real articles
    3. Validates each URL with a HEAD request (must return 200-399)
    4. Writes validated results to research_deep.json
    5. Archives previous version to research_archive.json

Safeguards against hallucination:
    - Every URL comes from Brave Search API results (never generated)
    - Every URL is validated via HTTP HEAD before inclusion
    - URLs that redirect to login/paywall pages are flagged
    - Duplicate URLs across theses are allowed (same article may be relevant)
    - Timestamps track when each article was found
"""
import json, os, re, sys, time, datetime
from urllib.parse import urlparse

# Add local pylibs
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..', 'pylibs'))
sys.path.insert(0, '/tmp/claude/pylibs')

try:
    import requests
except ImportError:
    print("ERROR: 'requests' not installed. Run: pip3 install requests")
    sys.exit(1)

BRAVE_API_KEY = os.environ.get('BRAVE_API_KEY', '')
BRAVE_SEARCH_URL = 'https://api.search.brave.com/res/v1/web/search'
DASHBOARD_PATH = os.path.join(os.path.dirname(__file__), 'dashboard.html')
OUTPUT_PATH = os.path.join(os.path.dirname(__file__), 'research_deep.json')
ARCHIVE_PATH = os.path.join(os.path.dirname(__file__), 'research_archive.json')

# ── RESEARCH CATEGORIES & PROVIDERS ────────────────────────────
CATEGORIES = {
    "Equity Research & Brokerages": {
        "US": [
            {"name": "Goldman Sachs", "domain": "goldmansachs.com"},
            {"name": "JP Morgan", "domain": "jpmorgan.com"},
            {"name": "Morgan Stanley", "domain": "morganstanley.com"},
            {"name": "UBS", "domain": "ubs.com"},
            {"name": "Barclays", "domain": "barclays.co.uk"},
            {"name": "Citi Research", "domain": "citigroup.com"},
        ],
        "IN": [
            {"name": "Goldman Sachs", "domain": "goldmansachs.com"},
            {"name": "JP Morgan", "domain": "jpmorgan.com"},
            {"name": "Morgan Stanley", "domain": "morganstanley.com"},
            {"name": "CLSA", "domain": "clsa.com"},
            {"name": "Nomura", "domain": "nomura.com"},
            {"name": "Motilal Oswal", "domain": "motilaloswal.com"},
        ],
    },
    "Tier 1 Consulting": {
        "US": [
            {"name": "McKinsey", "domain": "mckinsey.com"},
            {"name": "BCG", "domain": "bcg.com"},
            {"name": "Deloitte", "domain": "deloitte.com"},
            {"name": "PwC", "domain": "pwc.com"},
            {"name": "EY", "domain": "ey.com"},
        ],
        "IN": [
            {"name": "McKinsey", "domain": "mckinsey.com"},
            {"name": "BCG", "domain": "bcg.com"},
            {"name": "Deloitte India", "domain": "deloitte.com"},
            {"name": "KPMG India", "domain": "kpmg.com"},
            {"name": "EY India", "domain": "ey.com"},
            {"name": "Nasscom", "domain": "nasscom.in"},
        ],
    },
    "Central Banks & Development": {
        "US": [
            {"name": "Federal Reserve", "domain": "federalreserve.gov"},
            {"name": "ECB", "domain": "ecb.europa.eu"},
            {"name": "IMF", "domain": "imf.org"},
            {"name": "World Bank", "domain": "worldbank.org"},
            {"name": "BIS", "domain": "bis.org"},
        ],
        "IN": [
            {"name": "RBI", "domain": "rbi.org.in"},
            {"name": "IMF", "domain": "imf.org"},
            {"name": "World Bank", "domain": "worldbank.org"},
            {"name": "ADB", "domain": "adb.org"},
            {"name": "NITI Aayog", "domain": "niti.gov.in"},
        ],
    },
    "Financial News": {
        "US": [
            {"name": "Bloomberg", "domain": "bloomberg.com"},
            {"name": "Reuters", "domain": "reuters.com"},
            {"name": "Financial Times", "domain": "ft.com"},
            {"name": "CNBC", "domain": "cnbc.com"},
            {"name": "Wall Street Journal", "domain": "wsj.com"},
            {"name": "MarketWatch", "domain": "marketwatch.com"},
        ],
        "IN": [
            {"name": "Bloomberg", "domain": "bloomberg.com"},
            {"name": "Reuters", "domain": "reuters.com"},
            {"name": "Economic Times", "domain": "economictimes.indiatimes.com"},
            {"name": "Livemint", "domain": "livemint.com"},
            {"name": "Moneycontrol", "domain": "moneycontrol.com"},
            {"name": "CNBC TV18", "domain": "cnbctv18.com"},
        ],
    },
    "Asset Managers & Hedge Funds": {
        "US": [
            {"name": "BlackRock", "domain": "blackrock.com"},
            {"name": "Vanguard", "domain": "vanguard.com"},
            {"name": "PIMCO", "domain": "pimco.com"},
            {"name": "Fidelity", "domain": "fidelity.com"},
            {"name": "Schroders", "domain": "schroders.com"},
        ],
        "IN": [
            {"name": "BlackRock", "domain": "blackrock.com"},
            {"name": "HDFC AMC", "domain": "hdfcfund.com"},
            {"name": "SBI MF", "domain": "sbimf.com"},
            {"name": "ICICI Prudential", "domain": "icicipruamc.com"},
            {"name": "Nippon India", "domain": "nipponindiamf.com"},
        ],
    },
    "Podcasts, Blogs & Newsletters": {
        "US": [
            {"name": "Seeking Alpha", "domain": "seekingalpha.com"},
            {"name": "Morningstar", "domain": "morningstar.com"},
            {"name": "Investopedia", "domain": "investopedia.com"},
            {"name": "The Motley Fool", "domain": "fool.com"},
            {"name": "Finimize", "domain": "finimize.com"},
        ],
        "IN": [
            {"name": "Seeking Alpha", "domain": "seekingalpha.com"},
            {"name": "Morningstar India", "domain": "morningstar.in"},
            {"name": "Zerodha Varsity", "domain": "zerodha.com"},
            {"name": "Finshots", "domain": "finshots.in"},
            {"name": "ET Markets", "domain": "economictimes.indiatimes.com"},
        ],
    },
    "Indices & Data Sources": {
        "US": [
            {"name": "CME Group", "domain": "cmegroup.com"},
            {"name": "CBOE", "domain": "cboe.com"},
            {"name": "S&P Global", "domain": "spglobal.com"},
            {"name": "FRED", "domain": "fred.stlouisfed.org"},
            {"name": "Yahoo Finance", "domain": "finance.yahoo.com"},
        ],
        "IN": [
            {"name": "NSE India", "domain": "nseindia.com"},
            {"name": "BSE India", "domain": "bseindia.com"},
            {"name": "MCX", "domain": "mcxindia.com"},
            {"name": "S&P Global", "domain": "spglobal.com"},
            {"name": "TradingEconomics", "domain": "tradingeconomics.com"},
        ],
    },
}

# ── SEARCH KEYWORDS PER THESIS ─────────────────────────────────
THESIS_SEARCH_TERMS = {
    "Oil Supply Shock Spike": "oil supply shock crude WTI Iran geopolitical",
    "Gold Safe Haven Bid": "gold safe haven inflation geopolitical hedge",
    "US Vol Premium Harvest": "VIX volatility premium options selling",
    "Gold & Silver Debasement Cycle": "gold silver currency debasement sovereign debt precious metals",
    "AI Infrastructure Supercycle": "AI infrastructure semiconductor data center capex",
    "Copper & Silver Commodity Cycle": "copper silver demand EV AI data center supply",
    "US Equity Near-Term Weakness": "US equity market correction SPX bearish outlook",
    "Natural Gas Breakdown": "natural gas oversupply bearish prices",
    "Long Bond Structural Short": "long bond short treasury yields inflation fiscal deficit",
    "Sovereign Debt Crisis Tail Risk": "sovereign debt crisis global yields fiscal",
    "Gold MCX Spike \u2014 INR Amplification": "gold MCX India INR rupee amplification",
    "India Oversold Relief Bounce": "India Nifty oversold bounce relief rally",
    "India Infrastructure Supercycle": "India infrastructure capex government spending roads rail",
    "India Renewable Energy Build": "India renewable energy solar wind 500GW target",
    "India Financial Credit Growth": "India credit growth banking retail loans penetration",
    "India Broad Market Selloff Continues": "India market selloff FII outflows Nifty bear",
    "INR Depreciation Accelerating": "Indian rupee depreciation USD INR oil imports",
    "India IT Near-Term Weakness": "India IT sector weakness Nifty IT bearish",
    "India IT Structural Disruption": "India IT outsourcing AI disruption GenAI automation",
    "INR Structural Weakness": "Indian rupee structural weakness current account deficit",
}


def brave_search(query, count=10):
    """Search Brave and return list of {url, title, description}"""
    if not BRAVE_API_KEY:
        print(f"  [SKIP] No BRAVE_API_KEY set, skipping search: {query[:60]}")
        return []
    headers = {
        'Accept': 'application/json',
        'Accept-Encoding': 'gzip',
        'X-Subscription-Token': BRAVE_API_KEY,
    }
    params = {'q': query, 'count': count}
    try:
        resp = requests.get(BRAVE_SEARCH_URL, headers=headers, params=params, timeout=10)
        if resp.status_code != 200:
            print(f"  [WARN] Brave returned {resp.status_code} for: {query[:60]}")
            return []
        data = resp.json()
        results = []
        for r in data.get('web', {}).get('results', []):
            results.append({
                'url': r.get('url', ''),
                'title': r.get('title', ''),
                'description': r.get('description', ''),
            })
        return results
    except Exception as e:
        print(f"  [ERROR] Search failed: {e}")
        return []


def validate_url(url, timeout=5):
    """Validate URL returns a successful HTTP response (not a redirect to login)"""
    try:
        resp = requests.head(url, timeout=timeout, allow_redirects=True,
                            headers={'User-Agent': 'Mozilla/5.0 (compatible; ResearchBot/1.0)'})
        # Check for paywalls/login redirects
        final_url = resp.url
        login_indicators = ['login', 'signin', 'subscribe', 'register', 'paywall', 'access-denied']
        if any(ind in final_url.lower() for ind in login_indicators):
            return False
        return 200 <= resp.status_code < 400
    except Exception:
        # If HEAD fails, try GET with stream (some servers block HEAD)
        try:
            resp = requests.get(url, timeout=timeout, allow_redirects=True, stream=True,
                               headers={'User-Agent': 'Mozilla/5.0 (compatible; ResearchBot/1.0)'})
            resp.close()
            return 200 <= resp.status_code < 400
        except Exception:
            return False


def extract_thesis_titles():
    """Extract thesis titles from dashboard.html MARKETS object"""
    with open(DASHBOARD_PATH, 'r') as f:
        content = f.read()
    # Find MARKETS region
    start = content.index('const MARKETS = {')
    end = content.index('};;', start) + 3
    markets_text = content[start:end]

    # Extract titles and their market
    theses = []
    current_mkt = None
    for line in markets_text.split('\n'):
        stripped = line.strip()
        if stripped.startswith('US:'):
            current_mkt = 'US'
        elif stripped.startswith('IN:'):
            current_mkt = 'IN'
        m = re.search(r'title:"([^"]+)"', stripped)
        if m and current_mkt:
            theses.append((m.group(1), current_mkt))
    return theses


def search_for_thesis(title, market, category, providers):
    """Search for real articles for a thesis + category combination"""
    terms = THESIS_SEARCH_TERMS.get(title, title)
    articles_by_provider = {}

    for provider in providers:
        domain = provider['domain']
        name = provider['name']
        # Search for articles from this provider about this topic
        query = f'site:{domain} {terms}'
        results = brave_search(query, count=7)

        articles = []
        for r in results:
            if len(articles) >= 5:
                break
            url = r['url']
            # Quick domain check
            parsed = urlparse(url)
            if domain not in parsed.netloc:
                continue
            # Validate URL
            if validate_url(url):
                articles.append({
                    'title': r['title'],
                    'url': url,
                    'summary': r['description'][:200] if r['description'] else '',
                    'found_at': datetime.datetime.utcnow().isoformat() + 'Z',
                    'validated': True,
                })
                print(f"    [OK] {name}: {r['title'][:60]}")
            else:
                print(f"    [FAIL] {name}: URL validation failed: {url[:60]}")

        if articles:
            articles_by_provider[name] = {
                'source': name,
                'url': f'https://{domain}',
                'desc': f'{name} research and analysis',
                'articles': articles,
            }
        else:
            print(f"    [MISS] {name}: No valid articles found")

        # Rate limit
        time.sleep(0.5)

    return articles_by_provider


def build_research_deep():
    """Build the full research_deep.json"""
    theses = extract_thesis_titles()
    print(f"Found {len(theses)} theses")

    research = {
        'as_of': datetime.datetime.utcnow().isoformat() + 'Z',
        'theses': {},
    }

    for title, market in theses:
        print(f"\n{'='*60}")
        print(f"Thesis: {title} ({market})")
        print(f"{'='*60}")

        thesis_data = {'categories': {}}

        for cat_name, cat_providers in CATEGORIES.items():
            providers = cat_providers.get(market, cat_providers.get('US', []))
            print(f"\n  Category: {cat_name} ({len(providers)} providers)")

            provider_data = search_for_thesis(title, market, cat_name, providers)
            thesis_data['categories'][cat_name] = list(provider_data.values())

            provider_count = len(provider_data)
            article_count = sum(len(p['articles']) for p in provider_data.values())
            print(f"  Result: {provider_count} providers, {article_count} articles")

        research['theses'][title] = thesis_data

    return research


def archive_previous():
    """Archive the current research_deep.json into research_archive.json"""
    if not os.path.exists(OUTPUT_PATH):
        return
    try:
        with open(OUTPUT_PATH, 'r') as f:
            current = json.load(f)

        archive = {'theses': {}}
        if os.path.exists(ARCHIVE_PATH):
            with open(ARCHIVE_PATH, 'r') as f:
                archive = json.load(f)

        # Add current as a historical snapshot
        for title, data in current.get('theses', {}).items():
            if title not in archive['theses']:
                archive['theses'][title] = {'history': [], 'news_history': []}
            archive['theses'][title]['history'].append({
                'as_of': current.get('as_of', ''),
                'categories': data.get('categories', {}),
            })
            # Keep only last 10 snapshots
            archive['theses'][title]['history'] = archive['theses'][title]['history'][-10:]

        with open(ARCHIVE_PATH, 'w') as f:
            json.dump(archive, f, indent=2)
        print(f"Archived previous data to {ARCHIVE_PATH}")
    except Exception as e:
        print(f"Warning: Could not archive: {e}")


def main():
    if not BRAVE_API_KEY:
        print("WARNING: BRAVE_API_KEY not set. Set it to enable real article search.")
        print("Usage: BRAVE_API_KEY=xxx python3 refresh_news.py")
        print("\nGenerating structure with empty articles (refresh script ready for when key is available)...")

    archive_previous()
    research = build_research_deep()

    with open(OUTPUT_PATH, 'w') as f:
        json.dump(research, f, indent=2)

    # Stats
    total_articles = 0
    total_providers = 0
    for title, data in research['theses'].items():
        for cat, providers in data['categories'].items():
            total_providers += len(providers)
            for p in providers:
                total_articles += len(p.get('articles', []))

    print(f"\n{'='*60}")
    print(f"Research data written to {OUTPUT_PATH}")
    print(f"  Theses: {len(research['theses'])}")
    print(f"  Total providers: {total_providers}")
    print(f"  Total articles: {total_articles}")
    print(f"  Timestamp: {research['as_of']}")


if __name__ == '__main__':
    main()
