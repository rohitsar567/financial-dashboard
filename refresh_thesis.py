"""
refresh_thesis.py — Financial Intelligence Dashboard thesis refresher
Reads data.json + sources.json, calls Claude API, writes thesis.json

Run:  ANTHROPIC_API_KEY=sk-ant-... python3 ~/opsmatters-dashboard/refresh_thesis.py

DO NOT run without a valid ANTHROPIC_API_KEY set in the environment.
"""

# ---------------------------------------------------------------------------
# Bootstrap: install anthropic SDK if needed
# ---------------------------------------------------------------------------
import subprocess
import sys
import os

subprocess.run(
    [
        sys.executable, '-m', 'pip', 'install',
        '--target=/tmp/claude/pylibs',
        'anthropic',
    ],
    capture_output=True,
)
sys.path.insert(0, '/tmp/claude/pylibs')

# ---------------------------------------------------------------------------
# Standard imports
# ---------------------------------------------------------------------------
import json
import logging
import traceback
from datetime import datetime, timezone
from pathlib import Path

import anthropic

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
DASHBOARD_DIR  = Path('~/opsmatters-dashboard').expanduser()
DATA_JSON      = DASHBOARD_DIR / 'data.json'
SOURCES_JSON   = DASHBOARD_DIR / 'sources.json'
THESIS_JSON    = DASHBOARD_DIR / 'thesis.json'
LOG_FILE       = DASHBOARD_DIR / 'thesis_refresh.log'

MODEL          = 'claude-haiku-4-5-20251001'
MAX_TOKENS     = 8192
TODAY          = datetime.now(timezone.utc).strftime('%Y-%m-%d')

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s  %(levelname)-8s  %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def load_json(path: Path, default=None):
    """Load a JSON file; return default if missing or malformed."""
    try:
        return json.loads(path.read_text(encoding='utf-8'))
    except FileNotFoundError:
        log.info('%s not found, using default', path.name)
        return default
    except json.JSONDecodeError as exc:
        log.warning('Failed to parse %s: %s', path.name, exc)
        return default


def _safe_val(val, suffix=''):
    """Format a numeric value for the prompt, or return 'n/a'."""
    if val is None:
        return 'n/a'
    return f'{val}{suffix}'


# ---------------------------------------------------------------------------
# Market summary builder
# ---------------------------------------------------------------------------

def build_market_summary(data: dict) -> str:
    """
    Condense data.json into a compact, human-readable market summary
    suitable for injection into the Claude prompt.
    """
    lines = [f"Market data as of: {data.get('as_of', 'unknown')}", '']

    sections = [
        'US Indices', 'India Indices', 'Commodities', 'Bonds & Rates', 'Key ETFs',
    ]

    for section in sections:
        instruments = data.get(section, [])
        if not instruments:
            continue

        lines.append(f'## {section}')
        header = (
            f"{'Name':<22} {'Price':>10} {'RSI':>6} {'MACD dir':>10} "
            f"{'Trend':<20} {'1M%':>7} {'vs SMA20':>9}"
        )
        lines.append(header)
        lines.append('-' * len(header))

        for inst in instruments:
            name    = inst.get('name', '')[:21]
            current = _safe_val(inst.get('current'))
            tech    = inst.get('tech') or {}
            rsi     = _safe_val(tech.get('rsi'))
            macd    = tech.get('macd')
            signal  = tech.get('macd_signal')
            changes = inst.get('changes') or {}
            chg_1m  = _safe_val(changes.get('1M'), '%')
            vs_s20  = _safe_val(tech.get('vs_sma20'), '%')
            trend   = tech.get('trend') or 'n/a'

            # MACD direction
            if macd is not None and signal is not None:
                macd_dir = 'bullish' if macd > signal else 'bearish'
            else:
                macd_dir = 'n/a'

            lines.append(
                f'{name:<22} {str(current):>10} {str(rsi):>6} {macd_dir:>10} '
                f'{trend:<20} {str(chg_1m):>7} {str(vs_s20):>9}'
            )

        lines.append('')

    return '\n'.join(lines)


# ---------------------------------------------------------------------------
# Sources helper
# ---------------------------------------------------------------------------

def pick_top_sources(sources: dict, market: str, n: int = 10) -> list[str]:
    """
    Extract top-N source names/URLs for a given market key
    ('US' or 'IN') from sources.json.
    Falls back to an empty list if the structure doesn't match.
    """
    if not sources:
        return []
    # Support both flat list and keyed dict
    if isinstance(sources, list):
        return [str(s) for s in sources[:n]]
    if isinstance(sources, dict):
        subset = sources.get(market, sources.get('all', []))
        if isinstance(subset, list):
            return [str(s) for s in subset[:n]]
    return []


# ---------------------------------------------------------------------------
# Prompt builder
# ---------------------------------------------------------------------------

QUAD_LABELS = {
    'stl': 'Short-Term Long (bullish, near-term)',
    'ltl': 'Long-Term Long (bullish, multi-month)',
    'sts': 'Short-Term Short (bearish, near-term)',
    'lts': 'Long-Term Short (bearish, multi-month)',
}

THESIS_JSON_SCHEMA = """
{
  "generated_at": "<ISO8601 UTC>",
  "THESIS_META": {
    "<Thesis Title>": {
      "created":  "<YYYY-MM-DD>",
      "updated":  "<YYYY-MM-DD>",
      "retired":  null
    }
  },
  "MARKETS": {
    "US": {
      "stl": [
        {
          "title":      "<thesis title>",
          "desc":       "<2-3 sentence thesis description>",
          "conviction": "<HIGH|MEDIUM|LOW>",
          "nodes":      ["<key instrument or sector>"],
          "signals":    ["<supporting signal>"],
          "strategy":   "<options strategy or trade approach>"
        }
      ],
      "ltl": [],
      "sts": [],
      "lts": []
    },
    "IN": {
      "stl": [], "ltl": [], "sts": [], "lts": []
    }
  },
  "EXPANDED": {
    "US": {
      "stl": [
        {
          "impacts": ["<downstream impact>"],
          "research": ["<credible source or data point>"],
          "trades": {
            "commodities":   [{"name": "", "ticker": "", "type": "", "entry": "", "current": "", "cost": "", "duration": "", "rationale": ""}],
            "fixed_income":  [{"name": "", "ticker": "", "type": "", "entry": "", "current": "", "cost": "", "duration": "", "rationale": ""}],
            "currencies":    [{"name": "", "ticker": "", "type": "", "entry": "", "current": "", "cost": "", "duration": "", "rationale": ""}],
            "equities":      [{"name": "", "ticker": "", "type": "", "entry": "", "current": "", "cost": "", "duration": "", "rationale": ""}]
          }
        }
      ],
      "ltl": [], "sts": [], "lts": []
    },
    "IN": {
      "stl": [], "ltl": [], "sts": [], "lts": []
    }
  }
}
"""

def build_prompt(market_summary: str, existing_thesis: dict, us_sources: list, in_sources: list) -> str:
    """Compose the full prompt for Claude."""

    existing_str = json.dumps(existing_thesis, indent=2) if existing_thesis else 'None (first run)'

    us_src_str = '\n'.join(f'  - {s}' for s in us_sources) or '  (none provided)'
    in_src_str = '\n'.join(f'  - {s}' for s in in_sources) or '  (none provided)'

    prompt = f"""You are a professional options trader and market analyst generating investment thesis cards for a personal financial intelligence dashboard.

Today's date: {TODAY}

---
## CURRENT MARKET DATA

{market_summary}

---
## REFERENCE SOURCES

Top US market sources (use for US thesis research citations):
{us_src_str}

Top India market sources (use for India thesis research citations):
{in_src_str}

---
## EXISTING THESIS (for continuity)

{existing_str}

---
## YOUR TASK

Generate a complete, updated thesis JSON following the exact schema below.

Rules:
1. Maintain all four quads for both markets: stl, ltl, sts, lts
   - stl = Short-Term Long (bullish, near-term, days to weeks)
   - ltl = Long-Term Long (bullish, 1-6 months)
   - sts = Short-Term Short (bearish, near-term, days to weeks)
   - lts = Long-Term Short (bearish, 1-6 months)

2. Each quad must have 4-6 thesis cards. Prioritise the strongest opportunities given current data.

3. THESIS_META:
   - Include every thesis title that appears in MARKETS.
   - If a card is NEW (not in existing thesis), set created = "{TODAY}", updated = "{TODAY}", retired = null.
   - If a card CONTINUES from existing, keep its original created date, set updated = "{TODAY}" if its content changed, else leave updated unchanged.
   - If an old card is REMOVED, keep it in THESIS_META with retired = "{TODAY}".

4. EXPANDED section must mirror MARKETS exactly (same quads, same card order).
   Each card in EXPANDED must have:
   - impacts: list of 2-3 downstream impact statements
   - research: list of 2-3 credible data points or source citations
   - trades: exactly 4 categories (commodities, fixed_income, currencies, equities),
     each with exactly 2 trade objects having these fields:
       name, ticker, type (instrument type label e.g. "Options Call", "ETF", "Futures"),
       entry (entry price/level), current (current price from market data if available),
       cost (estimated cost per contract/unit), duration (e.g. "2-4 weeks"),
       rationale (1-2 sentences)

5. Base thesis on the live market data above — RSI, MACD direction, trend, 1M% changes, and SMA positioning are the primary signals.

6. conviction must be HIGH, MEDIUM, or LOW — calibrate honestly against the data.

7. Output JSON ONLY. No markdown code fences, no commentary before or after, no trailing commas.

---
## OUTPUT SCHEMA

{THESIS_JSON_SCHEMA}
"""
    return prompt


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    log.info('=== refresh_thesis.py started ===')

    # ---- API key check ----------------------------------------------------
    api_key = os.environ.get('ANTHROPIC_API_KEY', '').strip()
    if not api_key:
        log.error(
            'ANTHROPIC_API_KEY is not set. '
            'Export it before running: export ANTHROPIC_API_KEY=sk-ant-...'
        )
        sys.exit(1)

    # ---- Load inputs -------------------------------------------------------
    data = load_json(DATA_JSON)
    if not data:
        log.error('%s not found or empty — run fetch_data.py first', DATA_JSON)
        sys.exit(1)

    sources         = load_json(SOURCES_JSON, default={})
    existing_thesis = load_json(THESIS_JSON,  default=None)

    us_sources = pick_top_sources(sources, 'US', n=10)
    in_sources = pick_top_sources(sources, 'IN', n=10)

    # ---- Build prompt ------------------------------------------------------
    market_summary = build_market_summary(data)
    prompt         = build_prompt(market_summary, existing_thesis, us_sources, in_sources)

    log.info('Prompt length: %d chars', len(prompt))
    log.info('Calling Claude API (model=%s, max_tokens=%d) …', MODEL, MAX_TOKENS)

    # ---- Call Claude -------------------------------------------------------
    try:
        client = anthropic.Anthropic(api_key=api_key)

        message = client.messages.create(
            model=MODEL,
            max_tokens=MAX_TOKENS,
            messages=[
                {
                    'role': 'user',
                    'content': prompt,
                }
            ],
        )

        raw_text = message.content[0].text.strip()
        log.info(
            'API response received: %d chars, stop_reason=%s',
            len(raw_text),
            message.stop_reason,
        )

    except anthropic.AuthenticationError:
        log.error('Authentication failed — check ANTHROPIC_API_KEY value')
        sys.exit(1)
    except anthropic.RateLimitError:
        log.error('Rate limit hit — wait and retry')
        sys.exit(1)
    except Exception:  # noqa: BLE001
        log.error('API call failed:\n%s', traceback.format_exc())
        log.info('Keeping existing thesis.json unchanged')
        sys.exit(1)

    # ---- Parse JSON response -----------------------------------------------
    try:
        # Strip any accidental markdown fences Claude may have added
        text = raw_text
        if text.startswith('```'):
            # Remove opening fence (```json or ```)
            text = text.split('\n', 1)[1] if '\n' in text else text[3:]
        if text.endswith('```'):
            text = text.rsplit('```', 1)[0]
        text = text.strip()

        thesis = json.loads(text)
    except json.JSONDecodeError as exc:
        log.error('Failed to parse Claude response as JSON: %s', exc)
        log.error('Raw response (first 500 chars): %s', raw_text[:500])
        log.info('Keeping existing thesis.json unchanged')
        sys.exit(1)

    # ---- Inject/update generated_at ----------------------------------------
    thesis['generated_at'] = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')

    # ---- Write output -------------------------------------------------------
    THESIS_JSON.write_text(json.dumps(thesis, indent=2), encoding='utf-8')
    log.info('Wrote %s', THESIS_JSON)

    # ---- Summary ------------------------------------------------------------
    us_cards = sum(
        len(thesis.get('MARKETS', {}).get('US', {}).get(q, []))
        for q in ('stl', 'ltl', 'sts', 'lts')
    )
    in_cards = sum(
        len(thesis.get('MARKETS', {}).get('IN', {}).get(q, []))
        for q in ('stl', 'ltl', 'sts', 'lts')
    )
    meta_count = len(thesis.get('THESIS_META', {}))

    log.info(
        '=== refresh_thesis.py done: US=%d cards, IN=%d cards, meta=%d entries ===',
        us_cards, in_cards, meta_count,
    )


if __name__ == '__main__':
    main()
