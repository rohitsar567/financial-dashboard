"""
refresh_thesis.py — Weekly thesis refresher for the Financial Intelligence Dashboard.

Uses the `claude -p` CLI (Claude Code subscription compute — no API credits needed).
Reads data.json + research_archive.json + cards_data.json, generates updated thesis
conviction/narrative via Claude CLI, archives snapshot to research_archive.json,
and git-pushes.

Run:  python3 ~/opsmatters-dashboard/refresh_thesis.py
LaunchAgent: org.opsmatters.thesis-refresh.plist (weekly, Sunday 06:00)
"""

import subprocess, sys, os, json, re, logging, traceback
from datetime import datetime, timezone
from pathlib import Path

DASHBOARD_DIR  = Path('~/opsmatters-dashboard').expanduser()
DATA_JSON      = DASHBOARD_DIR / 'data.json'
ARCHIVE_JSON   = DASHBOARD_DIR / 'research_archive.json'
CARDS_JSON     = Path('/tmp/claude/cards_data.json')
LOG_FILE       = DASHBOARD_DIR / 'thesis_refresh.log'

TODAY = datetime.now(timezone.utc).strftime('%Y-%m-%d')

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
# Claude CLI helper
# ---------------------------------------------------------------------------

def call_claude(prompt, timeout=300):
    """
    Call the Claude Code CLI with a prompt string.
    Tries multiple invocation forms for compatibility.
    Returns the response text, or raises RuntimeError if unavailable.
    """
    candidates = [
        ['claude', '-p', prompt, '--output-format', 'text'],
        ['claude', '--print', prompt],
        ['claude', '-p', prompt],
    ]
    for cmd in candidates:
        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=timeout,
                env={**os.environ, 'NO_COLOR': '1'},
            )
            if result.returncode == 0 and result.stdout.strip():
                return result.stdout.strip()
        except FileNotFoundError:
            continue
        except subprocess.TimeoutExpired:
            log.warning('Claude CLI timed out after %ds', timeout)
            continue
        except Exception as e:
            log.warning('Claude CLI call failed: %s', e)
            continue
    raise RuntimeError('Claude CLI unavailable — is `claude` in PATH and logged in?')


def extract_json(text):
    """Strip markdown fences and extract first JSON object from Claude response."""
    # Remove code fences
    text = re.sub(r'^```[a-zA-Z]*\n?', '', text.strip())
    text = re.sub(r'\n?```$', '', text.strip())
    text = text.strip()
    # Find first { ... } blob
    match = re.search(r'\{[\s\S]*\}', text)
    if match:
        return json.loads(match.group())
    return json.loads(text)


# ---------------------------------------------------------------------------
# Market summary builder
# ---------------------------------------------------------------------------

def safe_val(val, suffix=''):
    if val is None:
        return 'n/a'
    return f'{val}{suffix}'


def build_market_summary(data):
    lines = [f"Market data as of: {data.get('as_of', 'unknown')}", '']
    sections = ['US Indices', 'India Indices', 'Commodities', 'Bonds & Rates', 'Key ETFs']
    for section in sections:
        instruments = data.get(section, [])
        if not instruments:
            continue
        lines.append(f'## {section}')
        for inst in instruments:
            name    = inst.get('name', '')[:22]
            current = safe_val(inst.get('current'))
            tech    = inst.get('tech') or {}
            rsi     = safe_val(tech.get('rsi'))
            macd    = tech.get('macd')
            signal  = tech.get('macd_signal')
            changes = inst.get('changes') or {}
            chg_1m  = safe_val(changes.get('1M'), '%')
            trend   = tech.get('trend') or 'n/a'
            macd_dir = ('bullish' if macd is not None and signal is not None and macd > signal
                        else 'bearish' if macd is not None and signal is not None else 'n/a')
            lines.append(f'  {name:<22} price={current}  RSI={rsi}  MACD={macd_dir}  trend={trend}  1M={chg_1m}')
        lines.append('')
    return '\n'.join(lines)


# ---------------------------------------------------------------------------
# Prompt builder — per card
# ---------------------------------------------------------------------------

QUAD_LABELS = {
    'stl': 'Short-Term Long (bullish, near-term)',
    'ltl': 'Long-Term Long (bullish, multi-month)',
    'sts': 'Short-Term Short (bearish, near-term)',
    'lts': 'Long-Term Short (bearish, multi-month)',
}

def build_thesis_prompt(card, market_summary, trades_snapshot=None):
    title = card['title']
    desc  = card.get('desc', '')
    strat = card.get('strategy', '')
    conv  = card.get('conviction', 'MEDIUM')
    quad  = card.get('quad', '')
    mkt   = card.get('mkt', '')

    trades_str = ''
    if trades_snapshot:
        rows = []
        for name, td in trades_snapshot.items():
            pct = f"{td.get('pct_from_entry', 0):+.1f}%" if td.get('pct_from_entry') is not None else '?%'
            status = td.get('status', 'unknown')
            rows.append(f"  {name[:40]}: {pct} ({status})")
        trades_str = '\n'.join(rows) if rows else '  (no trade data)'
    else:
        trades_str = '  (trade prices not available this run)'

    prompt = f"""You are a professional options trader and market analyst reviewing an existing investment thesis.

Today: {TODAY}
Thesis: {title}
Market: {mkt.upper()} — {QUAD_LABELS.get(quad, quad)}
Current conviction: {conv}
Description: {desc}
Strategy: {strat}

Trade performance since entry:
{trades_str}

Current market context:
{market_summary}

Review this thesis and return a JSON object ONLY with these exact fields:
{{
  "conviction": "HIGH" | "MEDIUM" | "LOW",
  "conviction_changed": true | false,
  "desc_updated": "<updated 2-3 sentence thesis description, or same as current if unchanged>",
  "desc_changed": true | false,
  "narrative_change": "<1-2 sentences explaining what changed in market conditions, or 'No material change' if conviction/desc unchanged>",
  "trades_commentary": "<1-2 sentences on trade performance — is the thesis tracking? Any adjustments needed?>",
  "key_risks": ["<risk 1>", "<risk 2>", "<risk 3>"]
}}

Rules:
- conviction: raise to HIGH only if 3+ signals align strongly; lower to LOW if thesis is breaking down
- desc_updated: if thesis is unchanged, return the original description verbatim
- Be concise and data-driven
- Output JSON ONLY — no markdown fences, no commentary"""
    return prompt


# ---------------------------------------------------------------------------
# Archive helpers
# ---------------------------------------------------------------------------

def get_latest_trades(archive, title):
    """Return the most recent trades snapshot for a thesis, or None."""
    th = (archive.get('theses') or {}).get(title)
    if not th:
        return None
    hist = th.get('trades_history') or []
    if hist:
        return hist[-1].get('trades')
    return None


def archive_thesis_snapshot(archive, card, update, start):
    """Append a weekly thesis snapshot to the archive."""
    title = card['title']
    if 'theses' not in archive:
        archive['theses'] = {}
    if title not in archive['theses']:
        archive['theses'][title] = {
            'mkt': card.get('mkt', ''),
            'quad': card.get('quad', ''),
            'idx': card.get('idx', 0),
            'first_seen': start.strftime('%Y-%m-%dT%H:%M:%SZ'),
            'last_active': start.strftime('%Y-%m-%dT%H:%M:%SZ'),
            'retired': None,
            'history': [],
            'news_history': [],
            'trades_history': [],
        }
    th = archive['theses'][title]
    th['last_active'] = start.strftime('%Y-%m-%dT%H:%M:%SZ')
    th.setdefault('history', []).append({
        'as_of': start.strftime('%Y-%m-%dT%H:%M:%SZ'),
        'conviction': update.get('conviction', card.get('conviction', 'MEDIUM')),
        'conviction_changed': update.get('conviction_changed', False),
        'desc': update.get('desc_updated', card.get('desc', '')),
        'desc_changed': update.get('desc_changed', False),
        'narrative_change': update.get('narrative_change', ''),
        'trades_commentary': update.get('trades_commentary', ''),
        'key_risks': update.get('key_risks', []),
    })
    # Keep last 52 weekly snapshots (1 year)
    th['history'] = th['history'][-52:]


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    start = datetime.now(timezone.utc)
    log.info('=== refresh_thesis.py start ===')

    if not CARDS_JSON.exists():
        log.error('cards_data.json not found at %s', CARDS_JSON)
        sys.exit(1)

    if not DATA_JSON.exists():
        log.error('data.json not found — run fetch_data.py first')
        sys.exit(1)

    cards   = json.loads(CARDS_JSON.read_text())
    data    = json.loads(DATA_JSON.read_text())
    archive = json.loads(ARCHIVE_JSON.read_text()) if ARCHIVE_JSON.exists() else {'theses': {}}

    market_summary = build_market_summary(data)
    log.info('Market summary: %d chars, %d cards to process', len(market_summary), len(cards))

    # Verify Claude CLI is available
    try:
        test = subprocess.run(['claude', '--version'], capture_output=True, text=True, timeout=10)
        log.info('Claude CLI: %s', test.stdout.strip() or 'available')
    except FileNotFoundError:
        log.error('`claude` CLI not found in PATH — thesis refresh requires Claude Code CLI')
        sys.exit(1)

    updated_count = 0
    skip_count    = 0

    for card in cards:
        title = card['title']
        log.info('  Processing: %s', title)

        trades_snapshot = get_latest_trades(archive, title)
        prompt = build_thesis_prompt(card, market_summary, trades_snapshot)

        try:
            raw = call_claude(prompt)
            update = extract_json(raw)

            # Apply update back to card (in-memory for archive; cards_data.json is source of truth)
            conviction_before = card.get('conviction', 'MEDIUM')
            if update.get('conviction_changed'):
                log.info('    Conviction: %s → %s', conviction_before, update.get('conviction'))
            if update.get('desc_changed'):
                log.info('    Description updated')
            if update.get('narrative_change') and update['narrative_change'] != 'No material change':
                log.info('    Narrative: %s', update['narrative_change'][:80])

            archive_thesis_snapshot(archive, card, update, start)
            updated_count += 1

        except RuntimeError as e:
            log.error('    Claude CLI unavailable: %s', e)
            sys.exit(1)
        except json.JSONDecodeError as e:
            log.warning('    JSON parse failed for %s: %s — skipping', title, e)
            skip_count += 1
        except Exception:
            log.warning('    Unexpected error for %s:\n%s', title, traceback.format_exc())
            skip_count += 1

    # Write archive
    ARCHIVE_JSON.write_text(json.dumps(archive, indent=2, ensure_ascii=False))
    log.info('Wrote research_archive.json (%d updated, %d skipped)', updated_count, skip_count)

    # Git push
    try:
        import subprocess as sp
        ts = start.strftime('%Y-%m-%d %H:%M UTC')
        sp.run(['git', 'add', 'research_archive.json'],
               cwd=DASHBOARD_DIR, check=True, capture_output=True)
        result = sp.run(['git', 'diff', '--cached', '--quiet'],
                        cwd=DASHBOARD_DIR, capture_output=True)
        if result.returncode != 0:
            sp.run(['git', 'commit', '-m', f'thesis: weekly refresh {ts}'],
                   cwd=DASHBOARD_DIR, check=True, capture_output=True)
            sp.run(['git', 'push'], cwd=DASHBOARD_DIR, check=True, capture_output=True)
            log.info('Pushed research_archive.json to GitHub Pages')
        else:
            log.info('No changes — skipped git push')
    except Exception as e:
        log.warning('Git push failed (non-fatal): %s', e)

    log.info('=== refresh_thesis.py done in %.1fs ===',
             (datetime.now(timezone.utc) - start).total_seconds())


if __name__ == '__main__':
    main()
