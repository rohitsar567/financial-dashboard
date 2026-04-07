"""
generate_devils_advocate.py — Devil's Advocate contra-thesis generator.

For each of the 34 investment thesis cards, generates a structured counter-argument:
why the thesis could fail, what signals would invalidate it, and the pain-trade scenario.

Uses `claude -p` CLI (Claude Code subscription compute — no API credits needed).
Writes devils_advocate.json and git-pushes.

Run:  python3 ~/opsmatters-dashboard/generate_devils_advocate.py
"""

import subprocess, sys, os, json, re, logging
from datetime import datetime, timezone
from pathlib import Path

DASHBOARD_DIR = Path('~/opsmatters-dashboard').expanduser()
CARDS_JSON    = DASHBOARD_DIR / 'cards_data.json'
if not CARDS_JSON.exists():
    CARDS_JSON = Path('/tmp/claude/cards_data.json')
OUTPUT_JSON   = DASHBOARD_DIR / 'devils_advocate.json'
LOG_FILE      = DASHBOARD_DIR / 'devils_advocate.log'

TODAY = datetime.now(timezone.utc).strftime('%Y-%m-%d')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s  %(levelname)-8s  %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Claude CLI
# ---------------------------------------------------------------------------

def call_claude(prompt, timeout=600):
    for cmd in [
        ['claude', '-p', prompt, '--output-format', 'text'],
        ['claude', '--print', prompt],
        ['claude', '-p', prompt],
    ]:
        try:
            result = subprocess.run(
                cmd, capture_output=True, text=True, timeout=timeout,
                env={**os.environ, 'NO_COLOR': '1'},
            )
            if result.returncode == 0 and result.stdout.strip():
                return result.stdout.strip()
        except FileNotFoundError:
            continue
        except subprocess.TimeoutExpired:
            log.warning('Claude CLI timed out after %ds', timeout)
            continue
    raise RuntimeError('Claude CLI unavailable')


def extract_json(text):
    text = re.sub(r'^```[a-zA-Z]*\n?', '', text.strip())
    text = re.sub(r'\n?```$', '', text.strip()).strip()
    match = re.search(r'\{[\s\S]*\}', text)
    if match:
        return json.loads(match.group())
    return json.loads(text)


# ---------------------------------------------------------------------------
# Prompt builder
# ---------------------------------------------------------------------------

QUAD_LABELS = {
    'stl': 'Short-Term Long (bullish, near-term)',
    'ltl': 'Long-Term Long (bullish, multi-month)',
    'sts': 'Short-Term Short (bearish, near-term)',
    'lts': 'Long-Term Short (bearish, multi-month)',
}

def build_batch_prompt(cards, market_label):
    thesis_list = []
    for c in cards:
        trades_summary = []
        for cat in ['commodities', 'fixed_income', 'currencies', 'equities']:
            for tr in (c.get('trades') or {}).get(cat, []):
                trades_summary.append(f"{tr.get('name','')} ({tr.get('ticker','')}): {tr.get('entry','')}")
        trades_str = '; '.join(trades_summary[:6]) if trades_summary else 'none listed'
        nodes_str = ', '.join(c.get('nodes') or [])

        thesis_list.append(
            f"Title: {c['title']}\n"
            f"Quad: {QUAD_LABELS.get(c['quad'], c['quad'])}\n"
            f"Description: {c.get('desc','')}\n"
            f"Strategy: {c.get('strategy','')}\n"
            f"Key instruments: {nodes_str}\n"
            f"Conviction: {c.get('conviction','MEDIUM')}\n"
            f"Trades: {trades_str}"
        )

    theses_block = '\n\n---\n\n'.join(
        f'[{i+1}] {t}' for i, t in enumerate(thesis_list)
    )

    schema_example = json.dumps({
        "thesis_title": {
            "bear_case": "2-3 sentence contra-narrative explaining why this thesis is wrong or premature",
            "failure_conditions": [
                "Specific market/macro condition that invalidates thesis #1",
                "Specific market/macro condition that invalidates thesis #2",
                "Specific market/macro condition that invalidates thesis #3"
            ],
            "counter_signals": [
                "Specific indicator/price level to watch that signals breakdown #1",
                "Specific indicator/price level to watch that signals breakdown #2",
                "Specific indicator/price level to watch that signals breakdown #3"
            ],
            "pain_trade": "1-2 sentences: what does the losing scenario look like — price action, magnitude of loss, timing",
            "overlooked_risk": "1 non-obvious, specific risk that is underappreciated by consensus",
            "risk_score": "HIGH"
        }
    }, indent=2)

    prompt = f"""You are a rigorous risk manager and contrarian analyst. Your job is to steelman the bear case against each investment thesis — not to be negative for the sake of it, but to identify the specific, credible ways each thesis could fail.

Today: {TODAY}
Market: {market_label}

Below are {len(cards)} investment thesis cards. For EACH thesis, write a structured devil's advocate / contra-thesis.

THESES:
{theses_block}

OUTPUT FORMAT:
Return a single JSON object where each key is the exact thesis title. Every thesis must have all 6 fields. Be specific — avoid generic risk statements like "markets could fall" or "geopolitical risks exist". Name specific price levels, specific policy triggers, specific macro conditions.

risk_score should reflect how fragile the thesis is: HIGH = the bear case is very plausible given current data; MEDIUM = real risks but thesis has support; LOW = thesis is robust, bear case requires unusual confluence.

JSON schema (for ONE entry — return this for ALL {len(cards)} theses):
{schema_example}

Return ONLY the JSON. No markdown fences. No commentary. Ensure all {len(cards)} theses are included as keys.
"""
    return prompt


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    start = datetime.now(timezone.utc)
    log.info('=== generate_devils_advocate.py start ===')

    if not CARDS_JSON.exists():
        log.error('cards_data.json not found at %s', CARDS_JSON)
        sys.exit(1)

    # Verify Claude CLI
    try:
        test = subprocess.run(['claude', '--version'], capture_output=True, text=True, timeout=10)
        log.info('Claude CLI: %s', (test.stdout or 'available').strip())
    except FileNotFoundError:
        log.error('`claude` CLI not found — this script requires Claude Code CLI')
        sys.exit(1)

    cards = json.loads(CARDS_JSON.read_text())
    us_cards = [c for c in cards if c['mkt'] == 'US']
    in_cards = [c for c in cards if c['mkt'] == 'IN']
    log.info('Loaded %d US cards, %d IN cards', len(us_cards), len(in_cards))

    # Load existing output to allow incremental updates
    existing = {}
    if OUTPUT_JSON.exists():
        try:
            existing = json.loads(OUTPUT_JSON.read_text()).get('theses', {})
            log.info('Loaded %d existing devil\'s advocate entries', len(existing))
        except Exception:
            pass

    result = dict(existing)

    for market_label, batch in [('US Markets', us_cards), ('India Markets', in_cards)]:
        log.info('Generating devil\'s advocate for %s (%d cards)…', market_label, len(batch))
        prompt = build_batch_prompt(batch, market_label)
        log.info('Prompt length: %d chars', len(prompt))

        try:
            raw = call_claude(prompt, timeout=600)
            parsed = extract_json(raw)
            # Validate — every card in the batch should be in the response
            found = 0
            for c in batch:
                title = c['title']
                if title in parsed:
                    result[title] = parsed[title]
                    found += 1
                else:
                    # Try case-insensitive match
                    match = next((k for k in parsed if k.lower() == title.lower()), None)
                    if match:
                        result[title] = parsed[match]
                        found += 1
                    else:
                        log.warning('Missing entry for: %s', title)
            log.info('Got %d/%d entries for %s', found, len(batch), market_label)
        except json.JSONDecodeError as e:
            log.error('JSON parse failed for %s: %s', market_label, e)
            log.error('Raw (first 500): %s', raw[:500] if 'raw' in dir() else '(no response)')
        except RuntimeError as e:
            log.error('%s', e)
            sys.exit(1)
        except Exception as e:
            log.error('Unexpected error for %s: %s', market_label, e)

    # Write output
    output = {
        'as_of': start.strftime('%Y-%m-%dT%H:%M:%SZ'),
        'theses': result,
    }
    OUTPUT_JSON.write_text(json.dumps(output, indent=2, ensure_ascii=False))
    log.info('Wrote devils_advocate.json (%d theses)', len(result))

    # Git push
    try:
        ts = start.strftime('%Y-%m-%d %H:%M UTC')
        subprocess.run(['git', 'add', 'devils_advocate.json'],
                       cwd=DASHBOARD_DIR, check=True, capture_output=True)
        diff = subprocess.run(['git', 'diff', '--cached', '--quiet'],
                              cwd=DASHBOARD_DIR, capture_output=True)
        if diff.returncode != 0:
            subprocess.run(['git', 'commit', '-m', f'feat: devils_advocate.json generated {ts}'],
                           cwd=DASHBOARD_DIR, check=True, capture_output=True)
            subprocess.run(['git', 'push'], cwd=DASHBOARD_DIR, check=True, capture_output=True)
            log.info('Pushed devils_advocate.json to GitHub Pages')
        else:
            log.info('No changes — skipped git push')
    except Exception as e:
        log.warning('Git push failed (non-fatal): %s', e)

    log.info('=== done in %.1fs ===', (datetime.now(timezone.utc) - start).total_seconds())


if __name__ == '__main__':
    main()
