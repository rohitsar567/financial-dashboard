#!/usr/bin/env python3
"""
refresh_thesis.py — Refresh thesis signals and scores using live market data.

Usage:
    python3 refresh_thesis.py

Updates THESIS_META timestamps and can be extended to re-score theses
based on current market conditions. Currently updates the 'updated' field
for all active (non-retired) theses.
"""
import json, os, re, sys, datetime

sys.path.insert(0, '/tmp/claude/pylibs')

DASHBOARD_PATH = os.path.join(os.path.dirname(__file__), 'dashboard.html')


def main():
    with open(DASHBOARD_PATH, 'r') as f:
        content = f.read()

    today = datetime.date.today().isoformat()

    # Update THESIS_META: set updated date to today for all non-retired theses
    def update_meta(match):
        text = match.group(0)
        if 'retired:null' in text:
            # Update the 'updated' field
            text = re.sub(r'updated:"[^"]+"', f'updated:"{today}"', text)
        return text

    content = re.sub(
        r'\{[^}]*created:"[^"]+"[^}]*updated:"[^"]+"[^}]*retired:[^}]+\}',
        update_meta,
        content
    )

    with open(DASHBOARD_PATH, 'w') as f:
        f.write(content)

    print(f"Thesis metadata updated to {today}")


if __name__ == '__main__':
    main()
