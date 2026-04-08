#!/usr/bin/env python3
"""
search_articles.py — Real-time article search API server.

Replaces the basic static file server with one that also handles:
  GET /api/search?q=<query>&count=5  →  JSON array of {url, title, desc}

Uses Brave Search API if BRAVE_API_KEY is set.
Falls back to returning empty results (browser shows cached articles).

Usage:
    BRAVE_API_KEY=xxx python3 search_articles.py
    # Or just: python3 search_articles.py (serves static files, search returns empty)
"""
import http.server
import json
import os
import sys
import urllib.parse

sys.path.insert(0, '/tmp/claude/pylibs')

PORT = 8765
BRAVE_API_KEY = os.environ.get('BRAVE_API_KEY', '')

try:
    import requests as req_lib
    HAS_REQUESTS = True
except ImportError:
    HAS_REQUESTS = False


def brave_search(query, count=5):
    """Search Brave API and return results."""
    if not BRAVE_API_KEY or not HAS_REQUESTS:
        return []
    try:
        resp = req_lib.get(
            'https://api.search.brave.com/res/v1/web/search',
            headers={
                'Accept': 'application/json',
                'X-Subscription-Token': BRAVE_API_KEY,
            },
            params={'q': query, 'count': count},
            timeout=8,
        )
        if resp.status_code != 200:
            return []
        data = resp.json()
        results = []
        for r in data.get('web', {}).get('results', []):
            results.append({
                'url': r.get('url', ''),
                'title': r.get('title', ''),
                'desc': r.get('description', '')[:200],
            })
        return results
    except Exception as e:
        print(f"[search] Error: {e}")
        return []


class DashboardHandler(http.server.SimpleHTTPRequestHandler):
    """Serves static files + /api/search endpoint."""

    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)

        if parsed.path == '/api/search':
            self.handle_search(parsed)
        else:
            super().do_GET()

    def handle_search(self, parsed):
        """Handle /api/search?q=...&count=5"""
        params = urllib.parse.parse_qs(parsed.query)
        query = params.get('q', [''])[0]
        count = int(params.get('count', ['5'])[0])

        if not query:
            self.send_json({'error': 'Missing q parameter', 'results': []})
            return

        results = brave_search(query, count)
        self.send_json({'query': query, 'results': results, 'count': len(results)})

    def send_json(self, data):
        body = json.dumps(data).encode('utf-8')
        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', str(len(body)))
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format, *args):
        # Only log API requests, not static file requests
        if '/api/' in (args[0] if args else ''):
            super().log_message(format, *args)


def main():
    os.chdir(os.path.dirname(os.path.abspath(__file__)))

    if BRAVE_API_KEY:
        print(f"[server] Brave Search API enabled")
    else:
        print(f"[server] No BRAVE_API_KEY — /api/search will return empty results")
        print(f"[server] Set BRAVE_API_KEY env var to enable real-time search")

    server = http.server.HTTPServer(('127.0.0.1', PORT), DashboardHandler)
    print(f"[server] Dashboard serving on http://localhost:{PORT}/dashboard.html")
    server.serve_forever()


if __name__ == '__main__':
    main()
