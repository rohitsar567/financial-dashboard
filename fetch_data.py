"""
fetch_data.py — Financial Intelligence Dashboard data fetcher
Fetches live market data via yfinance and writes ~/opsmatters-dashboard/data.json

Run:  python3 ~/opsmatters-dashboard/fetch_data.py
"""

# ---------------------------------------------------------------------------
# Bootstrap: install dependencies to /tmp/claude/pylibs if needed
# ---------------------------------------------------------------------------
import subprocess
import sys
import os

subprocess.run(
    [
        sys.executable, '-m', 'pip', 'install',
        '--target=/tmp/claude/pylibs',
        'yfinance', 'pandas', 'numpy',
    ],
    capture_output=True,
)
sys.path.insert(0, '/tmp/claude/pylibs')

# ---------------------------------------------------------------------------
# Standard imports (after path fixup)
# ---------------------------------------------------------------------------
import json
import logging
import traceback
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd
import yfinance as yf

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
DASHBOARD_DIR = Path('~/opsmatters-dashboard').expanduser()
OUTPUT_JSON   = DASHBOARD_DIR / 'data.json'
LOG_FILE      = DASHBOARD_DIR / 'fetch_data.log'

# Instruments: (display_name, yfinance_symbol)
INSTRUMENTS = {
    'US Indices': [
        ('S&P 500',       '^GSPC'),
        ('Nasdaq 100',    'QQQ'),
        ('Dow Jones',     '^DJI'),
        ('VIX',           '^VIX'),
        ('Russell 2000',  'IWM'),
        ('DXY (Dollar)',  'DX-Y.NYB'),
    ],
    'India Indices': [
        ('Nifty 50',      '^NSEI'),
        ('Bank Nifty',    '^NSEBANK'),
        ('Sensex',        '^BSESN'),
        ('Nifty IT',      '^CNXIT'),
        ('India VIX',     '^INDIAVIX'),
        ('USD/INR',       'USDINR=X'),
    ],
    'Commodities': [
        ('WTI Crude Oil', 'CL=F'),
        ('Brent Crude',   'BZ=F'),
        ('Gold',          'GC=F'),
        ('Silver',        'SI=F'),
        ('Copper',        'HG=F'),
        ('Natural Gas',   'NG=F'),
    ],
    'Bonds & Rates': [
        ('US 10Y Yield',  '^TNX'),
        ('US 30Y Yield',  '^TYX'),
        ('US 2Y Yield',   '^IRX'),
        ('TLT (Long Bond)', 'TLT'),
    ],
    'Key ETFs': [
        ('XLE (Energy)',       'XLE'),
        ('XLF (Financials)',   'XLF'),
        ('XLU (Utilities)',    'XLU'),
        ('ICLN (Clean Energy)','ICLN'),
        ('GDX (Gold Miners)',  'GDX'),
        ('INDA (India ETF)',   'INDA'),
    ],
}

# ---------------------------------------------------------------------------
# Logging setup
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
# Technical indicator helpers
# ---------------------------------------------------------------------------

def _safe_round(val, dp=2):
    """Return rounded float or None if NaN/None."""
    if val is None:
        return None
    try:
        v = float(val)
        return None if np.isnan(v) else round(v, dp)
    except (TypeError, ValueError):
        return None


def calc_rsi(close: pd.Series, period: int = 14) -> float:
    """Wilder RSI using exponential moving average (alpha = 1/period)."""
    delta = close.diff()
    gain  = delta.clip(lower=0)
    loss  = (-delta).clip(lower=0)
    alpha = 1.0 / period
    avg_gain = gain.ewm(alpha=alpha, adjust=False).mean()
    avg_loss = loss.ewm(alpha=alpha, adjust=False).mean()
    rs  = avg_gain / avg_loss.replace(0, np.nan)
    rsi = 100 - (100 / (1 + rs))
    return _safe_round(rsi.iloc[-1], 1)


def calc_macd(close: pd.Series):
    """MACD (12,26,9). Returns (macd, signal, hist)."""
    ema12  = close.ewm(span=12, adjust=False).mean()
    ema26  = close.ewm(span=26, adjust=False).mean()
    macd   = ema12 - ema26
    signal = macd.ewm(span=9, adjust=False).mean()
    hist   = macd - signal
    return (
        _safe_round(macd.iloc[-1],   2),
        _safe_round(signal.iloc[-1], 2),
        _safe_round(hist.iloc[-1],   2),
    )


def calc_bollinger(close: pd.Series, period: int = 20, num_std: float = 2.0):
    """Bollinger Bands. Returns (upper, lower, pct_b)."""
    sma    = close.rolling(period).mean()
    std    = close.rolling(period).std(ddof=0)
    upper  = sma + num_std * std
    lower  = sma - num_std * std
    pct_b  = (close - lower) / (upper - lower) * 100

    return (
        _safe_round(upper.iloc[-1],  2),
        _safe_round(lower.iloc[-1],  2),
        _safe_round(pct_b.iloc[-1],  1),
    )


def calc_atr(high: pd.Series, low: pd.Series, close: pd.Series, period: int = 14) -> float:
    """Wilder ATR."""
    prev_close = close.shift(1)
    tr = pd.concat([
        high - low,
        (high - prev_close).abs(),
        (low  - prev_close).abs(),
    ], axis=1).max(axis=1)

    alpha = 1.0 / period
    atr   = tr.ewm(alpha=alpha, adjust=False).mean()
    return _safe_round(atr.iloc[-1], 2)


def calc_trend(ma_score: int, rsi: float) -> str:
    """Classify trend from MA score and RSI."""
    if rsi is None:
        rsi = 50.0
    if ma_score == 3 and rsi > 55:
        return 'Strong Uptrend'
    if ma_score >= 2:
        return 'Uptrend'
    if ma_score == 0 and rsi < 45:
        return 'Strong Downtrend'
    if ma_score <= 1:
        return 'Downtrend'
    return 'Neutral'


# ---------------------------------------------------------------------------
# Per-symbol data fetcher
# ---------------------------------------------------------------------------

def fetch_symbol(name: str, symbol: str) -> dict:
    """Fetch one symbol and return the dashboard record."""
    log.info('Fetching %s (%s) …', name, symbol)

    record = {
        'name':    name,
        'symbol':  symbol,
        'current': None,
        'changes': {'1D': None, '1W': None, '1M': None, '6M': None, '1Y': None},
        'tech': {
            'sma20': None, 'sma50': None, 'sma200': None,
            'vs_sma20': None, 'vs_sma50': None, 'vs_sma200': None,
            'rsi': None,
            'macd': None, 'macd_signal': None, 'macd_hist': None,
            'bb_upper': None, 'bb_lower': None, 'bb_pct_b': None,
            'atr': None, 'atr_pct': None,
            'hi52': None, 'lo52': None, 'pct_from_hi': None, 'pct_from_lo': None,
            'trend': None, 'ma_score': None,
        },
    }

    try:
        ticker = yf.Ticker(symbol)

        # ---- 1-year daily history for all technicals ----------------------
        hist = ticker.history(period='1y', interval='1d', auto_adjust=True)

        if hist.empty:
            log.warning('No history returned for %s', symbol)
            return record

        close = hist['Close'].dropna()
        if len(close) < 2:
            log.warning('Too few data points for %s', symbol)
            return record

        current = float(close.iloc[-1])
        record['current'] = _safe_round(current, 2)

        # ---- SMAs ---------------------------------------------------------
        sma20  = _safe_round(close.rolling(20).mean().iloc[-1],  2)
        sma50  = _safe_round(close.rolling(50).mean().iloc[-1],  2)
        sma200 = _safe_round(close.rolling(200).mean().iloc[-1], 2)

        record['tech']['sma20']  = sma20
        record['tech']['sma50']  = sma50
        record['tech']['sma200'] = sma200

        def vs_sma(sma):
            if sma is None:
                return None
            return _safe_round(((current - sma) / sma) * 100, 1)

        record['tech']['vs_sma20']  = vs_sma(sma20)
        record['tech']['vs_sma50']  = vs_sma(sma50)
        record['tech']['vs_sma200'] = vs_sma(sma200)

        # ---- MA score & trend ---------------------------------------------
        ma_score = sum([
            1 if (sma20  is not None and current > sma20)  else 0,
            1 if (sma50  is not None and current > sma50)  else 0,
            1 if (sma200 is not None and current > sma200) else 0,
        ])
        record['tech']['ma_score'] = ma_score

        # ---- RSI ----------------------------------------------------------
        rsi = calc_rsi(close)
        record['tech']['rsi'] = rsi

        # ---- Trend label --------------------------------------------------
        record['tech']['trend'] = calc_trend(ma_score, rsi)

        # ---- MACD ---------------------------------------------------------
        macd, macd_signal, macd_hist = calc_macd(close)
        record['tech']['macd']        = macd
        record['tech']['macd_signal'] = macd_signal
        record['tech']['macd_hist']   = macd_hist

        # ---- Bollinger Bands ----------------------------------------------
        bb_upper, bb_lower, bb_pct_b = calc_bollinger(close)
        record['tech']['bb_upper']  = bb_upper
        record['tech']['bb_lower']  = bb_lower
        record['tech']['bb_pct_b']  = bb_pct_b

        # ---- ATR ----------------------------------------------------------
        if 'High' in hist.columns and 'Low' in hist.columns:
            high = hist['High'].dropna()
            low  = hist['Low'].dropna()
            # Align all three series
            idx  = close.index.intersection(high.index).intersection(low.index)
            atr  = calc_atr(high.loc[idx], low.loc[idx], close.loc[idx])
        else:
            atr = None

        record['tech']['atr'] = atr
        record['tech']['atr_pct'] = (
            _safe_round((atr / current) * 100, 2)
            if atr is not None and current
            else None
        )

        # ---- 52-week high/low ---------------------------------------------
        hi52 = _safe_round(float(close.max()), 2)
        lo52 = _safe_round(float(close.min()), 2)
        record['tech']['hi52'] = hi52
        record['tech']['lo52'] = lo52
        record['tech']['pct_from_hi'] = (
            _safe_round(((current - hi52) / hi52) * 100, 1) if hi52 else None
        )
        record['tech']['pct_from_lo'] = (
            _safe_round(((current - lo52) / lo52) * 100, 1) if lo52 else None
        )

        # ---- % changes ----------------------------------------------------
        periods = {
            '1D': '2d',
            '1W': '5d',
            '1M': '1mo',
            '6M': '6mo',
            '1Y': '1y',
        }
        for label, period_str in periods.items():
            try:
                ph = ticker.history(period=period_str, interval='1d', auto_adjust=True)
                if ph.empty or len(ph) < 2:
                    continue
                c = ph['Close'].dropna()
                if len(c) < 2:
                    continue
                pct = _safe_round(((float(c.iloc[-1]) / float(c.iloc[0])) - 1) * 100, 2)
                record['changes'][label] = pct
            except Exception as exc:  # noqa: BLE001
                log.warning('Change calc failed for %s/%s: %s', symbol, label, exc)

    except Exception:  # noqa: BLE001
        log.error('Error fetching %s:\n%s', symbol, traceback.format_exc())

    return record


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    log.info('=== fetch_data.py started ===')
    start = datetime.now(timezone.utc)

    output = {
        'as_of': start.strftime('%Y-%m-%dT%H:%M:%SZ'),
    }

    total   = 0
    success = 0
    failed  = []

    for section, instruments in INSTRUMENTS.items():
        section_data = []
        for name, symbol in instruments:
            total += 1
            record = fetch_symbol(name, symbol)
            section_data.append(record)
            if record['current'] is not None:
                success += 1
            else:
                failed.append(symbol)
        output[section] = section_data

    # Write JSON
    OUTPUT_JSON.write_text(json.dumps(output, indent=2), encoding='utf-8')
    log.info('Wrote %s', OUTPUT_JSON)

    elapsed = (datetime.now(timezone.utc) - start).total_seconds()
    status  = (
        f'SUCCESS  {success}/{total} symbols  '
        f'{len(failed)} failed ({", ".join(failed) if failed else "none"})  '
        f'{elapsed:.1f}s'
    )
    log.info('=== fetch_data.py done: %s ===', status)

    # Companion status file (single-line, easy to tail)
    companion = {
        'last_run': start.strftime('%Y-%m-%dT%H:%M:%SZ'),
        'status':   'ok' if not failed else 'partial',
        'success':  success,
        'total':    total,
        'failed':   failed,
        'elapsed_s': round(elapsed, 1),
    }
    (DASHBOARD_DIR / 'fetch_data.log').write_text(
        # Append a structured summary after the freeform log entries
        # The log file is already written by the FileHandler above;
        # we append a JSON summary line for machine consumption.
        (DASHBOARD_DIR / 'fetch_data.log').read_text(encoding='utf-8')
        + '\nLAST_RUN_JSON: ' + json.dumps(companion) + '\n',
        encoding='utf-8',
    )


if __name__ == '__main__':
    main()
