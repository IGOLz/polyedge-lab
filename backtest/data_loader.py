"""
Data loader for backtesting framework.
Efficiently loads market data and ticks from PostgreSQL into memory.
"""

import asyncio
import asyncpg
import numpy as np
import os
from dotenv import load_dotenv
from collections import defaultdict

load_dotenv()


async def _load_from_db():
    """Load all resolved markets and ticks from database."""
    conn = await asyncpg.connect(
        host=os.getenv('POSTGRES_HOST', 'localhost'),
        port=int(os.getenv('POSTGRES_PORT', '5432')),
        user=os.getenv('POSTGRES_USER', 'polymarket'),
        password=os.getenv('POSTGRES_PASSWORD', ''),
        database=os.getenv('POSTGRES_DB', 'polymarket_tracker'),
    )

    try:
        print("Loading resolved markets...")
        market_rows = await conn.fetch("""
            SELECT market_id, market_type, started_at, ended_at, final_outcome
            FROM market_outcomes
            WHERE resolved = TRUE
              AND final_outcome IN ('Up', 'Down')
            ORDER BY started_at
        """)

        markets_raw = [dict(r) for r in market_rows]
        print(f"  Found {len(markets_raw)} resolved markets")

        if not markets_raw:
            return []

        market_ids = [m['market_id'] for m in markets_raw]

        print("Loading ticks...")
        tick_rows = await conn.fetch("""
            SELECT market_id, time, up_price
            FROM market_ticks
            WHERE market_id = ANY($1)
            ORDER BY time
        """, market_ids)

        ticks_by_market = defaultdict(list)
        for r in tick_rows:
            ticks_by_market[r['market_id']].append({
                'time': r['time'],
                'up_price': float(r['up_price']),
            })

        print(f"  Loaded {len(tick_rows)} ticks across {len(ticks_by_market)} markets")

    finally:
        await conn.close()

    return markets_raw, ticks_by_market


def load_all_data():
    """
    Load all resolved markets with tick data.
    Returns list of market dicts, each containing:
      - market_id, market_type, asset, duration_minutes, total_seconds
      - started_at, ended_at, final_outcome, hour
      - ticks: numpy array indexed by second (NaN for missing)
    """
    markets_raw, ticks_by_market = asyncio.run(_load_from_db())

    if not markets_raw:
        print("No resolved markets found.")
        return []

    markets = []
    for m in markets_raw:
        mid = m['market_id']
        raw_ticks = ticks_by_market.get(mid, [])
        if len(raw_ticks) < 10:
            continue

        market_type = m['market_type']
        parts = market_type.split('_')
        asset = parts[0]
        duration_minutes = int(parts[1].replace('m', ''))
        total_seconds = duration_minutes * 60

        # Build tick array indexed by elapsed second
        tick_array = np.full(total_seconds, np.nan)
        started_at = m['started_at']
        for t in raw_ticks:
            elapsed = int((t['time'] - started_at).total_seconds())
            if 0 <= elapsed < total_seconds:
                tick_array[elapsed] = t['up_price']

        markets.append({
            'market_id': mid,
            'market_type': market_type,
            'asset': asset,
            'duration_minutes': duration_minutes,
            'total_seconds': total_seconds,
            'started_at': started_at,
            'ended_at': m['ended_at'],
            'final_outcome': m['final_outcome'],
            'hour': started_at.hour,
            'ticks': tick_array,
        })

    print(f"Processed {len(markets)} markets with sufficient tick data")

    # Print summary
    assets = defaultdict(int)
    durations = defaultdict(int)
    for m in markets:
        assets[m['asset']] += 1
        durations[m['duration_minutes']] += 1

    print(f"\nDataset summary:")
    for asset, count in sorted(assets.items()):
        print(f"  {asset}: {count} markets")
    for dur, count in sorted(durations.items()):
        print(f"  {dur}m: {count} markets")

    date_range_start = min(m['started_at'] for m in markets)
    date_range_end = max(m['ended_at'] for m in markets)
    days = (date_range_end - date_range_start).total_seconds() / 86400
    print(f"  Date range: {date_range_start} to {date_range_end}")
    print(f"  Duration: {days:.1f} days\n")

    return markets


def get_price_at_second(tick_array, target_sec, tolerance=5):
    """Get up_price at target second, or nearest within +/- tolerance."""
    if 0 <= target_sec < len(tick_array):
        price = tick_array[target_sec]
        if not np.isnan(price):
            return float(price)
    for offset in range(1, tolerance + 1):
        for s in [target_sec + offset, target_sec - offset]:
            if 0 <= s < len(tick_array):
                price = tick_array[s]
                if not np.isnan(price):
                    return float(price)
    return None


def filter_markets(markets, assets=None, durations=None):
    """Filter markets by asset list and/or duration list."""
    result = markets
    if assets:
        result = [m for m in result if m['asset'] in assets]
    if durations:
        result = [m for m in result if m['duration_minutes'] in durations]
    return result
