"""
Momentum Strategy Brute Force Parameter Sweep
Tries every meaningful combination of parameters and ranks by ROI/PnL.
Connects to DB read-only, saves nothing. Prints everything to stdout.
"""

import asyncpg
import asyncio
import os
import sys
import time
from dotenv import load_dotenv
from collections import defaultdict
from itertools import product

load_dotenv()

# --- Parameter space ---
PRICE_A_SECONDS = [15, 30, 45, 60, 75, 90]
PRICE_B_SECONDS = [30, 45, 60, 75, 90, 105, 120]
ENTRY_OFFSETS = [5, 10, 15]
MIN_THRESHOLDS = [0.01, 0.02, 0.03, 0.04, 0.05, 0.07, 0.10]
PRICE_MINS = [0.01, 0.30, 0.40, 0.50, 0.55, 0.60]
PRICE_MAXS = [0.99, 0.90, 0.85, 0.80, 0.75, 0.70]
STOP_LOSS_PRICES = [None, 0.50, 0.45, 0.40, 0.35, 0.30]
DIRECTION_FILTERS = ['both', 'up_only', 'down_only']
MARKET_FILTERS = ['all', 'btc_only', 'no_btc', 'xrp_sol_only']
HOUR_FILTERS = ['all', '00-12', '12-24', '08-20', '16-24', '00-08']

MIN_TRADES = 50

# --- Hour filter helper ---
HOUR_RANGES = {
    'all': None,
    '00-12': (0, 12),
    '12-24': (12, 24),
    '08-20': (8, 20),
    '16-24': (16, 24),
    '00-08': (0, 8),
}

MARKET_SETS = {
    'all': None,
    'btc_only': {'btc_5m'},
    'no_btc': {'eth_5m', 'xrp_5m', 'sol_5m'},
    'xrp_sol_only': {'xrp_5m', 'sol_5m'},
}


def index_ticks_by_second(ticks, started_at):
    """Index ticks by elapsed second for O(1) lookup."""
    result = {}
    for tick in ticks:
        elapsed = int((tick['time'] - started_at).total_seconds())
        result[elapsed] = tick['up_price']
    return result


def get_price_at_second(ticks_by_second, target_sec):
    """Get price at exact second, or interpolate within ±5s."""
    price = ticks_by_second.get(target_sec)
    if price is not None:
        return price
    for offset in range(1, 6):
        price = ticks_by_second.get(target_sec + offset)
        if price is not None:
            return price
        price = ticks_by_second.get(target_sec - offset)
        if price is not None:
            return price
    return None


def detect_signal(ticks_by_second, price_a_sec, price_b_sec, min_threshold):
    """Detect momentum signal with parametric timing."""
    price_a = get_price_at_second(ticks_by_second, price_a_sec)
    price_b = get_price_at_second(ticks_by_second, price_b_sec)

    if price_a is None or price_b is None:
        return None

    momentum = price_b - price_a

    if abs(momentum) < min_threshold:
        return None

    direction = 'Up' if momentum > 0 else 'Down'

    return {
        'direction': direction,
        'entry_up_price': price_b,
        'momentum': abs(momentum),
    }


def simulate_trade(direction, entry_up_price, market, ticks_by_second, entry_second, stop_loss_price):
    """Simulate trade with parametric entry and stop-loss."""
    # Token entry price
    token_entry = entry_up_price if direction == 'Up' else (1 - entry_up_price)

    # Stop loss monitor
    if stop_loss_price is not None:
        for sec in range(entry_second, 300, 5):
            price = ticks_by_second.get(sec)
            if price is None:
                continue
            token_price = price if direction == 'Up' else (1 - price)
            if token_price <= stop_loss_price:
                pnl = token_price - token_entry
                return 'stop_loss', pnl, token_entry

    # Resolution
    won = (direction == market['final_outcome'])
    pnl = (1 - token_entry) if won else (0 - token_entry)
    return ('win' if won else 'loss'), pnl, token_entry


def format_config(cfg):
    """Format config dict as compact string."""
    sl = f"SL={cfg['stop_loss']}" if cfg['stop_loss'] is not None else "SL=off"
    return (
        f"A={cfg['price_a']}s B={cfg['price_b']}s entry={cfg['entry_sec']}s "
        f"thr={cfg['threshold']} price={cfg['price_min']}-{cfg['price_max']} "
        f"{sl} dir={cfg['direction']} markets={cfg['markets']} hours={cfg['hours']}"
    )


def print_top_table(title, results, count):
    """Print a ranked table of top results."""
    print(f"\n{'=' * 140}")
    print(f"=== {title} ===")
    print(f"{'=' * 140}")
    header = f"{'Rank':>4} | {'Trades':>6} | {'Win%':>5} | {'W/SL/L':<14} | {'PnL':>9} | {'ROI':>6} | {'AvgPnL':>7} | Config"
    print(header)
    print('-' * 140)
    for i, r in enumerate(results[:count], 1):
        wsl = f"{r['wins']}/{r['sls']}/{r['losses']}"
        print(
            f"{i:>4} | {r['trades']:>6} | {r['win_rate']:>4.1f}% | {wsl:<14} | "
            f"${r['total_pnl']:>8.2f} | {r['roi']:>5.1f}% | ${r['avg_pnl']:>6.4f} | "
            f"{format_config(r['config'])}"
        )


def print_pattern_analysis(results, top_n=50):
    """Analyze which parameter values appear most in top configs."""
    print(f"\n{'=' * 80}")
    print(f"=== PARAMETER PATTERNS IN TOP {top_n} CONFIGS (by ROI) ===")
    print(f"{'=' * 80}")

    top = results[:top_n]

    param_keys = [
        ('price_a', 'price_a', 's'),
        ('price_b', 'price_b', 's'),
        ('entry_sec', 'entry_sec', 's'),
        ('threshold', 'threshold', ''),
        ('price_min', 'price_min', ''),
        ('price_max', 'price_max', ''),
        ('stop_loss', 'stop_loss', ''),
        ('direction', 'direction', ''),
        ('markets', 'markets', ''),
        ('hours', 'hours', ''),
    ]

    for label, key, suffix in param_keys:
        counts = defaultdict(int)
        for r in top:
            val = r['config'][key]
            counts[val] += 1
        sorted_vals = sorted(counts.items(), key=lambda x: -x[1])
        parts = []
        for val, cnt in sorted_vals[:5]:
            if val is None:
                parts.append(f"off ({cnt}x)")
            else:
                parts.append(f"{val}{suffix} ({cnt}x)")
        print(f"  {label:<12} most common: {', '.join(parts)}")


async def main():
    pg_host = os.getenv('POSTGRES_HOST', 'localhost')
    pg_port = int(os.getenv('POSTGRES_PORT', '5432'))
    pg_user = os.getenv('POSTGRES_USER', 'polymarket')
    pg_password = os.getenv('POSTGRES_PASSWORD', '')
    pg_db = os.getenv('POSTGRES_DB', 'polymarket_tracker')

    print("Connecting to database (read-only)...")
    conn = await asyncpg.connect(
        host=pg_host, port=pg_port,
        user=pg_user, password=pg_password,
        database=pg_db,
    )

    try:
        print("Loading resolved 5m markets...")
        markets_rows = await conn.fetch("""
            SELECT market_id, market_type, started_at, ended_at, final_outcome, final_up_price
            FROM market_outcomes
            WHERE resolved = TRUE
            AND market_type IN ('btc_5m', 'eth_5m', 'xrp_5m', 'sol_5m')
            ORDER BY started_at
        """)

        markets = [dict(r) for r in markets_rows]
        print(f"Loaded {len(markets)} resolved markets")

        if not markets:
            print("No resolved markets found. Exiting.")
            return

        market_ids = [m['market_id'] for m in markets]

        print("Loading ticks...")
        ticks_rows = await conn.fetch("""
            SELECT time, market_id, up_price
            FROM market_ticks
            WHERE market_id = ANY($1)
            ORDER BY time
        """, market_ids)

        ticks_by_market_raw = defaultdict(list)
        for r in ticks_rows:
            ticks_by_market_raw[r['market_id']].append({
                'time': r['time'],
                'up_price': float(r['up_price']),
            })
        print(f"Loaded {len(ticks_rows)} ticks across {len(ticks_by_market_raw)} markets")

    finally:
        await conn.close()
        print("Database connection closed.\n")

    # Precompute ticks indexed by second for each market
    print("Indexing ticks by second...")
    market_data = []  # list of (market_dict, ticks_by_second, hour)
    for m in markets:
        mid = m['market_id']
        raw = ticks_by_market_raw.get(mid, [])
        if not raw:
            continue
        tbs = index_ticks_by_second(raw, m['started_at'])
        hour = m['started_at'].hour
        market_data.append((m, tbs, hour))
    print(f"Indexed {len(market_data)} markets\n")

    # Generate all valid parameter combinations
    print("Generating parameter combinations...")
    combos = []
    for price_a, price_b in product(PRICE_A_SECONDS, PRICE_B_SECONDS):
        if price_b <= price_a:
            continue
        for entry_offset in ENTRY_OFFSETS:
            entry_sec = price_b + entry_offset
            if entry_sec >= 270:
                continue
            for threshold in MIN_THRESHOLDS:
                for pmin, pmax in product(PRICE_MINS, PRICE_MAXS):
                    if pmax - pmin < 0.20:
                        continue
                    if pmin >= pmax:
                        continue
                    for sl in STOP_LOSS_PRICES:
                        for direction_filter in DIRECTION_FILTERS:
                            for market_filter in MARKET_FILTERS:
                                for hour_filter in HOUR_FILTERS:
                                    combos.append({
                                        'price_a': price_a,
                                        'price_b': price_b,
                                        'entry_sec': entry_sec,
                                        'threshold': threshold,
                                        'price_min': pmin,
                                        'price_max': pmax,
                                        'stop_loss': sl,
                                        'direction': direction_filter,
                                        'markets': market_filter,
                                        'hours': hour_filter,
                                    })

    total_combos = len(combos)
    print(f"Total configs to test: {total_combos}", file=sys.stderr)

    # Pre-filter markets by market_filter and hour_filter to avoid repeated work
    # Group markets by (market_filter, hour_filter) for reuse
    print("Pre-filtering markets by market/hour combinations...")
    filtered_market_cache = {}
    for market_filter in MARKET_FILTERS:
        allowed_types = MARKET_SETS[market_filter]
        for hour_filter in HOUR_FILTERS:
            hour_range = HOUR_RANGES[hour_filter]
            filtered = []
            for m, tbs, hour in market_data:
                if allowed_types is not None and m['market_type'] not in allowed_types:
                    continue
                if hour_range is not None:
                    h_start, h_end = hour_range
                    if h_start < h_end:
                        if not (h_start <= hour < h_end):
                            continue
                    else:
                        if not (hour >= h_start or hour < h_end):
                            continue
                filtered.append((m, tbs))
            filtered_market_cache[(market_filter, hour_filter)] = filtered
    print(f"Cached {len(filtered_market_cache)} market/hour filter combos\n")

    # Run all combinations
    print("Running brute force sweep...")
    results = []
    start_time = time.time()
    estimate_printed = False

    for idx, cfg in enumerate(combos):
        if idx > 0 and idx % 500 == 0:
            elapsed = time.time() - start_time
            rate = idx / elapsed
            remaining = (total_combos - idx) / rate
            print(f"Progress: {idx}/{total_combos} ({idx/total_combos*100:.1f}%) | "
                  f"Elapsed: {elapsed/60:.1f}m | "
                  f"ETA: {remaining/60:.1f}m | "
                  f"Rate: {rate:.0f} configs/sec", file=sys.stderr)

        if not estimate_printed and idx == 100:
            elapsed = time.time() - start_time
            total_est = elapsed / 100 * total_combos
            print(f"  Estimated total time: {total_est:.0f}s ({total_est/60:.1f}m)", file=sys.stderr)
            estimate_printed = True

        price_a_sec = cfg['price_a']
        price_b_sec = cfg['price_b']
        entry_sec = cfg['entry_sec']
        threshold = cfg['threshold']
        price_min = cfg['price_min']
        price_max = cfg['price_max']
        stop_loss = cfg['stop_loss']
        direction_filter = cfg['direction']

        filtered_markets = filtered_market_cache[(cfg['markets'], cfg['hours'])]

        wins = 0
        sls = 0
        losses = 0
        total_pnl = 0.0
        total_entry_cost = 0.0
        trade_count = 0

        for m, tbs in filtered_markets:
            signal = detect_signal(tbs, price_a_sec, price_b_sec, threshold)
            if signal is None:
                continue

            direction = signal['direction']

            # Direction filter
            if direction_filter == 'up_only' and direction != 'Up':
                continue
            if direction_filter == 'down_only' and direction != 'Down':
                continue

            # Token entry price
            entry_up_price = signal['entry_up_price']
            token_entry = entry_up_price if direction == 'Up' else (1 - entry_up_price)

            # Price range filter on token entry
            if token_entry < price_min or token_entry > price_max:
                continue

            outcome, pnl, t_entry = simulate_trade(
                direction, entry_up_price, m, tbs, entry_sec, stop_loss
            )

            trade_count += 1
            total_pnl += pnl
            total_entry_cost += t_entry

            if outcome == 'win':
                wins += 1
            elif outcome == 'stop_loss':
                sls += 1
            else:
                losses += 1

        if trade_count < MIN_TRADES:
            continue

        win_rate = wins / trade_count * 100
        avg_pnl = total_pnl / trade_count
        roi = (total_pnl / total_entry_cost * 100) if total_entry_cost > 0 else 0

        results.append({
            'trades': trade_count,
            'wins': wins,
            'sls': sls,
            'losses': losses,
            'win_rate': win_rate,
            'total_pnl': total_pnl,
            'roi': roi,
            'avg_pnl': avg_pnl,
            'config': cfg,
        })

    elapsed_total = time.time() - start_time
    print(f"\nSweep complete in {elapsed_total:.1f}s ({elapsed_total/60:.1f}m)")
    print(f"Tested {total_combos} configs, {len(results)} had {MIN_TRADES}+ trades\n")

    if not results:
        print("No configs produced enough trades. Try lowering MIN_TRADES or check data.")
        return

    # Sort and print results
    by_roi = sorted(results, key=lambda x: x['roi'], reverse=True)
    by_avg_pnl = sorted(results, key=lambda x: x['avg_pnl'], reverse=True)
    by_total_pnl = sorted(results, key=lambda x: x['total_pnl'], reverse=True)

    print_top_table("TOP 30 CONFIGS BY ROI", by_roi, 30)
    print_top_table("TOP 30 CONFIGS BY AVG PNL PER TRADE", by_avg_pnl, 30)
    print_top_table("TOP 30 CONFIGS BY TOTAL PNL", by_total_pnl, 30)

    # High-volume configs
    results_200 = [r for r in results if r['trades'] >= 200]
    if results_200:
        by_roi_200 = sorted(results_200, key=lambda x: x['roi'], reverse=True)
        print_top_table("TOP 10 CONFIGS WITH 200+ TRADES BY ROI", by_roi_200, 10)

    results_500 = [r for r in results if r['trades'] >= 500]
    if results_500:
        by_roi_500 = sorted(results_500, key=lambda x: x['roi'], reverse=True)
        print_top_table("TOP 10 CONFIGS WITH 500+ TRADES BY ROI", by_roi_500, 10)

    # Pattern analysis on top 50 by ROI
    print_pattern_analysis(by_roi, top_n=50)

    print(f"\nDone. Total elapsed: {elapsed_total:.1f}s")
    print(f"Done in {elapsed_total/60:.1f} minutes", file=sys.stderr)


if __name__ == '__main__':
    asyncio.run(main())
