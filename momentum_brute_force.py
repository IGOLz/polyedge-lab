"""
Momentum Strategy Brute Force Parameter Sweep
Tries every meaningful combination of parameters and ranks by ROI/PnL.
Connects to DB read-only, saves nothing. Prints everything to stdout.
"""

import asyncpg
import asyncio
import os
import sys
import threading
import time
from dotenv import load_dotenv

sys.stdout.reconfigure(encoding='utf-8', line_buffering=True)
sys.stderr.reconfigure(encoding='utf-8', line_buffering=True)
from collections import defaultdict
from itertools import product

load_dotenv()

# --- Parameter space ---
PRICE_A_SECONDS = [30, 45, 60]
PRICE_B_SECONDS = [60, 75, 90]
ENTRY_OFFSETS = [5, 10]
MIN_THRESHOLDS = [0.02, 0.03, 0.05, 0.07, 0.10]
PRICE_MINS = [0.01, 0.40, 0.50]
PRICE_MAXS = [0.75, 0.80, 0.90]
STOP_LOSS_PRICES = [None, 0.40, 0.35]
DIRECTION_FILTERS = ['both', 'down_only']
MARKET_FILTERS = ['all', 'no_btc', 'xrp_sol_only']
HOUR_FILTERS = ['all']
CONTEXT_MAX_OPEN_DELTA = [None, 0.10, 0.20]
PRICE_OPEN_SECONDS = [0, 10]

# Dynamic stop-loss: move stop to breakeven (or better) after price moves in our favor
DYNAMIC_SL_TRIGGER_DELTA = [None, 0.05, 0.08, 0.10, 0.12, 0.15, 0.20]
DYNAMIC_SL_NEW_OFFSET = [-0.02, 0.0, 0.02, 0.05]

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


def get_price_at_second(ticks_by_second, target_sec, max_offset=5):
    """Get price at exact second, or interpolate within +/-max_offset s."""
    price = ticks_by_second.get(target_sec)
    if price is not None:
        return price
    for offset in range(1, max_offset + 1):
        price = ticks_by_second.get(target_sec + offset)
        if price is not None:
            return price
        price = ticks_by_second.get(target_sec - offset)
        if price is not None:
            return price
    return None


def detect_signal(ticks_by_second, price_a_sec, price_b_sec, min_threshold,
                   price_open_sec=0, context_max_open_delta=None):
    """Detect momentum signal with parametric timing and context filter."""
    price_a = get_price_at_second(ticks_by_second, price_a_sec)
    price_b = get_price_at_second(ticks_by_second, price_b_sec)

    if price_a is None or price_b is None:
        return None

    momentum = price_b - price_a

    if abs(momentum) < min_threshold:
        return None

    direction = 'Up' if momentum > 0 else 'Down'

    # Context filter: check if signal agrees with move from open
    if context_max_open_delta is not None:
        price_open = get_price_at_second(ticks_by_second, price_open_sec, max_offset=10)
        if price_open is not None:
            open_delta = price_b - price_open  # positive = market moved Up from open
            if direction == 'Down' and open_delta > context_max_open_delta:
                return None  # price is too far Up from open, Down signal is just a pullback
            if direction == 'Up' and open_delta < -context_max_open_delta:
                return None  # price is too far Down from open, Up signal is just a pullback

    return {
        'direction': direction,
        'entry_up_price': price_b,
        'momentum': abs(momentum),
    }


def simulate_trade(direction, entry_up_price, market, ticks_by_second, entry_second,
                   stop_loss_price, dynamic_sl_trigger=None, dynamic_sl_offset=None):
    """Simulate trade with parametric entry, static stop-loss, and dynamic stop-loss."""
    token_entry = entry_up_price if direction == 'Up' else (1 - entry_up_price)

    current_stop = stop_loss_price  # may be None
    dynamic_sl_activated = False

    # Monitor price ticks from entry to market end
    if current_stop is not None or dynamic_sl_trigger is not None:
        for sec in range(entry_second, 300, 5):
            price = ticks_by_second.get(sec)
            if price is None:
                continue
            token_price = price if direction == 'Up' else (1 - price)

            # Check current stop-loss (static or already-moved)
            if current_stop is not None and token_price <= current_stop:
                pnl = token_price - token_entry
                return 'stop_loss', pnl, token_entry

            # Check if dynamic SL should be triggered
            if (dynamic_sl_trigger is not None
                    and not dynamic_sl_activated
                    and token_price >= token_entry + dynamic_sl_trigger):
                new_stop = token_entry + (dynamic_sl_offset if dynamic_sl_offset is not None else 0.0)
                # Only move stop upward -- never make it worse than the original
                if current_stop is None or new_stop > current_stop:
                    current_stop = new_stop
                    dynamic_sl_activated = True

    # Resolution
    won = (direction == market['final_outcome'])
    pnl = (1 - token_entry) if won else (0 - token_entry)
    return ('win' if won else 'loss'), pnl, token_entry


def format_config(cfg):
    """Format config dict as compact string."""
    sl = f"SL={cfg['stop_loss']}" if cfg['stop_loss'] is not None else "SL=off"
    ctx = f"ctx={cfg['context_max_open_delta']}" if cfg.get('context_max_open_delta') is not None else "ctx=off"
    open_s = f"open={cfg['price_open_sec']}s" if cfg.get('price_open_sec') is not None else "open=0s"
    if cfg.get('dynamic_sl_trigger') is not None:
        dsl_offset = cfg.get('dynamic_sl_offset') if cfg.get('dynamic_sl_offset') is not None else 0.0
        dsl = f"dynSL=+{cfg['dynamic_sl_trigger']}->{dsl_offset:+.2f}"
    else:
        dsl = "dynSL=off"
    return (
        f"A={cfg['price_a']}s B={cfg['price_b']}s entry={cfg['entry_sec']}s "
        f"thr={cfg['threshold']} price={cfg['price_min']}-{cfg['price_max']} "
        f"{sl} {dsl} {ctx} {open_s} dir={cfg['direction']} markets={cfg['markets']} hours={cfg['hours']}"
    )


def print_top_table(title, results, count):
    """Print a ranked table of top results."""
    print(f"\n{'=' * 160}", flush=True)
    print(f"=== {title} ===", flush=True)
    print(f"{'=' * 160}", flush=True)
    header = f"{'Rank':>4} | {'Trades':>6} | {'Win%':>5} | {'W/SL/L':<14} | {'PnL':>9} | {'ROI':>6} | {'AvgPnL':>7} | Config"
    print(header, flush=True)
    print('-' * 160, flush=True)
    for i, r in enumerate(results[:count], 1):
        wsl = f"{r['wins']}/{r['sls']}/{r['losses']}"
        print(
            f"{i:>4} | {r['trades']:>6} | {r['win_rate']:>4.1f}% | {wsl:<14} | "
            f"${r['total_pnl']:>8.2f} | {r['roi']:>5.1f}% | ${r['avg_pnl']:>6.4f} | "
            f"{format_config(r['config'])}",
            flush=True
        )


def print_pattern_analysis(results, top_n=50):
    """Analyze which parameter values appear most in top configs."""
    print(f"\n{'=' * 80}", flush=True)
    print(f"=== PARAMETER PATTERNS IN TOP {top_n} CONFIGS (by ROI) ===", flush=True)
    print(f"{'=' * 80}", flush=True)

    top = results[:top_n]

    param_keys = [
        ('price_a', 'price_a', 's'),
        ('price_b', 'price_b', 's'),
        ('entry_sec', 'entry_sec', 's'),
        ('threshold', 'threshold', ''),
        ('price_min', 'price_min', ''),
        ('price_max', 'price_max', ''),
        ('stop_loss', 'stop_loss', ''),
        ('ctx_delta', 'context_max_open_delta', ''),
        ('open_sec', 'price_open_sec', 's'),
        ('dyn_trigger', 'dynamic_sl_trigger', ''),
        ('dyn_offset', 'dynamic_sl_offset', ''),
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
        print(f"  {label:<12} most common: {', '.join(parts)}", flush=True)


def generate_combos():
    """Yield all valid parameter combinations without holding them in memory."""
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
                        for dynamic_trigger in DYNAMIC_SL_TRIGGER_DELTA:
                            offsets_to_test = DYNAMIC_SL_NEW_OFFSET if dynamic_trigger is not None else [None]
                            for dynamic_offset in offsets_to_test:
                                for ctx_delta in CONTEXT_MAX_OPEN_DELTA:
                                    for open_sec in PRICE_OPEN_SECONDS:
                                        # Skip varying open_sec when context filter is disabled
                                        if ctx_delta is None and open_sec != 0:
                                            continue
                                        for direction_filter in DIRECTION_FILTERS:
                                            for market_filter in MARKET_FILTERS:
                                                for hour_filter in HOUR_FILTERS:
                                                    yield {
                                                        'price_a': price_a,
                                                        'price_b': price_b,
                                                        'entry_sec': entry_sec,
                                                        'threshold': threshold,
                                                        'price_min': pmin,
                                                        'price_max': pmax,
                                                        'stop_loss': sl,
                                                        'dynamic_sl_trigger': dynamic_trigger,
                                                        'dynamic_sl_offset': dynamic_offset,
                                                        'context_max_open_delta': ctx_delta,
                                                        'price_open_sec': open_sec,
                                                        'direction': direction_filter,
                                                        'markets': market_filter,
                                                        'hours': hour_filter,
                                                    }


async def main():
    pg_host = os.getenv('POSTGRES_HOST', 'localhost')
    pg_port = int(os.getenv('POSTGRES_PORT', '5432'))
    pg_user = os.getenv('POSTGRES_USER', 'polymarket')
    pg_password = os.getenv('POSTGRES_PASSWORD', '')
    pg_db = os.getenv('POSTGRES_DB', 'polymarket_tracker')

    print("Connecting to database (read-only)...", flush=True)
    conn = await asyncpg.connect(
        host=pg_host, port=pg_port,
        user=pg_user, password=pg_password,
        database=pg_db,
    )

    try:
        print("Loading resolved 5m markets...", flush=True)
        markets_rows = await conn.fetch("""
            SELECT market_id, market_type, started_at, ended_at, final_outcome, final_up_price
            FROM market_outcomes
            WHERE resolved = TRUE
            AND market_type IN ('btc_5m', 'eth_5m', 'xrp_5m', 'sol_5m')
            ORDER BY started_at
        """)

        markets = [dict(r) for r in markets_rows]
        print(f"Loaded {len(markets)} resolved markets", flush=True)

        if not markets:
            print("No resolved markets found. Exiting.", flush=True)
            return

        market_ids = [m['market_id'] for m in markets]

        print("Loading ticks...", flush=True)
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
        print(f"Loaded {len(ticks_rows)} ticks across {len(ticks_by_market_raw)} markets", flush=True)

    finally:
        await conn.close()
        print("Database connection closed.\n", flush=True)

    # Precompute ticks indexed by second for each market
    print("Indexing ticks by second...", flush=True)
    market_data = []  # list of (market_dict, ticks_by_second, hour)
    for m in markets:
        mid = m['market_id']
        raw = ticks_by_market_raw.get(mid, [])
        if not raw:
            continue
        tbs = index_ticks_by_second(raw, m['started_at'])
        hour = m['started_at'].hour
        market_data.append((m, tbs, hour))
    print(f"Indexed {len(market_data)} markets\n", flush=True)

    # Pre-filter markets by market_filter and hour_filter to avoid repeated work
    # Group markets by (market_filter, hour_filter) for reuse
    print("Pre-filtering markets by market/hour combinations...", flush=True)
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
    print(f"Cached {len(filtered_market_cache)} market/hour filter combos\n", flush=True)

    # Count combos in background so sweep starts immediately
    total_combos = None
    total_combos_lock = threading.Lock()

    def count_in_background():
        nonlocal total_combos
        count = sum(1 for _ in generate_combos())
        with total_combos_lock:
            total_combos = count

    counter_thread = threading.Thread(target=count_in_background, daemon=True)
    counter_thread.start()
    print("Counting combinations in background, sweep starting immediately...\n", file=sys.stderr, flush=True)

    # Run all combinations
    print("Running brute force sweep...", flush=True)
    results = []
    start_time = time.time()

    for idx, cfg in enumerate(generate_combos()):
        if idx > 0 and idx % 500 == 0:
            elapsed = time.time() - start_time
            rate = idx / elapsed if elapsed > 0 else 0
            with total_combos_lock:
                total = total_combos
            if total is not None and total > 0:
                pct = idx / total * 100
                remaining = (total - idx) / rate if rate > 0 else 0
                print(
                    f"Progress: {idx}/{total} ({pct:.1f}%) | "
                    f"Elapsed: {elapsed/60:.1f}m | "
                    f"ETA: {remaining/60:.1f}m | "
                    f"Rate: {rate:.0f} configs/sec",
                    file=sys.stderr, flush=True
                )
            else:
                print(
                    f"Progress: {idx} done (counting...) | "
                    f"Elapsed: {elapsed/60:.1f}m | "
                    f"Rate: {rate:.0f} configs/sec",
                    file=sys.stderr, flush=True
                )

        price_a_sec = cfg['price_a']
        price_b_sec = cfg['price_b']
        entry_sec = cfg['entry_sec']
        threshold = cfg['threshold']
        price_min = cfg['price_min']
        price_max = cfg['price_max']
        stop_loss = cfg['stop_loss']
        ctx_delta = cfg['context_max_open_delta']
        open_sec = cfg['price_open_sec']
        direction_filter = cfg['direction']

        filtered_markets = filtered_market_cache[(cfg['markets'], cfg['hours'])]

        wins = 0
        sls = 0
        losses = 0
        total_pnl = 0.0
        total_entry_cost = 0.0
        trade_count = 0

        for m, tbs in filtered_markets:
            signal = detect_signal(tbs, price_a_sec, price_b_sec, threshold,
                                   open_sec, ctx_delta)
            if signal is None:
                continue

            direction = signal['direction']

            # Direction filter
            if direction_filter == 'down_only' and direction != 'Down':
                continue

            # Token entry price
            entry_up_price = signal['entry_up_price']
            token_entry = entry_up_price if direction == 'Up' else (1 - entry_up_price)

            # Price range filter on token entry
            if token_entry < price_min or token_entry > price_max:
                continue

            outcome, pnl, t_entry = simulate_trade(
                direction, entry_up_price, m, tbs, entry_sec, stop_loss,
                dynamic_sl_trigger=cfg['dynamic_sl_trigger'],
                dynamic_sl_offset=cfg['dynamic_sl_offset'],
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
    print(f"\nSweep complete in {elapsed_total:.1f}s ({elapsed_total/60:.1f}m)", flush=True)
    print(f"Tested {idx + 1} configs, {len(results)} had {MIN_TRADES}+ trades\n", flush=True)

    if not results:
        print("No configs produced enough trades. Try lowering MIN_TRADES or check data.", flush=True)
        return

    # Sort and print results
    by_roi = sorted(results, key=lambda x: x['roi'], reverse=True)
    by_avg_pnl = sorted(results, key=lambda x: x['avg_pnl'], reverse=True)
    by_total_pnl = sorted(results, key=lambda x: x['total_pnl'], reverse=True)

    print_top_table("TOP 30 CONFIGS BY ROI", by_roi, 30)

    # Directional bias warning
    top30_dirs = [r['config']['direction'] for r in by_roi[:30]]
    down_only_count = sum(1 for d in top30_dirs if d == 'down_only')
    if down_only_count >= 24:  # 80% of 30
        print(f"\n*** WARNING: {down_only_count}/30 top configs are down_only. "
              f"Results may reflect bearish market bias, not strategy edge. ***", flush=True)

    # dir=both only leaderboard
    both_only = [r for r in results if r['config']['direction'] == 'both']
    if both_only:
        both_by_roi = sorted(both_only, key=lambda x: x['roi'], reverse=True)
        print_top_table("TOP 20 CONFIGS BY ROI -- dir=both ONLY", both_by_roi, 20)

    # Dynamic SL + dir=both leaderboard
    dyn_sl_both = [r for r in results
                   if r['config']['dynamic_sl_trigger'] is not None
                   and r['config']['direction'] == 'both']
    if dyn_sl_both:
        dyn_sl_both_roi = sorted(dyn_sl_both, key=lambda x: x['roi'], reverse=True)
        print_top_table("TOP 20 CONFIGS WITH DYNAMIC SL ENABLED -- dir=both ONLY", dyn_sl_both_roi, 20)

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

    print(f"\nDone. Total elapsed: {elapsed_total:.1f}s", flush=True)
    print(f"Done in {elapsed_total/60:.1f} minutes", file=sys.stderr, flush=True)


if __name__ == '__main__':
    asyncio.run(main())
