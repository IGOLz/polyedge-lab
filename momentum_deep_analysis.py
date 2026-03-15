"""
Momentum Strategy Deep Analysis
Connects to DB read-only, runs backtests across multiple configs,
prints detailed analysis to console. Saves nothing to DB.
"""

import asyncpg
import asyncio
import os
from dotenv import load_dotenv
from collections import defaultdict

load_dotenv()


def get_price_at_second(ticks, started_at, target_second):
    """Get the up_price closest to target_second after market start."""
    target_time = started_at + __import__('datetime').timedelta(seconds=target_second)
    best = None
    best_diff = None
    for t in ticks:
        diff = abs((t['time'] - target_time).total_seconds())
        if best_diff is None or diff < best_diff:
            best_diff = diff
            best = t['up_price']
        if diff < 1:
            break
    return best


def detect_momentum_signal(ticks, market, config):
    """Detect momentum signal exactly as the bot does."""
    started_at = market['started_at']
    min_threshold = config['min_threshold']
    price_min = config['price_min']
    price_max = config['price_max']

    price_30s = get_price_at_second(ticks, started_at, 30)
    price_60s = get_price_at_second(ticks, started_at, 60)

    if price_30s is None or price_60s is None:
        return None

    momentum = price_60s - price_30s

    if abs(momentum) < min_threshold:
        return None

    direction = 'Up' if momentum > 0 else 'Down'
    entry_price = price_60s if direction == 'Up' else (1 - price_60s)

    # Price filter on entry_price (the token price we'd pay)
    if entry_price < price_min or entry_price > price_max:
        return None

    return {
        'direction': direction,
        'entry_price': entry_price,
        'momentum': momentum,
        'price_30s': price_30s,
        'price_60s': price_60s,
        'raw_up_price': price_60s,
    }


def simulate_trade(signal, market, ticks, stop_loss_enabled, stop_loss_price, entry_second=65):
    """Simulate a trade with optional stop-loss."""
    direction = signal['direction']
    entry_price = signal['entry_price']
    final_outcome = market['final_outcome']
    started_at = market['started_at']

    stop_loss_triggered = False
    exit_price = None

    if stop_loss_enabled:
        for second in range(entry_second, 300, 5):
            price = get_price_at_second(ticks, started_at, second)
            if price is None:
                continue

            token_price = price if direction == 'Up' else (1 - price)

            if token_price <= stop_loss_price:
                stop_loss_triggered = True
                exit_price = token_price
                break

    if stop_loss_triggered:
        pnl_per_share = exit_price - entry_price
        outcome = 'stop_loss'
    else:
        won = (direction == 'Up' and final_outcome == 'Up') or \
              (direction == 'Down' and final_outcome == 'Down')
        pnl_per_share = (1 - entry_price) if won else (0 - entry_price)
        outcome = 'win' if won else 'loss'

    return {
        'outcome': outcome,
        'pnl_per_share': pnl_per_share,
        'stop_loss_triggered': stop_loss_triggered,
        'exit_price': exit_price,
    }


def get_entry_price_bucket(entry_price):
    if entry_price <= 0.30:
        return '0.01-0.30'
    elif entry_price <= 0.40:
        return '0.31-0.40'
    elif entry_price <= 0.50:
        return '0.41-0.50'
    elif entry_price <= 0.60:
        return '0.51-0.60'
    elif entry_price <= 0.70:
        return '0.61-0.70'
    elif entry_price <= 0.80:
        return '0.71-0.80'
    else:
        return '0.81-0.99'


def get_momentum_bucket(momentum):
    m = abs(momentum)
    if m <= 0.03:
        return '0.01-0.03'
    elif m <= 0.05:
        return '0.03-0.05'
    elif m <= 0.08:
        return '0.05-0.08'
    elif m <= 0.12:
        return '0.08-0.12'
    else:
        return '0.12+'


def get_hour_bucket(dt):
    h = dt.hour
    if h < 4:
        return '00-04'
    elif h < 8:
        return '04-08'
    elif h < 12:
        return '08-12'
    elif h < 16:
        return '12-16'
    elif h < 20:
        return '16-20'
    else:
        return '20-24'


DAY_NAMES = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']


def print_config_results(label, config, trades):
    """Print detailed analysis for one config."""
    min_threshold, stop_loss_enabled, stop_loss_price, price_min, price_max = (
        config['min_threshold'], config['stop_loss_enabled'],
        config['stop_loss_price'], config['price_min'], config['price_max']
    )

    sl_str = f"{stop_loss_price:.2f}" if stop_loss_enabled else "OFF"
    print(f"\n{'=' * 70}")
    print(f"=== CONFIG: {label} | SL: {sl_str} | Price: {price_min}-{price_max} ===")
    print(f"{'=' * 70}")

    if not trades:
        print("  No trades.")
        return

    total = len(trades)
    wins = sum(1 for t in trades if t['outcome'] == 'win')
    sls = sum(1 for t in trades if t['outcome'] == 'stop_loss')
    losses = sum(1 for t in trades if t['outcome'] == 'loss')
    total_pnl = sum(t['pnl_per_share'] for t in trades)
    avg_pnl = total_pnl / total if total else 0
    win_rate = wins / total * 100 if total else 0

    print(f"Total trades: {total}")
    print(f"Win rate: {win_rate:.1f}% | W: {wins} / SL: {sls} / L: {losses}")
    print(f"Total PnL: ${total_pnl:.2f} | Avg PnL/trade: ${avg_pnl:.4f}")

    # By market type
    print(f"\nBy market type:")
    by_market = defaultdict(list)
    for t in trades:
        by_market[t['market_type']].append(t)
    for mtype in sorted(by_market.keys()):
        mt = by_market[mtype]
        mw = sum(1 for x in mt if x['outcome'] == 'win')
        mp = sum(x['pnl_per_share'] for x in mt)
        mwr = mw / len(mt) * 100 if mt else 0
        print(f"  {mtype}: {len(mt)} trades | {mwr:.1f}% win | PnL: ${mp:.2f}")

    # By entry price bucket
    print(f"\nBy entry price bucket:")
    bucket_order = ['0.01-0.30', '0.31-0.40', '0.41-0.50', '0.51-0.60',
                    '0.61-0.70', '0.71-0.80', '0.81-0.99']
    by_price = defaultdict(list)
    for t in trades:
        by_price[get_entry_price_bucket(t['entry_price'])].append(t)
    for bucket in bucket_order:
        bt = by_price.get(bucket, [])
        if not bt:
            continue
        bw = sum(1 for x in bt if x['outcome'] == 'win')
        bp = sum(x['pnl_per_share'] for x in bt)
        bwr = bw / len(bt) * 100 if bt else 0
        flag = ''
        if bp < 0:
            flag = '  ← LOSING RANGE'
        elif bwr >= 35:
            flag = '  ← STRONG'
        print(f"  {bucket}: {len(bt)} trades | {bwr:.1f}% win | PnL: ${bp:.2f}{flag}")

    # By momentum strength
    print(f"\nBy momentum strength bucket:")
    mom_order = ['0.01-0.03', '0.03-0.05', '0.05-0.08', '0.08-0.12', '0.12+']
    by_mom = defaultdict(list)
    for t in trades:
        by_mom[get_momentum_bucket(t['momentum'])].append(t)
    for bucket in mom_order:
        bt = by_mom.get(bucket, [])
        if not bt:
            continue
        bw = sum(1 for x in bt if x['outcome'] == 'win')
        bp = sum(x['pnl_per_share'] for x in bt)
        bwr = bw / len(bt) * 100 if bt else 0
        print(f"  {bucket}: {len(bt)} trades | {bwr:.1f}% win | PnL: ${bp:.2f}")

    # By direction
    print(f"\nBy direction:")
    by_dir = defaultdict(list)
    for t in trades:
        by_dir[t['direction']].append(t)
    for d in ['Up', 'Down']:
        dt_list = by_dir.get(d, [])
        if not dt_list:
            continue
        dw = sum(1 for x in dt_list if x['outcome'] == 'win')
        dp = sum(x['pnl_per_share'] for x in dt_list)
        dwr = dw / len(dt_list) * 100 if dt_list else 0
        print(f"  {d}: {len(dt_list)} trades | {dwr:.1f}% win | PnL: ${dp:.2f}")

    # By hour of day
    print(f"\nBy hour of day (UTC):")
    hour_order = ['00-04', '04-08', '08-12', '12-16', '16-20', '20-24']
    by_hour = defaultdict(list)
    for t in trades:
        by_hour[get_hour_bucket(t['started_at'])].append(t)
    best_hour = None
    worst_hour = None
    for bucket in hour_order:
        bt = by_hour.get(bucket, [])
        if not bt:
            continue
        bw = sum(1 for x in bt if x['outcome'] == 'win')
        bp = sum(x['pnl_per_share'] for x in bt)
        bwr = bw / len(bt) * 100 if bt else 0
        if best_hour is None or bwr > best_hour[1]:
            best_hour = (bucket, bwr, bp)
        if worst_hour is None or bwr < worst_hour[1]:
            worst_hour = (bucket, bwr, bp)
    for bucket in hour_order:
        bt = by_hour.get(bucket, [])
        if not bt:
            continue
        bw = sum(1 for x in bt if x['outcome'] == 'win')
        bp = sum(x['pnl_per_share'] for x in bt)
        bwr = bw / len(bt) * 100 if bt else 0
        flag = ''
        if best_hour and bucket == best_hour[0]:
            flag = '  ← BEST WINDOW'
        elif worst_hour and bucket == worst_hour[0]:
            flag = '  ← WORST WINDOW'
        print(f"  {bucket}: {len(bt)} trades | {bwr:.1f}% win | PnL: ${bp:.2f}{flag}")

    # By day of week
    print(f"\nBy day of week:")
    by_day = defaultdict(list)
    for t in trades:
        by_day[t['started_at'].weekday()].append(t)
    parts = []
    for i, name in enumerate(DAY_NAMES):
        dt_list = by_day.get(i, [])
        parts.append(f"{name}: {len(dt_list)}")
    print(f"  {' | '.join(parts)}")
    for i, name in enumerate(DAY_NAMES):
        dt_list = by_day.get(i, [])
        if not dt_list:
            continue
        dw = sum(1 for x in dt_list if x['outcome'] == 'win')
        dp = sum(x['pnl_per_share'] for x in dt_list)
        dwr = dw / len(dt_list) * 100 if dt_list else 0
        print(f"  {name}: {dwr:.1f}% win | PnL: ${dp:.2f}")


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
        # Load all resolved 5m markets
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

        # Load all ticks into memory grouped by market_id
        print("Loading ticks (this may take a moment)...")
        ticks_rows = await conn.fetch("""
            SELECT time, market_id, up_price
            FROM market_ticks
            WHERE market_id = ANY($1)
            ORDER BY time
        """, market_ids)

        ticks_by_market = defaultdict(list)
        for r in ticks_rows:
            ticks_by_market[r['market_id']].append({
                'time': r['time'],
                'up_price': float(r['up_price']),
            })
        print(f"Loaded {len(ticks_rows)} ticks across {len(ticks_by_market)} markets")

    finally:
        await conn.close()
        print("Database connection closed.\n")

    # Configs to test
    configs = [
        # (min_threshold, stop_loss_enabled, stop_loss_price, price_min, price_max, label)
        (0.01, True, 0.50, 0.01, 0.99, 'baseline'),
        (0.02, True, 0.50, 0.01, 0.99, 'threshold_0.02'),
        (0.03, True, 0.50, 0.01, 0.99, 'threshold_0.03'),
        (0.05, True, 0.50, 0.01, 0.99, 'threshold_0.05'),
        (0.02, True, 0.40, 0.01, 0.99, 'sl_0.40'),
        (0.02, True, 0.30, 0.01, 0.99, 'sl_0.30'),
        (0.02, True, 0.50, 0.20, 0.80, 'price_filter_20_80'),
        (0.02, True, 0.50, 0.30, 0.70, 'price_filter_30_70'),
        (0.02, True, 0.50, 0.40, 0.70, 'price_filter_40_70'),
        (0.02, True, 0.50, 0.55, 0.85, 'price_filter_55_85'),
        (0.05, True, 0.50, 0.55, 0.85, 'strong_momentum_high_price'),
        (0.03, True, 0.40, 0.40, 0.75, 'combined_tight'),
    ]

    all_config_results = []

    # Collect best-segment data across all configs
    all_trades_all_configs = []

    for min_threshold, stop_loss_enabled, stop_loss_price, price_min, price_max, label in configs:
        config = {
            'min_threshold': min_threshold,
            'stop_loss_enabled': stop_loss_enabled,
            'stop_loss_price': stop_loss_price,
            'price_min': price_min,
            'price_max': price_max,
        }

        trades = []
        for market in markets:
            mid = market['market_id']
            ticks = ticks_by_market.get(mid, [])
            if not ticks:
                continue

            signal = detect_momentum_signal(ticks, market, config)
            if signal is None:
                continue

            result = simulate_trade(
                signal, market, ticks,
                stop_loss_enabled, stop_loss_price
            )

            trades.append({
                **result,
                'market_type': market['market_type'],
                'market_id': mid,
                'direction': signal['direction'],
                'entry_price': signal['entry_price'],
                'momentum': signal['momentum'],
                'started_at': market['started_at'],
                'config_label': label,
            })

        print_config_results(label, config, trades)

        total = len(trades)
        wins = sum(1 for t in trades if t['outcome'] == 'win')
        sls = sum(1 for t in trades if t['outcome'] == 'stop_loss')
        losses = sum(1 for t in trades if t['outcome'] == 'loss')
        total_pnl = sum(t['pnl_per_share'] for t in trades)
        avg_pnl = total_pnl / total if total else 0
        win_rate = wins / total * 100 if total else 0
        # ROI = total_pnl / total_cost where cost = sum of entry_prices
        total_cost = sum(t['entry_price'] for t in trades)
        roi = (total_pnl / total_cost * 100) if total_cost else 0

        all_config_results.append({
            'label': label,
            'trades': total,
            'wins': wins,
            'sls': sls,
            'losses': losses,
            'win_rate': win_rate,
            'total_pnl': total_pnl,
            'roi': roi,
            'avg_pnl': avg_pnl,
        })

        all_trades_all_configs.extend(trades)

    # Cross-config comparison table
    print(f"\n{'=' * 90}")
    print(f"=== COMPARISON TABLE ===")
    print(f"{'=' * 90}")
    header = f"{'Config':<30} | {'Trades':>6} | {'Win%':>5} | {'W/SL/L':<16} | {'PnL':>9} | {'ROI':>6} | {'AvgPnL':>7}"
    print(header)
    print('-' * 90)
    for r in all_config_results:
        wsl = f"{r['wins']}/{r['sls']}/{r['losses']}"
        print(f"{r['label']:<30} | {r['trades']:>6} | {r['win_rate']:>4.1f}% | {wsl:<16} | ${r['total_pnl']:>8.2f} | {r['roi']:>5.1f}% | ${r['avg_pnl']:>6.4f}")

    # Best performing segments across ALL configs
    print(f"\n{'=' * 70}")
    print(f"=== BEST PERFORMING SEGMENTS (across all configs) ===")
    print(f"{'=' * 70}")

    if not all_trades_all_configs:
        print("No trades to analyze.")
        return

    # Best/worst price range
    by_price = defaultdict(list)
    for t in all_trades_all_configs:
        by_price[get_entry_price_bucket(t['entry_price'])].append(t)

    best_price = None
    worst_price = None
    for bucket, bt in by_price.items():
        if len(bt) < 10:
            continue
        bw = sum(1 for x in bt if x['outcome'] == 'win')
        bp = sum(x['pnl_per_share'] for x in bt)
        bwr = bw / len(bt) * 100
        if best_price is None or bwr > best_price[1]:
            best_price = (bucket, bwr, bp)
        if worst_price is None or bwr < worst_price[1]:
            worst_price = (bucket, bwr, bp)

    if best_price:
        print(f"Best price range: {best_price[0]} (win rate {best_price[1]:.1f}%, PnL ${best_price[2]:.2f})")
    if worst_price:
        print(f"Worst price range: {worst_price[0]} (win rate {worst_price[1]:.1f}%, PnL ${worst_price[2]:.2f})")

    # Best/worst momentum threshold
    by_mom = defaultdict(list)
    for t in all_trades_all_configs:
        by_mom[get_momentum_bucket(t['momentum'])].append(t)

    best_mom = None
    for bucket, bt in by_mom.items():
        if len(bt) < 10:
            continue
        bw = sum(1 for x in bt if x['outcome'] == 'win')
        bp = sum(x['pnl_per_share'] for x in bt)
        bwr = bw / len(bt) * 100
        if best_mom is None or bwr > best_mom[1]:
            best_mom = (bucket, bwr, bp)

    if best_mom:
        print(f"Best momentum threshold: {best_mom[0]} (win rate {best_mom[1]:.1f}%, PnL ${best_mom[2]:.2f})")

    # Best/worst hours
    by_hour = defaultdict(list)
    for t in all_trades_all_configs:
        by_hour[get_hour_bucket(t['started_at'])].append(t)

    best_hour = None
    worst_hour = None
    for bucket, bt in by_hour.items():
        if len(bt) < 10:
            continue
        bw = sum(1 for x in bt if x['outcome'] == 'win')
        bp = sum(x['pnl_per_share'] for x in bt)
        bwr = bw / len(bt) * 100
        if best_hour is None or bwr > best_hour[1]:
            best_hour = (bucket, bwr, bp)
        if worst_hour is None or bwr < worst_hour[1]:
            worst_hour = (bucket, bwr, bp)

    if best_hour:
        print(f"Best hours: {best_hour[0]} UTC (win rate {best_hour[1]:.1f}%, PnL ${best_hour[2]:.2f})")
    if worst_hour:
        print(f"Worst hours: {worst_hour[0]} UTC (win rate {worst_hour[1]:.1f}%, PnL ${worst_hour[2]:.2f})")

    # Best/worst market
    by_market = defaultdict(list)
    for t in all_trades_all_configs:
        by_market[t['market_type']].append(t)

    best_market = None
    worst_market = None
    for mtype, bt in by_market.items():
        bw = sum(1 for x in bt if x['outcome'] == 'win')
        bp = sum(x['pnl_per_share'] for x in bt)
        bwr = bw / len(bt) * 100
        if best_market is None or bwr > best_market[1]:
            best_market = (mtype, bwr, bp)
        if worst_market is None or bwr < worst_market[1]:
            worst_market = (mtype, bwr, bp)

    if best_market:
        print(f"Best market: {best_market[0]} (win rate {best_market[1]:.1f}%, PnL ${best_market[2]:.2f})")
    if worst_market:
        print(f"Worst market: {worst_market[0]} (win rate {worst_market[1]:.1f}%, PnL ${worst_market[2]:.2f})")

    print(f"\nDone. Analyzed {len(markets)} markets, {len(all_trades_all_configs)} total trades across {len(configs)} configs.")


if __name__ == '__main__':
    asyncio.run(main())
