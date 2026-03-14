#!/usr/bin/env python3
"""
PolyEdge Lab — Probability Farming Strategy Backtest

Backtests the "Probability Farming" strategy against all historical
15-minute crypto market data and writes results to the database.

Called from run_analysis.py after the main analysis completes.
"""

import time

import numpy as np
import pandas as pd
from psycopg2.extras import execute_values

# ---------------------------------------------------------------------------
# Parameter grid
# ---------------------------------------------------------------------------

TRIGGER_POINTS = [0.65, 0.70, 0.75, 0.80, 0.85, 0.90]
EXIT_POINTS = [0.30, 0.40, 0.50, 0.60]
TRIGGER_MINUTES = [1, 2, 3, 5, 7, 10, 14]
MIN_COIN_DELTAS = [0.0, 0.05, 0.10, 0.15]
BET_SIZE = 10

# ---------------------------------------------------------------------------
# DDL
# ---------------------------------------------------------------------------

FARMING_DDL = """
CREATE TABLE IF NOT EXISTS farming_runs (
    id SERIAL PRIMARY KEY,
    run_id INT REFERENCES analysis_runs(id) ON DELETE CASCADE,
    ran_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    markets_tested INT,
    total_combinations INT,
    date_range_start TIMESTAMPTZ,
    date_range_end TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS farming_results (
    id SERIAL PRIMARY KEY,
    farming_run_id INT REFERENCES farming_runs(id) ON DELETE CASCADE,
    trigger_point NUMERIC(4,2),
    exit_point NUMERIC(4,2),
    trigger_minutes INT,
    min_coin_delta NUMERIC(4,2),
    market_type TEXT,
    total_markets INT,
    trades_taken INT,
    entry_rate NUMERIC(6,4),
    wins INT,
    losses INT,
    stop_losses INT,
    win_rate NUMERIC(6,4),
    total_pnl NUMERIC(10,2),
    roi NUMERIC(8,4),
    avg_pnl_per_trade NUMERIC(8,4),
    avg_entry_price NUMERIC(6,4),
    avg_coin_delta NUMERIC(6,4)
);
"""


# ---------------------------------------------------------------------------
# Backtest logic for a single market
# ---------------------------------------------------------------------------

def backtest_market(ticks_arr, started_at_ts, final_outcome,
                    trigger_point, exit_point, trigger_minutes, min_coin_delta):
    """
    Run the farming strategy on a single market.

    ticks_arr: numpy array with columns [elapsed_seconds, up_price]
    started_at_ts: timestamp of market start
    final_outcome: 'Up' or 'Down'

    Returns dict with trade details or None if no trade taken.
    """
    trigger_seconds = trigger_minutes * 60

    # Find opening price (tick closest to second 0)
    opening_idx = np.argmin(np.abs(ticks_arr[:, 0]))
    opening_price = ticks_arr[opening_idx, 1]

    # Find ticks after trigger_minute
    after_trigger = ticks_arr[ticks_arr[:, 0] >= trigger_seconds]
    if len(after_trigger) == 0:
        return None

    # Scan for trigger condition
    direction = None
    entry_price = None
    trigger_tick_idx = None

    for i in range(len(after_trigger)):
        up_price = after_trigger[i, 1]
        if up_price >= trigger_point:
            direction = 'Up'
            entry_price = up_price
            trigger_tick_idx = i
            break
        elif up_price <= (1 - trigger_point):
            direction = 'Down'
            entry_price = 1 - up_price
            trigger_tick_idx = i
            break

    if direction is None:
        return None

    # Delta gate
    coin_delta = abs(entry_price - opening_price)
    if coin_delta < min_coin_delta:
        return None

    # Monitor remaining ticks after entry for stop-loss
    remaining = after_trigger[trigger_tick_idx + 1:]
    outcome = None
    exit_price = None

    for i in range(len(remaining)):
        up_price = remaining[i, 1]
        if direction == 'Up' and up_price <= exit_point:
            outcome = 'stop_loss'
            exit_price = exit_point
            break
        elif direction == 'Down' and up_price >= (1 - exit_point):
            outcome = 'stop_loss'
            exit_price = exit_point
            break

    # Resolution exit if no stop-loss
    if outcome is None:
        if (direction == 'Up' and final_outcome == 'Up') or \
           (direction == 'Down' and final_outcome == 'Down'):
            outcome = 'win'
        else:
            outcome = 'loss'

    # PnL calculation
    fee = 0.02 * BET_SIZE

    if outcome == 'stop_loss':
        pnl = -(entry_price - exit_point) * BET_SIZE - fee
    elif outcome == 'win':
        pnl = (1.0 - entry_price) * BET_SIZE - fee
    else:  # loss
        pnl = -entry_price * BET_SIZE - fee

    return {
        'direction': direction,
        'entry_price': entry_price,
        'coin_delta': coin_delta,
        'outcome': outcome,
        'pnl': pnl,
    }


# ---------------------------------------------------------------------------
# Main backtest runner
# ---------------------------------------------------------------------------

def run_farming_backtest(conn, run_id):
    """Run the full farming strategy backtest and write results to DB."""
    t_start = time.time()
    print()
    print('=' * 60)
    print('FARMING STRATEGY BACKTEST — STARTING')
    print('=' * 60)

    cursor = conn.cursor()

    # Create tables
    cursor.execute(FARMING_DDL)
    conn.commit()

    # Load all 15m resolved markets
    df_outcomes = pd.read_sql("""
        SELECT market_id, market_type, started_at, ended_at, final_outcome
        FROM market_outcomes
        WHERE resolved = TRUE
          AND final_outcome IN ('Up', 'Down')
          AND market_type LIKE '%%15m%%'
        ORDER BY started_at ASC
    """, conn, parse_dates=['started_at', 'ended_at'])

    if df_outcomes.empty:
        print('[Farming] No 15m resolved markets found. Skipping.')
        cursor.close()
        return

    market_ids = df_outcomes['market_id'].tolist()
    placeholders = ','.join(['%s'] * len(market_ids))

    df_ticks = pd.read_sql(f"""
        SELECT mt.market_id, mt.time, mt.up_price
        FROM market_ticks mt
        WHERE mt.market_id IN ({placeholders})
        ORDER BY mt.market_id, mt.time
    """, conn, params=market_ids, parse_dates=['time'])

    # Pre-compute tick arrays: {market_id: np.array([[elapsed_s, up_price], ...])}
    tick_arrays = {}
    started_at_map = dict(zip(df_outcomes['market_id'], df_outcomes['started_at']))

    for mid, grp in df_ticks.groupby('market_id'):
        sa = started_at_map.get(mid)
        if sa is None:
            continue
        elapsed = (grp['time'] - sa).dt.total_seconds().values
        up_prices = grp['up_price'].values.astype(float)
        tick_arrays[mid] = np.column_stack([elapsed, up_prices])

    # Build market info list
    market_types = sorted(df_outcomes['market_type'].dropna().unique())
    markets_by_type = {}
    for mt in market_types:
        subset = df_outcomes[df_outcomes['market_type'] == mt]
        markets_by_type[mt] = [
            (row['market_id'], row['final_outcome'])
            for _, row in subset.iterrows()
            if row['market_id'] in tick_arrays
        ]

    # 'all' combines everything
    all_markets = []
    for mt_markets in markets_by_type.values():
        all_markets.extend(mt_markets)
    markets_by_type['all'] = all_markets

    total_markets = len(all_markets)
    date_start = df_outcomes['started_at'].min()
    date_end = df_outcomes['ended_at'].max()

    print(f'[Farming] Loaded {total_markets} markets across {len(market_types)} types')
    print(f'[Farming] Date range: {date_start} -> {date_end}')

    # Create farming run record
    cursor.execute("""
        INSERT INTO farming_runs (run_id, markets_tested, total_combinations,
                                  date_range_start, date_range_end)
        VALUES (%s, %s, %s, %s, %s)
        RETURNING id
    """, (run_id, total_markets,
          len(TRIGGER_POINTS) * len(EXIT_POINTS) * len(TRIGGER_MINUTES) * len(MIN_COIN_DELTAS),
          date_start, date_end))
    farming_run_id = cursor.fetchone()[0]
    conn.commit()

    # Run backtest for all combinations
    type_keys = list(market_types) + ['all']
    all_result_rows = []
    combo_count = 0
    total_combos = len(TRIGGER_POINTS) * len(EXIT_POINTS) * len(TRIGGER_MINUTES) * len(MIN_COIN_DELTAS)

    for tp in TRIGGER_POINTS:
        for ep in EXIT_POINTS:
            for tm in TRIGGER_MINUTES:
                for mcd in MIN_COIN_DELTAS:
                    combo_count += 1
                    if combo_count % 50 == 0:
                        print(f'[Farming] Progress: {combo_count}/{total_combos} combinations...')

                    for mt_key in type_keys:
                        market_list = markets_by_type.get(mt_key, [])
                        if not market_list:
                            continue

                        total_m = len(market_list)
                        trades = 0
                        wins = 0
                        losses = 0
                        stop_losses = 0
                        total_pnl = 0.0
                        entry_prices = []
                        coin_deltas = []

                        for mid, final_outcome in market_list:
                            ticks_arr = tick_arrays.get(mid)
                            if ticks_arr is None:
                                continue

                            result = backtest_market(
                                ticks_arr, None, final_outcome,
                                tp, ep, tm, mcd
                            )
                            if result is None:
                                continue

                            trades += 1
                            total_pnl += result['pnl']
                            entry_prices.append(result['entry_price'])
                            coin_deltas.append(result['coin_delta'])

                            if result['outcome'] == 'win':
                                wins += 1
                            elif result['outcome'] == 'loss':
                                losses += 1
                            else:
                                stop_losses += 1

                        entry_rate = trades / total_m if total_m > 0 else 0
                        win_rate = wins / trades if trades > 0 else 0
                        roi = total_pnl / (trades * BET_SIZE) if trades > 0 else 0
                        avg_pnl = total_pnl / trades if trades > 0 else 0
                        avg_entry = float(np.mean(entry_prices)) if entry_prices else 0
                        avg_delta = float(np.mean(coin_deltas)) if coin_deltas else 0

                        all_result_rows.append((
                            farming_run_id,
                            float(tp), float(ep), int(tm), float(mcd),
                            mt_key,
                            int(total_m), int(trades),
                            float(round(entry_rate, 4)),
                            int(wins), int(losses), int(stop_losses),
                            float(round(win_rate, 4)),
                            float(round(total_pnl, 2)),
                            float(round(roi, 4)),
                            float(round(avg_pnl, 4)),
                            float(round(avg_entry, 4)),
                            float(round(avg_delta, 4)),
                        ))

    # Bulk insert results
    if all_result_rows:
        execute_values(cursor, """
            INSERT INTO farming_results
                (farming_run_id, trigger_point, exit_point, trigger_minutes,
                 min_coin_delta, market_type, total_markets, trades_taken,
                 entry_rate, wins, losses, stop_losses, win_rate,
                 total_pnl, roi, avg_pnl_per_trade, avg_entry_price, avg_coin_delta)
            VALUES %s
        """, all_result_rows)
        conn.commit()

    duration = time.time() - t_start

    # Build summary from results
    # Convert to DataFrame for easy querying
    cols = ['farming_run_id', 'trigger_point', 'exit_point', 'trigger_minutes',
            'min_coin_delta', 'market_type', 'total_markets', 'trades_taken',
            'entry_rate', 'wins', 'losses', 'stop_losses', 'win_rate',
            'total_pnl', 'roi', 'avg_pnl_per_trade', 'avg_entry_price', 'avg_coin_delta']
    df_results = pd.DataFrame(all_result_rows, columns=cols)

    print()
    print('=' * 60)
    print('FARMING STRATEGY BACKTEST — COMPLETE')
    print('=' * 60)
    print(f'Farming Run ID: {farming_run_id}')
    print(f'Markets tested: {total_markets}')
    print(f'Combinations: {total_combos}')
    print(f'Duration: {duration:.0f}s')

    # Top 5 by total PnL
    print()
    print('TOP 5 BY TOTAL PNL:')
    top_pnl = df_results.nlargest(5, 'total_pnl')
    for _, r in top_pnl.iterrows():
        print(f'  trigger={r["trigger_point"]:.2f}, exit={r["exit_point"]:.2f}, '
              f'minute={int(r["trigger_minutes"])}, delta={r["min_coin_delta"]:.2f}, '
              f'type={r["market_type"]}')
        wr_pct = r["win_rate"] * 100
        print(f'  trades={int(r["trades_taken"])}, win_rate={wr_pct:.1f}%, '
              f'total_pnl=${r["total_pnl"]:.2f}, roi={r["roi"] * 100:.1f}%')
        print()

    # Top 5 by win rate (min 20 trades)
    print('TOP 5 BY WIN RATE (min 20 trades):')
    qualified = df_results[df_results['trades_taken'] >= 20]
    if not qualified.empty:
        top_wr = qualified.nlargest(5, 'win_rate')
        for _, r in top_wr.iterrows():
            print(f'  trigger={r["trigger_point"]:.2f}, exit={r["exit_point"]:.2f}, '
                  f'minute={int(r["trigger_minutes"])}, delta={r["min_coin_delta"]:.2f}, '
                  f'type={r["market_type"]}')
            wr_pct = r["win_rate"] * 100
            print(f'  trades={int(r["trades_taken"])}, win_rate={wr_pct:.1f}%, '
                  f'total_pnl=${r["total_pnl"]:.2f}, roi={r["roi"] * 100:.1f}%')
            print()
    else:
        print('  No combinations with >= 20 trades')
        print()

    # Best configuration overall (highest PnL with min 20 trades)
    print('BEST CONFIGURATION:')
    if not qualified.empty:
        best = qualified.loc[qualified['total_pnl'].idxmax()]
        print(f'  Trigger Point:   {best["trigger_point"]:.2f}')
        print(f'  Exit Point:      {best["exit_point"]:.2f}')
        print(f'  Trigger Minutes: {int(best["trigger_minutes"])}')
        print(f'  Min Coin Delta:  {best["min_coin_delta"]:.2f}')
        print(f'  Market Type:     {best["market_type"]}')
        print(f'  Total Markets:   {int(best["total_markets"])}')
        print(f'  Trades Taken:    {int(best["trades_taken"])}')
        print(f'  Entry Rate:      {best["entry_rate"] * 100:.1f}%')
        print(f'  Wins:            {int(best["wins"])}')
        print(f'  Losses:          {int(best["losses"])}')
        print(f'  Stop Losses:     {int(best["stop_losses"])}')
        print(f'  Win Rate:        {best["win_rate"] * 100:.1f}%')
        print(f'  Total PnL:       ${best["total_pnl"]:.2f}')
        print(f'  ROI:             {best["roi"] * 100:.1f}%')
        print(f'  Avg PnL/Trade:   ${best["avg_pnl_per_trade"]:.4f}')
        print(f'  Avg Entry Price: {best["avg_entry_price"]:.4f}')
        print(f'  Avg Coin Delta:  {best["avg_coin_delta"]:.4f}')
    else:
        print('  No qualified configurations (need >= 20 trades)')

    print('=' * 60)
    cursor.close()
