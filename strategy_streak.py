#!/usr/bin/env python3
"""
PolyEdge Lab — Streak Mean-Reversion Strategy Backtest

Backtests a mean-reversion strategy that fades consecutive streaks.
After N consecutive Up or Down outcomes in the same market type,
bet the opposite direction on the next window.

Called from run_analysis.py after the calibration backtest completes.
"""

import os
import time

import numpy as np
import pandas as pd
from psycopg2.extras import execute_values

# ---------------------------------------------------------------------------
# Parameter grid
# ---------------------------------------------------------------------------

STREAK_LENGTHS = [2, 3, 4, 5]              # N consecutive same outcomes required
STREAK_DIRECTIONS = ['Up', 'Down', 'both']  # which streak type to fade
BET_SIZE = 10

# ---------------------------------------------------------------------------
# DDL
# ---------------------------------------------------------------------------

STREAK_STRATEGY_DDL = """
CREATE TABLE IF NOT EXISTS streak_strategy_runs (
    id SERIAL PRIMARY KEY,
    run_id INT REFERENCES analysis_runs(id) ON DELETE CASCADE,
    ran_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    markets_tested INT,
    total_combinations INT,
    date_range_start TIMESTAMPTZ,
    date_range_end TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS streak_strategy_results (
    id SERIAL PRIMARY KEY,
    strategy_run_id INT REFERENCES streak_strategy_runs(id) ON DELETE CASCADE,
    streak_length INT,
    streak_direction TEXT,
    market_type TEXT,
    total_markets INT,
    trades_taken INT,
    entry_rate NUMERIC(6,4),
    wins INT,
    losses INT,
    win_rate NUMERIC(6,4),
    total_pnl NUMERIC(10,2),
    roi NUMERIC(8,4),
    avg_pnl_per_trade NUMERIC(8,4),
    avg_entry_price NUMERIC(6,4),
    up_trades INT,
    down_trades INT
);
"""


# ---------------------------------------------------------------------------
# Backtest logic for a single market in sequence context
# ---------------------------------------------------------------------------

def evaluate_trade(ticks_arr, final_outcome, direction):
    """
    Evaluate a trade on a single market.

    ticks_arr: numpy array with columns [elapsed_seconds, up_price]
    final_outcome: 'Up' or 'Down'
    direction: 'Up' or 'Down' — the direction we are betting

    Returns dict with trade details or None if no entry tick available.
    """
    # Entry at opening price (first tick within 30 seconds)
    entry_window = ticks_arr[ticks_arr[:, 0] <= 30]
    if len(entry_window) == 0:
        return None

    up_price = entry_window[0, 1]

    if direction == 'Up':
        entry_price = up_price
    else:
        entry_price = 1 - up_price

    # Determine outcome
    if (direction == 'Up' and final_outcome == 'Up') or \
       (direction == 'Down' and final_outcome == 'Down'):
        outcome = 'win'
    else:
        outcome = 'loss'

    # PnL calculation
    fee = 0.02 * BET_SIZE

    if outcome == 'win':
        pnl = (1.0 - entry_price) * BET_SIZE - fee
    else:  # loss
        pnl = -entry_price * BET_SIZE - fee

    return {
        'direction': direction,
        'entry_price': entry_price,
        'outcome': outcome,
        'pnl': pnl,
    }


# ---------------------------------------------------------------------------
# Main backtest runner
# ---------------------------------------------------------------------------

def run_streak_backtest(conn, run_id):
    """Run the full streak mean-reversion strategy backtest and write results to DB."""
    t_start = time.time()
    print()
    print('=' * 60)
    print('STREAK STRATEGY BACKTEST \u2014 STARTING')
    print('=' * 60)

    cursor = conn.cursor()

    # Create tables
    cursor.execute(STREAK_STRATEGY_DDL)
    conn.commit()

    # Load all resolved markets
    df_outcomes = pd.read_sql("""
        SELECT market_id, market_type, started_at, ended_at, final_outcome
        FROM market_outcomes
        WHERE resolved = TRUE
          AND final_outcome IN ('Up', 'Down')
        ORDER BY started_at ASC
    """, conn, parse_dates=['started_at', 'ended_at'])

    if df_outcomes.empty:
        print('[Streak] No resolved markets found. Skipping.')
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

    # Build ordered market lists per type (sorted by ended_at for streak detection)
    market_types = sorted(df_outcomes['market_type'].dropna().unique())
    markets_by_type = {}
    for mt in market_types:
        subset = df_outcomes[df_outcomes['market_type'] == mt].sort_values('ended_at')
        markets_by_type[mt] = [
            (row['market_id'], row['market_type'], row['final_outcome'])
            for _, row in subset.iterrows()
            if row['market_id'] in tick_arrays
        ]

    # 'all' combines everything sorted by ended_at
    all_sorted = df_outcomes[df_outcomes['market_id'].isin(tick_arrays)].sort_values('ended_at')
    markets_by_type['all'] = [
        (row['market_id'], row['market_type'], row['final_outcome'])
        for _, row in all_sorted.iterrows()
    ]

    total_markets = len(tick_arrays)
    date_start = df_outcomes['started_at'].min()
    date_end = df_outcomes['ended_at'].max()

    print(f'[Streak] Loaded {total_markets} markets across {len(market_types)} types')
    print(f'[Streak] Date range: {date_start} -> {date_end}')

    # Create strategy run record
    total_combos = len(STREAK_LENGTHS) * len(STREAK_DIRECTIONS)
    cursor.execute("""
        INSERT INTO streak_strategy_runs
            (run_id, markets_tested, total_combinations,
             date_range_start, date_range_end)
        VALUES (%s, %s, %s, %s, %s)
        RETURNING id
    """, (run_id, total_markets, total_combos, date_start, date_end))
    strategy_run_id = cursor.fetchone()[0]
    conn.commit()

    # Run backtest for all combinations
    type_keys = list(market_types) + ['all']
    all_result_rows = []
    combo_count = 0

    for streak_len in STREAK_LENGTHS:
        for streak_dir in STREAK_DIRECTIONS:
            combo_count += 1
            print(f'[Streak] Progress: {combo_count}/{total_combos} combinations...')

            for mt_key in type_keys:
                market_list = markets_by_type.get(mt_key, [])
                if not market_list:
                    continue

                total_m = len(market_list)
                trades = 0
                wins = 0
                losses = 0
                total_pnl = 0.0
                entry_prices = []
                up_trades = 0
                down_trades = 0

                # Walk through markets in order, tracking outcomes
                outcomes_history = []

                for mid, market_type, final_outcome in market_list:
                    # Check if we have a streak to fade
                    trade_direction = None

                    if len(outcomes_history) >= streak_len:
                        last_n = outcomes_history[-streak_len:]

                        if all(o == 'Up' for o in last_n):
                            # Streak of Ups — fade if direction allows
                            if streak_dir in ('Up', 'both'):
                                trade_direction = 'Down'

                        elif all(o == 'Down' for o in last_n):
                            # Streak of Downs — fade if direction allows
                            if streak_dir in ('Down', 'both'):
                                trade_direction = 'Up'

                    # Record outcome for history before potentially trading
                    outcomes_history.append(final_outcome)

                    if trade_direction is None:
                        continue

                    ticks_arr = tick_arrays.get(mid)
                    if ticks_arr is None:
                        continue

                    result = evaluate_trade(ticks_arr, final_outcome, trade_direction)
                    if result is None:
                        continue

                    trades += 1
                    total_pnl += result['pnl']
                    entry_prices.append(result['entry_price'])

                    if result['direction'] == 'Up':
                        up_trades += 1
                    else:
                        down_trades += 1

                    if result['outcome'] == 'win':
                        wins += 1
                    else:
                        losses += 1

                entry_rate = trades / total_m if total_m > 0 else 0
                win_rate = wins / trades if trades > 0 else 0
                roi = total_pnl / (trades * BET_SIZE) if trades > 0 else 0
                avg_pnl = total_pnl / trades if trades > 0 else 0
                avg_entry = float(np.mean(entry_prices)) if entry_prices else 0

                all_result_rows.append((
                    strategy_run_id,
                    int(streak_len),
                    streak_dir,
                    mt_key,
                    int(total_m), int(trades),
                    float(round(entry_rate, 4)),
                    int(wins), int(losses),
                    float(round(win_rate, 4)),
                    float(round(total_pnl, 2)),
                    float(round(roi, 4)),
                    float(round(avg_pnl, 4)),
                    float(round(avg_entry, 4)),
                    int(up_trades), int(down_trades),
                ))

    # Bulk insert results
    if all_result_rows:
        execute_values(cursor, """
            INSERT INTO streak_strategy_results
                (strategy_run_id, streak_length, streak_direction,
                 market_type, total_markets, trades_taken,
                 entry_rate, wins, losses, win_rate,
                 total_pnl, roi, avg_pnl_per_trade, avg_entry_price,
                 up_trades, down_trades)
            VALUES %s
        """, all_result_rows)
        conn.commit()

    duration = time.time() - t_start

    # Build summary from results
    cols = ['strategy_run_id', 'streak_length', 'streak_direction',
            'market_type', 'total_markets', 'trades_taken',
            'entry_rate', 'wins', 'losses', 'win_rate',
            'total_pnl', 'roi', 'avg_pnl_per_trade', 'avg_entry_price',
            'up_trades', 'down_trades']
    df_results = pd.DataFrame(all_result_rows, columns=cols)

    print()
    print('=' * 60)
    print('STREAK STRATEGY BACKTEST \u2014 COMPLETE')
    print('=' * 60)
    print(f'Strategy Run ID: {strategy_run_id}')
    print(f'Markets tested: {total_markets}')
    print(f'Combinations: {total_combos}')
    print(f'Duration: {duration:.0f}s')

    # Top 5 by total PnL
    print()
    print('TOP 5 BY TOTAL PNL:')
    top_pnl = df_results.nlargest(5, 'total_pnl')
    for _, r in top_pnl.iterrows():
        print(f'  streak_len={int(r["streak_length"])}, '
              f'direction={r["streak_direction"]}, '
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
            print(f'  streak_len={int(r["streak_length"])}, '
                  f'direction={r["streak_direction"]}, '
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
        print(f'  Streak Length:     {int(best["streak_length"])}')
        print(f'  Streak Direction:  {best["streak_direction"]}')
        print(f'  Market Type:       {best["market_type"]}')
        print(f'  Total Markets:     {int(best["total_markets"])}')
        print(f'  Trades Taken:      {int(best["trades_taken"])}')
        print(f'  Entry Rate:        {best["entry_rate"] * 100:.1f}%')
        print(f'  Wins:              {int(best["wins"])}')
        print(f'  Losses:            {int(best["losses"])}')
        print(f'  Win Rate:          {best["win_rate"] * 100:.1f}%')
        print(f'  Total PnL:         ${best["total_pnl"]:.2f}')
        print(f'  ROI:               {best["roi"] * 100:.1f}%')
        print(f'  Avg PnL/Trade:     ${best["avg_pnl_per_trade"]:.4f}')
        print(f'  Avg Entry Price:   {best["avg_entry_price"]:.4f}')
        print(f'  Up Trades:         {int(best["up_trades"])}')
        print(f'  Down Trades:       {int(best["down_trades"])}')
    else:
        print('  No qualified configurations (need >= 20 trades)')

    print('=' * 60)
    cursor.close()


# ---------------------------------------------------------------------------
# Standalone entry point
# ---------------------------------------------------------------------------

if __name__ == '__main__':
    import psycopg2
    from dotenv import load_dotenv
    load_dotenv()

    DB_HOST = os.getenv('POSTGRES_HOST', 'localhost')
    DB_PORT = os.getenv('POSTGRES_PORT', '5432')
    DB_USER = os.getenv('POSTGRES_USER', 'polymarket')
    DB_PASSWORD = os.getenv('POSTGRES_PASSWORD', '')
    DB_NAME = os.getenv('POSTGRES_DB', 'polymarket_tracker')

    conn = psycopg2.connect(
        host=DB_HOST, port=DB_PORT,
        user=DB_USER, password=DB_PASSWORD,
        dbname=DB_NAME,
    )
    conn.autocommit = False

    # Get latest run_id
    cur = conn.cursor()
    cur.execute("SELECT MAX(id) FROM analysis_runs")
    run_id = cur.fetchone()[0]
    cur.close()

    if run_id is None:
        print('No analysis runs found. Run run_analysis.py first.')
    else:
        print(f'Using latest analysis run_id: {run_id}')
        run_streak_backtest(conn, run_id)

    conn.close()
