#!/usr/bin/env python3
"""
PolyEdge Lab — Momentum Strategy Backtest

Backtests a momentum-based entry strategy against all historical
crypto market data and writes results to the database.

Strategy: Enter a position based on price momentum detected between
30s and 60s into the market window. If price is rising, enter Up.
If falling, enter Down. Hold to resolution with optional stop-loss.

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

MIN_MOMENTUM_THRESHOLDS = [0.01, 0.02, 0.03, 0.05, 0.08, 0.10]
EXIT_POINTS = [0.30, 0.40, 0.50]
USE_STOP_LOSS = [True, False]
BET_SIZE = 10

# ---------------------------------------------------------------------------
# DDL
# ---------------------------------------------------------------------------

MOMENTUM_DDL = """
CREATE TABLE IF NOT EXISTS momentum_strategy_runs (
    id SERIAL PRIMARY KEY,
    run_id INT REFERENCES analysis_runs(id) ON DELETE CASCADE,
    ran_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    markets_tested INT,
    total_combinations INT,
    date_range_start TIMESTAMPTZ,
    date_range_end TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS momentum_strategy_results (
    id SERIAL PRIMARY KEY,
    strategy_run_id INT REFERENCES momentum_strategy_runs(id) ON DELETE CASCADE,
    min_momentum NUMERIC(4,2),
    exit_point NUMERIC(4,2),
    use_stop_loss BOOLEAN,
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
    avg_momentum NUMERIC(6,4),
    up_trades INT,
    down_trades INT
);
"""


# ---------------------------------------------------------------------------
# Backtest logic for a single market
# ---------------------------------------------------------------------------

def backtest_market(ticks_arr, final_outcome,
                    min_momentum, exit_point, use_stop_loss):
    """
    Run the momentum strategy on a single market.

    ticks_arr: numpy array with columns [elapsed_seconds, up_price]
    final_outcome: 'Up' or 'Down'

    Returns dict with trade details or None if no trade taken.
    """
    elapsed = ticks_arr[:, 0]
    up_prices = ticks_arr[:, 1]

    # Find tick closest to 30s (within ±10s)
    diffs_30 = np.abs(elapsed - 30.0)
    idx_30 = np.argmin(diffs_30)
    if diffs_30[idx_30] > 10:
        return None
    price_30s = up_prices[idx_30]

    # Find tick closest to 60s (within ±10s)
    diffs_60 = np.abs(elapsed - 60.0)
    idx_60 = np.argmin(diffs_60)
    if diffs_60[idx_60] > 10:
        return None
    price_60s = up_prices[idx_60]

    # Calculate momentum
    momentum = price_60s - price_30s

    # Determine direction
    direction = None
    entry_price = None

    if momentum >= min_momentum:
        direction = 'Up'
        entry_price = price_60s
    elif momentum <= -min_momentum:
        direction = 'Down'
        entry_price = 1 - price_60s
    else:
        return None

    # Monitor remaining ticks after entry for stop-loss
    remaining = ticks_arr[ticks_arr[:, 0] > elapsed[idx_60]]
    outcome = None
    exit_price = None

    if use_stop_loss:
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
        pnl = -(entry_price - exit_price) * BET_SIZE - fee
    elif outcome == 'win':
        pnl = (1.0 - entry_price) * BET_SIZE - fee
    else:  # loss
        pnl = -entry_price * BET_SIZE - fee

    return {
        'direction': direction,
        'entry_price': entry_price,
        'momentum': abs(momentum),
        'outcome': outcome,
        'pnl': pnl,
    }


# ---------------------------------------------------------------------------
# Main backtest runner
# ---------------------------------------------------------------------------

def run_momentum_backtest(conn, run_id):
    """Run the full momentum strategy backtest and write results to DB."""
    t_start = time.time()
    print()
    print('=' * 60)
    print('MOMENTUM STRATEGY BACKTEST \u2014 STARTING')
    print('=' * 60)

    cursor = conn.cursor()

    # Create tables
    cursor.execute(MOMENTUM_DDL)
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
        print('[Momentum] No resolved markets found. Skipping.')
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

    print(f'[Momentum] Loaded {total_markets} markets across {len(market_types)} types')
    print(f'[Momentum] Date range: {date_start} -> {date_end}')

    # Create strategy run record
    total_combos = len(MIN_MOMENTUM_THRESHOLDS) * len(EXIT_POINTS) * len(USE_STOP_LOSS)
    cursor.execute("""
        INSERT INTO momentum_strategy_runs
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

    for mm in MIN_MOMENTUM_THRESHOLDS:
        for ep in EXIT_POINTS:
            for usl in USE_STOP_LOSS:
                combo_count += 1
                if combo_count % 12 == 0:
                    print(f'[Momentum] Progress: {combo_count}/{total_combos} combinations...')

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
                    momentums = []
                    up_trades = 0
                    down_trades = 0

                    for mid, final_outcome in market_list:
                        ticks_arr = tick_arrays.get(mid)
                        if ticks_arr is None:
                            continue

                        result = backtest_market(
                            ticks_arr, final_outcome,
                            mm, ep, usl
                        )
                        if result is None:
                            continue

                        trades += 1
                        total_pnl += result['pnl']
                        entry_prices.append(result['entry_price'])
                        momentums.append(result['momentum'])

                        if result['direction'] == 'Up':
                            up_trades += 1
                        else:
                            down_trades += 1

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
                    avg_mom = float(np.mean(momentums)) if momentums else 0

                    all_result_rows.append((
                        strategy_run_id,
                        float(mm), float(ep), bool(usl),
                        mt_key,
                        int(total_m), int(trades),
                        float(round(entry_rate, 4)),
                        int(wins), int(losses), int(stop_losses),
                        float(round(win_rate, 4)),
                        float(round(total_pnl, 2)),
                        float(round(roi, 4)),
                        float(round(avg_pnl, 4)),
                        float(round(avg_entry, 4)),
                        float(round(avg_mom, 4)),
                        int(up_trades), int(down_trades),
                    ))

    # Bulk insert results
    if all_result_rows:
        execute_values(cursor, """
            INSERT INTO momentum_strategy_results
                (strategy_run_id, min_momentum, exit_point, use_stop_loss,
                 market_type, total_markets, trades_taken,
                 entry_rate, wins, losses, stop_losses, win_rate,
                 total_pnl, roi, avg_pnl_per_trade, avg_entry_price,
                 avg_momentum, up_trades, down_trades)
            VALUES %s
        """, all_result_rows)
        conn.commit()

    duration = time.time() - t_start

    # Build summary from results
    cols = ['strategy_run_id', 'min_momentum', 'exit_point', 'use_stop_loss',
            'market_type', 'total_markets', 'trades_taken',
            'entry_rate', 'wins', 'losses', 'stop_losses', 'win_rate',
            'total_pnl', 'roi', 'avg_pnl_per_trade', 'avg_entry_price',
            'avg_momentum', 'up_trades', 'down_trades']
    df_results = pd.DataFrame(all_result_rows, columns=cols)

    print()
    print('=' * 60)
    print('MOMENTUM STRATEGY BACKTEST \u2014 COMPLETE')
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
        print(f'  momentum={r["min_momentum"]:.2f}, exit={r["exit_point"]:.2f}, '
              f'stop_loss={r["use_stop_loss"]}, type={r["market_type"]}')
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
            print(f'  momentum={r["min_momentum"]:.2f}, exit={r["exit_point"]:.2f}, '
                  f'stop_loss={r["use_stop_loss"]}, type={r["market_type"]}')
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
        print(f'  Min Momentum:    {best["min_momentum"]:.2f}')
        print(f'  Exit Point:      {best["exit_point"]:.2f}')
        print(f'  Use Stop Loss:   {best["use_stop_loss"]}')
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
        print(f'  Avg Momentum:    {best["avg_momentum"]:.4f}')
        print(f'  Up Trades:       {int(best["up_trades"])}')
        print(f'  Down Trades:     {int(best["down_trades"])}')
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
        run_momentum_backtest(conn, run_id)

    conn.close()
