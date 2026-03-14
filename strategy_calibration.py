#!/usr/bin/env python3
"""
PolyEdge Lab — Calibration Strategy Backtest

Backtests a calibration-based entry strategy against all historical
5-minute crypto market data and writes results to the database.

Strategy: Exploit systematic miscalibration found in calibration analysis.
Enter early when the price is in a historically mispriced bucket.

Called from run_analysis.py after the farming backtest completes.
"""

import os
import time

import numpy as np
import pandas as pd
from psycopg2.extras import execute_values

# ---------------------------------------------------------------------------
# Parameter grid
# ---------------------------------------------------------------------------

MAX_ENTRY_SECONDS = [30, 60, 120, 180]
ENTRY_PRICE_RANGES = [
    (0.48, 0.58),   # near 50/50 zone
    (0.45, 0.55),   # slightly wider
    (0.50, 0.60),   # slight Up bias zone
    (0.40, 0.60),   # wide zone
]
MIN_DEVIATIONS = [0.05, 0.08, 0.10, 0.15]
BET_SIZE = 10

# ---------------------------------------------------------------------------
# DDL
# ---------------------------------------------------------------------------

CALIBRATION_STRATEGY_DDL = """
CREATE TABLE IF NOT EXISTS calibration_strategy_runs (
    id SERIAL PRIMARY KEY,
    run_id INT REFERENCES analysis_runs(id) ON DELETE CASCADE,
    ran_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    markets_tested INT,
    total_combinations INT,
    date_range_start TIMESTAMPTZ,
    date_range_end TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS calibration_strategy_results (
    id SERIAL PRIMARY KEY,
    strategy_run_id INT REFERENCES calibration_strategy_runs(id) ON DELETE CASCADE,
    max_entry_seconds INT,
    entry_price_low NUMERIC(4,2),
    entry_price_high NUMERIC(4,2),
    min_deviation NUMERIC(4,2),
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
# Calibration map loader
# ---------------------------------------------------------------------------

def load_calibration_map(conn):
    """
    Query the latest calibration results to build a lookup table
    of which price buckets favor Up vs Down.

    Returns dict: (market_type, bucket) -> deviation
    """
    cursor = conn.cursor()
    cursor.execute("""
        SELECT market_type, price_bucket, actual_win_rate, expected_win_rate, deviation
        FROM calibration_results
        WHERE run_id = (SELECT MAX(id) FROM analysis_runs)
          AND checkpoint_seconds = 60
          AND sample_count >= 5
    """)
    rows = cursor.fetchall()
    cursor.close()

    calibration_map = {}
    for market_type, price_bucket, actual_win_rate, expected_win_rate, deviation in rows:
        bucket = float(round(float(price_bucket) * 20) / 20)  # round to nearest 0.05
        calibration_map[(market_type, bucket)] = float(deviation)

    return calibration_map


# ---------------------------------------------------------------------------
# Backtest logic for a single market
# ---------------------------------------------------------------------------

def backtest_market(ticks_arr, final_outcome, market_type,
                    calibration_map, max_entry_seconds,
                    entry_low, entry_high, min_deviation):
    """
    Run the calibration strategy on a single market.

    ticks_arr: numpy array with columns [elapsed_seconds, up_price]
    final_outcome: 'Up' or 'Down'
    market_type: e.g. 'btc_5m'
    calibration_map: dict (market_type, bucket) -> deviation

    Returns dict with trade details or None if no trade taken.
    """
    # Only look at ticks within the entry window
    entry_window = ticks_arr[ticks_arr[:, 0] <= max_entry_seconds]
    if len(entry_window) == 0:
        return None

    for i in range(len(entry_window)):
        up_price = entry_window[i, 1]

        # Check if up_price falls within target range
        if up_price < entry_low or up_price > entry_high:
            continue

        # Round to nearest 0.05 to match calibration bucket
        bucket = round(round(up_price / 0.05) * 0.05, 2)

        # Look up calibration deviation for this market type
        deviation = calibration_map.get((market_type, bucket))
        if deviation is None:
            # Also try 'all' market type
            deviation = calibration_map.get(('all', bucket))
        if deviation is None:
            continue

        # Determine direction based on calibration deviation
        direction = None
        entry_price = None

        if deviation < -min_deviation:
            # Bucket historically overprices Up (actual < implied) -> enter Down
            direction = 'Down'
            entry_price = 1 - up_price
        elif deviation > min_deviation:
            # Bucket historically underprices Up (actual > implied) -> enter Up
            direction = 'Up'
            entry_price = up_price
        else:
            # Deviation not large enough, skip
            continue

        # First valid trigger — take the trade and hold to resolution
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

    return None


# ---------------------------------------------------------------------------
# Main backtest runner
# ---------------------------------------------------------------------------

def run_calibration_backtest(conn, run_id):
    """Run the full calibration strategy backtest and write results to DB."""
    t_start = time.time()
    print()
    print('=' * 60)
    print('CALIBRATION STRATEGY BACKTEST \u2014 STARTING')
    print('=' * 60)

    cursor = conn.cursor()

    # Create tables
    cursor.execute(CALIBRATION_STRATEGY_DDL)
    conn.commit()

    # Load calibration map
    calibration_map = load_calibration_map(conn)
    if not calibration_map:
        print('[Calibration Strategy] No calibration data found. Skipping.')
        cursor.close()
        return

    print(f'[Calibration Strategy] Loaded {len(calibration_map)} calibration buckets')

    # Load all 5m resolved markets
    df_outcomes = pd.read_sql("""
        SELECT market_id, market_type, started_at, ended_at, final_outcome
        FROM market_outcomes
        WHERE resolved = TRUE
          AND final_outcome IN ('Up', 'Down')
          AND market_type LIKE '%%5m%%'
        ORDER BY started_at ASC
    """, conn, parse_dates=['started_at', 'ended_at'])

    if df_outcomes.empty:
        print('[Calibration Strategy] No 5m resolved markets found. Skipping.')
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
            (row['market_id'], row['market_type'], row['final_outcome'])
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

    print(f'[Calibration Strategy] Loaded {total_markets} markets across {len(market_types)} types')
    print(f'[Calibration Strategy] Date range: {date_start} -> {date_end}')

    # Create strategy run record
    total_combos = len(MAX_ENTRY_SECONDS) * len(ENTRY_PRICE_RANGES) * len(MIN_DEVIATIONS)
    cursor.execute("""
        INSERT INTO calibration_strategy_runs
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

    for mes in MAX_ENTRY_SECONDS:
        for (entry_low, entry_high) in ENTRY_PRICE_RANGES:
            for min_dev in MIN_DEVIATIONS:
                combo_count += 1
                if combo_count % 16 == 0:
                    print(f'[Calibration Strategy] Progress: {combo_count}/{total_combos} combinations...')

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

                    for mid, market_type, final_outcome in market_list:
                        ticks_arr = tick_arrays.get(mid)
                        if ticks_arr is None:
                            continue

                        result = backtest_market(
                            ticks_arr, final_outcome, market_type,
                            calibration_map, mes,
                            entry_low, entry_high, min_dev
                        )
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
                        int(mes),
                        float(entry_low), float(entry_high),
                        float(min_dev),
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
            INSERT INTO calibration_strategy_results
                (strategy_run_id, max_entry_seconds,
                 entry_price_low, entry_price_high, min_deviation,
                 market_type, total_markets, trades_taken,
                 entry_rate, wins, losses, win_rate,
                 total_pnl, roi, avg_pnl_per_trade, avg_entry_price,
                 up_trades, down_trades)
            VALUES %s
        """, all_result_rows)
        conn.commit()

    duration = time.time() - t_start

    # Build summary from results
    cols = ['strategy_run_id', 'max_entry_seconds',
            'entry_price_low', 'entry_price_high', 'min_deviation',
            'market_type', 'total_markets', 'trades_taken',
            'entry_rate', 'wins', 'losses', 'win_rate',
            'total_pnl', 'roi', 'avg_pnl_per_trade', 'avg_entry_price',
            'up_trades', 'down_trades']
    df_results = pd.DataFrame(all_result_rows, columns=cols)

    print()
    print('=' * 60)
    print('CALIBRATION STRATEGY BACKTEST \u2014 COMPLETE')
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
        print(f'  entry_seconds={int(r["max_entry_seconds"])}, '
              f'range={r["entry_price_low"]:.2f}-{r["entry_price_high"]:.2f}, '
              f'min_dev={r["min_deviation"]:.2f}, type={r["market_type"]}')
        wr_pct = r["win_rate"] * 100
        print(f'  trades={int(r["trades_taken"])}, win_rate={wr_pct:.1f}%, '
              f'total_pnl=${r["total_pnl"]:.2f}, roi={r["roi"] * 100:.1f}%')
        print()

    # Best configuration (highest PnL with min 10 trades)
    print('BEST CONFIGURATION:')
    qualified = df_results[df_results['trades_taken'] >= 10]
    if not qualified.empty:
        best = qualified.loc[qualified['total_pnl'].idxmax()]
        print(f'  Max Entry Seconds: {int(best["max_entry_seconds"])}')
        print(f'  Entry Price Range: {best["entry_price_low"]:.2f} - {best["entry_price_high"]:.2f}')
        print(f'  Min Deviation:     {best["min_deviation"]:.2f}')
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
        print('  No qualified configurations (need >= 10 trades)')

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
        run_calibration_backtest(conn, run_id)

    conn.close()
