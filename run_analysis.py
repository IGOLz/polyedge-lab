#!/usr/bin/env python3
"""
PolyEdge Lab — Full Statistical Analysis of Polymarket Data

Reads from market_ticks and market_outcomes (read-only),
writes results to dedicated analysis tables.

Usage:
    python run_analysis.py
    python run_analysis.py --dry-run
    python run_analysis.py --market-type btc_5m
"""

import argparse
import os
import sys
import time
from datetime import timedelta

from dotenv import load_dotenv
load_dotenv()

import numpy as np
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from scipy.stats import binomtest, ttest_ind

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

DB_HOST = os.getenv('POSTGRES_HOST', 'localhost')
DB_PORT = os.getenv('POSTGRES_PORT', '5432')
DB_USER = os.getenv('POSTGRES_USER', 'polymarket')
DB_PASSWORD = os.getenv('POSTGRES_PASSWORD', '')
DB_NAME = os.getenv('POSTGRES_DB', 'polymarket_tracker')

CHECKPOINTS = [30, 60, 120, 180, 240, 300]
PRICE_BUCKET_WIDTH = 0.05
MIN_BUCKET_SAMPLES = 5
MIN_TICKS_PER_MARKET = 10
SIGNIFICANCE_LEVEL = 0.15

# ---------------------------------------------------------------------------
# DDL — analysis tables
# ---------------------------------------------------------------------------

DDL = """
CREATE TABLE IF NOT EXISTS analysis_runs (
    id SERIAL PRIMARY KEY,
    ran_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    markets_analyzed INT,
    ticks_analyzed BIGINT,
    date_range_start TIMESTAMPTZ,
    date_range_end TIMESTAMPTZ,
    notes TEXT
);

CREATE TABLE IF NOT EXISTS calibration_results (
    id SERIAL PRIMARY KEY,
    run_id INT REFERENCES analysis_runs(id) ON DELETE CASCADE,
    market_type TEXT NOT NULL,
    checkpoint_seconds INT NOT NULL,
    price_bucket NUMERIC(4,2) NOT NULL,
    sample_count INT NOT NULL,
    up_wins INT NOT NULL,
    actual_win_rate NUMERIC(6,4) NOT NULL,
    expected_win_rate NUMERIC(6,4) NOT NULL,
    deviation NUMERIC(6,4) NOT NULL,
    p_value NUMERIC(8,6),
    significant BOOLEAN NOT NULL DEFAULT FALSE
);

CREATE TABLE IF NOT EXISTS trajectory_results (
    id SERIAL PRIMARY KEY,
    run_id INT REFERENCES analysis_runs(id) ON DELETE CASCADE,
    market_type TEXT NOT NULL,
    checkpoint_seconds INT NOT NULL,
    outcome TEXT NOT NULL,
    avg_price NUMERIC(6,4) NOT NULL,
    std_price NUMERIC(6,4),
    sample_count INT NOT NULL
);

CREATE TABLE IF NOT EXISTS timeofday_results (
    id SERIAL PRIMARY KEY,
    run_id INT REFERENCES analysis_runs(id) ON DELETE CASCADE,
    market_type TEXT NOT NULL,
    hour_utc INT NOT NULL,
    sample_count INT NOT NULL,
    up_wins INT NOT NULL,
    up_win_rate NUMERIC(6,4) NOT NULL,
    avg_price_range NUMERIC(6,4)
);

CREATE TABLE IF NOT EXISTS sequential_results (
    id SERIAL PRIMARY KEY,
    run_id INT REFERENCES analysis_runs(id) ON DELETE CASCADE,
    market_type TEXT NOT NULL,
    analysis_type TEXT NOT NULL,
    key TEXT NOT NULL,
    sample_count INT NOT NULL,
    up_win_rate NUMERIC(6,4) NOT NULL,
    notes TEXT
);
"""

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def get_connection():
    return psycopg2.connect(
        host=DB_HOST, port=DB_PORT,
        user=DB_USER, password=DB_PASSWORD,
        dbname=DB_NAME,
    )


def get_price_at_checkpoint(ticks_df, started_at, checkpoint_seconds):
    """Return up_price of tick closest to started_at + checkpoint_seconds (±10 s)."""
    target = started_at + timedelta(seconds=checkpoint_seconds)
    diffs = (ticks_df['time'] - target).abs()
    idx = diffs.idxmin()
    if diffs.loc[idx].total_seconds() > 10:
        return np.nan
    return float(ticks_df.loc[idx, 'up_price'])


def window_seconds_for(market_type):
    """Return window length in seconds based on market_type suffix."""
    if '15m' in str(market_type):
        return 900
    return 300


def to_python(val):
    """Convert numpy types to native Python types for psycopg2."""
    if isinstance(val, (np.floating,)):
        return float(val)
    if isinstance(val, (np.integer,)):
        return int(val)
    if isinstance(val, (np.bool_,)):
        return bool(val)
    return val


# ---------------------------------------------------------------------------
# Analysis functions
# ---------------------------------------------------------------------------


def run_calibration(df_outcomes, tick_dict, market_types, run_id, cursor, dry_run):
    """Category 1 — Calibration Analysis."""
    all_rows = []
    summary = {'significant_count': 0, 'strongest': None}

    type_groups = list(market_types) + ['all']

    for mt in type_groups:
        if mt == 'all':
            markets = df_outcomes
        else:
            markets = df_outcomes[df_outcomes['market_type'] == mt]

        for cp in CHECKPOINTS:
            prices, outcomes = [], []
            for _, m in markets.iterrows():
                ticks = tick_dict.get(m['market_id'])
                if ticks is None or len(ticks) < MIN_TICKS_PER_MARKET:
                    continue
                p = get_price_at_checkpoint(ticks, m['started_at'], cp)
                if np.isnan(p):
                    continue
                prices.append(p)
                outcomes.append(1 if m['final_outcome'] == 'Up' else 0)

            if not prices:
                continue

            prices = np.array(prices)
            outcomes = np.array(outcomes)

            # Bucket prices
            bucket_edges = np.arange(0, 1.001, PRICE_BUCKET_WIDTH)
            bucket_mids = (bucket_edges[:-1] + bucket_edges[1:]) / 2
            indices = np.digitize(prices, bucket_edges) - 1
            indices = np.clip(indices, 0, len(bucket_mids) - 1)

            bucket_sig_count = 0
            bucket_count = 0
            for bi, mid in enumerate(bucket_mids):
                mask = indices == bi
                n = int(mask.sum())
                if n < MIN_BUCKET_SAMPLES:
                    continue
                bucket_count += 1
                up_wins = int(outcomes[mask].sum())
                actual_wr = up_wins / n
                expected_wr = float(mid)
                deviation = actual_wr - expected_wr
                try:
                    pval = binomtest(up_wins, n, expected_wr).pvalue
                except ValueError:
                    pval = 1.0
                sig = pval < SIGNIFICANCE_LEVEL

                if sig:
                    bucket_sig_count += 1
                    summary['significant_count'] += 1
                    if (summary['strongest'] is None
                            or abs(deviation) > abs(summary['strongest']['deviation'])):
                        summary['strongest'] = {
                            'market_type': mt, 'checkpoint': cp,
                            'bucket': round(mid, 3), 'deviation': round(deviation, 4),
                            'p_value': round(pval, 6),
                        }

                all_rows.append((
                    to_python(run_id), mt, to_python(cp), float(round(mid, 2)),
                    int(n), int(up_wins),
                    float(round(actual_wr, 4)), float(round(expected_wr, 4)),
                    float(round(deviation, 4)), float(round(pval, 6)), bool(sig),
                ))

            print(f"[Calibration] {mt} @ {cp}s -> {bucket_count} buckets, "
                  f"{bucket_sig_count} significant deviations")

    if not dry_run and all_rows:
        execute_values(cursor, """
            INSERT INTO calibration_results
                (run_id, market_type, checkpoint_seconds, price_bucket,
                 sample_count, up_wins, actual_win_rate, expected_win_rate,
                 deviation, p_value, significant)
            VALUES %s
        """, all_rows)

    return summary


def run_trajectory(df_outcomes, tick_dict, market_types, run_id, cursor, dry_run):
    """Category 2 — Price Trajectory Analysis + sub-analyses."""
    traj_rows = []
    seq_rows = []
    summary = {'momentum_types': [], 'mean_reversion_count': 0}

    for mt in market_types:
        markets = df_outcomes[df_outcomes['market_type'] == mt]
        win_len = window_seconds_for(mt)

        # --- Main trajectory ---
        for cp in CHECKPOINTS:
            for outcome in ('Up', 'Down'):
                subset = markets[markets['final_outcome'] == outcome]
                cp_prices = []
                for _, m in subset.iterrows():
                    ticks = tick_dict.get(m['market_id'])
                    if ticks is None or len(ticks) < MIN_TICKS_PER_MARKET:
                        continue
                    p = get_price_at_checkpoint(ticks, m['started_at'], cp)
                    if not np.isnan(p):
                        cp_prices.append(p)

                if not cp_prices:
                    continue
                avg_p = float(np.mean(cp_prices))
                std_p = float(np.std(cp_prices)) if len(cp_prices) > 1 else 0.0
                traj_rows.append((
                    to_python(run_id), mt, to_python(cp), outcome,
                    float(round(avg_p, 4)), float(round(std_p, 4)),
                    int(len(cp_prices)),
                ))

        print(f"[Trajectory] {mt} — Up avg path vs Down avg path computed")

        # --- 2a Momentum effect ---
        rising_up, rising_total, falling_up, falling_total = 0, 0, 0, 0
        for _, m in markets.iterrows():
            ticks = tick_dict.get(m['market_id'])
            if ticks is None or len(ticks) < MIN_TICKS_PER_MARKET:
                continue
            p30 = get_price_at_checkpoint(ticks, m['started_at'], 30)
            p60 = get_price_at_checkpoint(ticks, m['started_at'], 60)
            if np.isnan(p30) or np.isnan(p60):
                continue
            is_up = m['final_outcome'] == 'Up'
            if p60 > p30:
                rising_total += 1
                rising_up += int(is_up)
            elif p60 < p30:
                falling_total += 1
                falling_up += int(is_up)

        if rising_total >= MIN_BUCKET_SAMPLES:
            wr = rising_up / rising_total
            seq_rows.append((
                to_python(run_id), mt, 'momentum', 'rising_60s',
                int(rising_total), float(round(wr, 4)),
                f'Rising price 30s->60s: {wr:.1%} Up win rate',
            ))
            print(f"[Momentum] {mt} — rising at 60s: {wr:.1%} Up win rate (n={rising_total})")
            if abs(wr - 0.5) > 0.04:
                summary['momentum_types'].append(mt)

        if falling_total >= MIN_BUCKET_SAMPLES:
            wr = falling_up / falling_total
            seq_rows.append((
                to_python(run_id), mt, 'momentum', 'falling_60s',
                int(falling_total), float(round(wr, 4)),
                f'Falling price 30s->60s: {wr:.1%} Up win rate',
            ))

        # --- 2b Mean reversion ---
        half_window = win_len / 2
        revert_up, revert_total = 0, 0
        for _, m in markets.iterrows():
            ticks = tick_dict.get(m['market_id'])
            if ticks is None or len(ticks) < MIN_TICKS_PER_MARKET:
                continue
            elapsed = (ticks['time'] - m['started_at']).dt.total_seconds()
            first_half = ticks[(elapsed >= 0) & (elapsed <= half_window)]
            if first_half.empty:
                continue
            if first_half['up_price'].max() >= 0.65:
                # Check if came back below 0.55
                after_peak_idx = first_half['up_price'].idxmax()
                after_peak = ticks.loc[after_peak_idx:]
                if (after_peak['up_price'] < 0.55).any():
                    revert_total += 1
                    revert_up += int(m['final_outcome'] == 'Up')

        if revert_total >= 5:
            wr = revert_up / revert_total
            seq_rows.append((
                to_python(run_id), mt, 'mean_reversion', 'peak_065_revert_055',
                int(revert_total), float(round(wr, 4)),
                f'Peaked >=0.65 then reverted <0.55: {wr:.1%} Up (n={revert_total})',
            ))
            summary['mean_reversion_count'] += revert_total
            print(f"[MeanReversion] {mt} — {revert_total} cases, {wr:.1%} Up")

        # --- 2c Threshold hold rate ---
        for threshold in [0.60, 0.70, 0.80]:
            held, crossed_total = 0, 0
            for _, m in markets.iterrows():
                ticks = tick_dict.get(m['market_id'])
                if ticks is None or len(ticks) < MIN_TICKS_PER_MARKET:
                    continue
                if (ticks['up_price'] >= threshold).any():
                    crossed_total += 1
                    # Check if held above threshold at end (final price)
                    last_price = float(ticks.iloc[-1]['up_price'])
                    if last_price >= threshold:
                        held += 1

            if crossed_total >= MIN_BUCKET_SAMPLES:
                hold_rate = held / crossed_total
                seq_rows.append((
                    to_python(run_id), mt, 'threshold_hold',
                    f'threshold_{threshold:.2f}',
                    int(crossed_total), float(round(hold_rate, 4)),
                    f'Crossed {threshold:.2f}: {hold_rate:.1%} held (n={crossed_total})',
                ))

    if not dry_run:
        if traj_rows:
            execute_values(cursor, """
                INSERT INTO trajectory_results
                    (run_id, market_type, checkpoint_seconds, outcome,
                     avg_price, std_price, sample_count)
                VALUES %s
            """, traj_rows)
        if seq_rows:
            execute_values(cursor, """
                INSERT INTO sequential_results
                    (run_id, market_type, analysis_type, key,
                     sample_count, up_win_rate, notes)
                VALUES %s
            """, seq_rows)

    return summary


def run_time_of_day(df_outcomes, tick_dict, market_types, run_id, cursor, dry_run):
    """Category 3 — Time of Day Analysis."""
    tod_rows = []
    summary = {'most_bullish': None, 'most_bearish': None}

    type_groups = list(market_types) + ['all']

    for mt in type_groups:
        if mt == 'all':
            markets = df_outcomes
        else:
            markets = df_outcomes[df_outcomes['market_type'] == mt]

        markets = markets.copy()
        markets['hour_utc'] = markets['started_at'].dt.hour

        # Compute price range per market
        price_ranges = {}
        for _, m in markets.iterrows():
            ticks = tick_dict.get(m['market_id'])
            if ticks is None or len(ticks) < MIN_TICKS_PER_MARKET:
                continue
            price_ranges[m['market_id']] = float(
                ticks['up_price'].max() - ticks['up_price'].min()
            )

        markets['price_range'] = markets['market_id'].map(price_ranges)
        markets['is_up'] = (markets['final_outcome'] == 'Up').astype(int)

        for hour in range(24):
            hourly = markets[markets['hour_utc'] == hour]
            n = len(hourly)
            if n == 0:
                continue
            up_wins = int(hourly['is_up'].sum())
            wr = up_wins / n
            avg_range = float(hourly['price_range'].mean()) if hourly['price_range'].notna().any() else None

            tod_rows.append((
                to_python(run_id), mt, int(hour), int(n), int(up_wins),
                float(round(wr, 4)),
                float(round(avg_range, 4)) if avg_range is not None else None,
            ))

            # Flag significant deviations (>= 10 samples, >10pp from 50%)
            if n >= MIN_BUCKET_SAMPLES and abs(wr - 0.5) > 0.10:
                label = 'significant'
                print(f"[TimeOfDay] {mt} — hour {hour} UTC: "
                      f"{wr:.1%} Up win rate (n={n}) <- {label}")

        # Track best/worst hours for combined 'all'
        if mt == 'all':
            hourly_stats = markets.groupby('hour_utc').agg(
                n=('is_up', 'size'), wr=('is_up', 'mean')
            )
            valid = hourly_stats[hourly_stats['n'] >= MIN_BUCKET_SAMPLES]
            if not valid.empty:
                best_hour = valid['wr'].idxmax()
                worst_hour = valid['wr'].idxmin()
                summary['most_bullish'] = (
                    int(best_hour), round(float(valid.loc[best_hour, 'wr']), 4)
                )
                summary['most_bearish'] = (
                    int(worst_hour), round(float(valid.loc[worst_hour, 'wr']), 4)
                )

    if not dry_run and tod_rows:
        execute_values(cursor, """
            INSERT INTO timeofday_results
                (run_id, market_type, hour_utc, sample_count,
                 up_wins, up_win_rate, avg_price_range)
            VALUES %s
        """, tod_rows)

    return summary


def run_sequential(df_outcomes, tick_dict, market_types, run_id, cursor, dry_run):
    """Category 4 — Sequential Pattern Analysis."""
    seq_rows = []
    summary = {'strongest_streak': None, 'strongest_cross': None}

    # --- 4a Streak analysis ---
    for mt in market_types:
        markets = df_outcomes[df_outcomes['market_type'] == mt].sort_values('ended_at').reset_index(drop=True)
        outcomes_list = markets['final_outcome'].tolist()

        for streak_len in range(1, 6):
            patterns = {}  # pattern string -> list of next outcomes
            for i in range(streak_len, len(outcomes_list)):
                prev = ''.join(o[0] for o in outcomes_list[i - streak_len:i])  # e.g. 'DDD'
                patterns.setdefault(prev, []).append(outcomes_list[i])

            for pattern, nexts in patterns.items():
                n = len(nexts)
                if n < 15:
                    continue
                up_wins = sum(1 for o in nexts if o == 'Up')
                wr = up_wins / n
                key = f'prev_{streak_len}_{pattern}'
                seq_rows.append((
                    to_python(run_id), mt, 'streak', key, int(n),
                    float(round(wr, 4)),
                    f'After {pattern}: {wr:.1%} Up (n={n})',
                ))

                if abs(wr - 0.5) > 0.08:
                    print(f"[Sequential] {mt} — after {pattern} streak: "
                          f"{wr:.1%} Up (n={n})")
                    if (summary['strongest_streak'] is None
                            or abs(wr - 0.5) > abs(summary['strongest_streak']['wr'] - 0.5)):
                        summary['strongest_streak'] = {
                            'market_type': mt, 'key': key,
                            'wr': round(wr, 4), 'n': n,
                        }

    # --- 4b Cross-asset correlation ---
    assets = ['btc', 'eth', 'sol', 'xrp']
    timeframes = ['5m', '15m']

    for tf in timeframes:
        for i, a1 in enumerate(assets):
            for a2 in assets[i + 1:]:
                mt1 = f'{a1}_{tf}'
                mt2 = f'{a2}_{tf}'
                m1 = df_outcomes[df_outcomes['market_type'] == mt1].copy()
                m2 = df_outcomes[df_outcomes['market_type'] == mt2].copy()

                if m1.empty or m2.empty:
                    continue

                # Find co-resolved markets (ended within 30s of each other)
                pairs = []
                for _, r1 in m1.iterrows():
                    close = m2[
                        (m2['ended_at'] - r1['ended_at']).abs() <= timedelta(seconds=30)
                    ]
                    for _, r2 in close.iterrows():
                        pairs.append((r1['final_outcome'], r2['final_outcome']))

                if len(pairs) < 10:
                    continue

                # When A is Up, what % is B also Up?
                a_up = [(o1, o2) for o1, o2 in pairs if o1 == 'Up']
                if len(a_up) >= 10:
                    co_up = sum(1 for _, o2 in a_up if o2 == 'Up')
                    co_rate = co_up / len(a_up)
                    key = f'{mt1}->{mt2}'
                    seq_rows.append((
                        to_python(run_id), mt1, 'cross_asset', key,
                        int(len(a_up)), float(round(co_rate, 4)),
                        f'When {mt1} Up, {mt2} Up {co_rate:.1%} (n={len(a_up)})',
                    ))
                    print(f"[CrossAsset] {key} — co-resolution Up: "
                          f"{co_rate:.1%} (n={len(a_up)})")

                    if (summary['strongest_cross'] is None
                            or abs(co_rate - 0.5) > abs(summary['strongest_cross']['rate'] - 0.5)):
                        summary['strongest_cross'] = {
                            'key': key, 'rate': round(co_rate, 4),
                            'n': len(a_up),
                        }

    # --- 4c Previous market influence ---
    for mt in market_types:
        markets = df_outcomes[df_outcomes['market_type'] == mt].sort_values('ended_at').reset_index(drop=True)

        after_down_prices, after_up_prices = [], []
        for i in range(1, len(markets)):
            prev_outcome = markets.iloc[i - 1]['final_outcome']
            cur = markets.iloc[i]
            ticks = tick_dict.get(cur['market_id'])
            if ticks is None or len(ticks) < MIN_TICKS_PER_MARKET:
                continue
            p30 = get_price_at_checkpoint(ticks, cur['started_at'], 30)
            if np.isnan(p30):
                continue
            if prev_outcome == 'Down':
                after_down_prices.append((p30, cur['final_outcome']))
            else:
                after_up_prices.append((p30, cur['final_outcome']))

        if len(after_down_prices) >= MIN_BUCKET_SAMPLES:
            avg_price = np.mean([p for p, _ in after_down_prices])
            up_wins = sum(1 for _, o in after_down_prices if o == 'Up')
            wr = up_wins / len(after_down_prices)
            seq_rows.append((
                to_python(run_id), mt, 'prev_influence', 'after_down',
                int(len(after_down_prices)), float(round(wr, 4)),
                f'After Down: avg 30s price={avg_price:.3f}, Up wr={wr:.1%}',
            ))

        if len(after_up_prices) >= MIN_BUCKET_SAMPLES:
            avg_price = np.mean([p for p, _ in after_up_prices])
            up_wins = sum(1 for _, o in after_up_prices if o == 'Up')
            wr = up_wins / len(after_up_prices)
            seq_rows.append((
                to_python(run_id), mt, 'prev_influence', 'after_up',
                int(len(after_up_prices)), float(round(wr, 4)),
                f'After Up: avg 30s price={avg_price:.3f}, Up wr={wr:.1%}',
            ))

    if not dry_run and seq_rows:
        execute_values(cursor, """
            INSERT INTO sequential_results
                (run_id, market_type, analysis_type, key,
                 sample_count, up_win_rate, notes)
            VALUES %s
        """, seq_rows)

    return summary


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main():
    parser = argparse.ArgumentParser(description='PolyEdge Lab — Full Analysis')
    parser.add_argument('--dry-run', action='store_true',
                        help='Run analysis and print results without writing to DB')
    parser.add_argument('--market-type', type=str, default=None,
                        help='Run analysis only for a specific market type (e.g. btc_5m)')
    args = parser.parse_args()

    t_start = time.time()
    print('[Analysis] Starting...')

    conn = get_connection()
    conn.autocommit = False
    cursor = conn.cursor()

    try:
        # ---- Step 1: Create analysis tables ----
        cursor.execute(DDL)
        conn.commit()

        # ---- Step 2: Load data ----
        market_type_filter = ""
        if args.market_type:
            market_type_filter = f" AND market_type = '{args.market_type}'"

        df_outcomes = pd.read_sql(f"""
            SELECT market_id, market_type, started_at, ended_at,
                   final_outcome, final_up_price
            FROM market_outcomes
            WHERE resolved = TRUE
              AND final_outcome IN ('Up', 'Down')
              {market_type_filter}
            ORDER BY started_at ASC
        """, conn, parse_dates=['started_at', 'ended_at'])

        if df_outcomes.empty:
            print('[Analysis] No resolved markets found. Exiting.')
            return

        market_ids = df_outcomes['market_id'].tolist()

        # Use parameterized query for tick loading
        placeholders = ','.join(['%s'] * len(market_ids))
        df_ticks = pd.read_sql(f"""
            SELECT mt.time, mt.market_id, mt.up_price
            FROM market_ticks mt
            JOIN market_outcomes mo ON mt.market_id = mo.market_id
            WHERE mo.resolved = TRUE
              AND mo.final_outcome IN ('Up', 'Down')
              AND mt.market_id IN ({placeholders})
            ORDER BY mt.market_id, mt.time
        """, conn, params=market_ids, parse_dates=['time'])

        # Build tick dict, filtering out markets with too few ticks
        tick_dict = {}
        for mid, grp in df_ticks.groupby('market_id'):
            if len(grp) >= MIN_TICKS_PER_MARKET:
                tick_dict[mid] = grp.reset_index(drop=True)

        total_ticks = sum(len(t) for t in tick_dict.values())
        date_start = df_outcomes['started_at'].min()
        date_end = df_outcomes['ended_at'].max()

        print(f'[Analysis] Loaded {len(df_outcomes)} resolved markets, {total_ticks} ticks')
        print(f'[Analysis] Date range: {date_start} -> {date_end}')

        market_types = sorted(df_outcomes['market_type'].dropna().unique())

        # ---- Step 3: Create analysis run record ----
        run_id = None
        if not args.dry_run:
            notes = f'market_type={args.market_type}' if args.market_type else 'full run'
            cursor.execute("""
                INSERT INTO analysis_runs (notes)
                VALUES (%s)
                RETURNING id
            """, (notes,))
            run_id = cursor.fetchone()[0]
        else:
            run_id = -1  # placeholder for dry run
            print('[Analysis] DRY RUN — no database writes')

        # ---- Step 4: Calibration Analysis ----
        print()
        cal_summary = run_calibration(
            df_outcomes, tick_dict, market_types, run_id, cursor, args.dry_run)

        # ---- Step 5: Trajectory Analysis ----
        print()
        traj_summary = run_trajectory(
            df_outcomes, tick_dict, market_types, run_id, cursor, args.dry_run)

        # ---- Step 6: Time of Day Analysis ----
        print()
        tod_summary = run_time_of_day(
            df_outcomes, tick_dict, market_types, run_id, cursor, args.dry_run)

        # ---- Step 7: Sequential Pattern Analysis ----
        print()
        seq_summary = run_sequential(
            df_outcomes, tick_dict, market_types, run_id, cursor, args.dry_run)

        # ---- Step 8: Update analysis run record ----
        if not args.dry_run:
            cursor.execute("""
                UPDATE analysis_runs
                SET markets_analyzed = %s,
                    ticks_analyzed = %s,
                    date_range_start = %s,
                    date_range_end = %s
                WHERE id = %s
            """, (len(df_outcomes), total_ticks, date_start, date_end, run_id))
            conn.commit()

        # ---- Step 9: Print summary report ----
        duration = time.time() - t_start

        print()
        print('=' * 60)
        print('POLYEDGE LAB ANALYSIS — COMPLETE')
        print('=' * 60)
        print(f'Run ID: {run_id}')
        print(f'Markets analyzed: {len(df_outcomes)}')
        print(f'Ticks analyzed: {total_ticks}')
        print(f'Date range: {date_start} -> {date_end}')
        print(f'Duration: {duration:.1f} seconds')

        print()
        print('CALIBRATION:')
        print(f'  Significant deviations found: {cal_summary["significant_count"]}')
        if cal_summary['strongest']:
            s = cal_summary['strongest']
            print(f'  Strongest: {s["market_type"]} @ {s["checkpoint"]}s, '
                  f'bucket {s["bucket"]}, deviation {s["deviation"]:+.4f} '
                  f'(p={s["p_value"]:.6f})')

        print()
        print('TRAJECTORY:')
        if traj_summary['momentum_types']:
            print(f'  Momentum effect found in: {", ".join(traj_summary["momentum_types"])}')
        else:
            print('  No strong momentum effects detected')
        print(f'  Mean reversion cases: {traj_summary["mean_reversion_count"]} markets')

        print()
        print('TIME OF DAY:')
        if tod_summary['most_bullish']:
            h, wr = tod_summary['most_bullish']
            print(f'  Most bullish hour: {h} UTC ({wr:.1%} Up across all types)')
        if tod_summary['most_bearish']:
            h, wr = tod_summary['most_bearish']
            print(f'  Most bearish hour: {h} UTC ({wr:.1%} Up across all types)')

        print()
        print('SEQUENTIAL:')
        if seq_summary['strongest_streak']:
            s = seq_summary['strongest_streak']
            print(f'  Strongest streak signal: {s["market_type"]} '
                  f'{s["key"]} -> {s["wr"]:.1%} Up (n={s["n"]})')
        else:
            print('  No strong streak signals detected')
        if seq_summary['strongest_cross']:
            s = seq_summary['strongest_cross']
            print(f'  Strongest cross-asset: {s["key"]} '
                  f'{s["rate"]:.1%} co-resolution (n={s["n"]})')
        else:
            print('  No strong cross-asset signals detected')

        print()
        if args.dry_run:
            print('DRY RUN — no results written to database.')
        else:
            print('Results written to database. Run dashboard /analysis page to view.')
        print('=' * 60)
        print(f'[Analysis] Done in {duration:.1f}s')

        # ---- Step 10: Farming Strategy Backtest ----
        if not args.dry_run:
            from strategy_farming import run_farming_backtest
            run_farming_backtest(conn, run_id)

        # ---- Step 11: Calibration Strategy Backtest ----
        if not args.dry_run:
            from strategy_calibration import run_calibration_backtest
            run_calibration_backtest(conn, run_id)

    except Exception as e:
        conn.rollback()
        print(f'[Analysis] ERROR: {e}', file=sys.stderr)
        raise
    finally:
        cursor.close()
        conn.close()


if __name__ == '__main__':
    main()
