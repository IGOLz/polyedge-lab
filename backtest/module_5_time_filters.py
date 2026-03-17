"""
Module 5: Time-Based Filters
Tests whether market timing (phase, seconds remaining) affects strategy profitability.
Uses a simple contrarian base signal filtered by time conditions.
"""

import numpy as np
import pandas as pd
from itertools import product
from .data_loader import get_price_at_second, filter_markets
from .engine import make_trade, compute_metrics, add_ranking_score, save_module_results

MODULE_NAME = "Module_5"

BASE_DEVIATION = 0.08  # Min deviation from 0.50 to trigger base signal

MARKET_PHASES = ['all', 'early', 'middle', 'late']
EARLY_CUTOFF_PCTS = [0.10, 0.20, 0.30, 0.40, 0.50]
LATE_START_PCTS = [0.50, 0.60, 0.70, 0.80, 0.90]
MIN_SECONDS_REMAINING = [10, 30, 60, 90, 120, 180, 300]

MIN_TRADES = 20


def generate_configs():
    configs = []
    cid = 0

    # Phase = 'all' (baseline with different min_seconds_remaining)
    for min_rem in MIN_SECONDS_REMAINING:
        cid += 1
        configs.append({
            'config_id': f"M5_{cid:04d}",
            'market_phase': 'all',
            'early_cutoff_pct': 0,
            'late_start_pct': 1.0,
            'min_seconds_remaining': min_rem,
        })

    # Phase = 'early'
    for cutoff, min_rem in product(EARLY_CUTOFF_PCTS, [0, 30, 60]):
        cid += 1
        configs.append({
            'config_id': f"M5_{cid:04d}",
            'market_phase': 'early',
            'early_cutoff_pct': cutoff,
            'late_start_pct': 1.0,
            'min_seconds_remaining': min_rem,
        })

    # Phase = 'late'
    for start, min_rem in product(LATE_START_PCTS, [0, 10, 30]):
        cid += 1
        configs.append({
            'config_id': f"M5_{cid:04d}",
            'market_phase': 'late',
            'early_cutoff_pct': 0,
            'late_start_pct': start,
            'min_seconds_remaining': min_rem,
        })

    # Phase = 'middle'
    for cutoff, start in product(
        [0.10, 0.20, 0.30],
        [0.70, 0.80, 0.90],
    ):
        if cutoff >= start:
            continue
        for min_rem in [0, 60]:
            cid += 1
            configs.append({
                'config_id': f"M5_{cid:04d}",
                'market_phase': 'middle',
                'early_cutoff_pct': cutoff,
                'late_start_pct': start,
                'min_seconds_remaining': min_rem,
            })

    return configs


def run_single_config(cfg, markets):
    """Run a single time filter config. Scan every valid second for base signal."""
    phase = cfg['market_phase']
    early_cutoff = cfg['early_cutoff_pct']
    late_start = cfg['late_start_pct']
    min_remaining = cfg['min_seconds_remaining']

    trades = []

    for m in markets:
        ticks = m['ticks']
        total = m['total_seconds']

        # Determine valid second range based on phase
        if phase == 'all':
            sec_start = 1
            sec_end = total
        elif phase == 'early':
            sec_start = 1
            sec_end = int(total * early_cutoff)
        elif phase == 'late':
            sec_start = int(total * late_start)
            sec_end = total
        elif phase == 'middle':
            sec_start = int(total * early_cutoff)
            sec_end = int(total * late_start)
        else:
            continue

        # Apply min_seconds_remaining filter
        sec_end = min(sec_end, total - min_remaining)

        if sec_start >= sec_end or sec_start < 1:
            continue

        # Scan for base signal (first trigger wins)
        for sec in range(sec_start, sec_end):
            price = ticks[sec] if sec < len(ticks) else np.nan
            if np.isnan(price):
                continue

            deviation = abs(price - 0.50)
            if deviation < BASE_DEVIATION:
                continue

            # Contrarian bet
            if price > 0.50:
                direction = 'Down'
                entry_price = 1.0 - price
            else:
                direction = 'Up'
                entry_price = price

            if entry_price <= 0.01 or entry_price >= 0.99:
                continue

            trades.append(make_trade(m, sec, entry_price, direction))
            break  # one bet per market

    return trades


def run(markets, output_dir):
    """Run Module 5 backtest."""
    print(f"\n{'='*60}")
    print(f"MODULE 5: TIME-BASED FILTERS")
    print(f"{'='*60}")

    configs = generate_configs()
    print(f"  Testing {len(configs)} configurations...")

    all_results = []
    trades_by_config = {}

    for i, cfg in enumerate(configs):
        if (i + 1) % 20 == 0:
            print(f"  Progress: {i+1}/{len(configs)}")

        trades = run_single_config(cfg, markets)
        trades_by_config[cfg['config_id']] = trades

        metrics = compute_metrics(trades, config_id=cfg['config_id'])
        row = {**cfg, **metrics}
        all_results.append(row)

    df = pd.DataFrame(all_results)
    df = df[df['total_bets'] >= MIN_TRADES].reset_index(drop=True)
    df = add_ranking_score(df)

    module_dir = f"{output_dir}/Module_5_Time_Filters"
    save_module_results(df, trades_by_config, MODULE_NAME, module_dir)

    profitable = df[df['total_pnl'] > 0]
    print(f"  Done. {len(df)} configs with {MIN_TRADES}+ trades, {len(profitable)} profitable.")

    return df
