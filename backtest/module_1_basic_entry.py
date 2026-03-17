"""
Module 1: Basic Entry Signals
Tests simple threshold-based entry conditions (contrarian).
Signal types:
  A) Price Threshold: If UP >= threshold → bet DOWN; if DOWN >= threshold → bet UP
  B) Deviation from 0.50: If |price - 0.50| >= deviation → contrarian bet
"""

import numpy as np
import pandas as pd
from itertools import product
from .data_loader import get_price_at_second, filter_markets
from .engine import make_trade, compute_metrics, add_ranking_score, save_module_results

MODULE_NAME = "Module_1"

# --- Parameter grid ---
ENTRY_WINDOWS = [
    (1, 30),
    (5, 60),
    (10, 90),
    (15, 120),
    (20, 180),
    (30, 300),
]

PRICE_THRESHOLDS = [0.55, 0.60, 0.65, 0.70, 0.75, 0.80, 0.85, 0.90, 0.95]
DEVIATION_PCTS = [0.05, 0.08, 0.10, 0.15, 0.20, 0.25, 0.30, 0.35, 0.40, 0.45]

DURATION_FILTERS = ['all', '5m', '15m']

MIN_TRADES = 20


def generate_configs():
    configs = []
    cid = 0

    for (sec_min, sec_max), dur_filter in product(ENTRY_WINDOWS, DURATION_FILTERS):
        # Price threshold configs
        for threshold in PRICE_THRESHOLDS:
            cid += 1
            configs.append({
                'config_id': f"M1_{cid:04d}",
                'signal_type': 'price_threshold',
                'entry_second_min': sec_min,
                'entry_second_max': sec_max,
                'threshold': threshold,
                'duration_filter': dur_filter,
            })

        # Deviation configs
        for dev in DEVIATION_PCTS:
            cid += 1
            configs.append({
                'config_id': f"M1_{cid:04d}",
                'signal_type': 'deviation',
                'entry_second_min': sec_min,
                'entry_second_max': sec_max,
                'threshold': dev,
                'duration_filter': dur_filter,
            })

    return configs


def run_single_config(cfg, markets):
    """Run a single config against all markets. Returns list of Trade."""
    durations = None
    if cfg['duration_filter'] == '5m':
        durations = [5]
    elif cfg['duration_filter'] == '15m':
        durations = [15]

    filtered = filter_markets(markets, durations=durations)
    signal_type = cfg['signal_type']
    threshold = cfg['threshold']
    sec_min = cfg['entry_second_min']
    sec_max = cfg['entry_second_max']

    trades = []

    for m in filtered:
        ticks = m['ticks']
        total_sec = m['total_seconds']
        effective_max = min(sec_max, total_sec - 1)

        for sec in range(sec_min, effective_max + 1):
            price = ticks[sec] if sec < len(ticks) else np.nan
            if np.isnan(price):
                continue

            direction = None
            entry_price = None

            if signal_type == 'price_threshold':
                if price >= threshold:
                    direction = 'Down'
                    entry_price = 1.0 - price
                elif (1.0 - price) >= threshold:
                    direction = 'Up'
                    entry_price = price
            else:  # deviation
                deviation = abs(price - 0.50)
                if deviation >= threshold:
                    if price > 0.50:
                        direction = 'Down'
                        entry_price = 1.0 - price
                    else:
                        direction = 'Up'
                        entry_price = price

            if direction and entry_price and entry_price > 0.01:
                trades.append(make_trade(m, sec, entry_price, direction))
                break  # one bet per market

    return trades


def run(markets, output_dir):
    """Run Module 1 backtest. Returns results DataFrame."""
    print(f"\n{'='*60}")
    print(f"MODULE 1: BASIC ENTRY SIGNALS")
    print(f"{'='*60}")

    configs = generate_configs()
    print(f"  Testing {len(configs)} configurations...")

    all_results = []
    trades_by_config = {}

    for i, cfg in enumerate(configs):
        if (i + 1) % 50 == 0:
            print(f"  Progress: {i+1}/{len(configs)}")

        trades = run_single_config(cfg, markets)
        trades_by_config[cfg['config_id']] = trades

        metrics = compute_metrics(trades, config_id=cfg['config_id'])

        # Merge config params into metrics
        row = {**cfg, **metrics}
        all_results.append(row)

    df = pd.DataFrame(all_results)
    # Filter to configs with enough trades
    df = df[df['total_bets'] >= MIN_TRADES].reset_index(drop=True)
    df = add_ranking_score(df)

    module_dir = f"{output_dir}/Module_1_Basic_Entry_Signals"
    save_module_results(df, trades_by_config, MODULE_NAME, module_dir)

    profitable = df[df['total_pnl'] > 0]
    print(f"  Done. {len(df)} configs with {MIN_TRADES}+ trades, {len(profitable)} profitable.")

    return df
