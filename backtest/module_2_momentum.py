"""
Module 2: Momentum Signals
Tests velocity and acceleration-based entry conditions (contrarian).
Calculates price velocity over a lookback window and bets against momentum.
"""

import numpy as np
import pandas as pd
from itertools import product
from .data_loader import get_price_at_second, filter_markets
from .engine import make_trade, compute_metrics, add_ranking_score, save_module_results

MODULE_NAME = "Module_2"

MOMENTUM_WINDOWS = [3, 5, 8, 10, 15, 20, 25, 30]
VELOCITY_THRESHOLDS = [0.001, 0.003, 0.005, 0.008, 0.010, 0.015, 0.020]
EVAL_SECONDS = [5, 10, 20, 30, 45, 60, 90, 120]
ACCEL_THRESHOLDS = [0.0002, 0.0005, 0.0008, 0.0010]

MIN_TRADES = 20


def generate_configs():
    configs = []
    cid = 0

    # Without acceleration
    for window, vel_thr, eval_sec in product(MOMENTUM_WINDOWS, VELOCITY_THRESHOLDS, EVAL_SECONDS):
        if eval_sec <= window:
            continue
        cid += 1
        configs.append({
            'config_id': f"M2_{cid:04d}",
            'momentum_window_sec': window,
            'velocity_threshold': vel_thr,
            'eval_second': eval_sec,
            'acceleration_enabled': False,
            'acceleration_threshold': 0,
        })

    # With acceleration (smaller subset)
    for window in [5, 10, 15, 20]:
        for vel_thr in [0.003, 0.005, 0.010]:
            for eval_sec in [15, 30, 60, 90]:
                if eval_sec <= window * 2:
                    continue
                for accel_thr in ACCEL_THRESHOLDS:
                    cid += 1
                    configs.append({
                        'config_id': f"M2_{cid:04d}",
                        'momentum_window_sec': window,
                        'velocity_threshold': vel_thr,
                        'eval_second': eval_sec,
                        'acceleration_enabled': True,
                        'acceleration_threshold': accel_thr,
                    })

    return configs


def run_single_config(cfg, markets):
    """Run a single momentum config against all markets."""
    window = cfg['momentum_window_sec']
    vel_thr = cfg['velocity_threshold']
    eval_sec = cfg['eval_second']
    use_accel = cfg['acceleration_enabled']
    accel_thr = cfg['acceleration_threshold']

    trades = []

    for m in markets:
        ticks = m['ticks']
        total = m['total_seconds']

        if eval_sec >= total:
            continue

        # Get prices at evaluation point and lookback
        price_now = get_price_at_second(ticks, eval_sec)
        price_prev = get_price_at_second(ticks, eval_sec - window)

        if price_now is None or price_prev is None:
            continue

        velocity = (price_now - price_prev) / window

        if abs(velocity) < vel_thr:
            continue

        # Acceleration check
        if use_accel:
            price_earlier = get_price_at_second(ticks, eval_sec - 2 * window)
            if price_earlier is None:
                continue
            velocity_prev = (price_prev - price_earlier) / window
            acceleration = (velocity - velocity_prev) / window
            if abs(acceleration) < accel_thr:
                continue

        # Contrarian bet: bet against momentum direction
        if velocity > 0:
            # UP momentum → bet DOWN
            direction = 'Down'
            entry_price = 1.0 - price_now
        else:
            # DOWN momentum → bet UP
            direction = 'Up'
            entry_price = price_now

        if entry_price <= 0.01 or entry_price >= 0.99:
            continue

        trades.append(make_trade(m, eval_sec, entry_price, direction))

    return trades


def run(markets, output_dir):
    """Run Module 2 backtest."""
    print(f"\n{'='*60}")
    print(f"MODULE 2: MOMENTUM SIGNALS")
    print(f"{'='*60}")

    configs = generate_configs()
    print(f"  Testing {len(configs)} configurations...")

    all_results = []
    trades_by_config = {}

    for i, cfg in enumerate(configs):
        if (i + 1) % 100 == 0:
            print(f"  Progress: {i+1}/{len(configs)}")

        trades = run_single_config(cfg, markets)
        trades_by_config[cfg['config_id']] = trades

        metrics = compute_metrics(trades, config_id=cfg['config_id'])
        row = {**cfg, **metrics}
        all_results.append(row)

    df = pd.DataFrame(all_results)
    df = df[df['total_bets'] >= MIN_TRADES].reset_index(drop=True)
    df = add_ranking_score(df)

    module_dir = f"{output_dir}/Module_2_Momentum_Signals"
    save_module_results(df, trades_by_config, MODULE_NAME, module_dir)

    profitable = df[df['total_pnl'] > 0]
    print(f"  Done. {len(df)} configs with {MIN_TRADES}+ trades, {len(profitable)} profitable.")

    return df
