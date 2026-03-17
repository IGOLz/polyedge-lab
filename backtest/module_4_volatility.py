"""
Module 4: Volatility-Based Signals
Tests spread conditions, price volatility, and spread expansion patterns.
Uses price deviation as base signal, filtered by volatility conditions.
"""

import numpy as np
import pandas as pd
from itertools import product
from .data_loader import get_price_at_second, filter_markets
from .engine import make_trade, compute_metrics, add_ranking_score, save_module_results

MODULE_NAME = "Module_4"

# Base signal: evaluate at this second, bet contrarian if |price - 0.50| >= base_deviation
BASE_EVAL_SECOND = 30
BASE_DEVIATION = 0.08

MIN_TRADES = 20


def generate_configs():
    configs = []
    cid = 0

    # A) Spread filter configs
    # Spread = |2*up_price - 1| = distance from 0.50 in either direction
    for min_sp, max_sp in product(
        [0.00, 0.02, 0.05, 0.10, 0.15],
        [0.10, 0.20, 0.30, 0.40, 0.50],
    ):
        if min_sp >= max_sp:
            continue
        cid += 1
        configs.append({
            'config_id': f"M4_{cid:04d}",
            'strategy_type': 'spread_filter',
            'min_spread': min_sp,
            'max_spread': max_sp,
            'volatility_window_sec': 0,
            'volatility_threshold': 0,
            'volatility_direction': 'none',
            'spread_expansion_threshold': 0,
            'spread_expansion_window': 0,
            'eval_second': BASE_EVAL_SECOND,
        })

    # B) Volatility filter configs
    for vol_win, vol_thr, vol_dir in product(
        [5, 10, 15, 20, 30, 60],
        [0.01, 0.02, 0.03, 0.05, 0.08, 0.10],
        ['high', 'low'],
    ):
        cid += 1
        configs.append({
            'config_id': f"M4_{cid:04d}",
            'strategy_type': 'volatility_filter',
            'min_spread': 0,
            'max_spread': 1.0,
            'volatility_window_sec': vol_win,
            'volatility_threshold': vol_thr,
            'volatility_direction': vol_dir,
            'spread_expansion_threshold': 0,
            'spread_expansion_window': 0,
            'eval_second': max(BASE_EVAL_SECOND, vol_win + 5),
        })

    # C) Spread expansion configs
    for exp_thr, exp_win in product(
        [0.10, 0.20, 0.30, 0.50, 0.80, 1.0],
        [5, 10, 15, 20, 30],
    ):
        cid += 1
        configs.append({
            'config_id': f"M4_{cid:04d}",
            'strategy_type': 'spread_expansion',
            'min_spread': 0,
            'max_spread': 1.0,
            'volatility_window_sec': 0,
            'volatility_threshold': 0,
            'volatility_direction': 'none',
            'spread_expansion_threshold': exp_thr,
            'spread_expansion_window': exp_win,
            'eval_second': max(BASE_EVAL_SECOND, exp_win + 5),
        })

    # D) Combined spread + volatility
    for min_sp, vol_win, vol_thr, vol_dir in product(
        [0.05, 0.10, 0.15],
        [10, 20, 30],
        [0.02, 0.05],
        ['high', 'low'],
    ):
        cid += 1
        configs.append({
            'config_id': f"M4_{cid:04d}",
            'strategy_type': 'combined',
            'min_spread': min_sp,
            'max_spread': 0.50,
            'volatility_window_sec': vol_win,
            'volatility_threshold': vol_thr,
            'volatility_direction': vol_dir,
            'spread_expansion_threshold': 0,
            'spread_expansion_window': 0,
            'eval_second': max(BASE_EVAL_SECOND, vol_win + 5),
        })

    return configs


def _compute_volatility(ticks, end_sec, window):
    """Compute std dev of up_price over [end_sec - window, end_sec]."""
    start = max(0, end_sec - window)
    segment = ticks[start:end_sec + 1]
    valid = segment[~np.isnan(segment)]
    if len(valid) < 3:
        return None
    return float(np.std(valid))


def _compute_spread_at(ticks, sec):
    """Spread = |2*up_price - 1| = distance from 0.50."""
    if sec < 0 or sec >= len(ticks):
        return None
    p = ticks[sec]
    if np.isnan(p):
        return None
    return abs(2 * p - 1)


def run_single_config(cfg, markets):
    """Run a single volatility config against all markets."""
    strat = cfg['strategy_type']
    eval_sec = cfg['eval_second']
    min_sp = cfg['min_spread']
    max_sp = cfg['max_spread']
    vol_win = cfg['volatility_window_sec']
    vol_thr = cfg['volatility_threshold']
    vol_dir = cfg['volatility_direction']
    exp_thr = cfg['spread_expansion_threshold']
    exp_win = cfg['spread_expansion_window']

    trades = []

    for m in markets:
        ticks = m['ticks']
        total = m['total_seconds']

        if eval_sec >= total:
            continue

        price = ticks[eval_sec] if eval_sec < len(ticks) else np.nan
        if np.isnan(price):
            continue

        # Base signal: must have enough deviation to bet
        deviation = abs(price - 0.50)
        if deviation < BASE_DEVIATION:
            continue

        # Apply filters based on strategy type
        if strat in ('spread_filter', 'combined'):
            spread = abs(2 * price - 1)
            if spread < min_sp or spread > max_sp:
                continue

        if strat in ('volatility_filter', 'combined'):
            vol = _compute_volatility(ticks, eval_sec, vol_win)
            if vol is None:
                continue
            if vol_dir == 'high' and vol < vol_thr:
                continue
            if vol_dir == 'low' and vol > vol_thr:
                continue

        if strat == 'spread_expansion':
            if exp_win > 0 and exp_thr > 0:
                spread_now = _compute_spread_at(ticks, eval_sec)
                spread_before = _compute_spread_at(ticks, eval_sec - exp_win)
                if spread_now is None or spread_before is None or spread_before < 0.001:
                    continue
                expansion = (spread_now - spread_before) / spread_before
                if expansion < exp_thr:
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

        trades.append(make_trade(m, eval_sec, entry_price, direction))

    return trades


def run(markets, output_dir):
    """Run Module 4 backtest."""
    print(f"\n{'='*60}")
    print(f"MODULE 4: VOLATILITY-BASED SIGNALS")
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
        row = {**cfg, **metrics}
        all_results.append(row)

    df = pd.DataFrame(all_results)
    df = df[df['total_bets'] >= MIN_TRADES].reset_index(drop=True)
    df = add_ranking_score(df)

    module_dir = f"{output_dir}/Module_4_Volatility_Signals"
    save_module_results(df, trades_by_config, MODULE_NAME, module_dir)

    profitable = df[df['total_pnl'] > 0]
    print(f"  Done. {len(df)} configs with {MIN_TRADES}+ trades, {len(profitable)} profitable.")

    return df
