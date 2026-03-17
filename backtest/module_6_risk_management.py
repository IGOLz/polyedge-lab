"""
Module 6: Risk Management Overlays
Tests whether filtering trades by historical performance improves results.
Uses walk-forward approach: train on first 2/3 of data, test on last 1/3.
Base signal: contrarian bet when |price - 0.50| >= 0.08 (same as Module 5).
"""

import numpy as np
import pandas as pd
from itertools import product
from collections import defaultdict
from .data_loader import get_price_at_second, filter_markets
from .engine import make_trade, compute_metrics, add_ranking_score, save_module_results

MODULE_NAME = "Module_6"

BASE_EVAL_SECOND = 30
BASE_DEVIATION = 0.08

MIN_WIN_RATES = [0.45, 0.48, 0.50, 0.52, 0.55, 0.58, 0.60, 0.65, 0.70]
MIN_EDGES = [0.00, 0.02, 0.04, 0.06, 0.08, 0.10, 0.15, 0.20]
MAX_EXPOSURES = [0.01, 0.02, 0.03, 0.05, 0.08, 0.10]

MIN_TRADES = 10


def _get_price_bucket(entry_price):
    """Bucket entry price into ranges for training analysis."""
    if entry_price < 0.10:
        return '0.00-0.10'
    elif entry_price < 0.20:
        return '0.10-0.20'
    elif entry_price < 0.30:
        return '0.20-0.30'
    elif entry_price < 0.40:
        return '0.30-0.40'
    elif entry_price < 0.50:
        return '0.40-0.50'
    else:
        return '0.50+'


def _get_pattern_key(market, entry_price):
    """Create a pattern key for grouping trades in training."""
    return (market['asset'], market['duration_minutes'], _get_price_bucket(entry_price))


def _generate_base_signals(markets, eval_second=BASE_EVAL_SECOND):
    """Generate base contrarian signals for all markets, returning (market, direction, entry_price, sec)."""
    signals = []
    for m in markets:
        ticks = m['ticks']
        total = m['total_seconds']
        if eval_second >= total:
            continue

        price = ticks[eval_second] if eval_second < len(ticks) else np.nan
        if np.isnan(price):
            continue

        deviation = abs(price - 0.50)
        if deviation < BASE_DEVIATION:
            continue

        if price > 0.50:
            direction = 'Down'
            entry_price = 1.0 - price
        else:
            direction = 'Up'
            entry_price = price

        if entry_price <= 0.01 or entry_price >= 0.99:
            continue

        signals.append((m, direction, entry_price, eval_second))

    return signals


def generate_configs():
    configs = []
    cid = 0

    for min_wr, min_edge, max_exp in product(
        MIN_WIN_RATES,
        [0.00, 0.02, 0.05, 0.10],
        [0.02, 0.05, 0.10],
    ):
        cid += 1
        configs.append({
            'config_id': f"M6_{cid:04d}",
            'min_win_rate_pct': min_wr,
            'min_edge_pct': min_edge,
            'max_exposure_pct': max_exp,
        })

    return configs


def run(markets, output_dir):
    """Run Module 6 backtest with walk-forward methodology."""
    print(f"\n{'='*60}")
    print(f"MODULE 6: RISK MANAGEMENT OVERLAYS")
    print(f"{'='*60}")

    # Sort markets by time and split train/test
    sorted_markets = sorted(markets, key=lambda m: m['started_at'])
    split_idx = int(len(sorted_markets) * 0.66)
    train_markets = sorted_markets[:split_idx]
    test_markets = sorted_markets[split_idx:]

    print(f"  Train: {len(train_markets)} markets, Test: {len(test_markets)} markets")

    # Generate base signals for training set
    train_signals = _generate_base_signals(train_markets)
    print(f"  Training base signals: {len(train_signals)}")

    # Compute training statistics by pattern
    pattern_stats = defaultdict(lambda: {'wins': 0, 'losses': 0, 'pnl_sum': 0.0})
    for m, direction, entry_price, sec in train_signals:
        key = _get_pattern_key(m, entry_price)
        won = (direction == m['final_outcome'])
        if won:
            pnl = (1.0 - entry_price) * 0.98  # after fee
        else:
            pnl = -entry_price

        if won:
            pattern_stats[key]['wins'] += 1
        else:
            pattern_stats[key]['losses'] += 1
        pattern_stats[key]['pnl_sum'] += pnl

    # Compute pattern win rates and edges
    pattern_metrics = {}
    for key, stats in pattern_stats.items():
        total = stats['wins'] + stats['losses']
        if total < 3:
            continue
        wr = stats['wins'] / total
        avg_pnl = stats['pnl_sum'] / total
        pattern_metrics[key] = {'win_rate': wr, 'avg_pnl': avg_pnl, 'count': total}

    print(f"  Patterns with 3+ trades: {len(pattern_metrics)}")

    # Generate test signals
    test_signals = _generate_base_signals(test_markets)
    print(f"  Test base signals: {len(test_signals)}")

    # Run configs
    configs = generate_configs()
    print(f"  Testing {len(configs)} configurations...")

    all_results = []
    trades_by_config = {}

    for i, cfg in enumerate(configs):
        min_wr = cfg['min_win_rate_pct']
        min_edge = cfg['min_edge_pct']

        trades = []
        filtered_out = 0

        for m, direction, entry_price, sec in test_signals:
            key = _get_pattern_key(m, entry_price)
            pm = pattern_metrics.get(key)

            # If no training data for this pattern, skip
            if pm is None:
                filtered_out += 1
                continue

            # Win rate filter
            if pm['win_rate'] < min_wr:
                filtered_out += 1
                continue

            # Edge filter (expected value)
            if pm['avg_pnl'] < min_edge:
                filtered_out += 1
                continue

            trades.append(make_trade(m, sec, entry_price, direction))

        trades_by_config[cfg['config_id']] = trades
        metrics = compute_metrics(trades, config_id=cfg['config_id'])
        metrics['total_signals'] = len(test_signals)
        metrics['filtered_out'] = filtered_out
        metrics['filtered_out_pct'] = round(
            filtered_out / len(test_signals) * 100, 1
        ) if test_signals else 0

        row = {**cfg, **metrics}
        all_results.append(row)

    df = pd.DataFrame(all_results)
    df = df[df['total_bets'] >= MIN_TRADES].reset_index(drop=True)
    df = add_ranking_score(df)

    module_dir = f"{output_dir}/Module_6_Risk_Management"
    save_module_results(df, trades_by_config, MODULE_NAME, module_dir)

    profitable = df[df['total_pnl'] > 0]
    print(f"  Done. {len(df)} configs with {MIN_TRADES}+ trades, {len(profitable)} profitable.")

    return df
