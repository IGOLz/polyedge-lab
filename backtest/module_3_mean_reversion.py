"""
Module 3: Mean Reversion / Pullback Strategy
Detects price spikes, waits for reversion, enters contrarian bet.
Most complex module - supports mid-market exit via profit target or time limit.
"""

import numpy as np
import pandas as pd
from itertools import product
from .data_loader import get_price_at_second, filter_markets
from .engine import make_trade, compute_metrics, add_ranking_score, save_module_results

MODULE_NAME = "Module_3"

SPIKE_THRESHOLDS = [0.65, 0.70, 0.75, 0.80, 0.85, 0.90, 0.95]
SPIKE_LOOKBACKS = [10, 15, 20, 30, 45, 60]
REVERSION_PCTS = [0.03, 0.05, 0.08, 0.10, 0.15, 0.20]
MIN_REVERSION_SECS = [15, 30, 60, 120, 180]
EXIT_TYPES = ['market_end', 'profit_target', 'time_based']
PROFIT_TARGET_PCTS = [0.10, 0.20, 0.30, 0.50]
TIME_LIMITS = [30, 60, 120, 180, 300]

MIN_TRADES = 10


def generate_configs():
    configs = []
    cid = 0

    # Market-end exit configs
    for spike_thr, lookback, rev_pct, min_rev in product(
        [0.70, 0.75, 0.80, 0.85, 0.90, 0.95],
        [15, 30, 60],
        [0.05, 0.08, 0.10, 0.15, 0.20],
        [30, 60, 120],
    ):
        cid += 1
        configs.append({
            'config_id': f"M3_{cid:04d}",
            'spike_threshold': spike_thr,
            'spike_lookback_window': lookback,
            'reversion_reversal_pct': rev_pct,
            'min_reversion_seconds': min_rev,
            'exit_type': 'market_end',
            'exit_profit_target_pct': 0,
            'exit_time_limit_sec': 0,
        })

    # Profit target exit configs (smaller grid)
    for spike_thr, lookback, rev_pct, target_pct in product(
        [0.75, 0.85, 0.95],
        [15, 30, 60],
        [0.05, 0.10, 0.15],
        PROFIT_TARGET_PCTS,
    ):
        cid += 1
        configs.append({
            'config_id': f"M3_{cid:04d}",
            'spike_threshold': spike_thr,
            'spike_lookback_window': lookback,
            'reversion_reversal_pct': rev_pct,
            'min_reversion_seconds': 60,
            'exit_type': 'profit_target',
            'exit_profit_target_pct': target_pct,
            'exit_time_limit_sec': 0,
        })

    # Time-based exit configs (smaller grid)
    for spike_thr, lookback, rev_pct, time_lim in product(
        [0.75, 0.85, 0.95],
        [15, 30, 60],
        [0.05, 0.10, 0.15],
        TIME_LIMITS,
    ):
        cid += 1
        configs.append({
            'config_id': f"M3_{cid:04d}",
            'spike_threshold': spike_thr,
            'spike_lookback_window': lookback,
            'reversion_reversal_pct': rev_pct,
            'min_reversion_seconds': 60,
            'exit_type': 'time_based',
            'exit_profit_target_pct': 0,
            'exit_time_limit_sec': time_lim,
        })

    return configs


def _find_spike(ticks, lookback, spike_threshold):
    """
    Scan first `lookback` seconds for a spike.
    Returns (spike_direction, peak_second, peak_price) or None.
    spike_direction: 'Up' if UP price spiked, 'Down' if DOWN price spiked.
    """
    window = ticks[:lookback]
    valid_mask = ~np.isnan(window)
    if not np.any(valid_mask):
        return None

    valid_prices = window[valid_mask]
    valid_indices = np.where(valid_mask)[0]

    # Check for UP spike
    max_price = np.max(valid_prices)
    if max_price >= spike_threshold:
        peak_idx = valid_indices[np.argmax(valid_prices)]
        return ('Up', int(peak_idx), float(max_price))

    # Check for DOWN spike (UP price drops low = DOWN price is high)
    min_price = np.min(valid_prices)
    if (1.0 - min_price) >= spike_threshold:
        peak_idx = valid_indices[np.argmin(valid_prices)]
        return ('Down', int(peak_idx), float(min_price))

    return None


def _find_reversion(ticks, spike_dir, peak_sec, peak_price, rev_pct, min_rev_sec, total_sec):
    """
    After spike, scan for reversion. Returns (reversion_second, entry_price) or None.
    """
    for sec in range(peak_sec + 1, min(peak_sec + min_rev_sec + 1, total_sec)):
        price = ticks[sec]
        if np.isnan(price):
            continue

        if spike_dir == 'Up':
            # UP spiked high, waiting for it to come back down
            reversion_amount = (peak_price - price) / peak_price
            if reversion_amount >= rev_pct:
                # Entry: bet DOWN (contrarian to the spike that's reverting)
                # Actually, spike was UP, reversion means it's coming down,
                # so the spike might still resolve UP. We bet DOWN = contrarian to original spike.
                entry_price = 1.0 - price  # buy DOWN at current DOWN price
                return (sec, entry_price, 'Down')
        else:
            # DOWN spiked (UP went very low), waiting for UP to recover
            reversion_amount = (price - peak_price) / (1.0 - peak_price) if (1.0 - peak_price) > 0 else 0
            if reversion_amount >= rev_pct:
                entry_price = price  # buy UP at current UP price
                return (sec, entry_price, 'Up')

    return None


def run_single_config(cfg, markets):
    """Run a single mean reversion config against all markets."""
    spike_thr = cfg['spike_threshold']
    lookback = cfg['spike_lookback_window']
    rev_pct = cfg['reversion_reversal_pct']
    min_rev = cfg['min_reversion_seconds']
    exit_type = cfg['exit_type']
    profit_target = cfg['exit_profit_target_pct']
    time_limit = cfg['exit_time_limit_sec']

    trades = []

    for m in markets:
        ticks = m['ticks']
        total = m['total_seconds']

        if lookback >= total:
            continue

        # Step 1: Find spike
        spike = _find_spike(ticks, lookback, spike_thr)
        if spike is None:
            continue

        spike_dir, peak_sec, peak_price = spike

        # Step 2: Find reversion
        rev = _find_reversion(ticks, spike_dir, peak_sec, peak_price, rev_pct, min_rev, total)
        if rev is None:
            continue

        entry_sec, entry_price, direction = rev

        if entry_price <= 0.01 or entry_price >= 0.99:
            continue

        # Step 3: Exit logic
        if exit_type == 'market_end':
            trades.append(make_trade(m, entry_sec, entry_price, direction))

        elif exit_type == 'profit_target':
            target_price = entry_price * (1 + profit_target)
            exit_sec = None
            exit_px = None

            for sec in range(entry_sec + 1, total):
                p = ticks[sec]
                if np.isnan(p):
                    continue
                # Our position's current price
                if direction == 'Up':
                    current_token_price = p
                else:
                    current_token_price = 1.0 - p

                if current_token_price >= target_price:
                    exit_sec = sec
                    exit_px = current_token_price
                    break

            if exit_sec is not None:
                trades.append(make_trade(m, entry_sec, entry_price, direction,
                                        second_exited=exit_sec, exit_price=exit_px))
            else:
                # Hold to resolution if target never hit
                trades.append(make_trade(m, entry_sec, entry_price, direction))

        elif exit_type == 'time_based':
            exit_sec = min(entry_sec + time_limit, total - 1)
            p = get_price_at_second(ticks, exit_sec)
            if p is not None:
                if direction == 'Up':
                    exit_px = p
                else:
                    exit_px = 1.0 - p
                trades.append(make_trade(m, entry_sec, entry_price, direction,
                                        second_exited=exit_sec, exit_price=exit_px))
            else:
                trades.append(make_trade(m, entry_sec, entry_price, direction))

    return trades


def run(markets, output_dir):
    """Run Module 3 backtest."""
    print(f"\n{'='*60}")
    print(f"MODULE 3: MEAN REVERSION / PULLBACK")
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

    module_dir = f"{output_dir}/Module_3_Mean_Reversion"
    save_module_results(df, trades_by_config, MODULE_NAME, module_dir)

    profitable = df[df['total_pnl'] > 0]
    print(f"  Done. {len(df)} configs with {MIN_TRADES}+ trades, {len(profitable)} profitable.")

    return df
