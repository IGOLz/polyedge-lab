"""
Module 7: Composite Strategies
Combines top-performing configs from Modules 1-6 with AND/OR logic.
Tests multi-signal ensembles and signal + filter combinations.
"""

import numpy as np
import pandas as pd
from itertools import product
from .data_loader import get_price_at_second, filter_markets
from .engine import (
    make_trade, compute_metrics, add_ranking_score,
    save_module_results, save_trade_log,
)

MODULE_NAME = "Module_7"
MIN_TRADES = 10


# --- Signal functions (simplified versions of each module's logic) ---

def _signal_price_threshold(ticks, total, sec_min, sec_max, threshold):
    """Module 1: price threshold contrarian signal."""
    effective_max = min(sec_max, total - 1)
    for sec in range(sec_min, effective_max + 1):
        price = ticks[sec] if sec < len(ticks) else np.nan
        if np.isnan(price):
            continue
        if price >= threshold:
            return sec, 'Down', 1.0 - price
        if (1.0 - price) >= threshold:
            return sec, 'Up', price
    return None


def _signal_deviation(ticks, total, sec_min, sec_max, deviation):
    """Module 1: deviation contrarian signal."""
    effective_max = min(sec_max, total - 1)
    for sec in range(sec_min, effective_max + 1):
        price = ticks[sec] if sec < len(ticks) else np.nan
        if np.isnan(price):
            continue
        if abs(price - 0.50) >= deviation:
            if price > 0.50:
                return sec, 'Down', 1.0 - price
            else:
                return sec, 'Up', price
    return None


def _signal_momentum(ticks, total, window, vel_thr, eval_sec):
    """Module 2: momentum contrarian signal."""
    if eval_sec >= total or eval_sec - window < 0:
        return None
    p_now = get_price_at_second(ticks, eval_sec)
    p_prev = get_price_at_second(ticks, eval_sec - window)
    if p_now is None or p_prev is None:
        return None
    velocity = (p_now - p_prev) / window
    if abs(velocity) < vel_thr:
        return None
    if velocity > 0:
        return eval_sec, 'Down', 1.0 - p_now
    else:
        return eval_sec, 'Up', p_now


def _signal_mean_reversion(ticks, total, spike_thr, lookback, rev_pct):
    """Module 3: mean reversion signal (simplified, hold to resolution)."""
    if lookback >= total:
        return None
    window = ticks[:lookback]
    valid_mask = ~np.isnan(window)
    if not np.any(valid_mask):
        return None
    valid_prices = window[valid_mask]
    valid_indices = np.where(valid_mask)[0]

    max_price = np.max(valid_prices)
    min_price = np.min(valid_prices)
    spike_dir = None
    peak_sec = 0
    peak_price = 0

    if max_price >= spike_thr:
        spike_dir = 'Up'
        peak_sec = int(valid_indices[np.argmax(valid_prices)])
        peak_price = float(max_price)
    elif (1.0 - min_price) >= spike_thr:
        spike_dir = 'Down'
        peak_sec = int(valid_indices[np.argmin(valid_prices)])
        peak_price = float(min_price)
    else:
        return None

    for sec in range(peak_sec + 1, total):
        p = ticks[sec]
        if np.isnan(p):
            continue
        if spike_dir == 'Up':
            reversion = (peak_price - p) / peak_price if peak_price > 0 else 0
            if reversion >= rev_pct:
                entry_price = 1.0 - p
                return sec, 'Down', entry_price
        else:
            reversion = (p - peak_price) / (1.0 - peak_price) if (1.0 - peak_price) > 0 else 0
            if reversion >= rev_pct:
                return sec, 'Up', p

    return None


def _filter_volatility(ticks, eval_sec, vol_window, vol_thr, vol_dir):
    """Module 4: volatility filter. Returns True if condition met."""
    start = max(0, eval_sec - vol_window)
    segment = ticks[start:eval_sec + 1]
    valid = segment[~np.isnan(segment)]
    if len(valid) < 3:
        return False
    vol = float(np.std(valid))
    if vol_dir == 'high':
        return vol >= vol_thr
    else:
        return vol <= vol_thr


def _filter_time(sec, total, phase, early_cutoff=0.20, late_start=0.80, min_remaining=30):
    """Module 5: time filter. Returns True if second is in valid phase."""
    remaining = total - sec
    if remaining < min_remaining:
        return False
    pct = sec / total
    if phase == 'early':
        return pct <= early_cutoff
    elif phase == 'late':
        return pct >= late_start
    elif phase == 'middle':
        return early_cutoff <= pct <= late_start
    return True  # 'all'


def generate_composite_configs(module_results):
    """
    Generate composite configs combining top results from modules 1-6.
    module_results: dict of module_name -> DataFrame of results.
    """
    configs = []
    cid = 0

    # Extract top parameters from each module
    def _top_params(df, n=3):
        """Get params from top n configs of a module."""
        if df is None or df.empty:
            return []
        return df.head(n).to_dict('records')

    m1_top = _top_params(module_results.get('m1'))
    m2_top = _top_params(module_results.get('m2'))
    m3_top = _top_params(module_results.get('m3'))
    m4_top = _top_params(module_results.get('m4'))
    m5_top = _top_params(module_results.get('m5'))

    # Strategy 1: Price threshold + Momentum (AND)
    for m1, m2 in product(m1_top[:3], m2_top[:3]):
        cid += 1
        configs.append({
            'config_id': f"M7_{cid:04d}",
            'strategy_name': 'threshold_AND_momentum',
            'combine_logic': 'AND',
            'primary': m1,
            'secondary': m2,
            'vol_filter': None,
            'time_filter': None,
        })

    # Strategy 2: Mean reversion + Volatility filter (AND)
    for m3, m4 in product(m3_top[:3], m4_top[:3]):
        cid += 1
        configs.append({
            'config_id': f"M7_{cid:04d}",
            'strategy_name': 'mean_rev_AND_volatility',
            'combine_logic': 'AND',
            'primary': m3,
            'secondary': None,
            'vol_filter': m4,
            'time_filter': None,
        })

    # Strategy 3: Momentum + Time filter
    for m2, m5 in product(m2_top[:3], m5_top[:3]):
        cid += 1
        configs.append({
            'config_id': f"M7_{cid:04d}",
            'strategy_name': 'momentum_AND_time',
            'combine_logic': 'AND',
            'primary': m2,
            'secondary': None,
            'vol_filter': None,
            'time_filter': m5,
        })

    # Strategy 4: Price threshold + Volatility filter
    for m1, m4 in product(m1_top[:3], m4_top[:3]):
        cid += 1
        configs.append({
            'config_id': f"M7_{cid:04d}",
            'strategy_name': 'threshold_AND_volatility',
            'combine_logic': 'AND',
            'primary': m1,
            'secondary': None,
            'vol_filter': m4,
            'time_filter': None,
        })

    # Strategy 5: Momentum + Volatility + Time (triple)
    for m2, m4, m5 in product(m2_top[:2], m4_top[:2], m5_top[:2]):
        cid += 1
        configs.append({
            'config_id': f"M7_{cid:04d}",
            'strategy_name': 'momentum_vol_time_triple',
            'combine_logic': 'AND',
            'primary': m2,
            'secondary': None,
            'vol_filter': m4,
            'time_filter': m5,
        })

    # Strategy 6: Price threshold OR Momentum (OR = either signal triggers)
    for m1, m2 in product(m1_top[:2], m2_top[:2]):
        cid += 1
        configs.append({
            'config_id': f"M7_{cid:04d}",
            'strategy_name': 'threshold_OR_momentum',
            'combine_logic': 'OR',
            'primary': m1,
            'secondary': m2,
            'vol_filter': None,
            'time_filter': None,
        })

    # Strategy 7: Mean reversion + Time filter
    for m3, m5 in product(m3_top[:3], m5_top[:3]):
        cid += 1
        configs.append({
            'config_id': f"M7_{cid:04d}",
            'strategy_name': 'mean_rev_AND_time',
            'combine_logic': 'AND',
            'primary': m3,
            'secondary': None,
            'vol_filter': None,
            'time_filter': m5,
        })

    return configs


def _get_signal(signal_type, m, params):
    """Get signal from a module's parameters."""
    ticks = m['ticks']
    total = m['total_seconds']

    if 'signal_type' in params:
        # Module 1
        sec_min = params.get('entry_second_min', 1)
        sec_max = params.get('entry_second_max', 60)
        thr = params.get('threshold', 0.70)
        if params['signal_type'] == 'price_threshold':
            return _signal_price_threshold(ticks, total, sec_min, sec_max, thr)
        else:
            return _signal_deviation(ticks, total, sec_min, sec_max, thr)

    elif 'momentum_window_sec' in params:
        # Module 2
        return _signal_momentum(
            ticks, total,
            params['momentum_window_sec'],
            params['velocity_threshold'],
            params['eval_second'],
        )

    elif 'spike_threshold' in params:
        # Module 3
        return _signal_mean_reversion(
            ticks, total,
            params['spike_threshold'],
            params['spike_lookback_window'],
            params['reversion_reversal_pct'],
        )

    return None


def run_single_composite(cfg, markets):
    """Run a single composite strategy config."""
    primary = cfg['primary']
    secondary = cfg['secondary']
    vol_filter = cfg['vol_filter']
    time_filter = cfg['time_filter']
    logic = cfg['combine_logic']

    trades = []

    for m in markets:
        ticks = m['ticks']
        total = m['total_seconds']

        # Get primary signal
        sig1 = _get_signal('primary', m, primary)

        if logic == 'AND':
            if sig1 is None:
                continue

            sec, direction, entry_price = sig1

            # Check secondary signal if present
            if secondary is not None:
                sig2 = _get_signal('secondary', m, secondary)
                if sig2 is None:
                    continue
                # Both must agree on direction
                if sig2[1] != direction:
                    continue

            # Check volatility filter
            if vol_filter is not None:
                vol_win = vol_filter.get('volatility_window_sec', 15)
                vol_thr = vol_filter.get('volatility_threshold', 0.03)
                vol_dir = vol_filter.get('volatility_direction', 'high')
                if vol_win > 0 and vol_thr > 0:
                    if not _filter_volatility(ticks, sec, vol_win, vol_thr, vol_dir):
                        continue

            # Check time filter
            if time_filter is not None:
                phase = time_filter.get('market_phase', 'all')
                early = time_filter.get('early_cutoff_pct', 0.20)
                late = time_filter.get('late_start_pct', 0.80)
                min_rem = time_filter.get('min_seconds_remaining', 30)
                if not _filter_time(sec, total, phase, early, late, min_rem):
                    continue

        elif logic == 'OR':
            # Either signal triggers
            sig2 = _get_signal('secondary', m, secondary) if secondary else None

            if sig1 is None and sig2 is None:
                continue

            # Use whichever signal fired (prefer primary)
            if sig1 is not None:
                sec, direction, entry_price = sig1
            else:
                sec, direction, entry_price = sig2

        else:
            continue

        if entry_price <= 0.01 or entry_price >= 0.99:
            continue

        trades.append(make_trade(m, sec, entry_price, direction))

    return trades


def run(markets, output_dir, module_results):
    """Run Module 7 composite backtest."""
    print(f"\n{'='*60}")
    print(f"MODULE 7: COMPOSITE STRATEGIES")
    print(f"{'='*60}")

    configs = generate_composite_configs(module_results)
    if not configs:
        print("  No composite configs generated (need results from modules 1-6).")
        return pd.DataFrame()

    print(f"  Testing {len(configs)} composite configurations...")

    all_results = []
    trades_by_config = {}

    for i, cfg in enumerate(configs):
        if (i + 1) % 20 == 0:
            print(f"  Progress: {i+1}/{len(configs)}")

        trades = run_single_composite(cfg, markets)
        trades_by_config[cfg['config_id']] = trades

        metrics = compute_metrics(trades, config_id=cfg['config_id'])
        row = {
            'config_id': cfg['config_id'],
            'strategy_name': cfg['strategy_name'],
            'combine_logic': cfg['combine_logic'],
            'has_vol_filter': cfg['vol_filter'] is not None,
            'has_time_filter': cfg['time_filter'] is not None,
            **metrics,
        }
        all_results.append(row)

    df = pd.DataFrame(all_results)
    df = df[df['total_bets'] >= MIN_TRADES].reset_index(drop=True)
    df = add_ranking_score(df)

    module_dir = f"{output_dir}/Module_7_Composite_Strategies"
    save_module_results(df, trades_by_config, MODULE_NAME, module_dir, top_n=20)

    # Save trade logs for top 3 strategies
    if not df.empty:
        final_dir = f"{output_dir}/FINAL_RESULTS"
        os.makedirs(final_dir, exist_ok=True)
        for rank, (_, row) in enumerate(df.head(3).iterrows(), 1):
            cid = row['config_id']
            if cid in trades_by_config:
                save_trade_log(
                    trades_by_config[cid],
                    f"{final_dir}/Trade_Log_Rank{rank}_{cid}.csv"
                )

    profitable = df[df['total_pnl'] > 0]
    print(f"  Done. {len(df)} configs with {MIN_TRADES}+ trades, {len(profitable)} profitable.")

    return df


# Need os for makedirs
import os
