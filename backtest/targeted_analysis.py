#!/usr/bin/env python3
"""
Targeted Analysis Script - Three investigations:
1. M3 profit target sweep (10%, 15%, 20%, 25%, 30%)
2. M4 per-asset breakdown (BTC, ETH, SOL, XRP)
3. M4 + time/duration filter combos
"""

import numpy as np
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from backtest.data_loader import load_all_data
from backtest.engine import calculate_pnl_exit, calculate_pnl_hold, compute_metrics, make_trade
from backtest.module_3_mean_reversion import _find_spike, _find_reversion


def run_m3_profit_target_sweep(markets):
    """
    Q1: M3 mean reversion with profit targets 10%-30%.
    Uses best M3 base config (spike_thr=0.75, lookback=15, rev_pct=0.15, min_rev=60).
    """
    print("\n" + "=" * 70)
    print("Q1: M3 PROFIT TARGET SWEEP")
    print("=" * 70)

    base_cfg = {
        'spike_threshold': 0.75,
        'spike_lookback_window': 15,
        'reversion_reversal_pct': 0.15,
        'min_reversion_seconds': 60,
    }

    profit_targets = [0.05, 0.10, 0.15, 0.20, 0.25, 0.30, 0.40, 0.50]

    # Also test market_end (no profit target) as baseline
    print(f"\n  Base config: spike_thr={base_cfg['spike_threshold']}, "
          f"lookback={base_cfg['spike_lookback_window']}s, "
          f"reversion={base_cfg['reversion_reversal_pct']*100:.0f}%, "
          f"min_rev={base_cfg['min_reversion_seconds']}s")

    # First, find all entries (shared across all profit target variants)
    entries = []
    for m in markets:
        ticks = m['ticks']
        total = m['total_seconds']
        lookback = base_cfg['spike_lookback_window']

        if lookback >= total:
            continue

        spike = _find_spike(ticks, lookback, base_cfg['spike_threshold'])
        if spike is None:
            continue

        spike_dir, peak_sec, peak_price = spike
        rev = _find_reversion(ticks, spike_dir, peak_sec, peak_price,
                              base_cfg['reversion_reversal_pct'],
                              base_cfg['min_reversion_seconds'], total)
        if rev is None:
            continue

        entry_sec, entry_price, direction = rev
        if entry_price <= 0.01 or entry_price >= 0.99:
            continue

        entries.append({
            'market': m,
            'entry_sec': entry_sec,
            'entry_price': entry_price,
            'direction': direction,
        })

    print(f"  Total entries found: {len(entries)}")

    # Baseline: hold to market end
    baseline_trades = []
    for e in entries:
        baseline_trades.append(make_trade(
            e['market'], e['entry_sec'], e['entry_price'], e['direction']
        ))
    baseline_metrics = compute_metrics(baseline_trades)

    # Calculate days in dataset for trades/day
    if entries:
        all_dates = set()
        for e in entries:
            all_dates.add(e['market']['started_at'].date())
        total_days = max(1, len(all_dates))
    else:
        total_days = 1

    print(f"  Trading days in dataset: {total_days}")

    print(f"\n  {'Target':>8} | {'Win%':>6} | {'Trades':>6} | {'Total PnL':>10} | "
          f"{'Avg PnL':>8} | {'Sharpe':>7} | {'Mid-Exit%':>9} | {'Trades/Day':>10}")
    print(f"  {'-'*8}-+-{'-'*6}-+-{'-'*6}-+-{'-'*10}-+-{'-'*8}-+-{'-'*7}-+-{'-'*9}-+-{'-'*10}")

    # Baseline row
    print(f"  {'hold':>8} | {baseline_metrics['win_rate_pct']:>5.1f}% | "
          f"{baseline_metrics['total_bets']:>6} | {baseline_metrics['total_pnl']:>10.4f} | "
          f"{baseline_metrics['avg_bet_pnl']:>8.4f} | {baseline_metrics['sharpe_ratio']:>7.4f} | "
          f"{'0%':>9} | {baseline_metrics['total_bets']/total_days:>10.1f}")

    results = []

    for target_pct in profit_targets:
        trades = []
        mid_exits = 0

        for e in entries:
            m = e['market']
            ticks = m['ticks']
            total = m['total_seconds']
            entry_sec = e['entry_sec']
            entry_price = e['entry_price']
            direction = e['direction']

            target_price = entry_price * (1 + target_pct)
            exit_sec = None
            exit_px = None

            for sec in range(entry_sec + 1, total):
                p = ticks[sec]
                if np.isnan(p):
                    continue
                if direction == 'Up':
                    current = p
                else:
                    current = 1.0 - p

                if current >= target_price:
                    exit_sec = sec
                    exit_px = current
                    break

            if exit_sec is not None:
                trades.append(make_trade(m, entry_sec, entry_price, direction,
                                        second_exited=exit_sec, exit_price=exit_px))
                mid_exits += 1
            else:
                trades.append(make_trade(m, entry_sec, entry_price, direction))

        metrics = compute_metrics(trades)
        mid_exit_pct = mid_exits / len(trades) * 100 if trades else 0

        results.append({
            'target_pct': target_pct,
            'metrics': metrics,
            'mid_exit_pct': mid_exit_pct,
        })

        print(f"  {target_pct*100:>7.0f}% | {metrics['win_rate_pct']:>5.1f}% | "
              f"{metrics['total_bets']:>6} | {metrics['total_pnl']:>10.4f} | "
              f"{metrics['avg_bet_pnl']:>8.4f} | {metrics['sharpe_ratio']:>7.4f} | "
              f"{mid_exit_pct:>8.1f}% | {metrics['total_bets']/total_days:>10.1f}")

    # Find sweet spot
    best = max(results, key=lambda r: r['metrics']['total_pnl'])
    print(f"\n  SWEET SPOT: {best['target_pct']*100:.0f}% profit target")
    print(f"    PnL: {best['metrics']['total_pnl']:.4f}, "
          f"Win rate: {best['metrics']['win_rate_pct']:.1f}%, "
          f"Sharpe: {best['metrics']['sharpe_ratio']:.4f}")

    # Also show best by Sharpe
    best_sharpe = max(results, key=lambda r: r['metrics']['sharpe_ratio'])
    if best_sharpe['target_pct'] != best['target_pct']:
        print(f"  BEST SHARPE: {best_sharpe['target_pct']*100:.0f}% profit target")
        print(f"    PnL: {best_sharpe['metrics']['total_pnl']:.4f}, "
              f"Win rate: {best_sharpe['metrics']['win_rate_pct']:.1f}%, "
              f"Sharpe: {best_sharpe['metrics']['sharpe_ratio']:.4f}")


def run_m4_asset_breakdown(markets):
    """
    Q2: M4_0128 performance broken down by asset.
    """
    print("\n" + "=" * 70)
    print("Q2: M4_0128 PER-ASSET BREAKDOWN")
    print("=" * 70)

    # M4_0128 config
    cfg = {
        'strategy_type': 'combined',
        'min_spread': 0.05,
        'max_spread': 0.50,
        'volatility_window_sec': 10,
        'volatility_threshold': 0.05,
        'volatility_direction': 'high',
        'eval_second': 30,
    }

    print(f"\n  Config: combined, min_spread={cfg['min_spread']}, max_spread={cfg['max_spread']}, "
          f"vol_win={cfg['volatility_window_sec']}s, vol_thr={cfg['volatility_threshold']}, "
          f"vol_dir={cfg['volatility_direction']}, eval_sec={cfg['eval_second']}")

    # Run strategy on all markets, collect trades with asset info
    all_trades = []
    for m in markets:
        ticks = m['ticks']
        total = m['total_seconds']
        eval_sec = cfg['eval_second']

        if eval_sec >= total:
            continue

        price = ticks[eval_sec] if eval_sec < len(ticks) else np.nan
        if np.isnan(price):
            continue

        deviation = abs(price - 0.50)
        if deviation < 0.08:  # BASE_DEVIATION
            continue

        # Spread filter
        spread = abs(2 * price - 1)
        if spread < cfg['min_spread'] or spread > cfg['max_spread']:
            continue

        # Volatility filter
        vol_win = cfg['volatility_window_sec']
        start = max(0, eval_sec - vol_win)
        segment = ticks[start:eval_sec + 1]
        valid = segment[~np.isnan(segment)]
        if len(valid) < 3:
            continue
        vol = float(np.std(valid))
        if vol < cfg['volatility_threshold']:
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

        all_trades.append(make_trade(m, eval_sec, entry_price, direction))

    print(f"\n  Total trades: {len(all_trades)}")

    # Overall metrics
    overall = compute_metrics(all_trades)
    print(f"  Overall: Win%={overall['win_rate_pct']:.1f}, PnL={overall['total_pnl']:.4f}, "
          f"Sharpe={overall['sharpe_ratio']:.4f}")

    # Per-asset breakdown
    assets = ['btc', 'eth', 'sol', 'xrp']
    print(f"\n  {'Asset':>6} | {'Trades':>6} | {'Wins':>5} | {'Win%':>6} | {'Total PnL':>10} | "
          f"{'Avg PnL':>8} | {'Sharpe':>7} | {'Profit Factor':>13} | {'Max DD':>7}")
    print(f"  {'-'*6}-+-{'-'*6}-+-{'-'*5}-+-{'-'*6}-+-{'-'*10}-+-{'-'*8}-+-{'-'*7}-+-{'-'*13}-+-{'-'*7}")

    asset_results = {}
    for asset in assets:
        asset_trades = [t for t in all_trades if t.asset == asset]
        if not asset_trades:
            print(f"  {asset:>6} | {'--':>6} | {'--':>5} | {'--':>6} | {'--':>10} | "
                  f"{'--':>8} | {'--':>7} | {'--':>13} | {'--':>7}")
            continue

        m = compute_metrics(asset_trades)
        asset_results[asset] = m
        print(f"  {asset:>6} | {m['total_bets']:>6} | {m['wins']:>5} | {m['win_rate_pct']:>5.1f}% | "
              f"{m['total_pnl']:>10.4f} | {m['avg_bet_pnl']:>8.4f} | {m['sharpe_ratio']:>7.4f} | "
              f"{m['profit_factor']:>13.4f} | {m['max_drawdown']:>7.4f}")

    # Per-duration per-asset breakdown
    print(f"\n  Per-duration per-asset:")
    print(f"  {'Asset':>6} {'Dur':>4} | {'Trades':>6} | {'Wins':>5} | {'Win%':>6} | {'PnL':>10}")
    print(f"  {'-'*6} {'-'*4}-+-{'-'*6}-+-{'-'*5}-+-{'-'*6}-+-{'-'*10}")

    for asset in assets:
        for dur in [5, 15]:
            dt = [t for t in all_trades if t.asset == asset and t.duration_minutes == dur]
            if not dt:
                print(f"  {asset:>6} {dur:>3}m | {0:>6} |   --  |   --   |    --")
                continue
            wins = sum(1 for t in dt if t.outcome == 'win')
            pnl = sum(t.pnl for t in dt)
            wr = wins / len(dt) * 100
            print(f"  {asset:>6} {dur:>3}m | {len(dt):>6} | {wins:>5} | {wr:>5.1f}% | {pnl:>10.4f}")

    # Identify which asset is driving the edge
    print("\n  ANALYSIS:")
    if asset_results:
        best_asset = max(asset_results.items(), key=lambda x: x[1]['total_pnl'])
        worst_asset = min(asset_results.items(), key=lambda x: x[1]['total_pnl'])
        print(f"  Best asset: {best_asset[0]} (PnL={best_asset[1]['total_pnl']:.4f}, "
              f"Win%={best_asset[1]['win_rate_pct']:.1f}%)")
        print(f"  Worst asset: {worst_asset[0]} (PnL={worst_asset[1]['total_pnl']:.4f}, "
              f"Win%={worst_asset[1]['win_rate_pct']:.1f}%)")

        # Is one asset driving >50% of PnL?
        total_pnl = sum(v['total_pnl'] for v in asset_results.values())
        for asset, m in sorted(asset_results.items(), key=lambda x: -x[1]['total_pnl']):
            pct = m['total_pnl'] / total_pnl * 100 if total_pnl != 0 else 0
            print(f"    {asset}: {pct:.1f}% of total PnL")


def run_m4_time_filter_combos(markets):
    """
    Q3: M4 with time and duration filter variations.
    """
    print("\n" + "=" * 70)
    print("Q3: M4 + TIME/DURATION FILTER COMBOS")
    print("=" * 70)

    # Base M4_0128 config
    base_cfg = {
        'min_spread': 0.05,
        'max_spread': 0.50,
        'volatility_window_sec': 10,
        'volatility_threshold': 0.05,
        'volatility_direction': 'high',
        'base_deviation': 0.08,
    }

    def run_m4_variant(markets_subset, eval_second, label, vol_threshold=None, vol_window=None):
        """Run M4 variant and return metrics."""
        vt = vol_threshold if vol_threshold is not None else base_cfg['volatility_threshold']
        vw = vol_window if vol_window is not None else base_cfg['volatility_window_sec']

        trades = []
        for m in markets_subset:
            ticks = m['ticks']
            total = m['total_seconds']

            if eval_second >= total:
                continue

            price = ticks[eval_second] if eval_second < len(ticks) else np.nan
            if np.isnan(price):
                continue

            deviation = abs(price - 0.50)
            if deviation < base_cfg['base_deviation']:
                continue

            spread = abs(2 * price - 1)
            if spread < base_cfg['min_spread'] or spread > base_cfg['max_spread']:
                continue

            start = max(0, eval_second - vw)
            segment = ticks[start:eval_second + 1]
            valid = segment[~np.isnan(segment)]
            if len(valid) < 3:
                continue
            vol = float(np.std(valid))
            if vol < vt:
                continue

            if price > 0.50:
                direction = 'Down'
                entry_price = 1.0 - price
            else:
                direction = 'Up'
                entry_price = price

            if entry_price <= 0.01 or entry_price >= 0.99:
                continue

            trades.append(make_trade(m, eval_second, entry_price, direction))

        return trades, compute_metrics(trades) if trades else None

    def print_result(label, trades, metrics):
        if metrics is None or metrics['total_bets'] == 0:
            print(f"  {label:<45} | {'No trades':>6}")
            return
        print(f"  {label:<45} | {metrics['total_bets']:>6} | {metrics['win_rate_pct']:>5.1f}% | "
              f"{metrics['total_pnl']:>10.4f} | {metrics['avg_bet_pnl']:>8.4f} | "
              f"{metrics['sharpe_ratio']:>7.4f} | {metrics['profit_factor']:>7.4f}")

    print(f"\n  {'Variant':<45} | {'Trades':>6} | {'Win%':>6} | {'Total PnL':>10} | "
          f"{'Avg PnL':>8} | {'Sharpe':>7} | {'PF':>7}")
    print(f"  {'-'*45}-+-{'-'*6}-+-{'-'*6}-+-{'-'*10}-+-{'-'*8}-+-{'-'*7}-+-{'-'*7}")

    # --- Section A: Duration filters ---
    m5 = [m for m in markets if m['duration_minutes'] == 5]
    m15 = [m for m in markets if m['duration_minutes'] == 15]

    trades, metrics = run_m4_variant(markets, 30, "BASELINE (all markets, eval@30)")
    print_result("BASELINE (all markets, eval@30)", trades, metrics)

    trades, metrics = run_m4_variant(m5, 30, "5m markets only, eval@30")
    print_result("5m markets only, eval@30", trades, metrics)

    trades, metrics = run_m4_variant(m15, 30, "15m markets only, eval@30")
    print_result("15m markets only, eval@30", trades, metrics)

    print()

    # --- Section B: Time-of-entry variants (5m markets) ---
    print("  --- Entry timing variants (5m markets) ---")

    for eval_sec in [10, 15, 20, 25, 30, 45, 60]:
        label = f"5m, eval@{eval_sec}s"
        trades, metrics = run_m4_variant(m5, eval_sec, label)
        print_result(label, trades, metrics)

    print()

    # --- Section C: Time-of-entry variants (15m markets) ---
    print("  --- Entry timing variants (15m markets) ---")

    for eval_sec in [10, 15, 20, 25, 30, 45, 60, 90, 120]:
        label = f"15m, eval@{eval_sec}s"
        trades, metrics = run_m4_variant(m15, eval_sec, label)
        print_result(label, trades, metrics)

    print()

    # --- Section D: Relaxed volatility for 15m markets ---
    print("  --- 15m with relaxed volatility thresholds ---")

    for vt in [0.01, 0.02, 0.03, 0.04, 0.05]:
        for vw in [10, 20, 30, 60]:
            label = f"15m, eval@30, vol_thr={vt}, vol_win={vw}s"
            trades, metrics = run_m4_variant(m15, 30, label,
                                             vol_threshold=vt, vol_window=vw)
            if metrics and metrics['total_bets'] >= 5:
                print_result(label, trades, metrics)

    print()

    # --- Section E: Relaxed volatility for 15m with later eval ---
    print("  --- 15m with relaxed vol + later eval ---")

    for eval_sec in [60, 90, 120]:
        for vt in [0.01, 0.02, 0.03]:
            for vw in [20, 30, 60]:
                label = f"15m, eval@{eval_sec}, vol_thr={vt}, vol_win={vw}s"
                trades, metrics = run_m4_variant(m15, eval_sec, label,
                                                 vol_threshold=vt, vol_window=vw)
                if metrics and metrics['total_bets'] >= 10:
                    print_result(label, trades, metrics)

    print()

    # --- Section F: Relaxed base deviation for 15m ---
    print("  --- 15m with lower base deviation ---")

    for base_dev in [0.04, 0.05, 0.06]:
        old_dev = base_cfg['base_deviation']
        base_cfg['base_deviation'] = base_dev
        for eval_sec in [30, 60, 120]:
            for vt in [0.01, 0.02, 0.03]:
                label = f"15m, eval@{eval_sec}, dev={base_dev}, vol_thr={vt}"
                trades, metrics = run_m4_variant(m15, eval_sec, label,
                                                 vol_threshold=vt)
                if metrics and metrics['total_bets'] >= 10:
                    print_result(label, trades, metrics)
        base_cfg['base_deviation'] = old_dev


def main():
    print("=" * 70)
    print("TARGETED ANALYSIS")
    print("=" * 70)

    markets = load_all_data()
    print(f"\nTotal markets loaded: {len(markets)}")

    run_m3_profit_target_sweep(markets)
    run_m4_asset_breakdown(markets)
    run_m4_time_filter_combos(markets)

    print("\n" + "=" * 70)
    print("ANALYSIS COMPLETE")
    print("=" * 70)


if __name__ == '__main__':
    main()
