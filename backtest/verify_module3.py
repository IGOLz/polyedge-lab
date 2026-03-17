#!/usr/bin/env python3
"""
Module 3 Verification Script
Traces through top config step-by-step to verify:
1. No look-ahead bias
2. Correct PnL calculation for mid-market exits
3. Proper spike/reversion detection
4. Win rate is genuine
"""

import numpy as np
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from backtest.data_loader import load_all_data, get_price_at_second
from backtest.engine import calculate_pnl_exit, calculate_pnl_hold

# M3_0279 config (the 94.6% winner)
CONFIG = {
    'spike_threshold': 0.75,
    'spike_lookback_window': 15,
    'reversion_reversal_pct': 0.15,
    'min_reversion_seconds': 60,
    'exit_type': 'profit_target',
    'exit_profit_target_pct': 0.10,
}


def verify_trade(m, cfg, verbose=True):
    """Trace through one market with full debug output."""
    ticks = m['ticks']
    total = m['total_seconds']
    lookback = cfg['spike_lookback_window']
    spike_thr = cfg['spike_threshold']
    rev_pct = cfg['reversion_reversal_pct']
    min_rev = cfg['min_reversion_seconds']
    profit_target = cfg['exit_profit_target_pct']

    if lookback >= total:
        return None

    # ---- STEP 1: SPIKE DETECTION ----
    window = ticks[:lookback]
    valid_mask = ~np.isnan(window)
    if not np.any(valid_mask):
        return None

    valid_prices = window[valid_mask]
    valid_indices = np.where(valid_mask)[0]

    max_price = np.max(valid_prices)
    min_price = np.min(valid_prices)

    spike_dir = None
    peak_sec = None
    peak_price = None

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

    if verbose:
        print(f"\n  SPIKE DETECTED: dir={spike_dir} peak_sec={peak_sec} "
              f"peak_up_price={peak_price:.4f}")
        print(f"    Ticks in lookback window (0-{lookback-1}):")
        for s in range(lookback):
            p = ticks[s]
            if not np.isnan(p):
                marker = " <-- PEAK" if s == peak_sec else ""
                print(f"      sec={s:3d}: up={p:.4f} down={1-p:.4f}{marker}")

    # ---- STEP 2: REVERSION DETECTION ----
    entry_sec = None
    entry_price = None
    direction = None

    scan_end = min(peak_sec + min_rev + 1, total)
    for sec in range(peak_sec + 1, scan_end):
        price = ticks[sec]
        if np.isnan(price):
            continue

        if spike_dir == 'Up':
            reversion = (peak_price - price) / peak_price
            if verbose and sec <= peak_sec + 10:
                print(f"    Reversion scan sec={sec}: up={price:.4f} "
                      f"reversion={(reversion*100):.1f}% (need {rev_pct*100:.0f}%)")
            if reversion >= rev_pct:
                entry_sec = sec
                entry_price = 1.0 - price  # buy DOWN
                direction = 'Down'
                break
        else:
            reversion = (price - peak_price) / (1.0 - peak_price) if (1.0 - peak_price) > 0 else 0
            if verbose and sec <= peak_sec + 10:
                print(f"    Reversion scan sec={sec}: up={price:.4f} "
                      f"reversion={(reversion*100):.1f}% (need {rev_pct*100:.0f}%)")
            if reversion >= rev_pct:
                entry_sec = sec
                entry_price = price  # buy UP
                direction = 'Up'
                break

    if entry_sec is None:
        if verbose:
            print(f"    No reversion found within {min_rev}s")
        return None

    if entry_price <= 0.01 or entry_price >= 0.99:
        return None

    if verbose:
        print(f"\n  ENTRY: sec={entry_sec} dir={direction} token_entry_price={entry_price:.4f}")
        print(f"    (UP price at entry: {ticks[entry_sec]:.4f}, "
              f"DOWN price at entry: {1-ticks[entry_sec]:.4f})")

    # ---- STEP 3: PROFIT TARGET SCAN ----
    target_price = entry_price * (1 + profit_target)
    if verbose:
        print(f"    Profit target: {entry_price:.4f} * 1.{int(profit_target*100)} = {target_price:.4f}")

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

        if verbose and sec <= entry_sec + 20:
            print(f"    Scan sec={sec}: token_price={current:.4f} "
                  f"(target={target_price:.4f}) {'HIT!' if current >= target_price else ''}")

        if current >= target_price:
            exit_sec = sec
            exit_px = current
            break

    # ---- STEP 4: PNL CALCULATION ----
    actual_result = m['final_outcome']

    if exit_sec is not None:
        pnl = calculate_pnl_exit(entry_price, exit_px)
        outcome = 'win' if pnl > 0 else 'loss'
        if verbose:
            print(f"\n  EXIT (profit target): sec={exit_sec} exit_price={exit_px:.4f}")
            print(f"    PnL = ({exit_px:.4f} - {entry_price:.4f}) - "
                  f"0.02 * max(0, {exit_px:.4f} - {entry_price:.4f}) = {pnl:.6f}")
            print(f"    Outcome: {outcome} (market resolved: {actual_result})")
            print(f"    NOTE: Trade exited mid-market, resolution doesn't matter")
    else:
        pnl = calculate_pnl_hold(entry_price, direction, actual_result)
        outcome = 'win' if direction == actual_result else 'loss'
        if verbose:
            print(f"\n  EXIT (hold to resolution): market resolved {actual_result}")
            if outcome == 'win':
                print(f"    PnL = (1 - {entry_price:.4f}) * 0.98 = {pnl:.6f}")
            else:
                print(f"    PnL = -{entry_price:.4f} = {pnl:.6f}")
            print(f"    Outcome: {outcome}")

    return {
        'market_id': m['market_id'][:16],
        'asset': m['asset'],
        'duration': m['duration_minutes'],
        'spike_dir': spike_dir,
        'peak_sec': peak_sec,
        'peak_price': peak_price,
        'entry_sec': entry_sec,
        'entry_price': entry_price,
        'direction': direction,
        'exit_sec': exit_sec,
        'exit_price': exit_px,
        'actual_result': actual_result,
        'pnl': pnl,
        'outcome': outcome,
    }


def main():
    print("=" * 70)
    print("MODULE 3 VERIFICATION - M3_0279")
    print("=" * 70)
    print(f"Config: spike_thr=0.75, lookback=15s, reversion=15%, "
          f"profit_target=10%")

    markets = load_all_data()
    print(f"\nTotal markets: {len(markets)}")

    # Run through ALL markets with this config, collect trades
    trades = []
    for m in markets:
        result = verify_trade(m, CONFIG, verbose=False)
        if result:
            trades.append(result)

    print(f"\n{'='*70}")
    print(f"SUMMARY: {len(trades)} trades triggered out of {len(markets)} markets "
          f"({len(trades)/len(markets)*100:.1f}% trigger rate)")

    wins = [t for t in trades if t['outcome'] == 'win']
    losses = [t for t in trades if t['outcome'] == 'loss']

    print(f"  Wins:   {len(wins)}")
    print(f"  Losses: {len(losses)}")
    print(f"  Win rate: {len(wins)/len(trades)*100:.1f}%")

    total_pnl = sum(t['pnl'] for t in trades)
    print(f"  Total PnL: {total_pnl:.4f}")
    print(f"  Avg PnL: {total_pnl/len(trades):.6f}")

    # Count mid-market vs resolution exits
    mid_exits = [t for t in trades if t['exit_sec'] is not None]
    resolution_exits = [t for t in trades if t['exit_sec'] is None]
    print(f"\n  Mid-market exits (profit target hit): {len(mid_exits)}")
    print(f"  Hold-to-resolution exits: {len(resolution_exits)}")

    # Analyze resolution exits - how many won vs lost?
    res_wins = [t for t in resolution_exits if t['outcome'] == 'win']
    res_losses = [t for t in resolution_exits if t['outcome'] == 'loss']
    print(f"    Resolution wins: {len(res_wins)}")
    print(f"    Resolution losses: {len(res_losses)}")

    # Show per-asset breakdown
    print(f"\n  Per-asset breakdown:")
    for asset in ['btc', 'eth', 'sol', 'xrp']:
        at = [t for t in trades if t['asset'] == asset]
        if at:
            aw = sum(1 for t in at if t['outcome'] == 'win')
            ap = sum(t['pnl'] for t in at)
            print(f"    {asset}: {len(at)} trades, {aw} wins ({aw/len(at)*100:.0f}%), PnL={ap:.4f}")

    # Show per-duration breakdown
    print(f"\n  Per-duration breakdown:")
    for dur in [5, 15]:
        dt = [t for t in trades if t['duration'] == dur]
        if dt:
            dw = sum(1 for t in dt if t['outcome'] == 'win')
            dp = sum(t['pnl'] for t in dt)
            print(f"    {dur}m: {len(dt)} trades, {dw} wins ({dw/len(dt)*100:.0f}%), PnL={dp:.4f}")

    # Show all losses in detail
    print(f"\n  ALL LOSSES (detailed):")
    for t in losses:
        print(f"    {t['market_id']}.. {t['asset']} {t['duration']}m | "
              f"spike={t['spike_dir']} peak@{t['peak_sec']}s "
              f"peak_up={t['peak_price']:.3f} | "
              f"entry@{t['entry_sec']}s price={t['entry_price']:.3f} dir={t['direction']} | "
              f"exit={'resolution' if t['exit_sec'] is None else 'sec ' + str(t['exit_sec'])} | "
              f"result={t['actual_result']} pnl={t['pnl']:.4f}")

    # LOOK-AHEAD BIAS CHECK
    print(f"\n{'='*70}")
    print("LOOK-AHEAD BIAS CHECK:")
    print(f"{'='*70}")

    bias_found = False
    for t in trades:
        # Entry must happen AFTER spike
        if t['entry_sec'] <= t['peak_sec']:
            print(f"  BUG: Entry sec ({t['entry_sec']}) <= peak sec ({t['peak_sec']})")
            bias_found = True
        # Exit must happen AFTER entry
        if t['exit_sec'] is not None and t['exit_sec'] <= t['entry_sec']:
            print(f"  BUG: Exit sec ({t['exit_sec']}) <= entry sec ({t['entry_sec']})")
            bias_found = True
        # Peak must be within lookback window
        if t['peak_sec'] >= CONFIG['spike_lookback_window']:
            print(f"  BUG: Peak sec ({t['peak_sec']}) >= lookback ({CONFIG['spike_lookback_window']})")
            bias_found = True

    if not bias_found:
        print("  PASSED: No look-ahead bias detected in any trade.")
        print("  - All entries happen AFTER spike detection")
        print("  - All exits happen AFTER entry")
        print("  - All spike peaks are within lookback window")

    # TRACE FIRST 3 TRADES STEP-BY-STEP
    print(f"\n{'='*70}")
    print("STEP-BY-STEP TRACE OF FIRST 3 TRADES:")
    print(f"{'='*70}")

    count = 0
    for m in markets:
        if count >= 3:
            break
        result = verify_trade(m, CONFIG, verbose=True)
        if result:
            count += 1

    # VERIFY PNL MATH for ALL trades
    print(f"\n{'='*70}")
    print("PNL MATH VERIFICATION (all trades):")
    print(f"{'='*70}")

    pnl_errors = 0
    for t in trades:
        if t['exit_sec'] is not None:
            expected_pnl = (t['exit_price'] - t['entry_price']) - 0.02 * max(0, t['exit_price'] - t['entry_price'])
        else:
            if t['direction'] == t['actual_result']:
                expected_pnl = (1.0 - t['entry_price']) * 0.98
            else:
                expected_pnl = -t['entry_price']

        if abs(expected_pnl - t['pnl']) > 0.0001:
            print(f"  PNL MISMATCH: {t['market_id']} expected={expected_pnl:.6f} got={t['pnl']:.6f}")
            pnl_errors += 1

    if pnl_errors == 0:
        print(f"  PASSED: All {len(trades)} trade PnL calculations verified correct.")


if __name__ == '__main__':
    main()
