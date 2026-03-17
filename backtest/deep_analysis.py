#!/usr/bin/env python3
"""
Deep analysis script addressing all open questions:
1. Side-by-side comparison of top 3 strategies
2. M4_0128 duration investigation
3. Full M3 trade log
4. Overfitting test (re-rank on Q3+Q4 only)
5. M3 win rate vs PnL gap explanation
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import numpy as np
from collections import defaultdict

from backtest.data_loader import load_all_data, get_price_at_second
from backtest.engine import make_trade, compute_metrics, calculate_pnl_exit, calculate_pnl_hold, save_trade_log


# ── Strategy reproductions ─────────────────────────────────────────────

def run_m3_0279(markets):
    """M3 best: spike=0.75, lookback=15, reversion=15%, profit_target=10%"""
    trades = []
    for m in markets:
        ticks = m['ticks']
        total = m['total_seconds']
        if 15 >= total:
            continue
        # Spike
        w = ticks[:15]
        vm = ~np.isnan(w)
        if not np.any(vm):
            continue
        vp = w[vm]
        vi = np.where(vm)[0]
        spike_dir = peak_sec = peak_price = None
        mx = np.max(vp)
        mn = np.min(vp)
        if mx >= 0.75:
            spike_dir, peak_sec, peak_price = 'Up', int(vi[np.argmax(vp)]), float(mx)
        elif (1.0 - mn) >= 0.75:
            spike_dir, peak_sec, peak_price = 'Down', int(vi[np.argmin(vp)]), float(mn)
        if spike_dir is None:
            continue
        # Reversion
        entry_sec = entry_price = direction = None
        for s in range(peak_sec + 1, min(peak_sec + 61, total)):
            p = ticks[s]
            if np.isnan(p):
                continue
            if spike_dir == 'Up':
                rev = (peak_price - p) / peak_price
                if rev >= 0.15:
                    entry_sec, entry_price, direction = s, 1.0 - p, 'Down'
                    break
            else:
                rev = (p - peak_price) / (1.0 - peak_price) if (1.0 - peak_price) > 0 else 0
                if rev >= 0.15:
                    entry_sec, entry_price, direction = s, p, 'Up'
                    break
        if entry_sec is None or entry_price <= 0.01 or entry_price >= 0.99:
            continue
        # Profit target
        target = entry_price * 1.10
        exit_sec = exit_px = None
        for s in range(entry_sec + 1, total):
            p = ticks[s]
            if np.isnan(p):
                continue
            cur = p if direction == 'Up' else 1.0 - p
            if cur >= target:
                exit_sec, exit_px = s, cur
                break
        if exit_sec is not None:
            trades.append(make_trade(m, entry_sec, entry_price, direction,
                                     second_exited=exit_sec, exit_price=exit_px))
        else:
            trades.append(make_trade(m, entry_sec, entry_price, direction))
    return trades


def run_m2_0064(markets):
    """M2 best PnL: window=5, vel_thr=0.003, eval=10, no accel"""
    trades = []
    for m in markets:
        ticks = m['ticks']
        total = m['total_seconds']
        if 10 >= total:
            continue
        p_now = get_price_at_second(ticks, 10)
        p_prev = get_price_at_second(ticks, 5)
        if p_now is None or p_prev is None:
            continue
        vel = (p_now - p_prev) / 5
        if abs(vel) < 0.003:
            continue
        if vel > 0:
            direction, entry_price = 'Down', 1.0 - p_now
        else:
            direction, entry_price = 'Up', p_now
        if entry_price <= 0.01 or entry_price >= 0.99:
            continue
        trades.append(make_trade(m, 10, entry_price, direction))
    return trades


def run_m4_0128(markets):
    """M4 best ranking: combined, min_spread=0.05, max_spread=0.50,
    vol_window=10, vol_thr=0.05, vol_dir=high, eval=30"""
    trades = []
    for m in markets:
        ticks = m['ticks']
        total = m['total_seconds']
        if 30 >= total:
            continue
        price = ticks[30] if 30 < len(ticks) else np.nan
        if np.isnan(price):
            continue
        dev = abs(price - 0.50)
        if dev < 0.08:
            continue
        spread = abs(2 * price - 1)
        if spread < 0.05 or spread > 0.50:
            continue
        seg = ticks[20:31]
        valid = seg[~np.isnan(seg)]
        if len(valid) < 3:
            continue
        vol = float(np.std(valid))
        if vol < 0.05:
            continue
        if price > 0.50:
            direction, entry_price = 'Down', 1.0 - price
        else:
            direction, entry_price = 'Up', price
        if entry_price <= 0.01 or entry_price >= 0.99:
            continue
        trades.append(make_trade(m, 30, entry_price, direction))
    return trades


# ── Analysis helpers ────────────────────────────────────────────────────

def print_strategy_metrics(name, trades):
    """Print full metrics for a strategy."""
    m = compute_metrics(trades)
    print(f"\n  {name}")
    print(f"  {'─'*60}")
    print(f"  Trades: {m['total_bets']}  |  Wins: {m['wins']}  |  Losses: {m['losses']}")
    print(f"  Win Rate:       {m['win_rate_pct']:>7.1f}%")
    print(f"  Total PnL:      {m['total_pnl']:>+10.4f}")
    print(f"  Avg PnL/trade:  {m['avg_bet_pnl']:>+10.6f}")
    print(f"  Profit Factor:  {m['profit_factor']:>10.4f}")
    print(f"  Expected Value: {m['expected_value']:>+10.6f}")
    print(f"  Sharpe:         {m['sharpe_ratio']:>10.4f}")
    print(f"  Sortino:        {m['sortino_ratio']:>10.4f}")
    print(f"  Max Drawdown:   {m['max_drawdown']:>10.4f}")
    print(f"  Profitable Assets:    {m['pct_profitable_assets']:.0f}%")
    print(f"  Profitable Durations: {m['pct_profitable_durations']:.0f}%")
    print(f"  Consistency:    {m['consistency_score']:>10.2f}")
    print(f"  Q1 PnL: {m['q1_pnl']:>+8.4f}  Q2: {m['q2_pnl']:>+8.4f}  "
          f"Q3: {m['q3_pnl']:>+8.4f}  Q4: {m['q4_pnl']:>+8.4f}")
    return m


def print_duration_breakdown(name, trades):
    """Break down trades by duration."""
    by_dur = defaultdict(list)
    for t in trades:
        by_dur[t.duration_minutes].append(t)

    print(f"\n  {name} - Duration Breakdown:")
    for dur in sorted(by_dur.keys()):
        tt = by_dur[dur]
        w = sum(1 for t in tt if t.outcome == 'win')
        pnl = sum(t.pnl for t in tt)
        wr = w / len(tt) * 100 if tt else 0
        print(f"    {dur:>2}m: {len(tt):>5} trades | {w:>4} wins ({wr:>5.1f}%) | PnL={pnl:>+10.4f}")


def print_asset_breakdown(name, trades):
    """Break down trades by asset."""
    by_asset = defaultdict(list)
    for t in trades:
        by_asset[t.asset].append(t)

    print(f"\n  {name} - Asset Breakdown:")
    for asset in sorted(by_asset.keys()):
        tt = by_asset[asset]
        w = sum(1 for t in tt if t.outcome == 'win')
        pnl = sum(t.pnl for t in tt)
        wr = w / len(tt) * 100 if tt else 0
        print(f"    {asset:>4}: {len(tt):>5} trades | {w:>4} wins ({wr:>5.1f}%) | PnL={pnl:>+10.4f}")


def run_overfitting_test(markets, all_strategies):
    """Re-evaluate strategies on Q3+Q4 data only (last ~40% of markets by time)."""
    sorted_markets = sorted(markets, key=lambda m: m['started_at'])
    split = int(len(sorted_markets) * 0.60)
    test_markets = sorted_markets[split:]

    print(f"\n  Test set (Q3+Q4): {len(test_markets)} markets "
          f"(from {test_markets[0]['started_at']} to {test_markets[-1]['ended_at']})")

    for name, runner in all_strategies:
        trades = runner(test_markets)
        m = compute_metrics(trades)
        print(f"    {name:<25} | {m['total_bets']:>5} trades | "
              f"Win%={m['win_rate_pct']:>5.1f} | "
              f"PnL={m['total_pnl']:>+9.4f} | "
              f"Sharpe={m['sharpe_ratio']:>+7.4f} | "
              f"{'PROFITABLE' if m['total_pnl'] > 0 else 'UNPROFITABLE'}")


def main():
    markets = load_all_data()

    # ── 1. Side-by-side comparison ──────────────────────────────────────
    print("\n" + "=" * 70)
    print("  1. SIDE-BY-SIDE COMPARISON")
    print("=" * 70)

    m3_trades = run_m3_0279(markets)
    m2_trades = run_m2_0064(markets)
    m4_trades = run_m4_0128(markets)

    m3_m = print_strategy_metrics("M3_0279 (Mean Reversion, profit_target=10%)", m3_trades)
    m2_m = print_strategy_metrics("M2_0064 (Momentum, window=5, eval=10)", m2_trades)
    m4_m = print_strategy_metrics("M4_0128 (Volatility Combined, vol>0.05)", m4_trades)

    # ── Comparison table ────────────────────────────────────────────────
    print(f"\n  {'─'*110}")
    print(f"  {'Strategy':<35} | {'Win%':>5} | {'PnL':>9} | {'Sharpe':>7} | "
          f"{'Sortino':>7} | {'Q1':>8} | {'Q2':>8} | {'Q3':>8} | {'Q4':>8} | "
          f"{'Assets':>6} | {'Dur':>4}")
    print(f"  {'─'*110}")
    for name, m in [("M3_0279 MeanReversion", m3_m),
                     ("M2_0064 Momentum", m2_m),
                     ("M4_0128 Volatility", m4_m)]:
        print(f"  {name:<35} | {m['win_rate_pct']:>5.1f} | {m['total_pnl']:>+9.4f} | "
              f"{m['sharpe_ratio']:>+7.4f} | {m['sortino_ratio']:>+7.4f} | "
              f"{m['q1_pnl']:>+8.3f} | {m['q2_pnl']:>+8.3f} | "
              f"{m['q3_pnl']:>+8.3f} | {m['q4_pnl']:>+8.3f} | "
              f"{m['pct_profitable_assets']:>5.0f}% | {m['pct_profitable_durations']:>3.0f}%")

    # ── 2. Duration investigation for M4_0128 ──────────────────────────
    print(f"\n\n{'='*70}")
    print("  2. M4_0128 DURATION INVESTIGATION")
    print("=" * 70)

    print_duration_breakdown("M4_0128", m4_trades)
    print_duration_breakdown("M3_0279", m3_trades)
    print_duration_breakdown("M2_0064", m2_trades)

    # Also asset breakdown
    print_asset_breakdown("M4_0128", m4_trades)
    print_asset_breakdown("M3_0279", m3_trades)
    print_asset_breakdown("M2_0064", m2_trades)

    # ── 3. Full M3 trade log ───────────────────────────────────────────
    print(f"\n\n{'='*70}")
    print("  3. M3_0279 COMPLETE TRADE LOG (all 56 trades)")
    print("=" * 70)

    out_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'outputs', 'FINAL_RESULTS')
    os.makedirs(out_dir, exist_ok=True)
    save_trade_log(m3_trades, os.path.join(out_dir, 'Trade_Log_M3_0279_Full.csv'))
    print(f"  Saved to outputs/FINAL_RESULTS/Trade_Log_M3_0279_Full.csv")

    print(f"\n  {'#':>3} | {'Market':>16} | {'Asset':>4} | {'Dur':>3} | {'Entry_s':>7} | "
          f"{'Dir':>4} | {'Entry$':>6} | {'Exit_s':>6} | {'Exit$':>6} | "
          f"{'Result':>6} | {'PnL':>8} | {'Out':>4}")
    print(f"  {'─'*110}")
    for i, t in enumerate(m3_trades, 1):
        exit_s = str(t.second_exited) if t.second_exited < t.duration_minutes * 60 else "resol"
        print(f"  {i:>3} | {t.market_id[:16]} | {t.asset:>4} | {t.duration_minutes:>2}m | "
              f"s={t.second_entered:>4} | {t.direction:>4} | {t.entry_price:>6.3f} | "
              f"{exit_s:>6} | {t.exit_price:>6.3f} | {t.actual_result:>6} | "
              f"{t.pnl:>+8.4f} | {t.outcome:>4}")

    # ── 4. Overfitting test: Q3+Q4 only ───────────────────────────────
    print(f"\n\n{'='*70}")
    print("  4. OVERFITTING TEST: Re-evaluate on Q3+Q4 data only")
    print("=" * 70)
    print("  (Training bias test: do strategies trained on full data hold up?)")

    strategies = [
        ("M3_0279 MeanReversion", run_m3_0279),
        ("M2_0064 Momentum", run_m2_0064),
        ("M4_0128 Volatility", run_m4_0128),
    ]
    run_overfitting_test(markets, strategies)

    # Also test on Q1+Q2 only (first 60%)
    print(f"\n  For comparison, Q1+Q2 only (first 60%):")
    sorted_markets = sorted(markets, key=lambda m: m['started_at'])
    split = int(len(sorted_markets) * 0.60)
    train_markets = sorted_markets[:split]
    print(f"  Train set (Q1+Q2): {len(train_markets)} markets")
    for name, runner in strategies:
        trades = runner(train_markets)
        m = compute_metrics(trades)
        print(f"    {name:<25} | {m['total_bets']:>5} trades | "
              f"Win%={m['win_rate_pct']:>5.1f} | "
              f"PnL={m['total_pnl']:>+9.4f} | "
              f"Sharpe={m['sharpe_ratio']:>+7.4f} | "
              f"{'PROFITABLE' if m['total_pnl'] > 0 else 'UNPROFITABLE'}")

    # ── 5. M3 win rate vs PnL gap explanation ──────────────────────────
    print(f"\n\n{'='*70}")
    print("  5. WHY M3's 94.6% WIN RATE DOESN'T BEAT M2/M4 ON PNL")
    print("=" * 70)

    m3_pnls = [t.pnl for t in m3_trades]
    m2_pnls = [t.pnl for t in m2_trades]
    m4_pnls = [t.pnl for t in m4_trades]

    m3_wins = [p for p in m3_pnls if p > 0]
    m3_losses = [p for p in m3_pnls if p <= 0]
    m2_wins = [p for p in m2_pnls if p > 0]
    m2_losses = [p for p in m2_pnls if p <= 0]
    m4_wins = [p for p in m4_pnls if p > 0]
    m4_losses = [p for p in m4_pnls if p <= 0]

    print(f"\n  PnL distribution analysis:")
    print(f"  {'─'*80}")
    print(f"  {'Strategy':<25} | {'Trades':>6} | {'AvgWin':>8} | {'AvgLoss':>8} | "
          f"{'Win:Loss':>8} | {'MaxWin':>8} | {'MaxLoss':>8}")
    print(f"  {'─'*80}")

    for name, wins, losses, total in [
        ("M3_0279 MeanReversion", m3_wins, m3_losses, m3_pnls),
        ("M2_0064 Momentum", m2_wins, m2_losses, m2_pnls),
        ("M4_0128 Volatility", m4_wins, m4_losses, m4_pnls),
    ]:
        avg_w = np.mean(wins) if wins else 0
        avg_l = np.mean(losses) if losses else 0
        max_w = max(wins) if wins else 0
        max_l = min(losses) if losses else 0
        ratio = f"{abs(avg_l/avg_w):.1f}:1" if avg_w != 0 else "N/A"
        print(f"  {name:<25} | {len(total):>6} | {avg_w:>+8.4f} | {avg_l:>+8.4f} | "
              f"{ratio:>8} | {max_w:>+8.4f} | {max_l:>+8.4f}")

    print(f"\n  Key insight:")
    print(f"  M3 wins are TINY (avg +${np.mean(m3_wins):.4f}) because profit target = 10%")
    print(f"  M3 losses are HUGE (avg ${np.mean(m3_losses):.4f}) because hold-to-resolution")
    print(f"  Loss:Win ratio = {abs(np.mean(m3_losses)/np.mean(m3_wins)):.1f}:1")
    print(f"  Need >{abs(np.mean(m3_losses)/np.mean(m3_wins))/(1+abs(np.mean(m3_losses)/np.mean(m3_wins)))*100:.0f}% win rate just to BREAK EVEN")
    print(f"  Actual win rate: 94.6% → profitable, but barely")
    print(f"")
    print(f"  M2 trades 3367x at higher stakes (entry ~0.40-0.50)")
    print(f"  Even at 47% win rate, volume * edge = ${sum(m2_pnls):.2f}")
    print(f"  M3 trades 56x with tiny edge per trade = ${sum(m3_pnls):.2f}")


if __name__ == '__main__':
    main()
