"""
Core backtesting engine.
Handles trade recording, PnL calculation, and performance metrics.
"""

import numpy as np
import pandas as pd
import os
from dataclasses import dataclass

DEFAULT_FEE_RATE = 0.02


@dataclass
class Trade:
    market_id: str
    asset: str
    duration_minutes: int
    second_entered: int
    entry_price: float      # token price paid (for the side we're betting on)
    direction: str           # 'Up' or 'Down'
    second_exited: int       # -1 for hold to resolution
    exit_price: float        # token price at exit (1.0 if win at resolution, 0.0 if loss)
    actual_result: str       # 'Up' or 'Down'
    pnl: float
    outcome: str             # 'win' or 'loss'
    hour: int


def calculate_pnl_hold(entry_price, direction, actual_result, fee_rate=DEFAULT_FEE_RATE):
    """PnL for hold-to-resolution. Win: (1 - entry) * (1 - fee). Loss: -entry."""
    won = (direction == actual_result)
    if won:
        gross = 1.0 - entry_price
        return gross - fee_rate * gross
    else:
        return -entry_price


def calculate_pnl_exit(entry_price, exit_price, fee_rate=DEFAULT_FEE_RATE):
    """PnL for mid-market exit. PnL = exit - entry, minus fee on profit."""
    gross = exit_price - entry_price
    fee = fee_rate * max(0.0, gross)
    return gross - fee


def make_trade(market, second_entered, entry_price, direction,
               second_exited=-1, exit_price=None, fee_rate=DEFAULT_FEE_RATE):
    """Create a Trade object with PnL calculated."""
    actual = market['final_outcome']

    if exit_price is not None and second_exited >= 0:
        pnl = calculate_pnl_exit(entry_price, exit_price, fee_rate)
        outcome = 'win' if pnl > 0 else 'loss'
    else:
        pnl = calculate_pnl_hold(entry_price, direction, actual, fee_rate)
        outcome = 'win' if direction == actual else 'loss'
        second_exited = market['total_seconds']
        exit_price = 1.0 if outcome == 'win' else 0.0

    return Trade(
        market_id=market['market_id'],
        asset=market['asset'],
        duration_minutes=market['duration_minutes'],
        second_entered=second_entered,
        entry_price=round(entry_price, 4),
        direction=direction,
        second_exited=second_exited,
        exit_price=round(exit_price, 4),
        actual_result=actual,
        pnl=round(pnl, 6),
        outcome=outcome,
        hour=market['hour'],
    )


def compute_metrics(trades, config_id=None):
    """Compute all performance metrics for a list of trades."""
    if not trades:
        return _empty_metrics(config_id)

    pnls = np.array([t.pnl for t in trades])
    wins = sum(1 for t in trades if t.outcome == 'win')
    losses = sum(1 for t in trades if t.outcome == 'loss')
    total = wins + losses

    if total == 0:
        return _empty_metrics(config_id)

    win_rate = wins / total * 100
    total_pnl = float(np.sum(pnls))
    avg_pnl = float(np.mean(pnls))

    # Profit factor
    winning_pnls = pnls[pnls > 0]
    losing_pnls = pnls[pnls < 0]
    sum_wins = float(np.sum(winning_pnls)) if len(winning_pnls) > 0 else 0.0
    sum_losses = float(np.abs(np.sum(losing_pnls))) if len(losing_pnls) > 0 else 0.001
    profit_factor = sum_wins / sum_losses

    # Expected value
    avg_win = float(np.mean(winning_pnls)) if len(winning_pnls) > 0 else 0.0
    avg_loss = float(np.mean(np.abs(losing_pnls))) if len(losing_pnls) > 0 else 0.0
    expected_value = (wins / total * avg_win) - (losses / total * avg_loss)

    # Risk metrics
    std_dev = float(np.std(pnls, ddof=1)) if len(pnls) > 1 else 0.001
    sharpe_ratio = avg_pnl / std_dev if std_dev > 0.0001 else 0.0

    downside_pnls = pnls[pnls < 0]
    downside_std = float(np.std(downside_pnls, ddof=1)) if len(downside_pnls) > 1 else 0.001
    sortino_ratio = avg_pnl / downside_std if downside_std > 0.0001 else 0.0

    # Max drawdown
    cumulative = np.cumsum(pnls)
    peak = np.maximum.accumulate(cumulative)
    drawdown = peak - cumulative
    max_drawdown = float(np.max(drawdown)) if len(drawdown) > 0 else 0.0

    # Robustness: per-asset profitability
    asset_pnls = {}
    for t in trades:
        asset_pnls.setdefault(t.asset, []).append(t.pnl)
    profitable_assets = sum(1 for v in asset_pnls.values() if sum(v) > 0)
    total_assets_seen = len(asset_pnls)
    pct_profitable_assets = profitable_assets / total_assets_seen * 100 if total_assets_seen > 0 else 0

    # Robustness: per-duration profitability
    dur_pnls = {}
    for t in trades:
        dur_pnls.setdefault(t.duration_minutes, []).append(t.pnl)
    profitable_durations = sum(1 for v in dur_pnls.values() if sum(v) > 0)
    total_durations_seen = len(dur_pnls)
    pct_profitable_durations = profitable_durations / total_durations_seen * 100 if total_durations_seen > 0 else 0

    # Consistency: 100 - stdev of per-asset win rates
    asset_wrs = []
    for v in asset_pnls.values():
        wr = sum(1 for p in v if p > 0) / len(v) * 100 if v else 0
        asset_wrs.append(wr)
    consistency_score = 100 - float(np.std(asset_wrs)) if len(asset_wrs) > 1 else 50.0
    consistency_score = max(0, min(100, consistency_score))

    # Quarters
    q_size = max(1, len(pnls) // 4)
    q_pnls = []
    for i in range(4):
        s = i * q_size
        e = s + q_size if i < 3 else len(pnls)
        q_pnls.append(float(np.sum(pnls[s:e])) if s < len(pnls) else 0.0)

    return {
        'config_id': config_id,
        'total_bets': total,
        'wins': wins,
        'losses': losses,
        'win_rate_pct': round(win_rate, 2),
        'total_pnl': round(total_pnl, 4),
        'avg_bet_pnl': round(avg_pnl, 6),
        'profit_factor': round(profit_factor, 4),
        'expected_value': round(expected_value, 6),
        'sharpe_ratio': round(sharpe_ratio, 4),
        'sortino_ratio': round(sortino_ratio, 4),
        'max_drawdown': round(max_drawdown, 4),
        'std_dev_pnl': round(std_dev, 6),
        'pct_profitable_assets': round(pct_profitable_assets, 1),
        'pct_profitable_durations': round(pct_profitable_durations, 1),
        'consistency_score': round(consistency_score, 2),
        'q1_pnl': round(q_pnls[0], 4),
        'q2_pnl': round(q_pnls[1], 4),
        'q3_pnl': round(q_pnls[2], 4),
        'q4_pnl': round(q_pnls[3], 4),
    }


def _empty_metrics(config_id):
    keys = [
        'config_id', 'total_bets', 'wins', 'losses', 'win_rate_pct',
        'total_pnl', 'avg_bet_pnl', 'profit_factor', 'expected_value',
        'sharpe_ratio', 'sortino_ratio', 'max_drawdown', 'std_dev_pnl',
        'pct_profitable_assets', 'pct_profitable_durations', 'consistency_score',
        'q1_pnl', 'q2_pnl', 'q3_pnl', 'q4_pnl',
    ]
    m = {k: 0 for k in keys}
    m['config_id'] = config_id
    return m


def add_ranking_score(df):
    """Add composite ranking score to results DataFrame.
    All components are percentile-ranked (0-100) for fair weighting:
      30% total_pnl_percentile   (most important: actual profit)
      25% expected_value_percentile (edge per trade)
      20% sharpe_ratio_percentile (risk-adjusted return)
      15% consistency_score      (cross-asset stability)
      10% win_rate_percentile    (least important alone)
    """
    if df.empty:
        df['ranking_score'] = []
        return df

    df = df.copy()
    for col in ['total_pnl', 'sharpe_ratio', 'expected_value', 'win_rate_pct']:
        if df[col].std() > 0:
            df[f'{col}_pctile'] = df[col].rank(pct=True) * 100
        else:
            df[f'{col}_pctile'] = 50.0

    df['ranking_score'] = (
        df['total_pnl_pctile'] * 0.30 +
        df['expected_value_pctile'] * 0.25 +
        df['sharpe_ratio_pctile'] * 0.20 +
        df['consistency_score'] * 0.15 +
        df['win_rate_pct_pctile'] * 0.10
    ).round(2)

    df.drop(columns=[c for c in df.columns if c.endswith('_pctile')], inplace=True)
    return df


def save_module_results(results_df, trades_by_config, module_name, module_dir, top_n=10):
    """Save CSV results, best configs text, analysis markdown, and sample trades."""
    os.makedirs(module_dir, exist_ok=True)

    if results_df.empty:
        with open(os.path.join(module_dir, f'{module_name}_Analysis.md'), 'w') as f:
            f.write(f"# {module_name} Analysis\n\nNo results produced.\n")
        return results_df

    results_df = results_df.sort_values('ranking_score', ascending=False).reset_index(drop=True)

    # CSV
    csv_path = os.path.join(module_dir, f'Test_{module_name}_Results.csv')
    results_df.to_csv(csv_path, index=False)
    print(f"  Saved {csv_path}")

    # Best configs
    best_path = os.path.join(module_dir, f'{module_name}_Best_Configs.txt')
    with open(best_path, 'w') as f:
        f.write(f"TOP {top_n} CONFIGURATIONS - {module_name}\n")
        f.write("=" * 100 + "\n\n")
        for idx, row in results_df.head(top_n).iterrows():
            f.write(f"Rank {idx + 1}:\n")
            for col in results_df.columns:
                f.write(f"  {col}: {row[col]}\n")
            f.write("\n")
            cid = row.get('config_id')
            if cid and cid in trades_by_config:
                samples = trades_by_config[cid][:20]
                if samples:
                    f.write("  Sample trades:\n")
                    for t in samples:
                        f.write(
                            f"    {t.market_id[:16]}.. s={t.second_entered:>3} "
                            f"dir={t.direction:<4} entry={t.entry_price:.3f} "
                            f"res={t.actual_result:<4} pnl={t.pnl:+.4f}\n"
                        )
                    f.write("\n")
    print(f"  Saved {best_path}")

    # Analysis markdown
    analysis_path = os.path.join(module_dir, f'{module_name}_Analysis.md')
    valid = results_df[results_df['total_bets'] > 0]
    profitable = valid[valid['total_pnl'] > 0]

    with open(analysis_path, 'w') as f:
        f.write(f"# {module_name} Analysis\n\n")
        f.write(f"## Summary\n\n")
        f.write(f"- Configurations tested: {len(results_df)}\n")
        f.write(f"- With trades: {len(valid)}\n")
        f.write(f"- Profitable: {len(profitable)}\n")
        f.write(f"- Unprofitable: {len(valid) - len(profitable)}\n\n")

        if not valid.empty:
            best = results_df.iloc[0]
            f.write(f"## Best Configuration (by ranking score)\n\n")
            f.write(f"| Metric | Value |\n|--------|-------|\n")
            for col in results_df.columns:
                f.write(f"| {col} | {best[col]} |\n")
            f.write(f"\n")

            f.write(f"## Metrics Distribution (configs with trades)\n\n")
            f.write(f"| Metric | Mean | Std | Min | Max |\n")
            f.write(f"|--------|------|-----|-----|-----|\n")
            for col in ['win_rate_pct', 'total_pnl', 'avg_bet_pnl', 'sharpe_ratio',
                         'profit_factor', 'max_drawdown', 'consistency_score']:
                if col in valid.columns:
                    f.write(
                        f"| {col} | {valid[col].mean():.4f} | {valid[col].std():.4f} "
                        f"| {valid[col].min():.4f} | {valid[col].max():.4f} |\n"
                    )

    print(f"  Saved {analysis_path}")
    return results_df


def save_trade_log(trades, filepath):
    """Save a list of Trade objects to CSV."""
    if not trades:
        return
    rows = []
    for t in trades:
        rows.append({
            'market_id': t.market_id,
            'asset': t.asset,
            'duration_minutes': t.duration_minutes,
            'second_entered': t.second_entered,
            'entry_price': t.entry_price,
            'direction': t.direction,
            'second_exited': t.second_exited,
            'exit_price': t.exit_price,
            'actual_result': t.actual_result,
            'pnl': t.pnl,
            'outcome': t.outcome,
            'hour': t.hour,
        })
    pd.DataFrame(rows).to_csv(filepath, index=False)
