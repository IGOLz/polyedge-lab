#!/usr/bin/env python3
"""
Comprehensive Strategy Backtesting Framework
Runs all 7 modules sequentially, generates output files.

Usage:
    python run_backtest.py                  # Run all modules
    python run_backtest.py --module 3       # Run single module
    python run_backtest.py --module 1,2,3   # Run specific modules
"""

import os
import sys
import time
import argparse
import pandas as pd

from backtest.data_loader import load_all_data
from backtest.engine import save_trade_log, add_ranking_score
from backtest import (
    module_1_basic_entry,
    module_2_momentum,
    module_3_mean_reversion,
    module_4_volatility,
    module_5_time_filters,
    module_6_risk_management,
    module_7_composite,
)

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), 'outputs')


def generate_final_results(all_module_results, output_dir):
    """Generate final summary files combining all module results."""
    final_dir = os.path.join(output_dir, 'FINAL_RESULTS')
    os.makedirs(final_dir, exist_ok=True)

    # Combine all results into one DataFrame
    combined = []
    for module_name, df in all_module_results.items():
        if df is not None and not df.empty:
            df_copy = df.copy()
            df_copy['module'] = module_name
            combined.append(df_copy)

    if not combined:
        print("\nNo results to combine.")
        return

    # Top 50 across all modules (re-rank)
    all_df = pd.concat(combined, ignore_index=True)
    if 'ranking_score' not in all_df.columns:
        all_df = add_ranking_score(all_df)

    all_df_sorted = all_df.sort_values('ranking_score', ascending=False).reset_index(drop=True)
    top50 = all_df_sorted.head(50)
    top50.to_csv(os.path.join(final_dir, 'Top_50_Strategies.csv'), index=False)

    # Best strategy details
    if not top50.empty:
        best = top50.iloc[0]
        with open(os.path.join(final_dir, 'Best_Strategy_Details.txt'), 'w') as f:
            f.write("BEST OVERALL STRATEGY\n")
            f.write("=" * 80 + "\n\n")
            for col in top50.columns:
                f.write(f"  {col}: {best[col]}\n")

    # Performance summary
    with open(os.path.join(final_dir, 'Performance_Summary.md'), 'w') as f:
        f.write("# Performance Summary - All Modules\n\n")

        for module_name, df in all_module_results.items():
            if df is None or df.empty:
                f.write(f"## {module_name}\n\nNo results.\n\n")
                continue

            profitable = df[df['total_pnl'] > 0]
            f.write(f"## {module_name}\n\n")
            f.write(f"- Configs tested: {len(df)}\n")
            f.write(f"- Profitable: {len(profitable)} ({len(profitable)/len(df)*100:.0f}%)\n")

            if not df.empty:
                best = df.iloc[0]
                f.write(f"- Best win rate: {df['win_rate_pct'].max():.1f}%\n")
                f.write(f"- Best total PnL: {df['total_pnl'].max():.4f}\n")
                f.write(f"- Best Sharpe: {df['sharpe_ratio'].max():.4f}\n")
                f.write(f"- Best ranking score: {df['ranking_score'].max():.2f}\n")

            f.write("\n")

    # Robustness report
    with open(os.path.join(final_dir, 'Robustness_Report.md'), 'w') as f:
        f.write("# Robustness Report\n\n")

        if not top50.empty:
            f.write("## Top 10 Strategies - Robustness Metrics\n\n")
            f.write("| Rank | Module | Config | Win% | PnL | Sharpe | "
                    "Assets% | Durations% | Consistency | Q1 | Q2 | Q3 | Q4 |\n")
            f.write("|------|--------|--------|------|-----|--------|"
                    "---------|------------|-------------|----|----|----|----|  \n")

            for i, (_, row) in enumerate(top50.head(10).iterrows(), 1):
                f.write(
                    f"| {i} | {row.get('module', '?')} | {row.get('config_id', '?')} "
                    f"| {row['win_rate_pct']:.1f} | {row['total_pnl']:.3f} "
                    f"| {row['sharpe_ratio']:.3f} "
                    f"| {row['pct_profitable_assets']:.0f} "
                    f"| {row['pct_profitable_durations']:.0f} "
                    f"| {row['consistency_score']:.1f} "
                    f"| {row['q1_pnl']:.3f} | {row['q2_pnl']:.3f} "
                    f"| {row['q3_pnl']:.3f} | {row['q4_pnl']:.3f} |\n"
                )

            f.write("\n## Interpretation\n\n")
            f.write("- **Assets%**: % of assets (BTC/ETH/SOL/XRP) showing profit\n")
            f.write("- **Durations%**: % of duration types (5m/15m) showing profit\n")
            f.write("- **Consistency**: 100 - stdev of per-asset win rates (higher = more consistent)\n")
            f.write("- **Q1-Q4**: PnL split into chronological quarters (reveals edge decay)\n")

    print(f"\nFinal results saved to {final_dir}/")


def main():
    parser = argparse.ArgumentParser(description='Run comprehensive strategy backtest')
    parser.add_argument('--module', type=str, default='all',
                        help='Module(s) to run: "all", or comma-separated numbers like "1,2,3"')
    args = parser.parse_args()

    # Parse module selection
    if args.module == 'all':
        modules_to_run = [1, 2, 3, 4, 5, 6, 7]
    else:
        modules_to_run = [int(x.strip()) for x in args.module.split(',')]

    print("=" * 70)
    print("  COMPREHENSIVE STRATEGY BACKTESTING FRAMEWORK")
    print("=" * 70)
    print(f"  Modules to run: {modules_to_run}")
    print(f"  Output directory: {OUTPUT_DIR}")
    print()

    # Load data
    start = time.time()
    markets = load_all_data()

    if not markets:
        print("ERROR: No market data loaded. Check database connection.")
        sys.exit(1)

    load_time = time.time() - start
    print(f"Data loaded in {load_time:.1f}s\n")

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # Run modules
    all_results = {}
    module_runners = {
        1: ('Module 1', module_1_basic_entry),
        2: ('Module 2', module_2_momentum),
        3: ('Module 3', module_3_mean_reversion),
        4: ('Module 4', module_4_volatility),
        5: ('Module 5', module_5_time_filters),
        6: ('Module 6', module_6_risk_management),
    }

    for mod_num in modules_to_run:
        if mod_num == 7:
            continue  # Run after others

        if mod_num not in module_runners:
            print(f"Unknown module: {mod_num}")
            continue

        name, runner = module_runners[mod_num]
        mod_start = time.time()
        result = runner.run(markets, OUTPUT_DIR)
        elapsed = time.time() - mod_start
        all_results[name] = result
        print(f"  {name} completed in {elapsed:.1f}s")

    # Module 7 (composite) needs results from modules 1-6
    if 7 in modules_to_run:
        module_map = {
            'm1': all_results.get('Module 1'),
            'm2': all_results.get('Module 2'),
            'm3': all_results.get('Module 3'),
            'm4': all_results.get('Module 4'),
            'm5': all_results.get('Module 5'),
        }

        # Only run if we have at least 2 module results
        has_results = sum(1 for v in module_map.values() if v is not None and not v.empty)
        if has_results >= 2:
            mod_start = time.time()
            result = module_7_composite.run(markets, OUTPUT_DIR, module_map)
            elapsed = time.time() - mod_start
            all_results['Module 7'] = result
            print(f"  Module 7 completed in {elapsed:.1f}s")
        else:
            print("\n  Skipping Module 7 (need results from at least 2 modules)")

    # Generate final summary
    generate_final_results(all_results, OUTPUT_DIR)

    total_elapsed = time.time() - start
    print(f"\n{'='*70}")
    print(f"  ALL DONE in {total_elapsed:.1f}s ({total_elapsed/60:.1f} min)")
    print(f"{'='*70}")

    # Print quick summary
    total_configs = sum(len(df) for df in all_results.values() if df is not None and not df.empty)
    total_profitable = sum(
        len(df[df['total_pnl'] > 0])
        for df in all_results.values()
        if df is not None and not df.empty
    )
    print(f"\n  Total configs with enough trades: {total_configs}")
    print(f"  Total profitable configs: {total_profitable}")
    if total_configs > 0:
        print(f"  Profitable ratio: {total_profitable/total_configs*100:.1f}%")


if __name__ == '__main__':
    main()
