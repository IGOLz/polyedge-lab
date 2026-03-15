"""
Analyze brute force results from results.txt
Applies statistical rules to find best strategy or combination of strategies.
Reads results.txt, saves nothing. Prints everything to stdout.
"""

import re
import sys
from collections import defaultdict


def read_results_file(path='results.txt'):
    """Read results.txt handling various encodings."""
    for encoding in ['utf-8', 'utf-16', 'utf-16-le', 'utf-16-be', 'latin-1']:
        try:
            with open(path, 'r', encoding=encoding) as f:
                content = f.read()
            # Sanity check — should contain recognizable text
            if 'TOP' in content or 'Progress' in content or 'Rank' in content:
                return content
        except (UnicodeDecodeError, UnicodeError):
            continue
    # Fallback: read as bytes, strip null bytes (UTF-16 without BOM)
    with open(path, 'rb') as f:
        raw = f.read()
    content = raw.replace(b'\x00', b'').decode('utf-8', errors='replace')
    return content


def parse_config_string(config_str):
    """Parse a config string like 'A=15s B=45s entry=60s thr=0.05 price=0.55-0.85 SL=0.40 dir=up markets=xrp_sol hours=16-24'"""
    cfg = {}
    try:
        m = re.search(r'A=(\d+)s', config_str)
        cfg['price_a'] = int(m.group(1)) if m else None

        m = re.search(r'B=(\d+)s', config_str)
        cfg['price_b'] = int(m.group(1)) if m else None

        m = re.search(r'entry=(\d+)s', config_str)
        cfg['entry_sec'] = int(m.group(1)) if m else None

        m = re.search(r'thr=([\d.]+)', config_str)
        cfg['threshold'] = float(m.group(1)) if m else None

        m = re.search(r'price=([\d.]+)-([\d.]+)', config_str)
        if m:
            cfg['price_min'] = float(m.group(1))
            cfg['price_max'] = float(m.group(2))
        else:
            cfg['price_min'] = None
            cfg['price_max'] = None

        m = re.search(r'SL=([\d.]+|off)', config_str)
        if m:
            cfg['stop_loss'] = None if m.group(1) == 'off' else float(m.group(1))
        else:
            cfg['stop_loss'] = None

        m = re.search(r'dir=(\S+)', config_str)
        cfg['direction'] = m.group(1) if m else None

        m = re.search(r'markets=(\S+)', config_str)
        cfg['markets'] = m.group(1) if m else None

        m = re.search(r'hours=(\S+)', config_str)
        cfg['hours'] = m.group(1) if m else None

    except Exception as e:
        print(f"  Warning: failed to parse config '{config_str}': {e}")
        return None

    return cfg


def parse_ranking_line(line):
    """Parse a ranking table line like:
       1    | 312    | 52.1% | 163/149/0    | $18.40 | 8.2%  | $0.059 | A=15s B=45s ...
    """
    # Strip and skip non-data lines
    line = line.strip()
    if not line or line.startswith('=') or line.startswith('-') or line.startswith('Rank'):
        return None

    # Match the table format
    pattern = r'(\d+)\s*\|\s*(\d+)\s*\|\s*([\d.]+)%\s*\|\s*(\d+)/(\d+)/(\d+)\s*\|\s*\$([-\d.]+)\s*\|\s*([-\d.]+)%\s*\|\s*\$([-\d.]+)\s*\|\s*(.*)'
    m = re.match(pattern, line)
    if not m:
        return None

    config_str = m.group(10).strip()
    cfg = parse_config_string(config_str)
    if cfg is None:
        return None

    return {
        'rank': int(m.group(1)),
        'trades': int(m.group(2)),
        'win_rate': float(m.group(3)) / 100,
        'wins': int(m.group(4)),
        'sls': int(m.group(5)),
        'losses': int(m.group(6)),
        'total_pnl': float(m.group(7)),
        'roi': float(m.group(8)) / 100,
        'avg_pnl': float(m.group(9)),
        **cfg,
        'config_str': config_str,
    }


def parse_all_results(content):
    """Parse all ranking tables from the results file."""
    results = []
    seen_configs = set()

    for line in content.split('\n'):
        parsed = parse_ranking_line(line)
        if parsed is None:
            continue
        # Deduplicate by config string
        key = parsed['config_str']
        if key not in seen_configs:
            seen_configs.add(key)
            results.append(parsed)

    return results


# --- Filtering rules ---

def passes_sanity_check(config):
    """Rule 2: win rate must correlate with entry price."""
    if config['price_min'] is not None and config['price_min'] < 0.50 and config['win_rate'] > 0.40:
        return False  # suspiciously high win rate at low prices
    return True


def acceptable_loss_ratio(config):
    """Rule 4: max 5% of trades as full losses."""
    if config['trades'] == 0:
        return False
    loss_pct = config['losses'] / config['trades']
    return loss_pct <= 0.05


def find_consensus(configs, top_n=10):
    """Find consensus parameters across top configs by ROI."""
    top = sorted(configs, key=lambda x: x['roi'], reverse=True)[:top_n]

    consensus = {}
    params = ['price_a', 'price_b', 'entry_sec', 'threshold',
              'price_min', 'price_max', 'stop_loss',
              'direction', 'markets', 'hours']

    for param in params:
        values = [c[param] for c in top if c.get(param) is not None]
        if not values:
            consensus[param] = {'value': None, 'agreement': '0/0', 'all_values': []}
            continue
        most_common = max(set(values), key=values.count)
        count = values.count(most_common)
        all_sorted = sorted(set(values), key=lambda v: values.count(v), reverse=True)
        consensus[param] = {
            'value': most_common,
            'agreement': f"{count}/{len(values)}",
            'all_values': all_sorted[:5],
        }

    return consensus, top


def main():
    print("=" * 70)
    print("=== MOMENTUM BRUTE FORCE RESULTS ANALYZER ===")
    print("=" * 70)

    # Step 1: Read and parse
    try:
        content = read_results_file('results.txt')
    except FileNotFoundError:
        print("\nERROR: results.txt not found.")
        print("Run: python momentum_brute_force.py > results.txt")
        sys.exit(1)

    print(f"\nFile loaded ({len(content)} chars)")

    results = parse_all_results(content)
    print(f"Parsed {len(results)} unique configs from ranking tables")

    if not results:
        print("\nERROR: No ranking table data found in results.txt")
        print("The brute force sweep may not have completed yet.")
        # Check if we can see progress info
        progress_lines = [l for l in content.split('\n') if 'Progress' in l or 'progress' in l.lower()]
        if progress_lines:
            last = progress_lines[-1].strip()
            print(f"Last progress line: {last}")
        print("\nRe-run the brute force sweep and pipe output to results.txt:")
        print("  python momentum_brute_force.py > results.txt 2>progress.txt")
        sys.exit(1)

    # Step 2: Apply filtering rules
    print(f"\n{'=' * 70}")
    print("=== FILTERING RULES ===")
    print(f"{'=' * 70}")

    # Rule 1: Minimum statistical significance
    tier_1 = [c for c in results if c['trades'] >= 500]
    tier_2 = [c for c in results if 200 <= c['trades'] < 500]
    tier_3 = [c for c in results if 50 <= c['trades'] < 200]

    print(f"\nTier 1 (500+ trades, high confidence): {len(tier_1)} configs")
    print(f"Tier 2 (200-499 trades, medium confidence): {len(tier_2)} configs")
    print(f"Tier 3 (50-199 trades, low confidence): {len(tier_3)} configs")

    # Rule 2: Sanity check
    before = len(tier_1)
    tier_1 = [c for c in tier_1 if passes_sanity_check(c)]
    print(f"\nAfter sanity check (Rule 2): {len(tier_1)} configs ({before - len(tier_1)} removed)")

    # Rule 3: Must have stop-loss
    tier_1_sl = [c for c in tier_1 if c['stop_loss'] is not None]
    print(f"After stop-loss requirement (Rule 3): {len(tier_1_sl)} configs")

    # Rule 4: Acceptable loss ratio
    before = len(tier_1_sl)
    tier_1_sl = [c for c in tier_1_sl if acceptable_loss_ratio(c)]
    print(f"After loss ratio filter (Rule 4): {len(tier_1_sl)} configs ({before - len(tier_1_sl)} removed)")

    # If Tier 1 is empty, fall back to Tier 2
    working_set = tier_1_sl
    working_label = "Tier 1"
    if not working_set:
        print("\nTier 1 empty after filtering, falling back to Tier 2...")
        working_set = [c for c in tier_2 if passes_sanity_check(c)
                       and c['stop_loss'] is not None
                       and acceptable_loss_ratio(c)]
        working_label = "Tier 2"
        print(f"Tier 2 after all filters: {len(working_set)} configs")

    if not working_set:
        print("\nTier 2 also empty, falling back to all results with stop-loss...")
        working_set = [c for c in results if c['stop_loss'] is not None and passes_sanity_check(c)]
        working_label = "All"
        print(f"Remaining after basic filters: {len(working_set)} configs")

    if not working_set:
        print("\nERROR: No configs survived filtering. Raw top 10 by ROI:")
        top10 = sorted(results, key=lambda x: x['roi'], reverse=True)[:10]
        for i, r in enumerate(top10, 1):
            print(f"  {i}. {r['trades']} trades | {r['win_rate']:.1%} win | ROI {r['roi']:.1%} | {r['config_str']}")
        sys.exit(1)

    print(f"\nWorking with {len(working_set)} configs from {working_label}")

    # Step 3: Cluster by price range
    print(f"\n{'=' * 70}")
    print("=== PRICE RANGE CLUSTERING ===")
    print(f"{'=' * 70}")

    price_buckets = {
        'high_price': [],    # price_min >= 0.65
        'mid_price': [],     # 0.45 <= price_min < 0.65
        'broad': [],         # price_min < 0.45
    }

    for c in working_set:
        if c['price_min'] is None:
            price_buckets['broad'].append(c)
        elif c['price_min'] >= 0.65:
            price_buckets['high_price'].append(c)
        elif c['price_min'] >= 0.45:
            price_buckets['mid_price'].append(c)
        else:
            price_buckets['broad'].append(c)

    for name, configs in price_buckets.items():
        print(f"  {name}: {len(configs)} configs")

    # Step 5: Proposed strategy configs
    print(f"\n{'=' * 70}")
    print("=== PROPOSED STRATEGY CONFIGURATIONS FOR THE BOT ===")
    print(f"{'=' * 70}")

    proposed_strategies = []

    for bucket_name in ['high_price', 'mid_price', 'broad']:
        bucket_configs = price_buckets[bucket_name]
        if len(bucket_configs) < 3:
            print(f"\n{bucket_name}: not enough data ({len(bucket_configs)} configs)")
            proposed_strategies.append(None)
            continue

        consensus, top = find_consensus(bucket_configs, top_n=min(10, len(bucket_configs)))
        best = top[0]

        print(f"\n--- STRATEGY: momentum_{bucket_name} ---")
        print(f"Based on {len(bucket_configs)} qualifying configs, top {len(top)} analyzed")
        print(f"Best single config: {best['trades']} trades | {best['win_rate']:.1%} win | "
              f"ROI {best['roi']:.1%} | AvgPnL ${best['avg_pnl']:.4f}")

        print(f"\nConsensus parameters:")
        for param, data in consensus.items():
            alts = str(data['all_values'][:3])
            print(f"  {param}: {data['value']} (agreement: {data['agreement']}) | alternatives: {alts}")

        print(f"\nBot config values:")
        print(f"  momentum_price_a_second = {consensus['price_a']['value']}")
        print(f"  momentum_price_b_second = {consensus['price_b']['value']}")
        print(f"  momentum_entry_second = {consensus['entry_sec']['value']}")
        print(f"  momentum_min_threshold = {consensus['threshold']['value']}")
        print(f"  momentum_price_min = {consensus['price_min']['value']}")
        print(f"  momentum_price_max = {consensus['price_max']['value']}")
        print(f"  momentum_stop_loss_exit_point = {consensus['stop_loss']['value']}")
        print(f"  momentum_direction_filter = {consensus['direction']['value']}")
        print(f"  momentum_market_filter = {consensus['markets']['value']}")
        print(f"  momentum_hour_filter = {consensus['hours']['value']}")

        # Store for overlap check
        proposed_strategies.append({
            'name': f'momentum_{bucket_name}',
            'price_min': consensus['price_min']['value'],
            'price_max': consensus['price_max']['value'],
            'consensus': consensus,
            'best': best,
            'top': top,
            'count': len(bucket_configs),
        })

    # Step 6: Combination strategy + overlap check
    print(f"\n{'=' * 70}")
    print("=== COMBINATION STRATEGY RECOMMENDATION ===")
    print(f"{'=' * 70}")
    print("Run these simultaneously in the bot as separate sub-strategies:")
    print("Each fires independently based on its own entry rules.")
    print("They share the same stop-loss monitor and outcome tracker.\n")

    strategies = [s for s in proposed_strategies if s is not None]

    if not strategies:
        print("No strategies could be proposed from the data.")
    else:
        # Overlap check
        for i, s1 in enumerate(strategies):
            for j, s2 in enumerate(strategies):
                if i >= j:
                    continue
                if s1['price_min'] is None or s2['price_min'] is None:
                    continue
                if s1['price_max'] is None or s2['price_max'] is None:
                    continue
                overlap = s1['price_max'] > s2['price_min'] and s2['price_max'] > s1['price_min']
                if overlap:
                    print(f"WARNING: {s1['name']} and {s2['name']} have overlapping price ranges")
                    print(f"  {s1['name']}: {s1['price_min']}-{s1['price_max']}")
                    print(f"  {s2['name']}: {s2['price_min']}-{s2['price_max']}")
                    print(f"  Consider adjusting boundaries to avoid double-firing on same market\n")

    # Step 7: Final summary table
    print(f"\n{'=' * 70}")
    print("=== FINAL RECOMMENDED BOT CONFIGURATION ===")
    print(f"{'=' * 70}")

    total_daily_trades = 0
    total_daily_pnl = 0
    bet_size = 5  # $5 per trade

    for i, s in enumerate(strategies, 1):
        b = s['best']
        c = s['consensus']

        # Estimate daily trades: total trades / estimated days in dataset
        # We don't know exact days, so show per-trade stats
        thr = c['threshold']['value']
        thr_str = f"{thr}" if thr else "?"
        sl = c['stop_loss']['value']
        sl_str = f"{sl}" if sl else "?"
        pmin = c['price_min']['value']
        pmax = c['price_max']['value']
        price_str = f"{pmin}-{pmax}" if pmin and pmax else "?"

        pa = c['price_a']['value']
        pb = c['price_b']['value']
        pa_str = f"{pa}s" if pa else "?"
        pb_str = f"{pb}s" if pb else "?"

        print(f"\nStrategy {i}: {s['name']}")
        print(f"  Entry: price {price_str} | A={pa_str} B={pb_str} | threshold={thr_str} | SL={sl_str}")
        print(f"  Direction: {c['direction']['value']} | Markets: {c['markets']['value']} | Hours: {c['hours']['value']}")
        print(f"  Best config stats: {b['trades']} trades | {b['win_rate']:.1%} win rate | ROI {b['roi']:.1%}")
        print(f"  Avg PnL per $1 trade: ${b['avg_pnl']:.4f}")

        if b['avg_pnl'] > 0:
            pnl_per_trade_at_bet = b['avg_pnl'] * bet_size
            print(f"  At ${bet_size}/trade: ${pnl_per_trade_at_bet:.3f} per trade")
            total_daily_pnl += pnl_per_trade_at_bet * b['trades']
            total_daily_trades += b['trades']

    if strategies:
        print(f"\n--- Combined (across dataset) ---")
        print(f"  Total trades across all strategies: {total_daily_trades}")
        if total_daily_trades > 0:
            print(f"  Combined PnL at ${bet_size}/trade: ${total_daily_pnl:.2f}")
            print(f"  Avg PnL per trade: ${total_daily_pnl/total_daily_trades:.4f}")

    # Bonus: show top 5 individual configs across all data
    print(f"\n{'=' * 70}")
    print("=== TOP 5 INDIVIDUAL CONFIGS (all data, by ROI) ===")
    print(f"{'=' * 70}")

    all_by_roi = sorted(results, key=lambda x: x['roi'], reverse=True)
    for i, r in enumerate(all_by_roi[:5], 1):
        print(f"\n  #{i}: ROI {r['roi']:.1%} | {r['trades']} trades | {r['win_rate']:.1%} win | "
              f"PnL ${r['total_pnl']:.2f} | AvgPnL ${r['avg_pnl']:.4f}")
        print(f"       {r['config_str']}")

    # Top 5 by total PnL
    print(f"\n{'=' * 70}")
    print("=== TOP 5 INDIVIDUAL CONFIGS (all data, by Total PnL) ===")
    print(f"{'=' * 70}")

    all_by_pnl = sorted(results, key=lambda x: x['total_pnl'], reverse=True)
    for i, r in enumerate(all_by_pnl[:5], 1):
        print(f"\n  #{i}: PnL ${r['total_pnl']:.2f} | {r['trades']} trades | {r['win_rate']:.1%} win | "
              f"ROI {r['roi']:.1%} | AvgPnL ${r['avg_pnl']:.4f}")
        print(f"       {r['config_str']}")

    print(f"\nAnalysis complete. {len(results)} configs analyzed.")


if __name__ == '__main__':
    main()
