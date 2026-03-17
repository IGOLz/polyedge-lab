"""
audit_trades.py — Full historical backtest of the exact live momentum configuration.

Runs the exact live strategy against every resolved XRP/SOL 5-minute market
in the entire database. Outputs detailed performance stats, breakdowns,
and monthly earnings projections.

Read-only — does not modify the database.
"""

import math
import os
import warnings
from datetime import datetime, timezone
from collections import defaultdict

import psycopg2
import pandas as pd
from dotenv import load_dotenv

warnings.filterwarnings('ignore', message='.*pandas only supports SQLAlchemy.*')

load_dotenv()

# ── Exact live strategy parameters (hardcoded, not from DB) ──────────────
PRICE_A_SECONDS     = 45
PRICE_B_SECONDS     = 60
PRICE_OPEN_SECONDS  = 0
ENTRY_AFTER_SECONDS = 65
ENTRY_UNTIL_SECONDS = 90
THRESHOLD           = 0.15
CONTEXT_MAX_DELTA   = 0.10
PRICE_MIN           = 0.50
PRICE_MAX           = 0.78
DIRECTION           = 'up_only'
MARKETS             = 'xrp_sol_only'
FEE_PER_SHARE       = 0.02
STOP_LOSS_PRICE     = 0.35

MARKET_TYPES = ['xrp_5m', 'sol_5m']

# ── Realistic execution parameters ───────────────────────────────────────
REALISTIC_DELAY_SECONDS = 3
REALISTIC_SLIPPAGE_PCT  = 0.005  # 0.5%


# ── Signal detection — same logic as brute force / live bot ──────────────

def index_ticks_by_second(ticks_df, started_at):
    """Index ticks by elapsed second for O(1) lookup."""
    result = {}
    for _, row in ticks_df.iterrows():
        elapsed = int((row['time'] - started_at).total_seconds())
        result[elapsed] = float(row['up_price'])
    return result


def get_price_at_second(ticks_by_second, target_sec):
    """Get price at exact second, or nearest within ±5s."""
    price = ticks_by_second.get(target_sec)
    if price is not None:
        return price
    for offset in range(1, 6):
        price = ticks_by_second.get(target_sec + offset)
        if price is not None:
            return price
        price = ticks_by_second.get(target_sec - offset)
        if price is not None:
            return price
    return None


def detect_signal(ticks_by_second):
    """
    Detect momentum signal applying all filters.
    Returns (signal_dict, rejection_reason).
    If signal fires: (signal, None)
    If blocked:      (None, "reason_key")
    """
    price_a = get_price_at_second(ticks_by_second, PRICE_A_SECONDS)
    price_b = get_price_at_second(ticks_by_second, PRICE_B_SECONDS)
    price_open = get_price_at_second(ticks_by_second, PRICE_OPEN_SECONDS)

    if price_a is None or price_b is None:
        return None, "no_ticks"

    momentum = price_b - price_a

    if abs(momentum) < THRESHOLD:
        return None, "threshold"

    direction = 'Up' if momentum > 0 else 'Down'

    # Direction filter
    if DIRECTION == 'up_only' and direction != 'Up':
        return None, "direction"
    if DIRECTION == 'down_only' and direction != 'Down':
        return None, "direction"

    # Token entry price
    token_entry = price_b if direction == 'Up' else (1 - price_b)

    # Context filter: opening price should be near neutral (0.50)
    if price_open is not None:
        open_delta = abs(price_open - 0.50)
        if open_delta > CONTEXT_MAX_DELTA:
            return None, "context"

    # Price range filter
    if token_entry < PRICE_MIN:
        return None, "price_range"
    if token_entry > PRICE_MAX:
        return None, "price_range"

    return {
        'direction': direction,
        'entry_up_price': price_b,
        'token_entry': token_entry,
        'momentum': momentum,
        'price_a': price_a,
        'price_b': price_b,
        'price_open': price_open,
    }, None


def simulate_trade(signal, market, ticks_by_second):
    """Simulate trade outcome with stop-loss monitoring."""
    direction = signal['direction']
    token_entry = signal['token_entry']

    # Stop-loss monitoring from entry time to market close
    if STOP_LOSS_PRICE is not None:
        for sec in range(ENTRY_AFTER_SECONDS, 300, 5):
            price = ticks_by_second.get(sec)
            if price is None:
                continue
            token_price = price if direction == 'Up' else (1 - price)
            if token_price <= STOP_LOSS_PRICE:
                pnl = (token_price - token_entry) - FEE_PER_SHARE
                return {'outcome': 'stop_loss', 'pnl': pnl, 'exit_price': token_price}

    # Hold to resolution
    won = (direction == market['final_outcome'])
    if won:
        pnl = (1.0 - token_entry) - FEE_PER_SHARE
    else:
        pnl = (0.0 - token_entry) - FEE_PER_SHARE

    return {
        'outcome': 'win' if won else 'loss',
        'pnl': pnl,
        'exit_price': 1.0 if won else 0.0,
    }


def simulate_trade_realistic(signal, market, ticks_by_second):
    """Simulate with delayed entry and slippage. Returns (trade_result, no_fill)."""
    direction = signal['direction']

    # Delayed entry price
    delayed_sec = ENTRY_AFTER_SECONDS + REALISTIC_DELAY_SECONDS
    delayed_price_b = get_price_at_second(ticks_by_second, delayed_sec)
    if delayed_price_b is None:
        delayed_price_b = signal['entry_up_price']

    raw_entry = delayed_price_b if direction == 'Up' else (1 - delayed_price_b)
    token_entry = raw_entry * (1 + REALISTIC_SLIPPAGE_PCT)

    # If slippage pushes above PRICE_MAX → no fill
    if token_entry > PRICE_MAX:
        return None, True

    # Stop-loss monitoring (from delayed entry)
    if STOP_LOSS_PRICE is not None:
        for sec in range(delayed_sec, 300, 5):
            price = ticks_by_second.get(sec)
            if price is None:
                continue
            token_price = price if direction == 'Up' else (1 - price)
            if token_price <= STOP_LOSS_PRICE:
                pnl = (token_price - token_entry) - FEE_PER_SHARE
                return {'outcome': 'stop_loss', 'pnl': pnl, 'exit_price': token_price, 'token_entry': token_entry}, False

    won = (direction == market['final_outcome'])
    if won:
        pnl = (1.0 - token_entry) - FEE_PER_SHARE
    else:
        pnl = (0.0 - token_entry) - FEE_PER_SHARE

    return {
        'outcome': 'win' if won else 'loss',
        'pnl': pnl,
        'exit_price': 1.0 if won else 0.0,
        'token_entry': token_entry,
    }, False


# ── Output helpers ───────────────────────────────────────────────────────

def fmt_pnl(pnl):
    if pnl is None or (isinstance(pnl, float) and math.isnan(pnl)):
        return '\u2014'
    return f"+${pnl:.2f}" if pnl >= 0 else f"-${abs(pnl):.2f}"


def fmt_time(dt):
    if dt is None:
        return '\u2014'
    return dt.strftime('%Y-%m-%d %H:%M UTC')


def bucket_stats(trades, key_fn, buckets):
    """Group trades by key_fn into predefined buckets and compute stats."""
    grouped = defaultdict(list)
    for t in trades:
        grouped[key_fn(t)].append(t)

    results = []
    for label, filter_fn in buckets:
        matching = []
        for k, tlist in grouped.items():
            if filter_fn(k):
                matching.extend(tlist)
        if not matching:
            continue
        wins = sum(1 for t in matching if t['outcome'] == 'win')
        total_pnl = sum(t['pnl'] for t in matching)
        total_cost = sum(t['token_entry'] for t in matching)
        roi = (total_pnl / total_cost * 100) if total_cost > 0 else 0
        results.append((label, len(matching), wins, roi, total_pnl))
    return results


# ── Database ─────────────────────────────────────────────────────────────

def get_connection():
    return psycopg2.connect(
        host=os.getenv('POSTGRES_HOST', 'localhost'),
        port=os.getenv('POSTGRES_PORT', '5432'),
        user=os.getenv('POSTGRES_USER', 'polymarket'),
        password=os.getenv('POSTGRES_PASSWORD', ''),
        dbname=os.getenv('POSTGRES_DB', 'polymarket_tracker'),
    )


# ── Main ─────────────────────────────────────────────────────────────────

def main():
    conn = get_connection()

    try:
        # ── Load ALL resolved markets (no date filter) ────────────────
        df_markets = pd.read_sql("""
            SELECT market_id, market_type, started_at, ended_at, final_outcome
            FROM market_outcomes
            WHERE resolved = TRUE
              AND market_type IN %s
            ORDER BY started_at
        """, conn, params=[tuple(MARKET_TYPES)],
            parse_dates=['started_at', 'ended_at'])

        market_ids = df_markets['market_id'].tolist()

        # ── Load ticks in batches ─────────────────────────────────────
        ticks_by_market = {}
        batch_size = 500
        for i in range(0, len(market_ids), batch_size):
            batch = market_ids[i:i + batch_size]
            placeholders = ','.join(['%s'] * len(batch))
            df_batch = pd.read_sql(f"""
                SELECT time, market_id, up_price
                FROM market_ticks
                WHERE market_id IN ({placeholders})
                ORDER BY market_id, time
            """, conn, params=batch, parse_dates=['time'])

            if not df_batch.empty:
                for mid, grp in df_batch.groupby('market_id'):
                    ticks_by_market[mid] = grp.reset_index(drop=True)

    finally:
        conn.close()

    # ── Run signal detection + simulation on every market ─────────────
    trades = []           # list of trade dicts (signals that fired + simulated)
    realistic_trades = [] # same but with delay/slippage
    realistic_no_fills = 0

    # Funnel counters
    funnel_total = len(df_markets)
    funnel_no_ticks = 0
    funnel_threshold = 0
    funnel_direction = 0
    funnel_context = 0
    funnel_price_range = 0

    for _, m in df_markets.iterrows():
        mid = m['market_id']
        ticks_df = ticks_by_market.get(mid)
        if ticks_df is None or ticks_df.empty:
            funnel_no_ticks += 1
            continue

        tbs = index_ticks_by_second(ticks_df, m['started_at'])
        signal, reason = detect_signal(tbs)

        if reason == "no_ticks":
            funnel_no_ticks += 1
        elif reason == "threshold":
            funnel_threshold += 1
        elif reason == "direction":
            funnel_direction += 1
        elif reason == "context":
            funnel_context += 1
        elif reason == "price_range":
            funnel_price_range += 1

        if signal is None:
            continue

        result = simulate_trade(signal, m, tbs)
        trade = {
            **signal,
            **result,
            'market_id': mid,
            'market_type': m['market_type'],
            'started_at': m['started_at'],
            'final_outcome': m['final_outcome'],
            'hour': m['started_at'].hour,
        }
        trades.append(trade)

        # Realistic version
        r_result, no_fill = simulate_trade_realistic(signal, m, tbs)
        if no_fill:
            realistic_no_fills += 1
        elif r_result is not None:
            realistic_trades.append({
                **signal,
                **r_result,
                'market_id': mid,
                'market_type': m['market_type'],
                'started_at': m['started_at'],
            })

    # ── Compute aggregates ────────────────────────────────────────────
    total_trades = len(trades)
    wins = [t for t in trades if t['outcome'] == 'win']
    stop_losses = [t for t in trades if t['outcome'] == 'stop_loss']
    losses = [t for t in trades if t['outcome'] == 'loss']
    total_pnl = sum(t['pnl'] for t in trades)
    total_cost = sum(t['token_entry'] for t in trades)
    roi = (total_pnl / total_cost * 100) if total_cost > 0 else 0
    avg_pnl = (total_pnl / total_trades) if total_trades > 0 else 0
    avg_entry = (sum(t['token_entry'] for t in trades) / total_trades) if total_trades > 0 else 0

    # Time coverage
    first_market = df_markets['started_at'].min()
    last_market = df_markets['started_at'].max()
    calendar_days = (last_market - first_market).total_seconds() / 86400 if len(df_markets) > 1 else 1
    trades_per_day = total_trades / calendar_days if calendar_days > 0 else 0
    pnl_per_day = total_pnl / calendar_days if calendar_days > 0 else 0

    xrp_count = sum(1 for _, m in df_markets.iterrows() if m['market_type'] == 'xrp_5m')
    sol_count = sum(1 for _, m in df_markets.iterrows() if m['market_type'] == 'sol_5m')
    active_hours = funnel_total * 5 / 60

    # ==================================================================
    # SECTION 1 — Database coverage
    # ==================================================================
    print()
    print('=' * 70)
    print('DATABASE COVERAGE')
    print('=' * 70)
    print(f'  First market:              {fmt_time(first_market)}')
    print(f'  Last market:               {fmt_time(last_market)}')
    print(f'  Total calendar days:       {calendar_days:.1f} days')
    print(f'  Total resolved markets:    {funnel_total} (XRP: {xrp_count} | SOL: {sol_count})')
    print(f'  Markets per day (avg):     {funnel_total / calendar_days:.1f}')
    print(f'  Active hours in database:  {active_hours:.1f} hours')
    print(f'    (calculated as: total markets \u00d7 5 minutes / 60)')

    # ==================================================================
    # SECTION 2 — Signal funnel
    # ==================================================================
    signals_fired = total_trades
    signal_pct = (signals_fired / funnel_total * 100) if funnel_total > 0 else 0
    up_signals = sum(1 for t in trades if t['direction'] == 'Up')

    print()
    print('=' * 70)
    print('SIGNAL FUNNEL')
    print('=' * 70)
    print(f'  Total markets evaluated:       {funnel_total}')
    print(f'    Missing tick data:            {funnel_no_ticks}')
    print(f'    Failed threshold (< {THRESHOLD}):   {funnel_threshold}')
    print(f'    Failed direction filter:      {funnel_direction}')
    print(f'    Failed context filter:         {funnel_context}')
    print(f'    Failed price range:            {funnel_price_range}')
    print(f'    \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500')
    print(f'    Signals fired:                 {signals_fired}  ({signal_pct:.1f}% of markets)')
    print(f'      Up signals:                  {up_signals}')

    # ==================================================================
    # SECTION 3 — Overall performance
    # ==================================================================
    print()
    print('=' * 70)
    print('OVERALL PERFORMANCE')
    print('=' * 70)
    print(f'  Total trades:          {total_trades}')
    print(f'  Wins:                  {len(wins)}  ({len(wins)/total_trades*100:.1f}%)' if total_trades else '  Wins:                  0')
    print(f'  Stop-losses:           {len(stop_losses)}  ({len(stop_losses)/total_trades*100:.1f}%)' if total_trades else '  Stop-losses:           0')
    print(f'  Losses:                {len(losses)}  ({len(losses)/total_trades*100:.1f}%)' if total_trades else '  Losses:                0')
    print()
    print(f'  Total PnL:             {fmt_pnl(total_pnl)}  (normalized, 1 share per trade)')
    print(f'  ROI:                   {roi:.1f}%')
    print(f'  Avg PnL per trade:     ${avg_pnl:.4f}')
    print(f'  Avg entry price:       {avg_entry:.2f}')
    print()
    print(f'  Trades per day (avg):  {trades_per_day:.1f}')
    print(f'  PnL per day (avg):     {fmt_pnl(pnl_per_day)}')

    # ==================================================================
    # SECTION 4 — By asset
    # ==================================================================
    print()
    print('=' * 70)
    print('BY ASSET')
    print('=' * 70)
    for mtype in MARKET_TYPES:
        label = mtype.replace('_5m', '').upper()
        mt_trades = [t for t in trades if t['market_type'] == mtype]
        if not mt_trades:
            print(f'  {label}:  0 trades')
            continue
        mt_wins = sum(1 for t in mt_trades if t['outcome'] == 'win')
        mt_pnl = sum(t['pnl'] for t in mt_trades)
        mt_cost = sum(t['token_entry'] for t in mt_trades)
        mt_roi = (mt_pnl / mt_cost * 100) if mt_cost > 0 else 0
        print(f'  {label}:  {len(mt_trades)} trades | {mt_wins/len(mt_trades)*100:.1f}% win | {mt_roi:.1f}% ROI | {fmt_pnl(mt_pnl)} PnL')

    # ==================================================================
    # SECTION 5 — By hour (UTC)
    # ==================================================================
    print()
    print('=' * 70)
    print('BY HOUR (UTC)  (min 3 trades)')
    print('=' * 70)

    hour_data = defaultdict(list)
    for t in trades:
        hour_data[t['hour']].append(t)

    hour_rows = []
    for hour in sorted(hour_data.keys()):
        ht = hour_data[hour]
        if len(ht) < 3:
            continue
        h_wins = sum(1 for t in ht if t['outcome'] == 'win')
        h_pnl = sum(t['pnl'] for t in ht)
        h_cost = sum(t['token_entry'] for t in ht)
        h_roi = (h_pnl / h_cost * 100) if h_cost > 0 else 0
        hour_rows.append((hour, len(ht), h_wins, h_roi, h_pnl))

    # Sort by ROI descending
    hour_rows.sort(key=lambda x: x[3], reverse=True)
    for hour, count, w, r, p in hour_rows:
        print(f'  Hour {hour:02d}:  {count:3d} trades | {w/count*100:5.1f}% win | {r:+6.1f}% ROI | {fmt_pnl(p)} PnL')

    # ==================================================================
    # SECTION 6 — By entry price bucket
    # ==================================================================
    print()
    print('=' * 70)
    print('BY ENTRY PRICE')
    print('=' * 70)

    price_buckets = [
        ('0.50\u20130.55', lambda t: 0.50 <= t['token_entry'] < 0.55),
        ('0.55\u20130.60', lambda t: 0.55 <= t['token_entry'] < 0.60),
        ('0.60\u20130.65', lambda t: 0.60 <= t['token_entry'] < 0.65),
        ('0.65\u20130.70', lambda t: 0.65 <= t['token_entry'] < 0.70),
        ('0.70\u20130.75', lambda t: 0.70 <= t['token_entry'] < 0.75),
        ('0.75\u20130.78', lambda t: 0.75 <= t['token_entry'] <= 0.78),
    ]
    for label, filt in price_buckets:
        bt = [t for t in trades if filt(t)]
        if not bt:
            print(f'  {label}:  0 trades')
            continue
        bw = sum(1 for t in bt if t['outcome'] == 'win')
        bp = sum(t['pnl'] for t in bt)
        bc = sum(t['token_entry'] for t in bt)
        br = (bp / bc * 100) if bc > 0 else 0
        print(f'  {label}:  {len(bt):3d} trades | {bw/len(bt)*100:5.1f}% win | {br:+6.1f}% ROI | {fmt_pnl(bp)} PnL')

    # ==================================================================
    # SECTION 7 — By momentum strength
    # ==================================================================
    print()
    print('=' * 70)
    print('BY MOMENTUM STRENGTH')
    print('=' * 70)

    mom_buckets = [
        ('0.15\u20130.20', lambda t: 0.15 <= abs(t['momentum']) < 0.20),
        ('0.20\u20130.25', lambda t: 0.20 <= abs(t['momentum']) < 0.25),
        ('0.25\u20130.30', lambda t: 0.25 <= abs(t['momentum']) < 0.30),
        ('0.30+',          lambda t: abs(t['momentum']) >= 0.30),
    ]
    for label, filt in mom_buckets:
        bt = [t for t in trades if filt(t)]
        if not bt:
            print(f'  {label:11s}:  0 trades')
            continue
        bw = sum(1 for t in bt if t['outcome'] == 'win')
        bp = sum(t['pnl'] for t in bt)
        bc = sum(t['token_entry'] for t in bt)
        br = (bp / bc * 100) if bc > 0 else 0
        print(f'  {label:11s}:  {len(bt):3d} trades | {bw/len(bt)*100:5.1f}% win | {br:+6.1f}% ROI | {fmt_pnl(bp)} PnL')

    # ==================================================================
    # SECTION 8 — Monthly projection
    # ==================================================================
    print()
    print('=' * 70)
    print('MONTHLY PROJECTION (based on historical averages)')
    print('=' * 70)

    avg_roi_per_trade = roi / 100 if total_trades > 0 else 0  # as decimal
    print(f'  Avg trades per day:          {trades_per_day:.1f}')
    print(f'  Avg ROI per trade:           {roi/total_trades:.2f}%' if total_trades else '  Avg ROI per trade:           0.00%')
    roi_per_trade_decimal = (roi / total_trades / 100) if total_trades > 0 else 0

    bet_pct = 0.02
    for start_bal in [200, 600]:
        print()
        print(f'  Starting balance ${start_bal} | bet {bet_pct*100:.0f}% per trade:')

        # Day 1
        balance = float(start_bal)
        day1_trades = round(trades_per_day)
        for _ in range(day1_trades):
            bet = balance * bet_pct
            pnl_t = bet * roi_per_trade_decimal
            balance += pnl_t
        day1_profit = balance - start_bal
        print(f'    Projected day 1:           {fmt_pnl(day1_profit)}')

        # Week 1
        balance = float(start_bal)
        week_trades = round(trades_per_day * 7)
        for _ in range(week_trades):
            bet = balance * bet_pct
            pnl_t = bet * roi_per_trade_decimal
            balance += pnl_t
        week_profit = balance - start_bal
        print(f'    Projected week 1:          {fmt_pnl(week_profit)}')

        # Month (30 days)
        balance = float(start_bal)
        month_trades = round(trades_per_day * 30)
        for _ in range(month_trades):
            bet = balance * bet_pct
            pnl_t = bet * roi_per_trade_decimal
            balance += pnl_t
        month_profit = balance - start_bal
        print(f'    Projected month:           {fmt_pnl(month_profit)}  (compounding)')

    print()
    print('  Note: projections assume constant ROI and ignore liquidity ceiling.')

    # ==================================================================
    # SECTION 9 — Realistic execution adjustment
    # ==================================================================
    print()
    print('=' * 70)
    print(f'REALISTIC EXECUTION ({REALISTIC_DELAY_SECONDS}s delay + {REALISTIC_SLIPPAGE_PCT*100:.1f}% slippage)')
    print('=' * 70)

    r_total = len(realistic_trades)
    r_wins = sum(1 for t in realistic_trades if t['outcome'] == 'win')
    r_sls = sum(1 for t in realistic_trades if t['outcome'] == 'stop_loss')
    r_losses = sum(1 for t in realistic_trades if t['outcome'] == 'loss')
    r_pnl = sum(t['pnl'] for t in realistic_trades)
    r_cost = sum(t['token_entry'] for t in realistic_trades)
    r_roi = (r_pnl / r_cost * 100) if r_cost > 0 else 0
    r_win_pct = (r_wins / r_total * 100) if r_total > 0 else 0

    ideal_win_pct = (len(wins) / total_trades * 100) if total_trades > 0 else 0
    exec_cost = total_pnl - r_pnl

    print(f'  {"":>18} {"IDEAL":>12} {"REALISTIC":>12}')
    print(f'  {"Trades:":>18} {total_trades:>12}   {r_total:>12}  ({realistic_no_fills} lost to slippage)')
    print(f'  {"Win rate:":>18} {ideal_win_pct:>11.1f}% {r_win_pct:>11.1f}%')
    print(f'  {"ROI:":>18} {roi:>11.1f}% {r_roi:>11.1f}%')
    print(f'  {"Total PnL:":>18} {fmt_pnl(total_pnl):>12} {fmt_pnl(r_pnl):>12}')
    print(f'  {"Execution cost:":>18} {"":>12} {fmt_pnl(exec_cost):>12}')
    print()


if __name__ == '__main__':
    main()
