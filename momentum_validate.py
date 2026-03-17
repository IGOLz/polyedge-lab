"""
Momentum Strategy Single-Config Validation
Runs the exact live bot configuration against all resolved XRP+SOL 5m markets.
Includes execution realism simulation and actual-vs-predicted comparison.
Read-only — saves nothing to the database.
"""

import asyncpg
import asyncio
import os
import sys
import time
from dotenv import load_dotenv
from collections import defaultdict

sys.stdout.reconfigure(encoding='utf-8', line_buffering=True)
sys.stderr.reconfigure(encoding='utf-8', line_buffering=True)

load_dotenv()

# ── Exact live configuration (DO NOT CHANGE) ──────────────────────────────────
PRICE_A_SECONDS     = 45
PRICE_B_SECONDS     = 60
ENTRY_AFTER_SECONDS = 65
ENTRY_UNTIL_SECONDS = 90
THRESHOLD           = 0.10
PRICE_MIN           = 0.50
PRICE_MAX           = 0.75
DIRECTION           = 'both'
MARKETS             = 'xrp_sol_only'
HOURS_START         = 0
HOURS_END           = 24
CONTEXT_MAX_DELTA   = 0.10
PRICE_OPEN_SECONDS  = 0
STOP_LOSS_PRICE     = 0.35
FEE_PER_SHARE       = 0.02

# ── Execution realism parameters ──────────────────────────────────────────────
ENTRY_DELAY_SECONDS = 3       # seconds of bot latency after signal fires
PRICE_SLIPPAGE      = 0.005   # worst-case price slip as fraction (0.5%)
PARTIAL_FILL_RATE   = 0.05    # 5% of trades lose 1 share due to price movement

ALLOWED_TYPES = {'xrp_5m', 'sol_5m'}


# ── Helpers (same logic as brute force) ───────────────────────────────────────

def index_ticks_by_second(ticks, started_at):
    result = {}
    for tick in ticks:
        elapsed = int((tick['time'] - started_at).total_seconds())
        result[elapsed] = tick['up_price']
    return result


def get_price_at_second(ticks_by_second, target_sec, max_offset=5):
    price = ticks_by_second.get(target_sec)
    if price is not None:
        return price
    for offset in range(1, max_offset + 1):
        price = ticks_by_second.get(target_sec + offset)
        if price is not None:
            return price
        price = ticks_by_second.get(target_sec - offset)
        if price is not None:
            return price
    return None


def simulate_trade(direction, entry_up_price, market, ticks_by_second, entry_second,
                   stop_loss_price):
    """Simulate trade with stop-loss. Returns (outcome, pnl, token_entry)."""
    token_entry = entry_up_price if direction == 'Up' else (1 - entry_up_price)

    if stop_loss_price is not None:
        for sec in range(entry_second, 300, 5):
            price = ticks_by_second.get(sec)
            if price is None:
                continue
            token_price = price if direction == 'Up' else (1 - price)
            if token_price <= stop_loss_price:
                pnl = token_price - token_entry
                return 'stop_loss', pnl, token_entry

    won = (direction == market['final_outcome'])
    pnl = (1 - token_entry) if won else (0 - token_entry)
    return ('win' if won else 'loss'), pnl, token_entry


def simulate_trade_realistic(direction, realistic_entry_price, market, ticks_by_second,
                             entry_second, stop_loss_price):
    """Simulate trade using a realistic (slipped) entry price.
    realistic_entry_price is already the token-direction entry with slippage applied."""
    token_entry = realistic_entry_price

    if stop_loss_price is not None:
        for sec in range(entry_second, 300, 5):
            price = ticks_by_second.get(sec)
            if price is None:
                continue
            token_price = price if direction == 'Up' else (1 - price)
            if token_price <= stop_loss_price:
                pnl = token_price - token_entry
                return 'stop_loss', pnl, token_entry

    won = (direction == market['final_outcome'])
    pnl = (1 - token_entry) if won else (0 - token_entry)
    return ('win' if won else 'loss'), pnl, token_entry


def pct(n, total):
    return (n / total * 100) if total > 0 else 0.0


# ── Main ──────────────────────────────────────────────────────────────────────

async def main():
    pg_host = os.getenv('POSTGRES_HOST', 'localhost')
    pg_port = int(os.getenv('POSTGRES_PORT', '5432'))
    pg_user = os.getenv('POSTGRES_USER', 'polymarket')
    pg_password = os.getenv('POSTGRES_PASSWORD', '')
    pg_db = os.getenv('POSTGRES_DB', 'polymarket_tracker')

    print("Connecting to database (read-only)...", flush=True)
    conn = await asyncpg.connect(
        host=pg_host, port=pg_port,
        user=pg_user, password=pg_password,
        database=pg_db,
    )

    real_trades_rows = []
    try:
        print("Loading resolved XRP+SOL 5m markets...", flush=True)
        markets_rows = await conn.fetch("""
            SELECT market_id, market_type, started_at, ended_at, final_outcome, final_up_price
            FROM market_outcomes
            WHERE resolved = TRUE
            AND market_type IN ('xrp_5m', 'sol_5m')
            ORDER BY started_at
        """)
        markets = [dict(r) for r in markets_rows]
        print(f"Loaded {len(markets)} resolved markets", flush=True)

        if not markets:
            print("No resolved markets found. Exiting.", flush=True)
            return

        market_ids = [m['market_id'] for m in markets]

        print("Loading ticks...", flush=True)
        ticks_rows = await conn.fetch("""
            SELECT time, market_id, up_price
            FROM market_ticks
            WHERE market_id = ANY($1)
            ORDER BY time
        """, market_ids)

        ticks_by_market_raw = defaultdict(list)
        for r in ticks_rows:
            ticks_by_market_raw[r['market_id']].append({
                'time': r['time'],
                'up_price': float(r['up_price']),
            })
        print(f"Loaded {len(ticks_rows)} ticks across {len(ticks_by_market_raw)} markets", flush=True)

        # ── Addition 2: Load actual bot trades ────────────────────────────────
        print("Loading actual bot trades...", flush=True)
        try:
            real_trades_rows = await conn.fetch("""
                SELECT t.market_id, t.direction, t.entry_price, t.shares,
                       t.outcome, t.pnl, t.signal_data
                FROM trades t
                WHERE t.strategy_name = 'momentum'
                AND t.outcome != 'Pending'
                ORDER BY t.created_at
            """)
            print(f"Loaded {len(real_trades_rows)} actual bot trades", flush=True)
        except asyncpg.exceptions.UndefinedTableError:
            print("  'trades' table does not exist — skipping actual vs predicted comparison", flush=True)
            real_trades_rows = []
        except asyncpg.exceptions.UndefinedColumnError as e:
            print(f"  Column mismatch in trades table ({e}) — skipping actual vs predicted", flush=True)
            real_trades_rows = []

    finally:
        await conn.close()
        print("Database connection closed.\n", flush=True)

    # Index ticks by second
    print("Indexing ticks by second...", flush=True)
    market_data = []
    for m in markets:
        mid = m['market_id']
        raw = ticks_by_market_raw.get(mid, [])
        if not raw:
            continue
        tbs = index_ticks_by_second(raw, m['started_at'])
        market_data.append((m, tbs))
    print(f"Indexed {len(market_data)} markets\n", flush=True)

    # ══════════════════════════════════════════════════════════════════════════
    # Run BOTH ideal and realistic simulations in a single pass
    # ══════════════════════════════════════════════════════════════════════════
    entry_sec_ideal = ENTRY_AFTER_SECONDS       # 65
    entry_sec_real = ENTRY_AFTER_SECONDS + ENTRY_DELAY_SECONDS  # 68

    total_markets = len(market_data)
    no_signal_missing_data = 0
    no_signal_threshold = 0
    no_signal_context = 0
    no_signal_price_range = 0

    trades_ideal = []
    trades_realistic = []
    no_fill_slippage = 0
    no_fill_delay_missing = 0

    for m, tbs in market_data:
        # Step 1: Get prices for signal detection (same for both)
        price_open = get_price_at_second(tbs, PRICE_OPEN_SECONDS, max_offset=10)
        price_a = get_price_at_second(tbs, PRICE_A_SECONDS)
        price_b = get_price_at_second(tbs, PRICE_B_SECONDS)

        if price_open is None or price_a is None or price_b is None:
            no_signal_missing_data += 1
            continue

        # Step 2: Momentum
        momentum = price_b - price_a
        if abs(momentum) < THRESHOLD:
            no_signal_threshold += 1
            continue

        # Step 3: Direction
        if momentum > 0:
            direction = 'Up'
            entry_price = price_b
        else:
            direction = 'Down'
            entry_price = 1 - price_b

        # Step 4: Context filter
        open_delta = price_b - price_open
        if direction == 'Down' and open_delta > CONTEXT_MAX_DELTA:
            no_signal_context += 1
            continue
        if direction == 'Up' and open_delta < -CONTEXT_MAX_DELTA:
            no_signal_context += 1
            continue

        # Step 5: Entry price filter (on ideal price)
        if entry_price < PRICE_MIN or entry_price > PRICE_MAX:
            no_signal_price_range += 1
            continue

        # ── IDEAL simulation ──────────────────────────────────────────────
        outcome_i, pnl_i, token_entry_i = simulate_trade(
            direction, price_b, m, tbs, entry_sec_ideal, STOP_LOSS_PRICE
        )
        pnl_i_fee = pnl_i - FEE_PER_SHARE

        trade_base = {
            'market_id': m['market_id'],
            'market_type': m['market_type'],
            'started_at': m['started_at'],
            'direction': direction,
            'entry_price': entry_price,
            'token_entry': token_entry_i,
            'momentum': abs(momentum),
            'outcome': outcome_i,
            'pnl_raw': pnl_i,
            'pnl': pnl_i_fee,
            'hour': m['started_at'].hour,
        }
        trades_ideal.append(trade_base)

        # ── REALISTIC simulation ──────────────────────────────────────────
        # Get delayed price at entry_sec + delay
        delayed_up_price = get_price_at_second(tbs, entry_sec_real)
        if delayed_up_price is None:
            # Can't get a price at delayed entry — trade would not fill
            no_fill_delay_missing += 1
            continue

        # Compute realistic token entry with slippage (always against the trader)
        if direction == 'Up':
            realistic_token_entry = delayed_up_price * (1 + PRICE_SLIPPAGE)
        else:
            realistic_token_entry = (1 - delayed_up_price) * (1 + PRICE_SLIPPAGE)

        # Check if slipped price exceeds PRICE_MAX — FOK would fail
        if realistic_token_entry > PRICE_MAX:
            no_fill_slippage += 1
            continue

        outcome_r, pnl_r, _ = simulate_trade_realistic(
            direction, realistic_token_entry, m, tbs, entry_sec_real, STOP_LOSS_PRICE
        )
        pnl_r_fee = pnl_r - FEE_PER_SHARE

        trades_realistic.append({
            'market_id': m['market_id'],
            'market_type': m['market_type'],
            'started_at': m['started_at'],
            'direction': direction,
            'entry_price': realistic_token_entry,
            'token_entry': realistic_token_entry,
            'momentum': abs(momentum),
            'outcome': outcome_r,
            'pnl_raw': pnl_r,
            'pnl': pnl_r_fee,
            'hour': m['started_at'].hour,
        })

    # ══════════════════════════════════════════════════════════════════════════
    # SECTION 1 — Signal Funnel
    # ══════════════════════════════════════════════════════════════════════════
    signals_fired = len(trades_ideal)
    up_signals = sum(1 for t in trades_ideal if t['direction'] == 'Up')
    down_signals = sum(1 for t in trades_ideal if t['direction'] == 'Down')

    print("=" * 70)
    print("SECTION 1 — Signal Funnel")
    print("=" * 70)
    print(f"  Total XRP+SOL 5m markets:        {total_markets}")
    print(f"    Missing data (skipped):         {no_signal_missing_data}")
    print(f"    Failed threshold (< {THRESHOLD}):     {no_signal_threshold}")
    print(f"    Failed context filter:          {no_signal_context}")
    print(f"    Failed price range:             {no_signal_price_range}")
    print(f"    {'─' * 40}")
    print(f"    Signals fired:                  {signals_fired}  ({pct(signals_fired, total_markets):.1f}% of markets)")
    print(f"      Up signals:                   {up_signals}  ({pct(up_signals, total_markets):.1f}%)")
    print(f"      Down signals:                 {down_signals}  ({pct(down_signals, total_markets):.1f}%)")

    if not trades_ideal:
        print("\nNo trades to analyze. Exiting.")
        return

    # ══════════════════════════════════════════════════════════════════════════
    # SECTION 2 — Overall Performance
    # ══════════════════════════════════════════════════════════════════════════
    wins = [t for t in trades_ideal if t['outcome'] == 'win']
    stop_losses = [t for t in trades_ideal if t['outcome'] == 'stop_loss']
    losses = [t for t in trades_ideal if t['outcome'] == 'loss']
    total_pnl = sum(t['pnl'] for t in trades_ideal)
    total_entry_cost = sum(t['token_entry'] for t in trades_ideal)
    roi = (total_pnl / total_entry_cost * 100) if total_entry_cost > 0 else 0
    avg_pnl = total_pnl / len(trades_ideal)
    avg_entry = sum(t['entry_price'] for t in trades_ideal) / len(trades_ideal)

    print(f"\n{'=' * 70}")
    print("SECTION 2 — Overall Performance")
    print("=" * 70)
    print(f"  Total trades:        {len(trades_ideal)}")
    print(f"  Wins:                {len(wins)}  ({pct(len(wins), len(trades_ideal)):.1f}%)")
    print(f"  Stop-losses:         {len(stop_losses)}  ({pct(len(stop_losses), len(trades_ideal)):.1f}%)")
    print(f"  Losses:              {len(losses)}  ({pct(len(losses), len(trades_ideal)):.1f}%)")
    print(f"  Total PnL:           ${total_pnl:.2f}  (per share, normalized)")
    print(f"  ROI:                 {roi:.1f}%")
    print(f"  Avg PnL/trade:       ${avg_pnl:.4f}")
    print(f"  Avg entry price:     {avg_entry:.2f}")

    # ══════════════════════════════════════════════════════════════════════════
    # SECTION 3 — By Asset
    # ══════════════════════════════════════════════════════════════════════════
    print(f"\n{'=' * 70}")
    print("SECTION 3 — By Asset")
    print("=" * 70)
    for asset in ['xrp_5m', 'sol_5m']:
        at = [t for t in trades_ideal if t['market_type'] == asset]
        if not at:
            print(f"  {asset.upper().replace('_5M','')}: 0 trades")
            continue
        a_wins = sum(1 for t in at if t['outcome'] == 'win')
        a_pnl = sum(t['pnl'] for t in at)
        a_entry = sum(t['token_entry'] for t in at)
        a_roi = (a_pnl / a_entry * 100) if a_entry > 0 else 0
        label = asset.split('_')[0].upper()
        print(f"  {label}:  {len(at)} trades | {pct(a_wins, len(at)):.1f}% win | {a_roi:.1f}% ROI | ${a_pnl:.2f} PnL")

    # ══════════════════════════════════════════════════════════════════════════
    # SECTION 4 — By Direction
    # ══════════════════════════════════════════════════════════════════════════
    print(f"\n{'=' * 70}")
    print("SECTION 4 — By Direction")
    print("=" * 70)
    for d in ['Up', 'Down']:
        dt = [t for t in trades_ideal if t['direction'] == d]
        if not dt:
            print(f"  {d}: 0 trades")
            continue
        d_wins = sum(1 for t in dt if t['outcome'] == 'win')
        d_pnl = sum(t['pnl'] for t in dt)
        d_entry = sum(t['token_entry'] for t in dt)
        d_roi = (d_pnl / d_entry * 100) if d_entry > 0 else 0
        print(f"  {d:<5} {len(dt)} trades | {pct(d_wins, len(dt)):.1f}% win | {d_roi:.1f}% ROI | ${d_pnl:.2f} PnL")

    # ══════════════════════════════════════════════════════════════════════════
    # SECTION 5 — By Hour (UTC)
    # ══════════════════════════════════════════════════════════════════════════
    print(f"\n{'=' * 70}")
    print("SECTION 5 — By Hour (UTC)")
    print("=" * 70)
    hours_map = defaultdict(list)
    for t in trades_ideal:
        hours_map[t['hour']].append(t)
    for h in range(24):
        ht = hours_map.get(h, [])
        if len(ht) < 5:
            continue
        h_wins = sum(1 for t in ht if t['outcome'] == 'win')
        h_pnl = sum(t['pnl'] for t in ht)
        h_entry = sum(t['token_entry'] for t in ht)
        h_roi = (h_pnl / h_entry * 100) if h_entry > 0 else 0
        print(f"  Hour {h:02d}: {len(ht):>4} trades | {pct(h_wins, len(ht)):.1f}% win | {h_roi:>6.1f}% ROI | ${h_pnl:>7.2f} PnL")

    # ══════════════════════════════════════════════════════════════════════════
    # SECTION 6 — By Entry Price Bucket (0.05 wide)
    # ══════════════════════════════════════════════════════════════════════════
    print(f"\n{'=' * 70}")
    print("SECTION 6 — By Entry Price Bucket")
    print("=" * 70)
    buckets_price = [
        (0.50, 0.55), (0.55, 0.60), (0.60, 0.65), (0.65, 0.70), (0.70, 0.75),
    ]
    for lo, hi in buckets_price:
        bt = [t for t in trades_ideal if lo <= t['entry_price'] < hi]
        if not bt:
            print(f"  {lo:.2f}–{hi:.2f}:  0 trades")
            continue
        b_wins = sum(1 for t in bt if t['outcome'] == 'win')
        b_pnl = sum(t['pnl'] for t in bt)
        b_entry = sum(t['token_entry'] for t in bt)
        b_roi = (b_pnl / b_entry * 100) if b_entry > 0 else 0
        print(f"  {lo:.2f}–{hi:.2f}:  {len(bt):>4} trades | {pct(b_wins, len(bt)):.1f}% win | {b_roi:>6.1f}% ROI")

    # ══════════════════════════════════════════════════════════════════════════
    # SECTION 7 — By Momentum Strength Bucket
    # ══════════════════════════════════════════════════════════════════════════
    print(f"\n{'=' * 70}")
    print("SECTION 7 — By Momentum Strength Bucket")
    print("=" * 70)
    momentum_buckets = [
        (0.10, 0.15, "0.10–0.15"),
        (0.15, 0.20, "0.15–0.20"),
        (0.20, 0.30, "0.20–0.30"),
        (0.30, 999,  "0.30+    "),
    ]
    for lo, hi, label in momentum_buckets:
        bt = [t for t in trades_ideal if lo <= t['momentum'] < hi]
        if not bt:
            print(f"  {label}:  0 trades")
            continue
        b_wins = sum(1 for t in bt if t['outcome'] == 'win')
        b_pnl = sum(t['pnl'] for t in bt)
        b_entry = sum(t['token_entry'] for t in bt)
        b_roi = (b_pnl / b_entry * 100) if b_entry > 0 else 0
        print(f"  {label}:  {len(bt):>4} trades | {pct(b_wins, len(bt)):.1f}% win | {b_roi:>6.1f}% ROI")

    # ══════════════════════════════════════════════════════════════════════════
    # SECTION 8 — Worst Streaks
    # ══════════════════════════════════════════════════════════════════════════
    print(f"\n{'=' * 70}")
    print("SECTION 8 — Worst Streaks")
    print("=" * 70)

    max_losing = 0
    max_winning = 0
    cur_losing = 0
    cur_winning = 0
    for t in trades_ideal:
        is_win = t['outcome'] == 'win'
        if is_win:
            cur_winning += 1
            cur_losing = 0
            max_winning = max(max_winning, cur_winning)
        else:
            cur_losing += 1
            cur_winning = 0
            max_losing = max(max_losing, cur_losing)

    cumulative = 0.0
    peak = 0.0
    max_dd = 0.0
    dd_trades = 0
    cur_dd_start = 0
    for i, t in enumerate(trades_ideal):
        cumulative += t['pnl']
        if cumulative > peak:
            peak = cumulative
            cur_dd_start = i
        dd = peak - cumulative
        if dd > max_dd:
            max_dd = dd
            dd_trades = i - cur_dd_start

    print(f"  Longest losing streak:   {max_losing} trades")
    print(f"  Longest winning streak:  {max_winning} trades")
    print(f"  Max drawdown (per share): ${max_dd:.2f} over {dd_trades} trades")

    # ══════════════════════════════════════════════════════════════════════════
    # SECTION 9 — Stop-Loss Analysis
    # ══════════════════════════════════════════════════════════════════════════
    print(f"\n{'=' * 70}")
    print("SECTION 9 — Stop-Loss Analysis")
    print("=" * 70)
    sl_trades = [t for t in trades_ideal if t['outcome'] == 'stop_loss']
    non_sl_losses = [t for t in trades_ideal if t['outcome'] == 'loss']

    print(f"  Trades where SL fired:    {len(sl_trades)}  ({pct(len(sl_trades), len(trades_ideal)):.1f}% of all trades)")
    if sl_trades:
        avg_sl_pnl = sum(t['pnl'] for t in sl_trades) / len(sl_trades)
        print(f"  Avg PnL when SL fires:    ${avg_sl_pnl:.4f}")
    if non_sl_losses:
        avg_loss_pnl = sum(t['pnl'] for t in non_sl_losses) / len(non_sl_losses)
        print(f"  Avg PnL when SL does NOT fire and trade loses: ${avg_loss_pnl:.4f}")

    # Re-simulate SL trades without stop-loss to measure savings
    sl_trade_ids = {t['market_id'] for t in sl_trades}
    sl_savings = 0.0
    for m, tbs in market_data:
        if m['market_id'] not in sl_trade_ids:
            continue
        price_a = get_price_at_second(tbs, PRICE_A_SECONDS)
        price_b = get_price_at_second(tbs, PRICE_B_SECONDS)
        if price_a is None or price_b is None:
            continue
        momentum_val = price_b - price_a
        direction = 'Up' if momentum_val > 0 else 'Down'

        _, pnl_with_sl, _ = simulate_trade(direction, price_b, m, tbs, entry_sec_ideal, STOP_LOSS_PRICE)
        _, pnl_no_sl, _ = simulate_trade(direction, price_b, m, tbs, entry_sec_ideal, None)
        sl_savings += (pnl_with_sl - pnl_no_sl)

    print(f"  SL saved (estimated):     ${sl_savings:.2f}  (positive = SL helped)")

    # ══════════════════════════════════════════════════════════════════════════
    # SECTION 10 — Context Filter Impact
    # ══════════════════════════════════════════════════════════════════════════
    print(f"\n{'=' * 70}")
    print("SECTION 10 — Context Filter Impact")
    print("=" * 70)

    trades_no_ctx = []
    blocked_by_ctx = []

    for m, tbs in market_data:
        price_open = get_price_at_second(tbs, PRICE_OPEN_SECONDS, max_offset=10)
        price_a = get_price_at_second(tbs, PRICE_A_SECONDS)
        price_b = get_price_at_second(tbs, PRICE_B_SECONDS)

        if price_open is None or price_a is None or price_b is None:
            continue

        momentum_val = price_b - price_a
        if abs(momentum_val) < THRESHOLD:
            continue

        if momentum_val > 0:
            direction = 'Up'
            ep = price_b
        else:
            direction = 'Down'
            ep = 1 - price_b

        if ep < PRICE_MIN or ep > PRICE_MAX:
            continue

        open_delta = price_b - price_open
        ctx_blocked = False
        if direction == 'Down' and open_delta > CONTEXT_MAX_DELTA:
            ctx_blocked = True
        if direction == 'Up' and open_delta < -CONTEXT_MAX_DELTA:
            ctx_blocked = True

        outcome, pnl, token_entry = simulate_trade(direction, price_b, m, tbs, entry_sec_ideal, STOP_LOSS_PRICE)
        pnl_after_fee = pnl - FEE_PER_SHARE

        trade_info = {
            'outcome': outcome,
            'pnl': pnl_after_fee,
            'token_entry': token_entry,
        }
        trades_no_ctx.append(trade_info)

        if ctx_blocked:
            blocked_by_ctx.append(trade_info)

    with_wins_n = len(wins)
    with_total = len(trades_ideal)
    with_roi = roi

    wo_total = len(trades_no_ctx)
    wo_wins = sum(1 for t in trades_no_ctx if t['outcome'] == 'win')
    wo_pnl = sum(t['pnl'] for t in trades_no_ctx)
    wo_entry = sum(t['token_entry'] for t in trades_no_ctx)
    wo_roi = (wo_pnl / wo_entry * 100) if wo_entry > 0 else 0

    blocked_total = len(blocked_by_ctx)
    blocked_wins = sum(1 for t in blocked_by_ctx if t['outcome'] == 'win')
    blocked_losses = blocked_total - blocked_wins
    filter_accuracy = (blocked_losses / blocked_total * 100) if blocked_total > 0 else 0

    print(f"  WITH context filter:     {with_total} trades | {pct(with_wins_n, with_total):.1f}% win | {with_roi:.1f}% ROI")
    print(f"  WITHOUT context filter:  {wo_total} trades | {pct(wo_wins, wo_total):.1f}% win | {wo_roi:.1f}% ROI")
    print(f"  Trades blocked by filter: {blocked_total}")
    if blocked_total > 0:
        print(f"    Would have been wins:   {blocked_wins}  ({pct(blocked_wins, blocked_total):.1f}%)")
        print(f"    Would have been losses: {blocked_losses}  ({pct(blocked_losses, blocked_total):.1f}%)")
    else:
        print(f"    Would have been wins:   0")
        print(f"    Would have been losses: 0")
    print(f"    Filter accuracy:        {filter_accuracy:.1f}% (% of blocked trades that were losses)")

    # ══════════════════════════════════════════════════════════════════════════
    # SECTION 11 — Execution Realism: Ideal vs Realistic
    # ══════════════════════════════════════════════════════════════════════════
    print(f"\n{'=' * 70}")
    print(f"SECTION 11 — Execution Realism: Ideal vs Realistic (+{ENTRY_DELAY_SECONDS}s delay, {PRICE_SLIPPAGE*100:.1f}% slip)")
    print("=" * 70)

    n_ideal = len(trades_ideal)
    n_real = len(trades_realistic)
    lost_to_execution = n_ideal - n_real

    wins_i = sum(1 for t in trades_ideal if t['outcome'] == 'win')
    wins_r = sum(1 for t in trades_realistic if t['outcome'] == 'win')
    pnl_i = sum(t['pnl'] for t in trades_ideal)
    pnl_r = sum(t['pnl'] for t in trades_realistic)
    entry_i = sum(t['token_entry'] for t in trades_ideal)
    entry_r = sum(t['token_entry'] for t in trades_realistic)
    roi_i = (pnl_i / entry_i * 100) if entry_i > 0 else 0
    roi_r = (pnl_r / entry_r * 100) if entry_r > 0 else 0
    exec_cost = pnl_i - pnl_r

    col1 = 24
    col2 = 15
    col3 = 40
    print(f"  {'':>{col1}}{'IDEAL':>{col2}}{'REALISTIC':>{col2}}")
    print(f"  {'─' * (col1 + col2 * 2 + 10)}")
    print(f"  {'Trades fired:':<{col1}}{n_ideal:>{col2}}    {n_real:>{col2}}  ({lost_to_execution} lost to slippage/delay)")
    print(f"  {'  No fill (delay):':<{col1}}{'':{col2}}    {no_fill_delay_missing:>{col2}}")
    print(f"  {'  No fill (slippage):':<{col1}}{'':{col2}}    {no_fill_slippage:>{col2}}")
    print(f"  {'Win rate:':<{col1}}{pct(wins_i, n_ideal):>{col2}.1f}%   {pct(wins_r, n_real):>{col2}.1f}%")
    print(f"  {'ROI:':<{col1}}{roi_i:>{col2}.1f}%   {roi_r:>{col2}.1f}%")
    print(f"  {'Total PnL:':<{col1}}{'${:.2f}'.format(pnl_i):>{col2}}   {'${:.2f}'.format(pnl_r):>{col2}}")
    print(f"  {'Avg PnL/trade:':<{col1}}{'${:.4f}'.format(pnl_i/n_ideal):>{col2}}   {'${:.4f}'.format(pnl_r/n_real if n_real > 0 else 0):>{col2}}")
    print(f"  {'Avg entry price:':<{col1}}{sum(t['entry_price'] for t in trades_ideal)/n_ideal:>{col2}.4f}   {sum(t['entry_price'] for t in trades_realistic)/n_real if n_real > 0 else 0:>{col2}.4f}")
    print(f"\n  Execution cost (ideal PnL - realistic PnL): ${exec_cost:.2f}")

    # Per-trade entry price comparison for matched trades
    ideal_by_market = {t['market_id']: t for t in trades_ideal}
    real_by_market = {t['market_id']: t for t in trades_realistic}
    matched_ids = set(ideal_by_market.keys()) & set(real_by_market.keys())
    if matched_ids:
        entry_diffs = []
        for mid in matched_ids:
            diff = real_by_market[mid]['entry_price'] - ideal_by_market[mid]['entry_price']
            entry_diffs.append(diff)
        avg_slip = sum(entry_diffs) / len(entry_diffs)
        max_slip = max(entry_diffs)
        print(f"  Matched trades:     {len(matched_ids)}")
        print(f"  Avg entry slip:     {avg_slip:+.4f} per trade")
        print(f"  Max entry slip:     {max_slip:+.4f}")

    # ══════════════════════════════════════════════════════════════════════════
    # SECTION 12 — Actual Bot Performance vs Backtest Prediction
    # ══════════════════════════════════════════════════════════════════════════
    print(f"\n{'=' * 70}")
    print("SECTION 12 — Actual Bot Performance vs Backtest Prediction")
    print("=" * 70)

    if not real_trades_rows:
        print("  No actual bot trades found in database.")
        print("  (Either the 'trades' table doesn't exist or has no momentum trades yet)")
        print("  This section will populate once the live bot logs individual trades")
        print("  to a 'trades' table with columns: market_id, direction, entry_price,")
        print("  shares, outcome, pnl, signal_data, strategy_name, created_at")
    else:
        real_trades = [dict(r) for r in real_trades_rows]
        backtest_by_market = {t['market_id']: t for t in trades_ideal}

        matched = []
        for rt in real_trades:
            bt = backtest_by_market.get(rt['market_id'])
            if bt is not None:
                matched.append((bt, rt))

        print(f"  Actual bot trades loaded:                      {len(real_trades)}")
        print(f"  Markets traded by bot that exist in backtest:  {len(matched)}")

        if matched:
            bt_entries = [bt['entry_price'] for bt, _ in matched]
            rt_entries = [float(rt['entry_price']) for _, rt in matched]
            bt_wins = sum(1 for bt, _ in matched if bt['outcome'] == 'win')
            rt_wins = sum(1 for _, rt in matched if rt['outcome'] in ('win', 'Win'))
            bt_pnl_total = sum(bt['pnl'] for bt, _ in matched)
            rt_pnl_total = sum(float(rt['pnl']) for _, rt in matched)
            bt_entry_cost = sum(bt['token_entry'] for bt, _ in matched)
            rt_entry_cost = sum(float(rt['entry_price']) for _, rt in matched)

            avg_bt_entry = sum(bt_entries) / len(bt_entries)
            avg_rt_entry = sum(rt_entries) / len(rt_entries)
            bt_wr = pct(bt_wins, len(matched))
            rt_wr = pct(rt_wins, len(matched))
            bt_roi_m = (bt_pnl_total / bt_entry_cost * 100) if bt_entry_cost > 0 else 0
            rt_roi_m = (rt_pnl_total / rt_entry_cost * 100) if rt_entry_cost > 0 else 0
            bt_avg_pnl = bt_pnl_total / len(matched)
            rt_avg_pnl = rt_pnl_total / len(matched)
            exec_drag = bt_pnl_total - rt_pnl_total

            col1 = 24
            col2 = 15
            print(f"\n  {'':>{col1}}{'BACKTEST':>{col2}}{'ACTUAL BOT':>{col2}}")
            print(f"  {'─' * (col1 + col2 * 2 + 10)}")
            print(f"  {'Avg entry price:':<{col1}}{avg_bt_entry:>{col2}.2f}   {avg_rt_entry:>{col2}.2f}   (diff: {avg_rt_entry - avg_bt_entry:+.2f} per trade)")
            print(f"  {'Win rate:':<{col1}}{bt_wr:>{col2}.1f}%   {rt_wr:>{col2}.1f}%")
            print(f"  {'ROI:':<{col1}}{bt_roi_m:>{col2}.1f}%   {rt_roi_m:>{col2}.1f}%")
            print(f"  {'Avg PnL/trade:':<{col1}}{'${:.4f}'.format(bt_avg_pnl):>{col2}}   {'${:.4f}'.format(rt_avg_pnl):>{col2}}")
            print(f"  {'Total execution drag:':<{col1}}{'${:.2f}'.format(exec_drag):>{col2}}   (backtest PnL minus actual PnL)")

            # Significant slippage events
            slip_events = []
            for bt, rt in matched:
                price_diff = abs(float(rt['entry_price']) - bt['entry_price'])
                if price_diff > 0.02:
                    slip_events.append({
                        'market_id': bt['market_id'],
                        'bt_price': bt['entry_price'],
                        'rt_price': float(rt['entry_price']),
                        'diff': price_diff,
                        'outcome': rt['outcome'],
                    })

            print(f"\n  Significant slippage events (>0.02 price diff): {len(slip_events)}")
            if slip_events:
                print(f"    {'market_id':<40} {'backtest':>10} {'actual':>10} {'diff':>8} {'outcome':>10}")
                print(f"    {'─' * 80}")
                for se in slip_events:
                    mid_short = se['market_id'][:36] + '...' if len(se['market_id']) > 36 else se['market_id']
                    print(f"    {mid_short:<40} {se['bt_price']:>10.4f} {se['rt_price']:>10.4f} {se['diff']:>8.4f} {se['outcome']:>10}")
            else:
                print(f"    None — all fills within 0.02 of backtest price")

    print(f"\n{'=' * 70}")
    print("Validation complete.")
    print("=" * 70)


if __name__ == '__main__':
    asyncio.run(main())
