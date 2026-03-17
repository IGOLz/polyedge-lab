from itertools import product

PRICE_A_SECONDS = [30, 45, 60]
PRICE_B_SECONDS = [60, 75, 90]
ENTRY_OFFSETS = [5, 10]
MIN_THRESHOLDS = [0.02, 0.03, 0.05, 0.07, 0.10]
PRICE_MINS = [0.01, 0.40, 0.50]
PRICE_MAXS = [0.75, 0.80, 0.90]
STOP_LOSS_PRICES = [None, 0.40, 0.35]
DIRECTION_FILTERS = ['both', 'down_only']
MARKET_FILTERS = ['all', 'no_btc', 'xrp_sol_only']
HOUR_FILTERS = ['all', '12-24', '16-24']
CONTEXT_MAX_OPEN_DELTA = [None, 0.10, 0.20]
PRICE_OPEN_SECONDS = [0, 10]
DYNAMIC_SL_TRIGGER_DELTA = [None, 0.05, 0.08, 0.10, 0.12, 0.15, 0.20]
DYNAMIC_SL_NEW_OFFSET = [-0.02, 0.0, 0.02, 0.05]

count = 0
for price_a, price_b in product(PRICE_A_SECONDS, PRICE_B_SECONDS):
    if price_b <= price_a:
        continue
    for entry_offset in ENTRY_OFFSETS:
        entry_sec = price_b + entry_offset
        if entry_sec >= 270:
            continue
        for threshold in MIN_THRESHOLDS:
            for pmin, pmax in product(PRICE_MINS, PRICE_MAXS):
                if pmax - pmin < 0.20:
                    continue
                if pmin >= pmax:
                    continue
                for sl in STOP_LOSS_PRICES:
                    for direction in DIRECTION_FILTERS:
                        for market in MARKET_FILTERS:
                            for hour in HOUR_FILTERS:
                                for ctx in CONTEXT_MAX_OPEN_DELTA:
                                    for open_sec in PRICE_OPEN_SECONDS:
                                        for dyn_trigger in DYNAMIC_SL_TRIGGER_DELTA:
                                            if dyn_trigger is None:
                                                count += 1
                                            else:
                                                count += len(DYNAMIC_SL_NEW_OFFSET)

print(f"Total combinations: {count:,}")