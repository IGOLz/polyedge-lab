# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

PolyEdge Lab is a quantitative trading research platform for Polymarket prediction markets. It performs statistical analysis on historical market data (BTC, ETH, SOL, XRP — 5min and 15min windows) and backtests algorithmic trading strategies.

## Running

```bash
# Main analysis pipeline
python run_analysis.py [--dry-run] [--market-type btc_5m]

# Individual strategy backtests (standalone, uses latest analysis run_id from DB)
python strategy_farming.py
python strategy_momentum.py
python strategy_calibration.py
python strategy_streak.py

# Parameter sweep (read-only, no DB writes)
python momentum_brute_force.py

# Live trading audit (read-only)
python audit_trades.py
```

Docker: `docker-compose up` (uses python:3.11-slim, entrypoint is `run_analysis.py`).

## Dependencies

Install with `pip install -r requirements.txt`. Key deps: numpy, pandas, psycopg2-binary, scipy, python-dotenv.

## Environment

Requires PostgreSQL connection via `.env` file (see `.env.example`): `POSTGRES_HOST`, `POSTGRES_PORT`, `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_DB`.

## Architecture

### Data Pipeline (4 stages)

1. **Input**: `market_outcomes` (resolved Up/Down) and `market_ticks` (price history) tables in Postgres
2. **Analysis** (`run_analysis.py`): Main orchestrator that computes calibration, trajectory, time-of-day, and sequential pattern analyses → writes to `calibration_results`, `trajectory_results`, `sequential_results`, `timeofday_results`, `analysis_runs`
3. **Strategy Backtests**: Each strategy reads analysis results and simulates trades → writes to its own `{strategy}_runs` + `{strategy}_results` tables
4. **Audit/Analysis**: `momentum_brute_force.py` sweeps parameter space; `audit_trades.py` validates live bot execution against expected signals

### Strategy Modules

Each strategy module is self-contained with its own parameter grid, entry/exit logic, and DB tables:

- **Momentum** (`strategy_momentum.py`): Enters on detected price momentum between 30s-60s checkpoints
- **Farming** (`strategy_farming.py`): Scalps extreme price triggers (0.65-0.90) with stop-loss
- **Calibration** (`strategy_calibration.py`): Exploits miscalibrated price buckets from calibration analysis
- **Streak** (`strategy_streak.py`): Mean-reversion fade after N consecutive same-direction outcomes

All strategies use: $10 bet size, 2% fee ($0.20/bet), parameterized SQL, numpy→Python type conversion helpers.

### Key Constants (run_analysis.py)

- `CHECKPOINTS = [30, 60, 120, 180, 240, 300]` — price observation windows in seconds
- `PRICE_BUCKET_WIDTH = 0.05` — calibration bucket size
- `SIGNIFICANCE_LEVEL = 0.15` — p-value threshold for statistical tests
- `MIN_TICKS_PER_MARKET = 10` — minimum data quality filter
