# Robustness Report

## Top 10 Strategies - Robustness Metrics

| Rank | Module | Config | Win% | PnL | Sharpe | Assets% | Durations% | Consistency | Q1 | Q2 | Q3 | Q4 |
|------|--------|--------|------|-----|--------|---------|------------|-------------|----|----|----|----|  
| 1 | Module 4 | M4_0128 | 43.4 | 15.079 | 0.190 | 100 | 50 | 98.4 | 7.794 | 3.053 | 1.192 | 3.040 |
| 2 | Module 4 | M4_0140 | 43.4 | 15.079 | 0.190 | 100 | 50 | 98.4 | 7.794 | 3.053 | 1.192 | 3.040 |
| 3 | Module 4 | M4_0152 | 43.4 | 15.079 | 0.190 | 100 | 50 | 98.4 | 7.794 | 3.053 | 1.192 | 3.040 |
| 4 | Module 4 | M4_0007 | 45.5 | 23.765 | 0.085 | 75 | 100 | 96.6 | 1.192 | 2.328 | 10.292 | 9.953 |
| 5 | Module 4 | M4_0012 | 45.5 | 23.765 | 0.085 | 75 | 100 | 96.6 | 1.192 | 2.328 | 10.292 | 9.953 |
| 6 | Module 4 | M4_0016 | 45.5 | 23.765 | 0.085 | 75 | 100 | 96.6 | 1.192 | 2.328 | 10.292 | 9.953 |
| 7 | Module 4 | M4_0020 | 45.5 | 23.765 | 0.085 | 75 | 100 | 96.6 | 1.192 | 2.328 | 10.292 | 9.953 |
| 8 | Module 4 | M4_0002 | 45.5 | 23.765 | 0.085 | 75 | 100 | 96.6 | 1.192 | 2.328 | 10.292 | 9.953 |
| 9 | Module 4 | M4_0042 | 38.7 | 16.317 | 0.166 | 100 | 50 | 96.1 | 8.426 | 4.711 | 0.203 | 2.977 |
| 10 | Module 2 | M2_0446 | 47.1 | 11.190 | 0.062 | 75 | 100 | 98.4 | 2.057 | 3.811 | 5.875 | -0.553 |

## Interpretation

- **Assets%**: % of assets (BTC/ETH/SOL/XRP) showing profit
- **Durations%**: % of duration types (5m/15m) showing profit
- **Consistency**: 100 - stdev of per-asset win rates (higher = more consistent)
- **Q1-Q4**: PnL split into chronological quarters (reveals edge decay)
