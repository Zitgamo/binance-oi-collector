# Binance V1 Forward OI Collector

Collects one atomic hourly Open Interest snapshot for the fixed Research V1
universe using Binance USD-M Futures `GET /fapi/v1/openInterest`.

The collector writes only after all 10 symbols pass validation. It deduplicates
by `symbol + UTC hour`, preserves the first observation within an hour, and
atomically replaces the CSV so a failed batch cannot corrupt existing data.

## Universe

`BTCUSDT, ETHUSDT, BNBUSDT, SOLUSDT, XRPUSDT, DOGEUSDT, ADAUSDT, LINKUSDT, AVAXUSDT, SUIUSDT`

## Run locally

```bash
python -m unittest discover -s tests -v
python collect_oi.py --output data/binance_v1_forward_oi.csv
```

No Binance API key is required. The endpoint is public and read-only.

## GitHub Actions

`.github/workflows/collect-oi.yml` runs at minute 7 of every UTC hour and can
also be triggered manually. It runs the tests, collects a full snapshot, and
commits the updated CSV. Repository Actions must be allowed to write contents.

GitHub schedules are best-effort and can be delayed. For strict collection
timing, run the same command from a VPS cron or systemd timer instead.

## Data contract

```text
timestamp_utc,symbol,open_interest
```

New snapshots use a canonical hour timestamp such as `2026-07-16T10:00:00Z`.
Older timestamps may contain minutes and seconds; deduplication still floors
them to their UTC hour.
