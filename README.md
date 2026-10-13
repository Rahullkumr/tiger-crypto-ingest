# tiger-crypto-ingest

Real-time financial data pipeline into Tiger Cloud (TimescaleDB), built for the
Global Hack Week: Hacktoberfest challenge *"Ingest real-time financial data into Tiger Cloud"*.

```
Coinbase public websocket (ticker channel)
        │  BTC-USD, ETH-USD, SOL-USD ticks
        ▼
ingest.py  ── batches every 50 rows / 2 s ──▶  crypto_ticks (hypertable, 1-day chunks)
                                                     │
                                                     ▼
                                    crypto_candles_1m (continuous aggregate, OHLC,
                                                       refreshed every minute, real-time)
```

No exchange API key needed. The database URI comes from `tiger db uri --with-password`
at startup, so the password stays in the system keyring and never touches this repo.
Set `DATABASE_URL` to override.

## Schema

```sql
CREATE TABLE crypto_ticks (
  time       TIMESTAMPTZ      NOT NULL,
  symbol     TEXT             NOT NULL,
  price      DOUBLE PRECISION NOT NULL,
  best_bid   DOUBLE PRECISION,
  best_ask   DOUBLE PRECISION,
  last_size  DOUBLE PRECISION,
  volume_24h DOUBLE PRECISION
);
SELECT create_hypertable('crypto_ticks', by_range('time', INTERVAL '1 day'));
CREATE INDEX ON crypto_ticks (symbol, time DESC);

CREATE MATERIALIZED VIEW crypto_candles_1m
WITH (timescaledb.continuous, timescaledb.materialized_only = false) AS
SELECT time_bucket('1 minute', time) AS bucket, symbol,
       first(price, time) AS open, max(price) AS high,
       min(price) AS low, last(price, time) AS close, count(*) AS ticks
FROM crypto_ticks
GROUP BY bucket, symbol
WITH NO DATA;

SELECT add_continuous_aggregate_policy('crypto_candles_1m',
  start_offset => INTERVAL '1 hour', end_offset => INTERVAL '1 minute',
  schedule_interval => INTERVAL '1 minute');
```

## Run

```bash
python -m venv .venv
.venv/Scripts/python -m pip install -r requirements.txt   # Windows
.venv/Scripts/python ingest.py --duration 90
```

## Query

```sql
SELECT bucket, symbol, open, high, low, close, ticks
FROM crypto_candles_1m
WHERE symbol = 'BTC-USD'
ORDER BY bucket DESC
LIMIT 10;
```
