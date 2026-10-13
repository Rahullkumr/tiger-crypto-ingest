"""Stream live crypto ticks from Coinbase's public websocket into Tiger Cloud.

No exchange API key needed. The database connection string is fetched from the
Tiger CLI at startup (password comes from the system keyring), so no secret is
stored in this repo.

Usage:
    python ingest.py                      # run until Ctrl+C
    python ingest.py --duration 120       # run for 2 minutes
    python ingest.py --symbols BTC-USD ETH-USD
"""

import argparse
import asyncio
import json
import os
import shutil
import subprocess
import time
from datetime import datetime

import psycopg
import websockets

COINBASE_WS = "wss://ws-feed.exchange.coinbase.com"
DEFAULT_SYMBOLS = ["BTC-USD", "ETH-USD", "SOL-USD"]
FLUSH_EVERY_ROWS = 50
FLUSH_EVERY_SECONDS = 2.0

INSERT_SQL = """
    INSERT INTO crypto_ticks (time, symbol, price, best_bid, best_ask, last_size, volume_24h)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
"""


def find_tiger() -> str:
    found = shutil.which("tiger")
    if found:
        return found
    windows_default = os.path.join(
        os.environ.get("LOCALAPPDATA", ""), "Programs", "TigerCLI", "tiger.exe"
    )
    if os.path.exists(windows_default):
        return windows_default
    raise SystemExit("Tiger CLI not found. Install it and run `tiger auth login`.")


def get_db_uri(service: str | None) -> str:
    if os.environ.get("DATABASE_URL"):
        return os.environ["DATABASE_URL"]
    cmd = [find_tiger(), "db", "uri", "--with-password"]
    if service:
        cmd.append(service)
    return subprocess.run(cmd, check=True, capture_output=True, text=True).stdout.strip()


def parse_tick(msg: dict) -> tuple | None:
    if msg.get("type") != "ticker" or "price" not in msg:
        return None

    def num(key):
        value = msg.get(key)
        return float(value) if value not in (None, "") else None

    return (
        datetime.fromisoformat(msg["time"].replace("Z", "+00:00")),
        msg["product_id"],
        float(msg["price"]),
        num("best_bid"),
        num("best_ask"),
        num("last_size"),
        num("volume_24h"),
    )


async def run(symbols: list[str], duration: float | None, service: str | None) -> None:
    uri = get_db_uri(service)
    total = 0
    batch: list[tuple] = []
    started = time.monotonic()
    last_flush = started

    with psycopg.connect(uri, autocommit=True) as conn:
        def flush():
            nonlocal total, last_flush
            if batch:
                with conn.cursor() as cur:
                    cur.executemany(INSERT_SQL, batch)
                total += len(batch)
                print(f"inserted {len(batch):>3} rows  (total {total})")
                batch.clear()
            last_flush = time.monotonic()

        async with websockets.connect(COINBASE_WS, ping_interval=20) as ws:
            await ws.send(json.dumps({
                "type": "subscribe",
                "product_ids": symbols,
                "channels": ["ticker"],
            }))
            print(f"subscribed to {', '.join(symbols)}; writing to crypto_ticks")

            try:
                while duration is None or time.monotonic() - started < duration:
                    try:
                        raw = await asyncio.wait_for(ws.recv(), timeout=FLUSH_EVERY_SECONDS)
                        row = parse_tick(json.loads(raw))
                        if row:
                            batch.append(row)
                    except asyncio.TimeoutError:
                        pass
                    if len(batch) >= FLUSH_EVERY_ROWS or time.monotonic() - last_flush >= FLUSH_EVERY_SECONDS:
                        flush()
            finally:
                flush()

    print(f"done: {total} ticks ingested")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--symbols", nargs="+", default=DEFAULT_SYMBOLS)
    parser.add_argument("--duration", type=float, help="seconds to run (default: until Ctrl+C)")
    parser.add_argument("--service", help="Tiger service name or id (default: CLI default service)")
    args = parser.parse_args()
    try:
        asyncio.run(run(args.symbols, args.duration, args.service))
    except KeyboardInterrupt:
        print("stopped")


if __name__ == "__main__":
    main()
