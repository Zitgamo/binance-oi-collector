#!/usr/bin/env python3
"""Collect an atomic hourly Binance USD-M open-interest snapshot."""

from __future__ import annotations

import argparse
import csv
import json
import os
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Callable, Iterable, Mapping
from urllib.parse import urlencode
from urllib.request import Request, urlopen


SYMBOLS = (
    "BTCUSDT",
    "ETHUSDT",
    "BNBUSDT",
    "SOLUSDT",
    "XRPUSDT",
    "DOGEUSDT",
    "ADAUSDT",
    "LINKUSDT",
    "AVAXUSDT",
    "SUIUSDT",
)
CSV_HEADER = ("timestamp_utc", "symbol", "open_interest")
DEFAULT_BASE_URL = "https://fapi.binance.com"
SYMBOL_ORDER = {symbol: index for index, symbol in enumerate(SYMBOLS)}


class CollectionError(RuntimeError):
    """Raised when a complete, coherent snapshot cannot be collected."""


@dataclass(frozen=True)
class SnapshotRow:
    timestamp_utc: str
    symbol: str
    open_interest: str

    @property
    def hour_ms(self) -> int:
        return timestamp_to_hour_ms(self.timestamp_utc)

    @property
    def key(self) -> tuple[int, str]:
        return (self.hour_ms, self.symbol)


def timestamp_to_hour_ms(value: str) -> int:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError(f"timestamp must include timezone: {value}")
    parsed = parsed.astimezone(UTC).replace(minute=0, second=0, microsecond=0)
    return int(parsed.timestamp() * 1000)


def hour_ms_to_iso(hour_ms: int) -> str:
    return datetime.fromtimestamp(hour_ms / 1000, UTC).strftime("%Y-%m-%dT%H:00:00Z")


def normalize_open_interest(value: object) -> str:
    try:
        number = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise CollectionError(f"invalid open interest: {value!r}") from exc
    if not number.is_finite() or number <= 0:
        raise CollectionError(f"open interest must be positive: {value!r}")
    return format(number, "f")


def fetch_json(
    symbol: str,
    *,
    base_url: str = DEFAULT_BASE_URL,
    timeout: float = 15.0,
    attempts: int = 3,
) -> Mapping[str, object]:
    url = f"{base_url.rstrip('/')}/fapi/v1/openInterest?{urlencode({'symbol': symbol})}"
    request = Request(url, headers={"User-Agent": "binance-research-oi-collector/1.0"})
    last_error: Exception | None = None
    for attempt in range(attempts):
        try:
            with urlopen(request, timeout=timeout) as response:
                payload = json.loads(response.read().decode("utf-8"))
            if not isinstance(payload, dict):
                raise CollectionError(f"unexpected response type for {symbol}")
            return payload
        except Exception as exc:  # Network/HTTP/JSON errors all invalidate the batch.
            last_error = exc
            if attempt + 1 < attempts:
                time.sleep(0.5 * (2**attempt))
    raise CollectionError(f"failed to fetch {symbol} after {attempts} attempts: {last_error}")


def collect_complete_snapshot(
    fetcher: Callable[[str], Mapping[str, object]],
    *,
    max_workers: int = 5,
) -> list[SnapshotRow]:
    responses: dict[str, Mapping[str, object]] = {}
    errors: dict[str, str] = {}
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(fetcher, symbol): symbol for symbol in SYMBOLS}
        for future in as_completed(futures):
            symbol = futures[future]
            try:
                responses[symbol] = future.result()
            except Exception as exc:
                errors[symbol] = str(exc)

    if errors or set(responses) != set(SYMBOLS):
        missing = sorted(set(SYMBOLS) - set(responses), key=SYMBOL_ORDER.get)
        details = "; ".join(f"{symbol}: {message}" for symbol, message in sorted(errors.items()))
        raise CollectionError(f"incomplete snapshot; missing={missing}; errors={details}")

    validated: dict[str, tuple[int, str]] = {}
    for requested_symbol in SYMBOLS:
        payload = responses[requested_symbol]
        returned_symbol = str(payload.get("symbol", ""))
        if returned_symbol != requested_symbol:
            raise CollectionError(
                f"symbol mismatch: requested {requested_symbol}, received {returned_symbol!r}"
            )
        try:
            server_time_ms = int(payload["time"])
        except (KeyError, TypeError, ValueError) as exc:
            raise CollectionError(f"missing/invalid server time for {requested_symbol}") from exc
        hour_ms = server_time_ms - server_time_ms % 3_600_000
        validated[requested_symbol] = (
            hour_ms,
            normalize_open_interest(payload.get("openInterest")),
        )

    hours = {hour for hour, _ in validated.values()}
    if len(hours) != 1:
        rendered = {symbol: hour_ms_to_iso(hour) for symbol, (hour, _) in validated.items()}
        raise CollectionError(f"responses crossed a UTC hour boundary: {rendered}")

    snapshot_hour = hours.pop()
    timestamp = hour_ms_to_iso(snapshot_hour)
    return [
        SnapshotRow(timestamp, symbol, validated[symbol][1])
        for symbol in SYMBOLS
    ]


def read_existing(path: Path) -> list[SnapshotRow]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        if tuple(reader.fieldnames or ()) != CSV_HEADER:
            raise CollectionError(
                f"unexpected CSV schema in {path}: {reader.fieldnames}; expected {CSV_HEADER}"
            )
        rows: list[SnapshotRow] = []
        for line_number, raw in enumerate(reader, start=2):
            try:
                symbol = raw["symbol"]
                if symbol not in SYMBOL_ORDER:
                    raise ValueError(f"unexpected symbol {symbol!r}")
                row = SnapshotRow(
                    raw["timestamp_utc"],
                    symbol,
                    normalize_open_interest(raw["open_interest"]),
                )
                _ = row.hour_ms
                rows.append(row)
            except Exception as exc:
                raise CollectionError(f"invalid row {line_number} in {path}: {exc}") from exc
    return rows


def deduplicate(rows: Iterable[SnapshotRow]) -> list[SnapshotRow]:
    # First observation wins, making repeated runs within an hour idempotent.
    unique: dict[tuple[int, str], SnapshotRow] = {}
    for row in rows:
        unique.setdefault(row.key, row)
    return sorted(unique.values(), key=lambda row: (row.hour_ms, SYMBOL_ORDER[row.symbol]))


def atomic_write(path: Path, rows: Iterable[SnapshotRow]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    file_descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.", suffix=".tmp", dir=path.parent
    )
    try:
        with os.fdopen(file_descriptor, "w", encoding="utf-8", newline="") as handle:
            writer = csv.writer(handle, lineterminator="\n")
            writer.writerow(CSV_HEADER)
            for row in rows:
                writer.writerow((row.timestamp_utc, row.symbol, row.open_interest))
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary_name, path)
    finally:
        if os.path.exists(temporary_name):
            os.unlink(temporary_name)


def merge_snapshot(path: Path, snapshot: list[SnapshotRow]) -> dict[str, object]:
    if len(snapshot) != len(SYMBOLS) or {row.symbol for row in snapshot} != set(SYMBOLS):
        raise CollectionError("refusing to write anything except a complete 10/10 snapshot")
    if len({row.hour_ms for row in snapshot}) != 1:
        raise CollectionError("refusing to write a snapshot spanning multiple UTC hours")

    existing = read_existing(path)
    before_keys = {row.key for row in deduplicate(existing)}
    merged = deduplicate([*existing, *snapshot])
    added = sum(row.key not in before_keys for row in snapshot)
    atomic_write(path, merged)
    complete_hours: dict[int, set[str]] = {}
    for row in merged:
        complete_hours.setdefault(row.hour_ms, set()).add(row.symbol)
    return {
        "snapshot_hour_utc": snapshot[0].timestamp_utc,
        "snapshot_symbols": len(snapshot),
        "rows_added": added,
        "dataset_rows": len(merged),
        "complete_hours": sum(symbols == set(SYMBOLS) for symbols in complete_hours.values()),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("data/binance_v1_forward_oi.csv"),
        help="CSV file to append and deduplicate (default: data/binance_v1_forward_oi.csv)",
    )
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL)
    parser.add_argument("--timeout", type=float, default=15.0)
    parser.add_argument("--attempts", type=int, default=3)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.attempts < 1:
        raise SystemExit("--attempts must be at least 1")
    fetcher = lambda symbol: fetch_json(  # noqa: E731 - concise injected configuration.
        symbol,
        base_url=args.base_url,
        timeout=args.timeout,
        attempts=args.attempts,
    )
    try:
        snapshot = collect_complete_snapshot(fetcher)
        result = merge_snapshot(args.output, snapshot)
    except CollectionError as exc:
        print(json.dumps({"status": "failed", "error": str(exc)}, ensure_ascii=False))
        return 1
    print(json.dumps({"status": "succeeded", **result}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
