from __future__ import annotations

import csv
import tempfile
import unittest
from datetime import UTC, datetime
from pathlib import Path

from collect_oi import (
    SYMBOLS,
    CollectionError,
    collect_complete_snapshot,
    merge_snapshot,
)


SNAPSHOT_TIME_MS = int(datetime(2026, 7, 16, 10, 5, tzinfo=UTC).timestamp() * 1000)


def good_fetcher(symbol: str) -> dict[str, object]:
    return {
        "symbol": symbol,
        "openInterest": str(1_000_000 + SYMBOLS.index(symbol)),
        "time": SNAPSHOT_TIME_MS,
    }


class CollectorTests(unittest.TestCase):
    def test_full_snapshot_is_idempotent(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "oi.csv"
            snapshot = collect_complete_snapshot(good_fetcher)
            first = merge_snapshot(path, snapshot)
            second = merge_snapshot(path, snapshot)

            self.assertEqual(first["rows_added"], 10)
            self.assertEqual(second["rows_added"], 0)
            with path.open(newline="", encoding="utf-8") as handle:
                rows = list(csv.DictReader(handle))
            self.assertEqual(len(rows), 10)
            self.assertEqual({row["symbol"] for row in rows}, set(SYMBOLS))

    def test_fetch_failure_leaves_existing_file_unchanged(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "oi.csv"
            original = "timestamp_utc,symbol,open_interest\n2026-07-15T09:03:00Z,BTCUSDT,123\n"
            path.write_text(original, encoding="utf-8")

            def failing_fetcher(symbol: str) -> dict[str, object]:
                if symbol == "SUIUSDT":
                    raise RuntimeError("simulated outage")
                return good_fetcher(symbol)

            with self.assertRaises(CollectionError):
                collect_complete_snapshot(failing_fetcher)
            self.assertEqual(path.read_text(encoding="utf-8"), original)

    def test_partial_hour_is_filled_without_overwriting_first_observation(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "oi.csv"
            original_btc = "777"
            path.write_text(
                "timestamp_utc,symbol,open_interest\n"
                f"2026-07-16T10:01:02Z,BTCUSDT,{original_btc}\n",
                encoding="utf-8",
            )
            result = merge_snapshot(path, collect_complete_snapshot(good_fetcher))
            with path.open(newline="", encoding="utf-8") as handle:
                rows = list(csv.DictReader(handle))

            self.assertEqual(result["rows_added"], 9)
            self.assertEqual(len(rows), 10)
            btc = next(row for row in rows if row["symbol"] == "BTCUSDT")
            self.assertEqual(btc["open_interest"], original_btc)
            self.assertEqual(btc["timestamp_utc"], "2026-07-16T10:01:02Z")

    def test_cross_hour_snapshot_is_rejected(self) -> None:
        def crossing_fetcher(symbol: str) -> dict[str, object]:
            payload = good_fetcher(symbol)
            if symbol == "SUIUSDT":
                payload["time"] = SNAPSHOT_TIME_MS + 3_600_000
            return payload

        with self.assertRaises(CollectionError):
            collect_complete_snapshot(crossing_fetcher)

    def test_wrong_returned_symbol_is_rejected(self) -> None:
        def mismatch_fetcher(symbol: str) -> dict[str, object]:
            payload = good_fetcher(symbol)
            if symbol == "BTCUSDT":
                payload["symbol"] = "ETHUSDT"
            return payload

        with self.assertRaises(CollectionError):
            collect_complete_snapshot(mismatch_fetcher)


if __name__ == "__main__":
    unittest.main()
