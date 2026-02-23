"""
Фоновый цикл синхронизации:
Google Sheets (public xlsx) → SQLite.

Запуск:
  python -m app.sync_loop
"""

from __future__ import annotations

import logging
import os
import signal
import time

from .sync import sync_once

_STOP = False


def _handle_stop(_sig, _frame) -> None:
    global _STOP
    _STOP = True


def _get_int_env(name: str, default: int) -> int:
    raw = (os.getenv(name, "") or "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except Exception:
        return default


def main() -> None:
    logging.basicConfig(
        level=(os.getenv("LOG_LEVEL", "INFO") or "INFO").upper(),
        format="%(asctime)s %(levelname)s sync_loop: %(message)s",
    )

    # Интервал синка (сек)
    interval = _get_int_env("SYNC_INTERVAL_SEC", 300)
    interval = max(30, interval)  # fail-safe: не даём синку долбить слишком часто

    # Backoff при ошибках (сек)
    backoff = 5
    max_backoff = min(interval, 300)

    signal.signal(signal.SIGTERM, _handle_stop)
    signal.signal(signal.SIGINT, _handle_stop)

    logging.info(
        "started (interval=%ss, DB_PATH=%s)",
        interval,
        os.getenv("DB_PATH", ""),
    )

    while not _STOP:
        t0 = time.time()
        try:
            res = sync_once()
            logging.info("sync ok: %s", res)
            backoff = 5  # сброс backoff после успеха
        except Exception:
            logging.exception("sync failed")
            # чтобы не спамить и не DDOS-ить источник при неверном URL/401
            time.sleep(backoff)
            backoff = min(max_backoff, backoff * 2)
            continue

        elapsed = time.time() - t0
        sleep_for = max(1, interval - int(elapsed))

        # Спим “мелко”, чтобы быстро реагировать на SIGTERM/SIGINT
        for _ in range(sleep_for):
            if _STOP:
                break
            time.sleep(1)

    logging.info("stopped")


if __name__ == "__main__":
    main()