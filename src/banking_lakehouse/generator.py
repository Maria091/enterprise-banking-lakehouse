"""Reproducible synthetic posted transactions."""

import random
from datetime import datetime, timedelta, timezone

CATEGORIES = ("grocery", "retail", "travel", "utilities", "fuel", "dining")
CHANNELS = ("card", "online", "mobile", "branch")
COUNTRIES = ("US", "US", "US", "US", "CA", "GB", "IN")


def generate_transactions(count: int = 100, seed: int = 42) -> list[dict[str, str]]:
    if count < 1:
        raise ValueError("count must be >= 1")
    rng = random.Random(seed)
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    records = []
    for i in range(count):
        cents = rng.randint(500, 140_000)
        if i % 19 == 0:
            cents = rng.randint(600_000, 1_200_000)
        records.append({
            "transaction_id": f"txn-{i + 1:07d}",
            "account_id": f"acct-{rng.randint(1, 12):03d}",
            "event_time": (start + timedelta(minutes=i * 37)).isoformat().replace("+00:00", "Z"),
            "amount": f"{cents // 100}.{cents % 100:02d}",
            "currency": "USD",
            "merchant_category": rng.choice(CATEGORIES),
            "channel": rng.choice(CHANNELS),
            "country": rng.choice(COUNTRIES),
            "status": "posted",
        })
    return records
