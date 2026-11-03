"""Offline Bronze/Silver/Gold processing with quarantine and reconciliation."""

import csv
import json
import re
from collections import defaultdict
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path

REQUIRED = ("transaction_id", "account_id", "event_time", "amount", "currency",
            "merchant_category", "channel", "country", "status")
CENTS = Decimal("0.01")


class DataQualityError(ValueError):
    pass


def validate_transaction(record):
    if not isinstance(record, dict):
        raise DataQualityError("not_an_object")
    missing = [name for name in REQUIRED if name not in record]
    if missing:
        raise DataQualityError("missing_fields:" + ",".join(missing))
    transaction_id = str(record["transaction_id"]).strip()
    account_id = str(record["account_id"]).strip()
    if not re.fullmatch(r"txn-[A-Za-z0-9_-]+", transaction_id):
        raise DataQualityError("invalid_transaction_id")
    if not re.fullmatch(r"acct-\d{3}", account_id):
        raise DataQualityError("invalid_account_id")
    if record["currency"] != "USD":
        raise DataQualityError("unsupported_currency")
    if record["status"] != "posted":
        raise DataQualityError("unsupported_status")
    country = str(record["country"])
    if not re.fullmatch(r"[A-Z]{2}", country):
        raise DataQualityError("invalid_country")
    category, channel = str(record["merchant_category"]).strip(), str(record["channel"]).strip()
    if not category or not channel:
        raise DataQualityError("empty_category_or_channel")
    try:
        amount = Decimal(str(record["amount"]))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise DataQualityError("invalid_amount") from exc
    if not amount.is_finite() or amount <= 0 or amount.as_tuple().exponent < -2 or amount > Decimal("9999999999.99"):
        raise DataQualityError("invalid_amount")
    try:
        timestamp = datetime.fromisoformat(str(record["event_time"]).replace("Z", "+00:00"))
        if timestamp.tzinfo is None or timestamp.utcoffset() is None:
            raise ValueError("timezone missing")
        timestamp = timestamp.astimezone(timezone.utc)
    except (ValueError, TypeError, OverflowError) as exc:
        raise DataQualityError("invalid_event_time") from exc
    return {
        "transaction_id": transaction_id, "account_id": account_id,
        "event_time": timestamp.isoformat().replace("+00:00", "Z"),
        "amount": str(amount.quantize(CENTS)), "currency": "USD",
        "merchant_category": category, "channel": channel, "country": country,
        "status": "posted",
    }


def risk_reasons(row):
    amount = Decimal(row["amount"])
    hour = datetime.fromisoformat(row["event_time"].replace("Z", "+00:00")).hour
    codes = []
    if amount > Decimal("5000"):
        codes.append("HIGH_AMOUNT")
    if row["country"] != "US":
        codes.append("CROSS_BORDER")
    if hour < 5 and amount > Decimal("1000"):
        codes.append("LATE_NIGHT_LARGE")
    return codes


def write_jsonl(path, rows):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as stream:
        for row in rows:
            stream.write(json.dumps(row, sort_keys=True) + "\n")


def run_pipeline(records, out_dir="data"):
    """Full-refresh local batch. First valid transaction ID wins within the batch."""
    root = Path(out_dir)
    bronze = list(records)
    write_jsonl(root / "bronze/transactions.jsonl", bronze)
    accepted, rejected, alerts = [], [], []
    keys = set()
    aggregates = defaultdict(lambda: {"count": 0, "amount": Decimal("0.00"), "alerts": 0})
    for raw in bronze:
        try:
            row = validate_transaction(raw)
            if row["transaction_id"] in keys:
                raise DataQualityError("duplicate_transaction_id")
        except DataQualityError as exc:
            rejected.append({"reason": str(exc), "record": raw})
            continue
        keys.add(row["transaction_id"])
        accepted.append(row)
        agg = aggregates[(row["account_id"], row["event_time"][:10])]
        agg["count"] += 1
        agg["amount"] += Decimal(row["amount"])
        reasons = risk_reasons(row)
        if reasons:
            agg["alerts"] += 1
            alerts.append({
                "transaction_id": row["transaction_id"],
                "account_id": row["account_id"],
                "amount": row["amount"], "reasons": reasons,
                "decision": "REVIEW_ONLY",
            })
    write_jsonl(root / "silver/transactions.jsonl", accepted)
    write_jsonl(root / "quarantine/rejected.jsonl", rejected)
    write_jsonl(root / "gold/risk_alerts.jsonl", alerts)
    metric_path = root / "gold/daily_account_metrics.csv"
    metric_path.parent.mkdir(parents=True, exist_ok=True)
    with metric_path.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=[
            "account_id", "event_date", "transaction_count", "total_amount", "risk_alert_count"
        ])
        writer.writeheader()
        for (account, day), value in sorted(aggregates.items()):
            writer.writerow({
                "account_id": account, "event_date": day,
                "transaction_count": value["count"],
                "total_amount": str(value["amount"].quantize(CENTS)),
                "risk_alert_count": value["alerts"],
            })
    total = sum((Decimal(row["amount"]) for row in accepted), Decimal("0.00"))
    summary = {
        "bronze_count": len(bronze), "silver_count": len(accepted),
        "quarantined_count": len(rejected),
        "duplicate_count": sum(item["reason"] == "duplicate_transaction_id" for item in rejected),
        "gold_account_days": len(aggregates), "risk_alert_count": len(alerts),
        "accepted_amount_usd": str(total.quantize(CENTS)),
        "row_count_reconciled": len(bronze) == len(accepted) + len(rejected),
    }
    report = root / "reports/run_summary.json"
    report.parent.mkdir(parents=True, exist_ok=True)
    report.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return summary
