import csv
import json
from decimal import Decimal

import pytest

from banking_lakehouse.generator import generate_transactions
from banking_lakehouse.pipeline import DataQualityError, risk_reasons, run_pipeline, validate_transaction


def read_jsonl(path):
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]


def test_deterministic_generator_and_quality():
    assert generate_transactions(100, 7) == generate_transactions(100, 7)
    assert generate_transactions(100, 7) != generate_transactions(100, 8)
    for row in generate_transactions(100):
        assert validate_transaction(row)["currency"] == "USD"
    with pytest.raises(ValueError):
        generate_transactions(0)


def test_local_end_to_end(tmp_path):
    records = generate_transactions(100)
    report = run_pipeline(records, tmp_path)
    assert report["bronze_count"] == report["silver_count"] == 100
    assert report["quarantined_count"] == 0 and report["row_count_reconciled"]
    assert report["risk_alert_count"] > 0
    silver = read_jsonl(tmp_path / "silver/transactions.jsonl")
    with (tmp_path / "gold/daily_account_metrics.csv").open() as stream:
        gold = list(csv.DictReader(stream))
    assert sum(int(x["transaction_count"]) for x in gold) == 100
    assert sum((Decimal(x["total_amount"]) for x in gold), Decimal("0")) == sum(
        (Decimal(x["amount"]) for x in silver), Decimal("0"))
    assert report == run_pipeline(records, tmp_path)


def test_replays_and_invalid_records_quarantined(tmp_path):
    a, b = generate_transactions(2)
    invalid = dict(b, transaction_id="txn-invalid", currency="EUR")
    report = run_pipeline([a, b, dict(a), invalid], tmp_path)
    assert report["silver_count"] == 2
    assert report["quarantined_count"] == 2
    assert report["duplicate_count"] == 1
    assert {x["reason"] for x in read_jsonl(tmp_path / "quarantine/rejected.jsonl")} == {
        "duplicate_transaction_id", "unsupported_currency"}


def test_risk_reasons():
    row = dict(generate_transactions(1)[0], amount="6000.00", country="CA",
               event_time="2026-01-01T02:00:00Z")
    assert risk_reasons(validate_transaction(row)) == [
        "HIGH_AMOUNT", "CROSS_BORDER", "LATE_NIGHT_LARGE"]


@pytest.mark.parametrize("change,reason", [
    ({"amount": "-1.00"}, "invalid_amount"),
    ({"amount": "NaN"}, "invalid_amount"),
    ({"amount": "1.234"}, "invalid_amount"),
    ({"event_time": "2026-01-01T00:00:00"}, "invalid_event_time"),
    ({"country": "USA"}, "invalid_country"),
    ({"status": "pending"}, "unsupported_status"),
])
def test_invalid_contract(change, reason):
    with pytest.raises(DataQualityError, match=reason):
        validate_transaction(dict(generate_transactions(1)[0], **change))
