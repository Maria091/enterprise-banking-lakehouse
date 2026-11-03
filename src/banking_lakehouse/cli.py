"""Local demo CLI."""

import argparse
import json
from pathlib import Path

from banking_lakehouse.generator import generate_transactions
from banking_lakehouse.pipeline import run_pipeline, write_jsonl


def main():
    parser = argparse.ArgumentParser(description="Synthetic banking lakehouse")
    commands = parser.add_subparsers(dest="command", required=True)
    demo = commands.add_parser("demo")
    demo.add_argument("--count", type=int, default=100)
    demo.add_argument("--seed", type=int, default=42)
    demo.add_argument("--out-dir", default="data")
    demo.add_argument("--duplicate-every", type=int, default=0)
    demo.add_argument("--ml", action="store_true")
    args = parser.parse_args()
    if args.duplicate_every < 0:
        parser.error("--duplicate-every must be >= 0")
    try:
        rows = generate_transactions(args.count, args.seed)
    except ValueError as exc:
        parser.error(str(exc))
    if args.duplicate_every:
        rows.extend(dict(row) for i, row in enumerate(rows, 1) if i % args.duplicate_every == 0)
    summary = run_pipeline(rows, args.out_dir)
    if args.ml:
        from banking_lakehouse.ai import detect_anomalies
        path = Path(args.out_dir) / "silver/transactions.jsonl"
        clean = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]
        anomalies = detect_anomalies(clean, seed=args.seed)
        write_jsonl(Path(args.out_dir) / "gold/ml_anomalies.jsonl", anomalies)
        summary["ml_anomaly_count"] = len(anomalies)
        (Path(args.out_dir) / "reports/run_summary.json").write_text(
            json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
