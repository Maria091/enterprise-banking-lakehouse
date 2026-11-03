# Enterprise Banking Data Lakehouse

**Python • Azure Databricks / PySpark • Delta Lake • Bronze/Silver/Gold • Data Quality • ML**

An educational data engineering project that transforms **100% synthetic banking transactions** into audited account analytics and explainable transaction alerts. Includes a fully offline Python implementation, tests, CI, an optional experimental Isolation Forest model, and a separate Databricks/Delta Lake reference job.

> **Status:** The local Python pipeline is implemented. Databricks code is a reference that needs a configured workspace. No real Azure infrastructure, streaming deployment, or customer data is included. Risk flags and ML anomalies are not fraud decisions.

## Architecture

~~~mermaid
flowchart LR
    A[Synthetic transaction generator] --> B[Bronze raw JSONL]
    B --> C{Validate + deduplicate}
    C -->|accepted| D[Silver normalized transactions]
    C -->|rejected| Q[Quarantine]
    D --> E[Gold daily account aggregates]
    D --> F[Gold explainable risk alerts]
    D -. optional ML .-> G[Isolation Forest anomaly signals]
~~~

## Quick start (free and fully offline)

Python 3.10 or newer. No credit card, Docker, Azure, or Databricks account required.

~~~bash
python -m venv .venv
# macOS/Linux:
source .venv/bin/activate
# Windows PowerShell: .venv\Scripts\Activate.ps1
python -m pip install -e ".[dev]"
python -m banking_lakehouse.cli demo --count 100 --seed 42
python -m pytest -q
~~~

The demo writes:

- data/bronze/transactions.jsonl — raw fictional transactions
- data/silver/transactions.jsonl — validated unique transactions
- data/quarantine/rejected.jsonl — invalid or duplicate transactions, with reason codes
- data/gold/daily_account_metrics.csv — accounts per UTC event day and exact USD sums
- data/gold/risk_alerts.jsonl — explainable review-only flags
- data/reports/run_summary.json — counts and reconciliation

Try intentional duplicate replays: 

~~~bash
python -m banking_lakehouse.cli demo --count 120 --duplicate-every 20
~~~

Optional anomaly detection (extra library and **not** a fraud probability):

~~~bash
python -m pip install -e ".[ai]"
python -m banking_lakehouse.cli demo --count 120 --ml
~~~

## Databricks

The standalone reference script is at databricks/lakehouse_job.py; it uses PySpark and Delta tables. Stage synthetic JSONL into a Databricks-accessible volume, choose an existing catalog/schema with create permissions, then invoke run_lakehouse(spark, input_path=..., catalog=..., schema=...). It is not executed by the offline CI. See docs/ARCHITECTURE.md.

## Data contract and caveats

Required transaction fields: transaction_id, account_id, event_time (ISO-8601 with timezone), amount (positive USD with up to two decimal places), currency USD, merchant_category, channel, country (two-letter uppercase code), status posted.

Example risk rules: amount > 5,000 USD, non-US country, or a late-night amount > 1,000 USD. Human review only. Daily aggregation uses Decimal arithmetic. Identical local runs overwrite output artifacts deterministically.

Planned rather than deployed: Event Hubs/Kafka streaming, Azure Data Lake Storage, Unity Catalog governance policies, Terraform, job orchestration, CDC, alerting, data retention and full Azure deployment. See docs/ROADMAP.md.

**Do not submit customer records, credentials, PAN, SSNs, or any other sensitive information.**
