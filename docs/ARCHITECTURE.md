# Architecture and quality guarantees

## Local implementation

1. **Bronze** stores the full incoming batch (including intended replays).
2. **Silver** validates mandatory attributes, finite positive USD decimal amount, valid timezone-aware timestamp, categorical fields, and unique transaction ID. First valid occurrence wins.
3. **Quarantine** holds invalid or duplicated records with explicit reason codes.
4. **Gold** creates per-account/day counts and precise monetary totals, plus explainable review-only risk signals.
5. **Optional ML** scores synthetic samples with a per-batch Isolation Forest experiment; no learned model is production-served.

Grain: Silver = transaction_id, Gold metrics = (account_id, UTC event_date), Gold alert = flagged transaction_id.

Quality invariant: bronze_count = silver_count + quarantined_count. Sum of daily Gold amounts equals the accepted Silver monetary total, using Python Decimal. This project does not reconcile against a real bank ledger.

## Databricks reference implementation

The Databricks job reads JSONL with a known schema, creates Delta Bronze/Silver tables and merges by transaction_id. It builds refreshed Gold Delta tables using PySpark aggregations. This separate example requires Databricks with Delta Lake, configured storage access and Unity Catalog or Hive metastore permissions.

**Limitations:** The Databricks Bronze table is a keyed latest view rather than a fully immutable source archive. It does not implement quarantine persistence, conflicting-event CDC resolution, timezone validation parity with the offline path, schema evolution, end-to-end lineage, streaming or production-scale performance tuning. The Databricks job is supplied for hands-on adaptation, not claimed as validated in a workspace.

## Responsible use

No actual customer records, PAN, SSN, account identifiers, or credential material should enter the repository. All demo accounts are invented. Threshold alerts and optional anomalies are screening demonstrations, not verified fraud or credit decisions. A real financial service implementation would need governance, monitoring, validation, security review and audited human oversight.
