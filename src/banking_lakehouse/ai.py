"""Optional unsupervised model. REVIEW_ONLY: not verified fraud detection."""

import math
from datetime import datetime
from decimal import Decimal


def detect_anomalies(rows, seed=42):
    if len(rows) < 20:
        raise ValueError("Need at least 20 valid rows for ML demo")
    try:
        from sklearn.ensemble import IsolationForest
    except ImportError as exc:
        raise RuntimeError('Install extra dependencies with pip install -e ".[ai]"') from exc
    channels = {"card": 0, "mobile": 1, "online": 2, "branch": 3}
    vectors = [[
        math.log1p(float(Decimal(row["amount"]))),
        datetime.fromisoformat(row["event_time"].replace("Z", "+00:00")).hour,
        int(row["country"] != "US"), channels.get(row["channel"], 4)
    ] for row in rows]
    model = IsolationForest(n_estimators=100, contamination=0.08, random_state=seed)
    labels = model.fit_predict(vectors)
    scores = model.decision_function(vectors)
    return [{
        "transaction_id": row["transaction_id"], "model": "IsolationForest",
        "score": round(float(score), 6), "decision": "REVIEW_ONLY",
        "note": "Unsupervised outlier, not a fraud probability."
    } for row, label, score in zip(rows, labels, scores) if label == -1]
