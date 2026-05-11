"""Integration test do pipeline silver — exige Spark local."""

from __future__ import annotations

import pytest

pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def spark():
    from cacm.utils.spark_session import get_spark

    s = get_spark("cacm-tests")
    yield s
    s.stop()


def test_clean_drops_invalid_rows(spark):
    from datetime import datetime

    from cacm.transformations.silver import clean_transactions

    rows = [
        ("t1", "PIX_OUT", 100.0, datetime(2026, 5, 1, 10, 0)),
        ("t2", "PIX_OUT", 0.0, datetime(2026, 5, 1, 11, 0)),  # zero -> drop
        ("t3", None, 50.0, datetime(2026, 5, 1, 12, 0)),  # tipo null OK; mas...
        (None, "TED", 200.0, datetime(2026, 5, 1, 13, 0)),  # transacao_id null -> drop
        ("t1", "PIX_OUT", 100.0, datetime(2026, 5, 1, 10, 0)),  # dup -> dedup
    ]
    df = spark.createDataFrame(rows, ["transacao_id", "tipo", "valor", "dt_transacao"])
    out = clean_transactions(df)
    assert out.count() == 2
