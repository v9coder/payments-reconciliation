import pandas as pd

from src.reconcile import reconcile


def test_timing_gap_detected_cross_month_boundary():
    transactions = pd.DataFrame(
        [
            {"txn_id": "TXN_047", "customer_id": "C1", "amount": 100.0, "txn_date": "2025-01-31", "status": "completed"},
        ]
    )
    settlements = pd.DataFrame(
        [
            {"settlement_id": "S1", "txn_id": "TXN_047", "settled_amount": 100.0, "settled_date": "2025-02-01", "batch_id": "B1"},
        ]
    )
    out = reconcile(transactions, settlements, month=1, year=2025)
    assert (out["gap_type"] == "TIMING_GAP").any()


def test_rounding_diff_detected_when_individual_gap_under_10_cents_but_sum_nonzero():
    transactions = pd.DataFrame(
        [
            {"txn_id": f"TXN_{i:03d}", "customer_id": "C1", "amount": 10.005, "txn_date": "2025-01-10", "status": "completed"}
            for i in range(1, 11)
        ]
    )
    settlements = pd.DataFrame(
        [
            {"settlement_id": f"S{i:03d}", "txn_id": f"TXN_{i:03d}", "settled_amount": 10.00, "settled_date": "2025-01-11", "batch_id": "B1"}
            for i in range(1, 11)
        ]
    )
    out = reconcile(transactions, settlements, month=1, year=2025)
    rounding = out[out["gap_type"] == "ROUNDING_DIFF"]
    assert len(rounding) == 10
    assert abs(rounding["difference"].sum()) > 0
    assert (rounding["difference"].abs() < 0.10).all()


def test_duplicate_detected_when_same_txn_id_appears_twice_in_settlements():
    transactions = pd.DataFrame(
        [
            {"txn_id": "TXN_023", "customer_id": "C1", "amount": 25.0, "txn_date": "2025-01-05", "status": "completed"},
        ]
    )
    settlements = pd.DataFrame(
        [
            {"settlement_id": "S1", "txn_id": "TXN_023", "settled_amount": 25.0, "settled_date": "2025-01-06", "batch_id": "B1"},
            {"settlement_id": "S2", "txn_id": "TXN_023", "settled_amount": 25.0, "settled_date": "2025-01-06", "batch_id": "B1"},
        ]
    )
    out = reconcile(transactions, settlements, month=1, year=2025)
    dup = out[out["gap_type"] == "DUPLICATE"]
    assert len(dup) == 2


def test_orphan_refund_detected_when_negative_settlement_has_no_parent_transaction():
    transactions = pd.DataFrame(
        [
            {"txn_id": "TXN_001", "customer_id": "C1", "amount": 100.0, "txn_date": "2025-01-10", "status": "completed"},
        ]
    )
    settlements = pd.DataFrame(
        [
            {"settlement_id": "SREF", "txn_id": "TXN_REF_099", "settled_amount": -50.0, "settled_date": "2025-01-12", "batch_id": "B1"},
        ]
    )
    out = reconcile(transactions, settlements, month=1, year=2025)
    orphan = out[out["gap_type"] == "ORPHAN_REFUND"]
    assert len(orphan) == 1
    assert orphan.iloc[0]["txn_id"] == "TXN_REF_099"


def test_missing_settlement_detected_when_transaction_has_no_settlement_anywhere():
    transactions = pd.DataFrame(
        [
            {"txn_id": "TXN_061", "customer_id": "C1", "amount": 77.0, "txn_date": "2025-01-15", "status": "completed"},
        ]
    )
    settlements = pd.DataFrame(
        [
            {"settlement_id": "S1", "txn_id": "TXN_999", "settled_amount": 10.0, "settled_date": "2025-01-16", "batch_id": "B1"},
        ]
    )
    out = reconcile(transactions, settlements, month=1, year=2025)
    missing = out[out["gap_type"] == "MISSING_SETTLEMENT"]
    assert len(missing) == 1
    assert missing.iloc[0]["txn_id"] == "TXN_061"


def test_clean_data_returns_empty_dataframe():
    transactions = pd.DataFrame(
        [
            {"txn_id": "TXN_001", "customer_id": "C1", "amount": 100.0, "txn_date": "2025-01-10", "status": "completed"},
        ]
    )
    settlements = pd.DataFrame(
        [
            {"settlement_id": "S1", "txn_id": "TXN_001", "settled_amount": 100.0, "settled_date": "2025-01-11", "batch_id": "B1"},
        ]
    )
    out = reconcile(transactions, settlements, month=1, year=2025)
    assert out.empty

