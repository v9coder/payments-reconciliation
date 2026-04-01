from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import pandas as pd


@dataclass(frozen=True)
class ReconcilePeriod:
    month: int
    year: int

    @property
    def label(self) -> str:
        return f"{self.year:04d}-{self.month:02d}"


def _to_datetime(s: pd.Series) -> pd.Series:
    return pd.to_datetime(s, errors="coerce")


def reconcile(
    transactions_df: pd.DataFrame, settlements_df: pd.DataFrame, month: int, year: int
) -> pd.DataFrame:
    """
    Reconcile platform transactions vs settlements for a given calendar month/year.

    Returns a DataFrame with columns:
    txn_id, gap_type, platform_amount, settled_amount, difference, notes
    """
    period = ReconcilePeriod(month=month, year=year)

    txns = transactions_df.copy()
    sets = settlements_df.copy()

    txns["txn_date"] = _to_datetime(txns["txn_date"])
    sets["settled_date"] = _to_datetime(sets["settled_date"])

    in_txn_period = (txns["txn_date"].dt.month == period.month) & (txns["txn_date"].dt.year == period.year)
    in_set_period = (sets["settled_date"].dt.month == period.month) & (sets["settled_date"].dt.year == period.year)

    txns_m = txns.loc[in_txn_period].copy()
    sets_m = sets.loc[in_set_period].copy()

    gaps: list[dict] = []

    # DUPLICATE: txn_id appears more than once in settlements (within the month view).
    dup_ids = sets_m["txn_id"].value_counts()
    dup_ids = dup_ids[dup_ids > 1].index.tolist()
    if dup_ids:
        dups = sets_m[sets_m["txn_id"].isin(dup_ids)].copy()
        txns_lookup = txns_m.set_index("txn_id")["amount"].to_dict()
        for _, r in dups.iterrows():
            tid = str(r["txn_id"])
            platform_amt = txns_lookup.get(tid)
            settled_amt = float(r["settled_amount"])
            gaps.append(
                {
                    "txn_id": tid,
                    "gap_type": "DUPLICATE",
                    "platform_amount": platform_amt,
                    "settled_amount": settled_amt,
                    "difference": (0.0 if platform_amt is None else float(platform_amt)) - settled_amt,
                    "notes": f"Duplicate settlement in {period.label} (settlement_id={r.get('settlement_id')})",
                }
            )

    # ORPHAN_REFUND: negative settled_amount with no matching txn_id in transactions (within month view).
    orphan_refunds = sets_m[sets_m["settled_amount"].astype(float) < 0].copy()
    if not orphan_refunds.empty:
        txn_ids_in_txns = set(txns["txn_id"].astype(str))
        orphan_refunds = orphan_refunds[~orphan_refunds["txn_id"].astype(str).isin(txn_ids_in_txns)]
        for _, r in orphan_refunds.iterrows():
            tid = str(r["txn_id"])
            settled_amt = float(r["settled_amount"])
            gaps.append(
                {
                    "txn_id": tid,
                    "gap_type": "ORPHAN_REFUND",
                    "platform_amount": None,
                    "settled_amount": settled_amt,
                    "difference": 0.0 - settled_amt,
                    "notes": f"Refund with no matching transaction (settlement_id={r.get('settlement_id')})",
                }
            )

    # MISSING_SETTLEMENT: txn_id in transactions with no entry in settlements at all (not just month-filtered).
    all_set_txn_ids = set(sets["txn_id"].astype(str))
    for _, r in txns_m.iterrows():
        tid = str(r["txn_id"])
        if tid not in all_set_txn_ids:
            platform_amt = float(r["amount"])
            gaps.append(
                {
                    "txn_id": tid,
                    "gap_type": "MISSING_SETTLEMENT",
                    "platform_amount": platform_amt,
                    "settled_amount": None,
                    "difference": platform_amt,
                    "notes": f"No settlement found for transaction in any period",
                }
            )

    # TIMING_GAP: transaction in month, settlement exists but settlement date is outside that month.
    # We need to look at ALL settlements for txns in the month, not just the month-filtered settlement slice.
    sets_for_txns = sets[sets["txn_id"].astype(str).isin(set(txns_m["txn_id"].astype(str)))].copy()
    if not sets_for_txns.empty:
        sets_for_txns["in_period"] = (
            (sets_for_txns["settled_date"].dt.month == period.month)
            & (sets_for_txns["settled_date"].dt.year == period.year)
        )
        grouped = sets_for_txns.groupby(sets_for_txns["txn_id"].astype(str))
        for tid, g in grouped:
            if g["in_period"].any():
                continue
            # Settlement exists, but not within the month.
            txn_row = txns_m[txns_m["txn_id"].astype(str) == tid].iloc[0]
            platform_amt = float(txn_row["amount"])
            settled_amt = float(g.iloc[0]["settled_amount"])
            settled_dates = ", ".join(sorted(d.date().isoformat() for d in g["settled_date"].dropna()))
            gaps.append(
                {
                    "txn_id": tid,
                    "gap_type": "TIMING_GAP",
                    "platform_amount": platform_amt,
                    "settled_amount": settled_amt,
                    "difference": platform_amt - settled_amt,
                    "notes": f"Settlement outside {period.label}: {settled_dates}",
                }
            )

    # ROUNDING_DIFF: matched txn where 0 < abs(amount - settled_amount) < 0.10 (within month-filtered join).
    joined = txns_m.merge(sets_m, on="txn_id", how="inner", suffixes=("_txn", "_set"))
    if not joined.empty:
        joined["amount"] = joined["amount"].astype(float)
        joined["settled_amount"] = joined["settled_amount"].astype(float)
        joined["abs_diff"] = (joined["amount"] - joined["settled_amount"]).abs()
        rounding = joined[(joined["abs_diff"] > 0) & (joined["abs_diff"] < 0.10)].copy()
        for _, r in rounding.iterrows():
            tid = str(r["txn_id"])
            gaps.append(
                {
                    "txn_id": tid,
                    "gap_type": "ROUNDING_DIFF",
                    "platform_amount": float(r["amount"]),
                    "settled_amount": float(r["settled_amount"]),
                    "difference": float(r["amount"] - r["settled_amount"]),
                    "notes": "Small per-txn diff (< $0.10) consistent with rounding",
                }
            )

    out = pd.DataFrame(gaps, columns=["txn_id", "gap_type", "platform_amount", "settled_amount", "difference", "notes"])
    if out.empty:
        return out

    # Normalize types for display.
    out["platform_amount"] = pd.to_numeric(out["platform_amount"], errors="coerce")
    out["settled_amount"] = pd.to_numeric(out["settled_amount"], errors="coerce")
    out["difference"] = pd.to_numeric(out["difference"], errors="coerce")
    out = out.sort_values(["gap_type", "txn_id"]).reset_index(drop=True)
    return out

