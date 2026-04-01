from __future__ import annotations

import random
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path

import pandas as pd


@dataclass(frozen=True)
class Config:
    year: int = 2025
    month: int = 1
    n_transactions: int = 50
    seed: int = 42


def _date_in_jan_2025(rng: random.Random, *, max_day: int = 30) -> date:
    day = rng.randint(1, max_day)
    return date(2025, 1, day)


def _money(rng: random.Random, lo: float, hi: float) -> float:
    return float(round(rng.uniform(lo, hi), 2))


def generate_transactions(cfg: Config) -> pd.DataFrame:
    rng = random.Random(cfg.seed)

    # Build 50 unique ids while ensuring required ids exist.
    required_ids = ["TXN_023", "TXN_047", "TXN_061"]
    pool = [f"TXN_{i:03d}" for i in range(1, 200) if f"TXN_{i:03d}" not in required_ids]
    rng.shuffle(pool)
    txn_ids = required_ids + pool[: cfg.n_transactions - len(required_ids)]
    rng.shuffle(txn_ids)

    customer_ids = [f"CUST_{rng.randint(1, 25):03d}" for _ in range(cfg.n_transactions)]
    amounts = [_money(rng, 10, 500) for _ in range(cfg.n_transactions)]
    txn_dates = [_date_in_jan_2025(rng, max_day=30) for _ in range(cfg.n_transactions)]
    statuses = ["completed"] * cfg.n_transactions

    txns = pd.DataFrame(
        {
            "txn_id": txn_ids,
            "customer_id": customer_ids,
            "amount": amounts,
            "txn_date": [d.isoformat() for d in txn_dates],
            "status": statuses,
        }
    )

    # GAP 1 (TIMING): TXN_047 txn_date 2025-01-31, settles 2025-02-01
    txns.loc[txns["txn_id"] == "TXN_047", "txn_date"] = date(2025, 1, 31).isoformat()
    txns.loc[txns["txn_id"] == "TXN_047", "amount"] = 120.34

    # GAP 2 (ROUNDING): 10 txns amount 10.005 (platform), settle as 10.00
    rounding_ids = [tid for tid in txn_ids if tid not in {"TXN_047", "TXN_023", "TXN_061"}][:10]
    txns.loc[txns["txn_id"].isin(rounding_ids), "amount"] = 10.005

    return txns


def generate_settlements(transactions: pd.DataFrame, cfg: Config) -> pd.DataFrame:
    rng = random.Random(cfg.seed + 1)

    txns = transactions.copy()
    txns["txn_date"] = pd.to_datetime(txns["txn_date"])

    rows: list[dict] = []
    batch_counter = 1
    settlement_counter = 1

    def add_settlement(txn_id: str, amount: float, settled_dt: date) -> None:
        nonlocal settlement_counter, batch_counter
        rows.append(
            {
                "settlement_id": f"SET_{settlement_counter:04d}",
                "txn_id": txn_id,
                "settled_amount": float(round(amount, 2)),
                "settled_date": settled_dt.isoformat(),
                "batch_id": f"BATCH_{batch_counter:03d}",
            }
        )
        settlement_counter += 1
        if settlement_counter % 8 == 0:
            batch_counter += 1

    # Normal settlements: 1–2 day lag, all within Jan 2025 except the planted timing gap.
    for _, r in txns.iterrows():
        txn_id = str(r["txn_id"])
        if txn_id == "TXN_061":
            # GAP 5 (MISSING SETTLEMENT): no settlement row at all
            continue

        txn_dt = r["txn_date"].date()
        lag_days = rng.choice([1, 2])
        settled_dt = txn_dt + timedelta(days=lag_days)

        if txn_id == "TXN_047":
            # GAP 1 (TIMING)
            settled_dt = date(2025, 2, 1)

        # Ensure we don't accidentally create other timing gaps.
        if txn_id != "TXN_047" and settled_dt.month != 1:
            settled_dt = date(2025, 1, 31)

        platform_amount = float(r["amount"])
        settled_amount = platform_amount

        add_settlement(txn_id, settled_amount, settled_dt)

    # GAP 2 (ROUNDING): platform amount is 10.005, settle as 10.00
    rounding_txns = txns[txns["amount"] == 10.005]["txn_id"].tolist()
    for tid in rounding_txns:
        # Update the existing settlement row for those txn_ids to 10.00
        for row in rows:
            if row["txn_id"] == tid:
                row["settled_amount"] = 10.00
                break

    # GAP 3 (DUPLICATE): TXN_023 appears twice in settlements.csv, same amount
    txn_023 = txns[txns["txn_id"] == "TXN_023"].iloc[0]
    txn_023_dt = txn_023["txn_date"].date()
    dup_settle_dt = txn_023_dt + timedelta(days=1)
    if dup_settle_dt.month != 1:
        dup_settle_dt = date(2025, 1, 31)
    add_settlement("TXN_023", float(txn_023["amount"]), dup_settle_dt)

    # GAP 4 (ORPHAN REFUND): settlement entry with no matching transaction
    add_settlement("TXN_REF_099", -50.00, date(2025, 1, 20))

    return pd.DataFrame(rows)


def main() -> None:
    cfg = Config()
    root = Path(__file__).resolve().parents[1]
    data_dir = root / "data"
    data_dir.mkdir(parents=True, exist_ok=True)

    transactions = generate_transactions(cfg)
    settlements = generate_settlements(transactions, cfg)

    (data_dir / "transactions.csv").write_text(transactions.to_csv(index=False), encoding="utf-8")
    (data_dir / "settlements.csv").write_text(settlements.to_csv(index=False), encoding="utf-8")

    print(f"Wrote {len(transactions)} transactions to {data_dir / 'transactions.csv'}")
    print(f"Wrote {len(settlements)} settlements to {data_dir / 'settlements.csv'}")


if __name__ == "__main__":
    main()

