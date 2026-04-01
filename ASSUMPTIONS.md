# Assumptions

- USD only, no multi-currency
- Amounts stored as floats
- Month-end cutoff is strictly calendar (Jan 31 ≠ Feb 1)
- 1–2 day settlement lag is normal and not flagged
- Refunds identified by negative settled_amount
- Duplicate = same txn_id more than once in settlements
- Partial settlements not handled (one txn split across multiple batches)

