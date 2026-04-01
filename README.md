# Payments Reconciliation Tool

A Streamlit-based reconciliation dashboard that compares platform transactions and settlements, detects common reconciliation gaps, and summarizes financial impact by gap type.

## Features

- Generates realistic January 2025 sample data with planted reconciliation gaps
- Detects:
  - `TIMING_GAP`
  - `ROUNDING_DIFF`
  - `DUPLICATE`
  - `ORPHAN_REFUND`
  - `MISSING_SETTLEMENT`
- Visual dashboard with summary cards and detailed gap table
- Pytest suite with 6 focused reconciliation tests

## Project Structure

- `app.py` - Streamlit dashboard
- `src/generate_data.py` - CSV data generation
- `src/reconcile.py` - reconciliation logic
- `data/` - generated transactions and settlements CSV files
- `tests/test_reconciliation.py` - test suite
- `ASSUMPTIONS.md` - business and technical assumptions

## Run Locally

1. Install dependencies:

```bash
python -m pip install -r requirements.txt
```

2. Generate sample data:

```bash
python src/generate_data.py
```

3. Start the app:

```bash
streamlit run app.py
```

## Run Tests

```bash
pytest tests/test_reconciliation.py -v
```

## Deployed App

Add your deployed Streamlit URL here after deployment:

- [Streamlit App](https://your-streamlit-app-url)

