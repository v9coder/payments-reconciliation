import os
from pathlib import Path

import pandas as pd
import streamlit as st

from src.generate_data import generate_data
from src.reconcile import reconcile

if not os.path.exists("data/transactions.csv") or not os.path.exists("data/settlements.csv"):
    generate_data()


ROOT = Path(__file__).resolve().parent
DATA_DIR = ROOT / "data"


def _load_csv(path: Path) -> pd.DataFrame:
    return pd.read_csv(path)


def _format_money(x) -> str:
    if pd.isna(x):
        return "—"
    return f"${float(x):,.2f}"


def _impact(df: pd.DataFrame) -> float:
    if df.empty:
        return 0.0
    return float(df["difference"].fillna(0).abs().sum())


def _duplicate_double_counted_impact(df: pd.DataFrame) -> float:
    """
    For duplicate groups, only count the extra settlement rows (n-1 per txn_id)
    as double-counted impact.
    """
    if df.empty:
        return 0.0

    impact = 0.0
    for _, grp in df.groupby("txn_id", dropna=False):
        extra_rows = max(len(grp) - 1, 0)
        if extra_rows == 0:
            continue
        # Duplicate entries are same amount by definition in this use case.
        settled_amt = float(pd.to_numeric(grp["settled_amount"], errors="coerce").fillna(0).iloc[0])
        impact += settled_amt * extra_rows
    return float(abs(impact))


def _timing_gap_unsettled_impact(df: pd.DataFrame) -> float:
    """For timing gaps, impact is platform amount not settled in the selected month."""
    if df.empty:
        return 0.0
    return float(pd.to_numeric(df["platform_amount"], errors="coerce").fillna(0).abs().sum())


def _ensure_data() -> None:
    tx_path = DATA_DIR / "transactions.csv"
    st_path = DATA_DIR / "settlements.csv"
    if tx_path.exists() and st_path.exists():
        return

    st.warning("CSV data not found. Generate sample data to continue.")
    if st.button("Generate sample January 2025 data"):
        generate_data()
        st.success("Data generated. Re-run is not required; the app will load it now.")
        st.rerun()

    st.stop()


def _gap_style(gap_type: str) -> str:
    if gap_type in {"DUPLICATE", "ORPHAN_REFUND"}:
        return "card card-red"
    return "card card-amber"


def main() -> None:
    st.set_page_config(page_title="Payments Reconciliation", layout="wide")

    st.markdown(
        """
        <style>
          .title-wrap { margin-bottom: 0.20rem; }
          .subtitle {
            color: #9ca3af;
            margin-top: -0.15rem;
            margin-bottom: 1rem;
            font-size: 0.95rem;
          }

          .card {
            border-radius: 14px;
            padding: 14px 16px 12px 16px;
            border: 1px solid rgba(148, 163, 184, 0.22);
            background: #f8fafc;
            box-shadow: 0 2px 8px rgba(15, 23, 42, 0.08);
            height: 100%;
            color: #0f172a !important;
          }
          .card * {
            color: #0f172a !important;
            text-shadow: none !important;
          }
          .card-red { border-left: 6px solid #dc2626; }
          .card-amber { border-left: 6px solid #f59e0b; }
          .card-title {
            margin: 0 0 0.45rem 0;
            font-size: 1.04rem;
            font-weight: 700;
            line-height: 1.25;
          }
          .metric-row {
            display: flex;
            justify-content: space-between;
            align-items: center;
            gap: 10px;
          }
          .metric-block .k {
            color: #475569 !important;
            font-size: 0.78rem;
            letter-spacing: 0.01em;
            text-transform: uppercase;
            font-weight: 600;
          }
          .metric-block .v {
            font-weight: 800;
            font-size: 1.18rem;
            margin-top: -0.08rem;
          }
          .metric-block .sub {
            margin-top: 0.08rem;
            color: #64748b !important;
            font-size: 0.74rem;
            font-weight: 600;
            letter-spacing: 0.01em;
          }
          .metric-block.right { text-align: right; }
        </style>
        """,
        unsafe_allow_html=True,
    )

    st.markdown("<div class='title-wrap'><h2>Payments Reconciliation</h2></div>", unsafe_allow_html=True)
    st.markdown("<div class='subtitle'>Detect timing, rounding, duplicates, orphan refunds, and missing settlements.</div>", unsafe_allow_html=True)

    _ensure_data()

    tx_path = DATA_DIR / "transactions.csv"
    st_path = DATA_DIR / "settlements.csv"
    transactions = _load_csv(tx_path)
    settlements = _load_csv(st_path)

    with st.container():
        c1, c2, c3 = st.columns([2, 2, 6])
        with c1:
            month_name = st.selectbox(
                "Month",
                options=[
                    "January",
                    "February",
                    "March",
                    "April",
                    "May",
                    "June",
                    "July",
                    "August",
                    "September",
                    "October",
                    "November",
                    "December",
                ],
                index=0,
            )
        with c2:
            year = st.selectbox("Year", options=[2024, 2025, 2026], index=1)
        with c3:
            st.write("")

        month = [
            "January",
            "February",
            "March",
            "April",
            "May",
            "June",
            "July",
            "August",
            "September",
            "October",
            "November",
            "December",
        ].index(month_name) + 1

    gaps = reconcile(transactions, settlements, month=month, year=year)

    gap_types = ["TIMING_GAP", "ROUNDING_DIFF", "DUPLICATE", "ORPHAN_REFUND", "MISSING_SETTLEMENT"]
    summary = []
    for gt in gap_types:
        df = gaps[gaps["gap_type"] == gt] if not gaps.empty else gaps
        if gt == "DUPLICATE":
            impact_value = _duplicate_double_counted_impact(df)
        elif gt == "TIMING_GAP":
            impact_value = _timing_gap_unsettled_impact(df)
        else:
            impact_value = _impact(df)
        summary.append(
            {
                "gap_type": gt,
                "count": int(len(df)),
                "impact": impact_value,
            }
        )
    summary_df = pd.DataFrame(summary)

    cols = st.columns(5)
    for i, gt in enumerate(gap_types):
        row = summary_df[summary_df["gap_type"] == gt].iloc[0]
        subtitle_text = "(double-counted)" if gt == "DUPLICATE" else "\u00a0"
        with cols[i]:
            st.markdown(
                f"""
                <div class="{_gap_style(gt)}">
                  <div class="card-title">{gt.replace("_", " ").title()}</div>
                  <div class="metric-row">
                    <div class="metric-block">
                      <div class="k">Count</div>
                      <div class="v">{int(row["count"])}</div>
                    </div>
                    <div class="metric-block right">
                      <div class="k">Total $ impact</div>
                      <div class="v">{_format_money(row["impact"])}</div>
                      <div class="sub">{subtitle_text}</div>
                    </div>
                  </div>
                </div>
                """,
                unsafe_allow_html=True,
            )

    st.markdown("### Detailed gaps")
    if gaps.empty:
        st.info("No gaps detected for the selected period.")
    else:
        st.dataframe(
            gaps,
            use_container_width=True,
            hide_index=True,
            column_config={
                "platform_amount": st.column_config.NumberColumn(format="$%.2f"),
                "settled_amount": st.column_config.NumberColumn(format="$%.2f"),
                "difference": st.column_config.NumberColumn(format="$%.2f"),
            },
        )

    with st.expander("View raw data"):
        st.markdown("#### Raw transactions")
        st.dataframe(transactions, use_container_width=True, hide_index=True)
        st.markdown("#### Raw settlements")
        st.dataframe(settlements, use_container_width=True, hide_index=True)


if __name__ == "__main__":
    main()

