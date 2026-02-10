#!/usr/bin/env python3
"""
Convert Vantage CSV files to match the Enhance Health column structure.

Only 3 files need conversion:
  - payment_trend.csv:    drop extra column "Payment Applied Amount"
  - user_time_spread.csv: drop extra columns "Facility Name", "Office Name", "Audit ID"
  - write_off_trend.csv:  add missing column "Patient Credits"
"""

import pandas as pd
from pathlib import Path

# Define the base paths
BASE_DIR = Path(__file__).parent / "csv_files"
VANTAGE_DIR = BASE_DIR / "vantage"
OUTPUT_DIR = BASE_DIR / "vantage"


# --- Conversion definitions per file ---

# Columns to drop from vantage
COLUMNS_TO_DROP = {
    "payment_trend.csv": ["Payment Applied Amount"],
    "user_time_spread.csv": ["Facility Name", "Office Name", "Audit ID"],
}

# Columns to add (column_name -> value to fill)
COLUMNS_TO_ADD = {
    "write_off_trend.csv": {
        "column": "Patient Credits",
        "after": "Patient Ins Credits",  # insert after this column
        "default": "",
    },
}

# Target column order (enhance_health structure) — only for files that need reordering
TARGET_COLUMN_ORDER = {
    "payment_trend.csv": [
        "customer_account", "instance_key", "Practice Name", "Office Name",
        "Facility Name", "Charge Entered Date", "Charge First Bill Date",
        "Charge From Date", "Charge To Date", "Patient Full Name",
        "Payment Source", "Payment Allowed Amount", "Charge Patient ID",
        "Charge ID", "Claim ID", "Charge Rev Code", "Charge CPT Code",
        "Type of Bill", "Charge Primary Payer Name", "Primary Payer Member ID",
        "Charge Amount", "Insurance Paid Amount", "Payment Total Paid",
        "Payment Total Applied", "Charge Insurance Adjustments",
        "Charge Patient Adjustments", "Charge Total Adjustments",
        "Payment Received", "Payment Entered", "Insurance Applied Amount",
        "Patient Applied Amount", "Payment Unapplied Amount",
    ],
    "user_time_spread.csv": [
        "customer_account", "instance_key", "Practice Name", "Audit Username",
        "Audit Action", "Audit Type", "Audit Entered Date", "Audit Entity ID",
        "Audit Patient ID", "Patient Full Name",
    ],
    "write_off_trend.csv": [
        "customer_account", "instance_key", "Practice Name", "Facility Name",
        "Office Name", "Charge Entered Date", "Claim First Billed Date",
        "Charge From Date", "Charge To Date", "Patient Total Credits",
        "Payment Username", "Patient ID", "Charge ID", "Charge Claim ID",
        "Payment Received", "Payment Total Applied", "Patient Full Name",
        "Credit Source(w/o Payer)", "Adjustment Code(s)", "Patient Ins Credits",
        "Patient Credits", "Patient Total Credits", "Credit Payer Name",
        "Payment Payer ID",
    ],
}


def convert_csv(filename: str) -> None:
    """Convert a single vantage CSV to enhance_health format."""
    vantage_path = VANTAGE_DIR / filename
    output_path = OUTPUT_DIR / filename

    if not vantage_path.exists():
        print(f"  Skipping {filename} - not found in vantage folder")
        return

    print(f"  Processing {filename}...")

    df = pd.read_csv(vantage_path, dtype=str)
    original_col_count = len(df.columns)

    # Drop columns not needed in enhance_health
    cols_to_drop = COLUMNS_TO_DROP.get(filename, [])
    existing_to_drop = [c for c in cols_to_drop if c in df.columns]
    if existing_to_drop:
        df = df.drop(columns=existing_to_drop)
        print(f"    Dropped: {existing_to_drop}")

    # Add missing columns
    add_info = COLUMNS_TO_ADD.get(filename)
    if add_info:
        col_name = add_info["column"]
        after_col = add_info["after"]
        default_val = add_info["default"]
        if col_name not in df.columns and after_col in df.columns:
            idx = df.columns.get_loc(after_col) + 1
            df.insert(idx, col_name, default_val)
            print(f"    Added empty column: {col_name}")

    # Reorder to match enhance_health
    target_order = TARGET_COLUMN_ORDER.get(filename)
    if target_order:
        ordered = [c for c in target_order if c in df.columns]
        remaining = [c for c in df.columns if c not in ordered]
        df = df[ordered + remaining]

    df.to_csv(output_path, index=False)
    print(f"    Done: {original_col_count} cols -> {len(df.columns)} cols")


def convert_vantage_to_enhance():
    """Main entry point."""
    print("Vantage -> Enhance Health CSV Converter")
    print("=" * 50)

    csv_files = [
        "payment_trend.csv",
        "user_time_spread.csv",
        "write_off_trend.csv",
    ]

    print(f"\nConverting {len(csv_files)} files that differ...\n")

    for filename in csv_files:
        convert_csv(filename)

    print("\nSkipped (already matching): ar_aging, claim_stage_breakdown,")
    print("  denial_trends, gross_billing, rcm_productivity")
    print("\nConversion complete!")


if __name__ == "__main__":
    convert_vantage_to_enhance()
