#!/usr/bin/env python3
"""
Convert Vantage CSV files to match the Enhance Health column structure.

This script reads CSV files from the vantage folder, renames columns to match
the enhance_health naming conventions, reorders columns, and outputs the
transformed files to a new folder.
"""

import pandas as pd
from pathlib import Path
import os

# Define the base paths
BASE_DIR = Path(__file__).parent / "csv_files"
print("base dir", BASE_DIR)
VANTAGE_DIR = BASE_DIR / "vantage"
ENHANCE_DIR = BASE_DIR / "enhance_health"
OUTPUT_DIR = BASE_DIR / "vantage"


# Column mappings: vantage_column -> enhance_health_column
COLUMN_MAPPINGS = {
    "ar_aging.csv": {
        "Claim ID": "Charge Claim ID",
        "Claim First Billed Date": "Charge First Bill Date",
        "Revenue Code": "Charge Billed Revenue Code",
        "Patient Full Name": "Patient Name/ID",
    },
    "charges_on_hold.csv": {
        "Payer Type": "Charge Primary Payer Type",
        "Claim Set to Status": "Charge Set to Status",
        "Charge/Debit ID": "Charge ID",  # Map to existing column
    },
    "claim_stage_breakdown.csv": {
        "Patient ID": "Charge Patient ID",
        "Claim ID": "Charge Claim ID",
        "Charge Rev Code": "Charge Billed Revenue Code",
    },
    "denial_trends.csv": {
        "Patient ID": "Charge Patient ID",
        "Payer Name": "Charge Primary Payer Name",  # Map to existing similar column
    },
    "gross_billing.csv": {
        # Primarily column reordering needed
    },
    "payment_trend.csv": {
        "Charge ID": "Payment Charge ID",
        "Charge Rev Code": "Charge Billed Revenue Code",
        "Charge Claim ID": "Claim ID",
        "Payer Name": "Charge Primary Payer Name",
    },
    "rcm_productivity.csv": {
        "Claim Last Billed Date": "Claim First Billed Date",
    },
    "write_off_trend.csv": {
        "Credit Charge ID": "Payment Charge ID",
        "Credit recieved": "Payment Received",
        "Credit claim ID": "Payment Claim ID",
        "Credit Applied": "Payment Total Applied",
        "Credit Adjustment Codes": "Adjustment Code(s)",
        "Credit amount": "Patient Ins Credits",
        "Credit Patient ID": "Payment Patient ID",
    },
}

# Columns to drop (vantage-only columns not needed in enhance structure)
COLUMNS_TO_DROP = {
    "ar_aging.csv": ["Office Name", "Charge Current Payer Name", "Facility Name", "Claim Patient ID"],
    "charges_on_hold.csv": ["Practice Name/ID", "Facility Name", "Office Name", 
                            "Charge Primary Payer Name", "Charge Entered Age", "Times Billed"],
    "claim_stage_breakdown.csv": ["Facility Name", "Office Name"],
    "denial_trends.csv": ["Facility Name", "Office Name", "Payer Name"],
    "gross_billing.csv": ["Office Name", "Facility Name"],
    "payment_trend.csv": ["Facility Name", "Office Name", "Payment Source", "Payment Applied Amount"],
    "rcm_productivity.csv": ["Claim From Date"],
    "write_off_trend.csv": ["Office Name", "Facility Name"],
}

# Target column order for each file (from enhance_health)
TARGET_COLUMN_ORDER = {
    "ar_aging.csv": [
        "customer_account", "instance_key", "Charge ID", "Charge Claim ID",
        "Charge First Bill Date", "Charge From Date", "Charge To Date",
        "Charge Entered Date", "Charge Primary Payer Name", "Charge Primary Payer ID",
        "Practice Name", "Charge CPT Code", "Charge CPT Description",
        "Charge Billed Revenue Code", "Patient Name/ID", "Charge Fromdate Age",
        "Charge Fromdate Age (Days)", "Charge First Bill Date Age",
        "Charge First Bill Date Age (Days)", "Charge Balance", "Charge Balance Due Ins",
        "Charge Balance Due Other", "Charge Balance Due Pat", "Charge Balance At Collections",
        "Charge Insurance Payments", "Charge Patient Payments", "Charge Total Payments",
        "Patient Stmts Sent Electronically", "Patient Statements Printed",
        "Claim Status", "Charge Amount"
    ],
    "charges_on_hold.csv": [
        "customer_account", "instance_key", "Practice Name", "Charge Primary Payer Type",
        "Charge Patient ID", "Charge ID", "Charge Claim ID", "Charge Patient ID",
        "Patient Full Name", "Charge Entered Date", "Charge CPT Code", "Charge Rev Code",
        "Type of Bill", "Charge Entered Age (Days)", "Charge From Date", "Charge To Date",
        "Charge Amount", "Charge Primary Payer Type", "Claim Status", "Charge Set to Status"
    ],
    "claim_stage_breakdown.csv": [
        "customer_account", "instance_key", "Practice Name", "Charge Patient ID",
        "Charge Claim ID", "Charge ID", "Patient Full Name", "Charge From Date",
        "Charge To Date", "Charge Entered Date", "Type of Bill", "Charge CPT Code",
        "Charge Billed Revenue Code", "Charge Amount", "Charge Primary Payer Name",
        "Charge Current Payer Name", "Claim Status", "Charge Balance",
        "Charge Balance Due Ins", "Charge Balance Due Other", "Charge Balance Due Pat",
        "Charge Balance At Collections"
    ],
    "denial_trends.csv": [
        "customer_account", "instance_key", "Practice Name", "Charge Entered Date",
        "Charge From Date", "Charge To Date", "Charge First Bill Date",
        "Charge Patient ID", "Patient Full Name", "Charge ID", "Charge Rev Code",
        "Charge CPT Code", "Remark Code(s)", "Unpaid Reason Code(s)",
        "Charge Primary Payment Date", "Payment Received", "Payment Entered",
        "Charge Primary Payer Name", "Charge Amount", "Insurance Paid Amount"
    ],
    "gross_billing.csv": [
        "customer_account", "instance_key", "Practice Name", "Charge Primary Payer Name",
        "Charge Patient ID", "Charge Claim ID", "Charge ID", "Patient Full Name",
        "Primary Payer Member ID", "Charge From Date", "Charge To Date",
        "Charge Entered Date", "Type of Bill", "Claim First Billed Date",
        "Charge CPT Code", "Charge Rev Code", "Charge Units (Sum)", "Charge Amount",
        "Charge Primary Payer Name", "Charge Current Payer Name", "Claim Status"
    ],
    "payment_trend.csv": [
        "customer_account", "instance_key", "Practice Name", "Charge Entered Date",
        "Charge First Bill Date", "Charge From Date", "Charge To Date",
        "Patient Full Name", "Payment Allowed Amount", "Charge Patient ID",
        "Payment Charge ID", "Claim ID", "Charge Billed Revenue Code", "Charge CPT Code",
        "Type of Bill", "Charge Primary Payer Name", "Primary Payer Member ID",
        "Charge Amount", "Insurance Paid Amount", "Payment Total Paid",
        "Payment Total Applied", "Charge Insurance Adjustments",
        "Charge Patient Adjustments", "Charge Total Adjustments", "Payment Received",
        "Payment Entered", "Insurance Applied Amount", "Patient Applied Amount",
        "Payment Unapplied Amount"
    ],
    "rcm_productivity.csv": [
        "customer_account", "instance_key", "Created By User", "Practice Name",
        "Office Name", "Note Count (CntUnq)", "Claim ID", "Claim First Billed Date",
        "Last Note Date (Max)", "Claim Status"
    ],
    "write_off_trend.csv": [
        "customer_account", "instance_key", "Practice Name", "Charge Entered Date",
        "Claim First Billed Date", "Charge From Date", "Charge To Date",
        "Patient Total Credits", "Payment Username", "Payment Patient ID",
        "Payment Charge ID", "Payment Claim ID", "Payment Received",
        "Payment Total Applied", "Patient Full Name", "Credit Source(w/o Payer)",
        "Adjustment Code(s)", "Patient Ins Credits", "Patient Credits",
        "Patient Total Credits", "Credit Payer Name", "Payment Payer ID"
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
    
    # Read the vantage CSV
    df = pd.read_csv(vantage_path)
    original_cols = list(df.columns)
    
    # Remove duplicate columns (pandas creates .1, .2, etc for duplicate column names)
    duplicate_cols = [c for c in df.columns if '.' in c and c.split('.')[-1].isdigit()]
    if duplicate_cols:
        print(f"    Removing duplicate columns: {duplicate_cols}")
        df = df.drop(columns=duplicate_cols)

    # Apply column mappings
    mappings = COLUMN_MAPPINGS.get(filename, {})
    df = df.rename(columns=mappings)
    
    # Drop vantage-only columns
    cols_to_drop = COLUMNS_TO_DROP.get(filename, [])
    existing_cols_to_drop = [c for c in cols_to_drop if c in df.columns]
    if existing_cols_to_drop:
        df = df.drop(columns=existing_cols_to_drop)
    
    # Reorder columns to match enhance_health structure
    target_order = TARGET_COLUMN_ORDER.get(filename, [])
    if target_order:
        # Keep only columns that exist in the dataframe, in target order
        # Then append any remaining columns not in target order
        ordered_cols = [c for c in target_order if c in df.columns]
        remaining_cols = [c for c in df.columns if c not in ordered_cols]
        df = df[ordered_cols + remaining_cols]
    
    # Save the converted CSV
    df.to_csv(output_path, index=False)
    print(f"    Converted: {len(original_cols)} cols -> {len(df.columns)} cols")


def convert_vantage_to_enhance():
    """Main entry point."""
    print("Vantage to Enhance Health CSV Converter")
    print("=" * 50)
    
    # Create output directory
    OUTPUT_DIR.mkdir(exist_ok=True)
    print(f"\nOutput directory: {OUTPUT_DIR}")
    
    # Get list of CSV files to convert
    csv_files = [
        "ar_aging.csv",
        "charges_on_hold.csv",
        "claim_stage_breakdown.csv",
        "denial_trends.csv",
        "gross_billing.csv",
        "payment_trend.csv",
        "rcm_productivity.csv",
        "write_off_trend.csv",
    ]
    
    print(f"\nConverting {len(csv_files)} files...\n")
    
    for filename in csv_files:
        convert_csv(filename)
    
    # Note about user_time_spread.csv
    print("\nNote: user_time_spread.csv exists only in vantage and has no")
    print("      equivalent in enhance_health - skipped.")
    
    print("\nConversion complete!")


if __name__ == "__main__":
    convert_vantage_to_enhance()
