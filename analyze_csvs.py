#!/usr/bin/env python3
import pandas as pd
from pathlib import Path

vantage_dir = Path("enhance_health_group/csv_files/vantage")
enhance_dir = Path("enhance_health_group/csv_files/enhance_health")

print("=" * 80)
print("VANTAGE CSV ANALYSIS")
print("=" * 80)

for csv_file in sorted(vantage_dir.glob("*.csv")):
    df = pd.read_csv(csv_file)
    dupes = [c for c in df.columns if '.' in c and c.split('.')[-1].isdigit()]
    print(f"\n{csv_file.name}:")
    print(f"  Columns: {len(df.columns)}")
    if dupes:
        print(f"  ✗ Duplicate column markers: {dupes}")
    else:
        print(f"  ✓ No duplicate column markers")
    print(f"  All columns: {list(df.columns)[:5]}... ({len(df.columns)} total)")

print("\n" + "=" * 80)
print("ENHANCE CSV ANALYSIS")
print("=" * 80)

for csv_file in sorted(enhance_dir.glob("*.csv")):
    df = pd.read_csv(csv_file)
    print(f"\n{csv_file.name}:")
    print(f"  Columns: {len(df.columns)}")
    print(f"  All columns: {list(df.columns)[:5]}... ({len(df.columns)} total)")

