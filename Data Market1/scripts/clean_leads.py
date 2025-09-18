#!/usr/bin/env python3
"""
Clean Marketing Qualified Leads Dataset

This script applies minimal cleaning rules to the marketing qualified leads dataset:
- Load CSV with explicit encoding and dtype inference off
- Trim whitespace on string columns
- Standardize dates to ISO YYYY-MM-DD format
- Lowercase column names
- Deduplicate rows (full-row duplicates)
- Handle missing values: drop rows where key columns are missing
- Print column list & non-null counts to console
"""

import pandas as pd
import sys
from pathlib import Path


def clean_leads_data(input_file, output_file):
    """
    Clean the marketing qualified leads dataset according to minimal cleaning rules.
    
    Args:
        input_file (str): Path to input CSV file
        output_file (str): Path to output cleaned CSV file
    """
    try:
        # Load CSV with explicit encoding and dtype inference off
        print(f"Loading data from: {input_file}")
        df = pd.read_csv(input_file, encoding='utf-8', dtype=str)
        
        print(f"Original shape: {df.shape}")
        
        # Lowercase column names
        df.columns = df.columns.str.lower()
        
        # Trim whitespace on string columns
        string_columns = df.select_dtypes(include=['object']).columns
        for col in string_columns:
            df[col] = df[col].astype(str).str.strip()
        
        # Standardize dates to ISO YYYY-MM-DD format
        if 'first_contact_date' in df.columns:
            df['first_contact_date'] = pd.to_datetime(df['first_contact_date'], errors='coerce')
            df['first_contact_date'] = df['first_contact_date'].dt.strftime('%Y-%m-%d')
        
        # Handle missing values: drop rows where key columns are missing
        key_columns = ['mql_id']
        for col in key_columns:
            if col in df.columns:
                initial_count = len(df)
                df = df.dropna(subset=[col])
                dropped_count = initial_count - len(df)
                if dropped_count > 0:
                    print(f"Dropped {dropped_count} rows with missing {col}")
        
        # Deduplicate rows (full-row duplicates)
        initial_count = len(df)
        df = df.drop_duplicates()
        duplicate_count = initial_count - len(df)
        if duplicate_count > 0:
            print(f"Removed {duplicate_count} duplicate rows")
        
        # Basic schema check: print column list & non-null counts
        print("\nColumn list and non-null counts:")
        print("=" * 50)
        for col in df.columns:
            non_null_count = df[col].notna().sum()
            print(f"{col}: {non_null_count}/{len(df)} ({non_null_count/len(df)*100:.1f}%)")
        
        print(f"\nFinal shape: {df.shape}")
        
        # Create output directory if it doesn't exist
        output_path = Path(output_file)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Save cleaned data
        df.to_csv(output_file, index=False, encoding='utf-8')
        print(f"Cleaned data saved to: {output_file}")
        
    except Exception as e:
        print(f"Error processing data: {str(e)}", file=sys.stderr)
        sys.exit(1)


def main():
    # Default input and output paths
    input_file = "data/olist_marketing_qualified_leads_dataset.csv"
    output_file = "data/clean/leads_clean.csv"
    
    # Validate input file exists
    if not Path(input_file).exists():
        print(f"Error: Input file '{input_file}' does not exist", file=sys.stderr)
        print("Please ensure the file is in the correct location.", file=sys.stderr)
        sys.exit(1)
    
    clean_leads_data(input_file, output_file)


if __name__ == "__main__":
    main()
